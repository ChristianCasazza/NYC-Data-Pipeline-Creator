# opendata_framework/dagster/assets/checks.py
"""
Row accounting checks for the 2-stage Landing → Clean pipeline.

Generates asset checks that reconcile row counts across stages:
  1. Landing CSV lines (raw byte count — no parsing)
  2. Parsed rows (what scan_csv with ignore_errors=True produces)
  3. Clean Parquet rows (final output)

Gap 1 (landing lines vs parsed rows) detects silent CSV parse errors.
Gap 2 (parsed rows vs clean rows) detects intentional/unintentional row drops in transforms.

For monthly→yearly pipelines, provides per-month breakdown with coverage tracking.
"""

from __future__ import annotations

import csv
import gzip
from dataclasses import dataclass
from typing import Any, Literal

import polars as pl
import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, MetadataValue
from upath import UPath


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class MonthDetail:
    """Per-month landing data for monthly→yearly reconciliation."""

    month_key: str        # "2025-01"
    exists: bool          # directory exists on disk
    files: int            # number of .csv.gz files
    rows: int             # raw CSV lines (excl headers)
    parsed_rows: int      # rows after scan_csv(ignore_errors=True)


# ---------------------------------------------------------------------------
# Low-level counting helpers (unchanged)
# ---------------------------------------------------------------------------

def _count_csv_lines(directory, glob_pattern: str = "*.csv.gz") -> dict[str, int]:
    """Count logical CSV records in gzipped CSV files.

    Uses csv.reader to correctly handle quoted fields that contain
    embedded newlines (e.g., Socrata location fields like "(lat, lon)").
    Returns a dict of {filename: record_count} excluding the header row.
    """
    counts: dict[str, int] = {}
    files = list(directory.glob(glob_pattern))
    for f in files:
        with f.open("rb") as raw:
            with gzip.open(raw, "rt", errors="replace") as gz:
                reader = csv.reader(gz)
                n_records = sum(1 for _ in reader)
        # Subtract 1 for header row; floor at 0 for empty files
        counts[f.name] = max(0, n_records - 1)
    return counts


def _count_parsed_csv_rows(directory, glob_pattern: str = "*.csv.gz") -> int:
    """Count rows that survive pl.scan_csv(ignore_errors=True)."""
    files = [str(f) for f in directory.glob(glob_pattern)]
    if not files:
        return 0
    try:
        return pl.scan_csv(
            files, infer_schema_length=0, ignore_errors=True
        ).select(pl.len()).collect().item()
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, FileNotFoundError):
        return 0


def _count_parquet_rows(path) -> int:
    """Count rows in a parquet file or directory of parquet files."""
    p = UPath(path) if not hasattr(path, "exists") else path
    if not p.exists():
        return 0

    if p.is_file():
        try:
            return pl.scan_parquet(str(p)).select(pl.len()).collect().item()
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, FileNotFoundError):
            return 0

    # Directory: glob for parquet files
    parquet_files = list(p.glob("**/*.parquet"))
    if not parquet_files:
        return 0
    try:
        paths = [str(f) for f in parquet_files]
        return pl.scan_parquet(paths, hive_partitioning=True).select(
            pl.len()
        ).collect().item()
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, FileNotFoundError):
        return 0


# ---------------------------------------------------------------------------
# Directory / month resolution
# ---------------------------------------------------------------------------

def _resolve_landing_dirs(
    landing_io,
    landing_key: str,
    partition_key: str | None,
    partition_grain: str,
) -> list:
    """Return list of landing directories to scan for a given clean partition.

    Used by unpartitioned and same_grain checks.
    """
    if partition_grain == "unpartitioned":
        return [landing_io.get_dir_for_asset(landing_key)]

    if partition_grain == "same_grain":
        return [landing_io.get_dir_for_asset(landing_key, partition_key)]

    if partition_grain == "monthly_to_yearly":
        dirs = []
        for month in range(1, 13):
            monthly_key = f"{partition_key}-{month:02d}"
            d = landing_io.get_dir_for_asset(landing_key, monthly_key)
            if d.exists():
                dirs.append(d)
        return dirs

    # Fallback
    return [landing_io.get_dir_for_asset(landing_key, partition_key)]


def _resolve_monthly_landing(
    landing_io,
    landing_key: str,
    year_partition: str,
) -> list[MonthDetail]:
    """Build a MonthDetail for every month in a yearly partition.

    Always returns 12 entries (Jan–Dec), including months where the
    landing directory does not exist.
    """
    months: list[MonthDetail] = []
    for m in range(1, 13):
        monthly_key = f"{year_partition}-{m:02d}"
        d = landing_io.get_dir_for_asset(landing_key, monthly_key)
        exists = d.exists()

        if exists:
            file_counts = _count_csv_lines(d)
            n_files = len(file_counts)
            rows = sum(file_counts.values())
            parsed = _count_parsed_csv_rows(d)
        else:
            n_files = 0
            rows = 0
            parsed = 0

        months.append(MonthDetail(
            month_key=monthly_key,
            exists=exists,
            files=n_files,
            rows=rows,
            parsed_rows=parsed,
        ))
    return months


# ---------------------------------------------------------------------------
# Markdown table builders
# ---------------------------------------------------------------------------

def _build_accounting_table_flat(
    file_counts: dict[str, int],
    landing_total: int,
    parsed_rows: int,
    clean_rows: int,
) -> str:
    """Build a flat markdown table for unpartitioned / same_grain checks."""
    lines = [
        "| Stage | Detail | Rows |",
        "| :--- | :--- | ---: |",
    ]
    for fname, count in sorted(file_counts.items()):
        lines.append(f"| Landing file | `{fname}` | {count:,} |")
    lines.append(f"| **Landing total** | CSV lines (excl. headers) | **{landing_total:,}** |")
    lines.append(f"| **Parsed rows** | After `scan_csv(ignore_errors=True)` | **{parsed_rows:,}** |")

    parse_gap = landing_total - parsed_rows
    gap_label = f"{parse_gap:,} rows lost" if parse_gap > 0 else "0"
    lines.append(f"| Parse gap | Lines that failed CSV parsing | {gap_label} |")

    lines.append(f"| **Clean rows** | Parquet output | **{clean_rows:,}** |")

    transform_gap = parsed_rows - clean_rows
    if transform_gap > 0:
        lines.append(f"| Transform gap | Rows removed by enrichment | {transform_gap:,} |")
    elif transform_gap < 0:
        lines.append(f"| Transform gap | **Row EXPANSION (unexpected)** | **{transform_gap:,}** |")
    else:
        lines.append(f"| Transform gap | None | 0 |")

    return "\n".join(lines)


def _build_accounting_table_monthly(
    months: list[MonthDetail],
    landing_total: int,
    parsed_total: int,
    clean_rows: int,
) -> str:
    """Build a monthly breakdown table for monthly→yearly checks."""
    lines = [
        "**Monthly Landing Breakdown**",
        "",
        "| Month | Status | Files | Rows |",
        "| :--- | :--- | ---: | ---: |",
    ]

    for md in months:
        if not md.exists:
            lines.append(f"| {md.month_key} | not materialized | — | — |")
        elif md.rows > 0:
            lines.append(f"| {md.month_key} | materialized | {md.files} | {md.rows:,} |")
        else:
            lines.append(f"| {md.month_key} | empty | {md.files} | 0 |")

    # Coverage summary
    materialized = sum(1 for m in months if m.exists)
    with_data = sum(1 for m in months if m.rows > 0)
    empty = materialized - with_data
    missing = 12 - materialized

    coverage_parts = []
    if with_data:
        coverage_parts.append(f"{with_data} with data")
    if empty:
        coverage_parts.append(f"{empty} empty")
    if missing:
        coverage_parts.append(f"{missing} not materialized")
    coverage_str = ", ".join(coverage_parts)

    lines.append("")
    lines.append(f"**Coverage**: {materialized} of 12 months materialized ({coverage_str})")

    # Reconciliation section
    parse_gap = landing_total - parsed_total
    transform_gap = parsed_total - clean_rows

    lines.append("")
    lines.append("**Reconciliation**")
    lines.append("")
    lines.append("| Stage | Rows |")
    lines.append("| :--- | ---: |")
    lines.append(f"| Landing total | {landing_total:,} |")
    lines.append(f"| Parsed rows | {parsed_total:,} |")

    if parse_gap > 0:
        lines.append(f"| **Parse gap** | **{parse_gap:,} rows lost** |")
    else:
        lines.append(f"| Parse gap | 0 |")

    lines.append(f"| Clean rows | {clean_rows:,} |")

    if transform_gap > 0:
        lines.append(f"| Transform gap | {transform_gap:,} |")
    elif transform_gap < 0:
        lines.append(f"| **Transform gap** | **{transform_gap:,} (row expansion)** |")
    else:
        lines.append(f"| Transform gap | 0 |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Check generator
# ---------------------------------------------------------------------------

def define_row_accounting_check(
    clean_key: str,
    landing_key: str,
    partition_grain: str,
    allows_row_drop: bool = False,
    clean_io_manager_key: str = "clean_large_io_manager",
    partitions_def: Any = None,
) -> dg.AssetChecksDefinition:
    """Generate a row accounting check for a clean asset.

    Parameters
    ----------
    clean_key:
        Asset key of the clean asset to check.
    landing_key:
        Asset key of the upstream landing asset.
    partition_grain:
        One of "unpartitioned", "same_grain", "monthly_to_yearly".
    allows_row_drop:
        If True, transform gaps produce WARN instead of ERROR.
    clean_io_manager_key:
        Resource key for the clean IO manager.
    partitions_def:
        Partitions definition for the clean asset (passed through to the check).
    """
    check_name = f"{clean_key}_row_accounting"

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(clean_key),
        blocking=True,
        required_resource_keys={"landing_io_manager", clean_io_manager_key},
        description=(
            "Reconciles row counts across Landing CSV → Clean Parquet. "
            "Detects silent CSV parse errors and unexpected row drops in transforms."
        ),
    )
    def _row_accounting_check(context) -> AssetCheckResult:
        landing_io = context.resources.landing_io_manager
        clean_io = getattr(context.resources, clean_io_manager_key)
        op_ctx = context.op_execution_context
        partition_key = op_ctx.partition_key if op_ctx.has_partition_key else None

        # --- Count clean parquet rows (same for all grains) ---
        clean_path = clean_io.get_path_for_asset(AssetKey(clean_key), partition_key)
        clean_rows = _count_parquet_rows(clean_path)

        # --- Branch by grain ---
        if partition_grain == "monthly_to_yearly" and partition_key:
            return _check_monthly_to_yearly(
                context, landing_io, landing_key, partition_key,
                clean_rows, allows_row_drop,
            )

        return _check_flat(
            context, landing_io, landing_key, partition_key, partition_grain,
            clean_rows, allows_row_drop,
        )

    return _row_accounting_check


# ---------------------------------------------------------------------------
# Grain-specific check logic
# ---------------------------------------------------------------------------

def _check_flat(
    context,
    landing_io,
    landing_key: str,
    partition_key: str | None,
    partition_grain: str,
    clean_rows: int,
    allows_row_drop: bool,
) -> AssetCheckResult:
    """Row accounting for unpartitioned and same_grain assets."""
    landing_dirs = _resolve_landing_dirs(
        landing_io, landing_key, partition_key, partition_grain
    )
    all_file_counts: dict[str, int] = {}
    for d in landing_dirs:
        if d.exists():
            all_file_counts.update(_count_csv_lines(d))

    landing_total = sum(all_file_counts.values())

    parsed_rows = 0
    for d in landing_dirs:
        if d.exists():
            parsed_rows += _count_parsed_csv_rows(d)

    table_md = _build_accounting_table_flat(
        all_file_counts, landing_total, parsed_rows, clean_rows
    )

    parse_gap = landing_total - parsed_rows
    transform_gap = parsed_rows - clean_rows

    metadata: dict[str, Any] = {
        "row_accounting": MetadataValue.md(table_md),
        "landing_csv_lines": landing_total,
        "parsed_rows": parsed_rows,
        "clean_rows": clean_rows,
        "parse_gap": parse_gap,
        "transform_gap": transform_gap,
    }

    return _evaluate_pass_fail(
        metadata, landing_total, parsed_rows, clean_rows,
        parse_gap, transform_gap, allows_row_drop,
    )


def _check_monthly_to_yearly(
    context,
    landing_io,
    landing_key: str,
    year_partition: str,
    clean_rows: int,
    allows_row_drop: bool,
) -> AssetCheckResult:
    """Row accounting for monthly→yearly assets with per-month breakdown."""
    months = _resolve_monthly_landing(landing_io, landing_key, year_partition)

    landing_total = sum(m.rows for m in months)
    parsed_total = sum(m.parsed_rows for m in months)
    parse_gap = landing_total - parsed_total
    transform_gap = parsed_total - clean_rows

    months_materialized = sum(1 for m in months if m.exists)
    months_with_data = sum(1 for m in months if m.rows > 0)
    months_empty = months_materialized - months_with_data
    months_missing = 12 - months_materialized

    table_md = _build_accounting_table_monthly(
        months, landing_total, parsed_total, clean_rows
    )

    metadata: dict[str, Any] = {
        "row_accounting": MetadataValue.md(table_md),
        "landing_csv_lines": landing_total,
        "parsed_rows": parsed_total,
        "clean_rows": clean_rows,
        "parse_gap": parse_gap,
        "transform_gap": transform_gap,
        "months_materialized": months_materialized,
        "months_with_data": months_with_data,
        "months_empty": months_empty,
        "months_missing": months_missing,
    }

    # Monthly-specific: all materialized months are empty
    if months_materialized > 0 and months_with_data == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata=metadata,
            description=(
                f"All {months_materialized} materialized months have 0 rows for partition {year_partition}. "
                "Data source may not have published yet."
            ),
        )

    return _evaluate_pass_fail(
        metadata, landing_total, parsed_total, clean_rows,
        parse_gap, transform_gap, allows_row_drop,
    )


# ---------------------------------------------------------------------------
# Shared pass/fail evaluation
# ---------------------------------------------------------------------------

def _evaluate_pass_fail(
    metadata: dict[str, Any],
    landing_total: int,
    parsed_rows: int,
    clean_rows: int,
    parse_gap: int,
    transform_gap: int,
    allows_row_drop: bool,
) -> AssetCheckResult:
    """Shared pass/fail logic for all grain types."""

    # Parse errors: always ERROR (silent data loss)
    if parse_gap > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata=metadata,
            description=(
                f"{parse_gap:,} rows lost during CSV parsing (ignore_errors=True). "
                "These rows were silently dropped due to malformed CSV data."
            ),
        )

    # Row expansion: always ERROR (unexpected)
    if clean_rows > parsed_rows and parsed_rows > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata=metadata,
            description=(
                f"Clean output has {clean_rows - parsed_rows:,} MORE rows than landing. "
                "This suggests a join or cross-product bug in the transform."
            ),
        )

    # Total data loss: ERROR
    if clean_rows == 0 and landing_total > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata=metadata,
            description="Clean output is empty but landing has data. Complete data loss in transform.",
        )

    # Transform gap: WARN if allowed, ERROR if not
    if transform_gap > 0:
        if allows_row_drop:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata=metadata,
                description=(
                    f"{transform_gap:,} rows removed by transform (expected — allows_row_drop=True)."
                ),
            )
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata=metadata,
            description=(
                f"{transform_gap:,} rows lost in transform but allows_row_drop=False. "
                "This pipeline should not drop rows. Investigate the post_transform_fn."
            ),
        )

    # All clear
    return AssetCheckResult(
        passed=True,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Cross-partition summary check (non-blocking, informational)
# ---------------------------------------------------------------------------

@dataclass
class YearSummary:
    """Per-year row summary for cross-partition reporting."""

    year: str
    months_with_data: int
    months_materialized: int
    landing_rows: int
    clean_rows: int
    parse_gap: int
    transform_gap: int


def _discover_yearly_partitions(clean_io, clean_key: str) -> list[str]:
    """Find all year partitions that have clean parquet files on disk."""
    asset_root = clean_io._base_path / clean_key
    if not asset_root.exists():
        return []

    years = []
    for d in sorted(asset_root.iterdir()):
        if d.is_dir() and d.name.startswith("year="):
            year = d.name.split("=")[1]
            if len(year) == 4 and year.isdigit():
                years.append(year)
    return years


def _build_cross_partition_table(
    summaries: list[YearSummary],
    partition_grain: str,
) -> str:
    """Build a multi-year summary markdown table."""
    total_landing = sum(s.landing_rows for s in summaries)
    total_clean = sum(s.clean_rows for s in summaries)
    total_parse_gap = sum(s.parse_gap for s in summaries)
    total_transform_gap = sum(s.transform_gap for s in summaries)

    lines = [
        "**Cross-Partition Summary**",
        "",
    ]

    if partition_grain == "monthly_to_yearly":
        lines.extend([
            "| Year | Months w/ Data | Landing Rows | Clean Rows | Parse Gap | Transform Gap |",
            "| :--- | :--- | ---: | ---: | ---: | ---: |",
        ])
        for s in summaries:
            lines.append(
                f"| {s.year} | {s.months_with_data}/{s.months_materialized} "
                f"| {s.landing_rows:,} | {s.clean_rows:,} "
                f"| {s.parse_gap} | {s.transform_gap} |"
            )
    else:
        lines.extend([
            "| Partition | Landing Rows | Clean Rows | Parse Gap | Transform Gap |",
            "| :--- | ---: | ---: | ---: | ---: |",
        ])
        for s in summaries:
            lines.append(
                f"| {s.year} | {s.landing_rows:,} | {s.clean_rows:,} "
                f"| {s.parse_gap} | {s.transform_gap} |"
            )

    lines.append(
        f"| **Total** | **{total_landing:,}** | **{total_clean:,}** "
        f"| **{total_parse_gap}** | **{total_transform_gap}** |"
    )

    lines.append("")
    lines.append(f"**{len(summaries)} partitions**, **{total_clean:,} total clean rows**")

    # Flag any years with issues
    issues = []
    for s in summaries:
        if s.parse_gap > 0:
            issues.append(f"{s.year}: {s.parse_gap:,} parse errors")
        if s.transform_gap > 0:
            issues.append(f"{s.year}: {s.transform_gap:,} transform gap")
        if s.landing_rows > 0 and s.clean_rows == 0:
            issues.append(f"{s.year}: landing has data but clean is empty")

    if issues:
        lines.append("")
        lines.append("**Issues**")
        for issue in issues:
            lines.append(f"- {issue}")

    return "\n".join(lines)


def define_cross_partition_summary_check(
    clean_key: str,
    landing_key: str,
    partition_grain: str,
    clean_io_manager_key: str = "clean_large_io_manager",
) -> dg.AssetChecksDefinition:
    """Generate a non-blocking cross-partition summary check.

    Scans all existing partitions on disk and produces a multi-year
    row accounting summary. Runs after every materialization but never
    blocks downstream assets.
    """
    check_name = f"{clean_key}_cross_partition_summary"

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(clean_key),
        blocking=False,
        required_resource_keys={"landing_io_manager", clean_io_manager_key},
        description=(
            "Cross-partition row summary across all materialized years. "
            "Non-blocking — informational only."
        ),
    )
    def _cross_partition_summary_check(context) -> AssetCheckResult:
        landing_io = context.resources.landing_io_manager
        clean_io = getattr(context.resources, clean_io_manager_key)

        # Discover all yearly partitions on disk
        years = _discover_yearly_partitions(clean_io, clean_key)

        if not years:
            return AssetCheckResult(
                passed=True,
                metadata={"cross_partition_summary": MetadataValue.md("No partitions materialized yet.")},
            )

        summaries: list[YearSummary] = []

        for year in years:
            # Count clean rows
            clean_path = clean_io.get_path_for_asset(AssetKey(clean_key), year)
            clean_rows = _count_parquet_rows(clean_path)

            if partition_grain == "monthly_to_yearly":
                # Get monthly details for this year
                months = _resolve_monthly_landing(landing_io, landing_key, year)
                landing_rows = sum(m.rows for m in months)
                parsed_rows = sum(m.parsed_rows for m in months)
                months_mat = sum(1 for m in months if m.exists)
                months_data = sum(1 for m in months if m.rows > 0)
            else:
                # Same grain: single directory per partition
                dirs = _resolve_landing_dirs(landing_io, landing_key, year, "same_grain")
                landing_rows = 0
                parsed_rows = 0
                for d in dirs:
                    if d.exists():
                        counts = _count_csv_lines(d)
                        landing_rows += sum(counts.values())
                        parsed_rows += _count_parsed_csv_rows(d)
                months_mat = 1 if landing_rows > 0 or clean_rows > 0 else 0
                months_data = 1 if landing_rows > 0 else 0

            summaries.append(YearSummary(
                year=year,
                months_with_data=months_data,
                months_materialized=months_mat,
                landing_rows=landing_rows,
                clean_rows=clean_rows,
                parse_gap=landing_rows - parsed_rows,
                transform_gap=parsed_rows - clean_rows,
            ))

        table_md = _build_cross_partition_table(summaries, partition_grain)

        total_clean = sum(s.clean_rows for s in summaries)
        total_parse_gap = sum(s.parse_gap for s in summaries)
        total_transform_gap = sum(s.transform_gap for s in summaries)
        years_with_issues = sum(
            1 for s in summaries
            if s.parse_gap > 0 or s.transform_gap > 0 or (s.landing_rows > 0 and s.clean_rows == 0)
        )

        metadata: dict[str, Any] = {
            "cross_partition_summary": MetadataValue.md(table_md),
            "total_partitions": len(summaries),
            "total_clean_rows": total_clean,
            "total_parse_gap": total_parse_gap,
            "total_transform_gap": total_transform_gap,
            "partitions_with_issues": years_with_issues,
        }

        if years_with_issues > 0:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata=metadata,
                description=f"{years_with_issues} partition(s) have row accounting issues. See summary table.",
            )

        return AssetCheckResult(
            passed=True,
            metadata=metadata,
        )

    return _cross_partition_summary_check


# ---------------------------------------------------------------------------
# Dual-source (year-routed union) checks
# ---------------------------------------------------------------------------

PartitionGrain = Literal["unpartitioned", "same_grain", "monthly_to_yearly"]
SourceLayer = Literal["landing", "clean"]


@dataclass
class SourceConfig:
    """Defines one source in a dual-source (year-routed) pipeline."""

    landing_key: str             # "nyc_311_historic_landing"
    year_start: int              # 2010
    year_end: int | None         # 2019 (inclusive), or None for open-ended
    grain: PartitionGrain        # "monthly_to_yearly" or "same_grain"
    layer: SourceLayer = "landing"


def _find_active_source(sources: list[SourceConfig], year: int) -> SourceConfig | None:
    """Find which source covers a given year."""
    for s in sources:
        end = s.year_end if s.year_end is not None else 9999
        if s.year_start <= year <= end:
            return s
    return None


def _count_source_rows(
    source: SourceConfig,
    landing_io,
    clean_io,
    partition_key: str,
) -> tuple[int, int, list[MonthDetail] | None]:
    """Count rows for a source based on its layer and grain.

    Returns (landing_total, parsed_total, month_details_or_none).
    For clean-layer sources, landing_total == parsed_total (no parse gap).
    """
    if source.layer == "clean":
        # Parquet-to-parquet passthrough: count source parquet
        source_path = clean_io.get_path_for_asset(AssetKey(source.landing_key), partition_key)
        rows = _count_parquet_rows(source_path)
        return rows, rows, None

    if source.grain == "monthly_to_yearly":
        months = _resolve_monthly_landing(landing_io, source.landing_key, partition_key)
        landing_total = sum(m.rows for m in months)
        parsed_total = sum(m.parsed_rows for m in months)
        return landing_total, parsed_total, months

    # same_grain landing
    dirs = _resolve_landing_dirs(landing_io, source.landing_key, partition_key, "same_grain")
    landing_total = 0
    parsed_total = 0
    for d in dirs:
        if d.exists():
            counts = _count_csv_lines(d)
            landing_total += sum(counts.values())
            parsed_total += _count_parsed_csv_rows(d)
    return landing_total, parsed_total, None


def define_dual_source_row_accounting_check(
    clean_key: str,
    sources: list[SourceConfig],
    allows_row_drop: bool = False,
    clean_io_manager_key: str = "clean_large_io_manager",
) -> dg.AssetChecksDefinition:
    """Row accounting check for dual-source (year-routed) assets.

    Routes to the correct upstream source based on partition year,
    then performs the same reconciliation as the single-source check.
    """
    check_name = f"{clean_key}_row_accounting"

    # Build required resource keys based on source layers
    resource_keys = {clean_io_manager_key}
    if any(s.layer == "landing" for s in sources):
        resource_keys.add("landing_io_manager")

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(clean_key),
        blocking=True,
        required_resource_keys=resource_keys,
        description="Reconciles row counts across dual-source Landing/Clean → unified Clean.",
    )
    def _check(context) -> AssetCheckResult:
        op_ctx = context.op_execution_context
        partition_key = op_ctx.partition_key if op_ctx.has_partition_key else None
        year = int(partition_key) if partition_key else 0

        clean_io = getattr(context.resources, clean_io_manager_key)
        landing_io = getattr(context.resources, "landing_io_manager", None)

        # Find active source for this year
        source = _find_active_source(sources, year)
        if source is None:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata={"row_accounting": MetadataValue.md(f"No source configured for year {year}.")},
                description=f"No source configured for partition year {year}.",
            )

        # Count rows
        landing_total, parsed_total, months = _count_source_rows(
            source, landing_io, clean_io, partition_key
        )

        # Count clean output
        clean_path = clean_io.get_path_for_asset(AssetKey(clean_key), partition_key)
        clean_rows = _count_parquet_rows(clean_path)

        parse_gap = landing_total - parsed_total
        transform_gap = parsed_total - clean_rows

        # Build table
        if months is not None:
            table_md = _build_accounting_table_monthly(months, landing_total, parsed_total, clean_rows)
            months_materialized = sum(1 for m in months if m.exists)
            months_with_data = sum(1 for m in months if m.rows > 0)
            months_empty = months_materialized - months_with_data
            months_missing = 12 - months_materialized
        else:
            # Flat: build a simple table for same_grain or clean-layer
            file_counts = {f"source:{source.landing_key}": landing_total}
            table_md = _build_accounting_table_flat(file_counts, landing_total, parsed_total, clean_rows)
            months_materialized = months_with_data = months_empty = months_missing = 0

        # Add source info to the table
        table_md = f"**Source**: `{source.landing_key}` (years {source.year_start}–{source.year_end or 'present'})\n\n" + table_md

        metadata: dict[str, Any] = {
            "row_accounting": MetadataValue.md(table_md),
            "active_source": source.landing_key,
            "landing_csv_lines": landing_total,
            "parsed_rows": parsed_total,
            "clean_rows": clean_rows,
            "parse_gap": parse_gap,
            "transform_gap": transform_gap,
        }
        if months is not None:
            metadata.update({
                "months_materialized": months_materialized,
                "months_with_data": months_with_data,
                "months_empty": months_empty,
                "months_missing": months_missing,
            })

        # Monthly-specific: all materialized months empty
        if months is not None and months_materialized > 0 and months_with_data == 0:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata=metadata,
                description=(
                    f"All {months_materialized} materialized months have 0 rows for partition {partition_key}. "
                    "Data source may not have published yet."
                ),
            )

        return _evaluate_pass_fail(
            metadata, landing_total, parsed_total, clean_rows,
            parse_gap, transform_gap, allows_row_drop,
        )

    return _check


def define_dual_source_cross_partition_summary_check(
    clean_key: str,
    sources: list[SourceConfig],
    clean_io_manager_key: str = "clean_large_io_manager",
) -> dg.AssetChecksDefinition:
    """Cross-partition summary for dual-source assets. Non-blocking."""
    check_name = f"{clean_key}_cross_partition_summary"

    resource_keys = {clean_io_manager_key}
    if any(s.layer == "landing" for s in sources):
        resource_keys.add("landing_io_manager")

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(clean_key),
        blocking=False,
        required_resource_keys=resource_keys,
        description="Cross-partition row summary across all years for dual-source asset.",
    )
    def _check(context) -> AssetCheckResult:
        clean_io = getattr(context.resources, clean_io_manager_key)
        landing_io = getattr(context.resources, "landing_io_manager", None)

        years = _discover_yearly_partitions(clean_io, clean_key)
        if not years:
            return AssetCheckResult(
                passed=True,
                metadata={"cross_partition_summary": MetadataValue.md("No partitions materialized yet.")},
            )

        summaries: list[YearSummary] = []
        for year in years:
            clean_path = clean_io.get_path_for_asset(AssetKey(clean_key), year)
            clean_rows = _count_parquet_rows(clean_path)

            source = _find_active_source(sources, int(year))
            if source is None:
                summaries.append(YearSummary(year, 0, 0, 0, clean_rows, 0, -clean_rows))
                continue

            landing_rows, parsed_rows, months = _count_source_rows(
                source, landing_io, clean_io, year
            )
            months_mat = sum(1 for m in months if m.exists) if months else (1 if landing_rows > 0 else 0)
            months_data = sum(1 for m in months if m.rows > 0) if months else (1 if landing_rows > 0 else 0)

            summaries.append(YearSummary(
                year=year,
                months_with_data=months_data,
                months_materialized=months_mat,
                landing_rows=landing_rows,
                clean_rows=clean_rows,
                parse_gap=landing_rows - parsed_rows,
                transform_gap=parsed_rows - clean_rows,
            ))

        # Determine grain for table formatting (use first landing source's grain)
        display_grain = next((s.grain for s in sources if s.layer == "landing"), "same_grain")
        table_md = _build_cross_partition_table(summaries, display_grain)

        total_clean = sum(s.clean_rows for s in summaries)
        total_parse_gap = sum(s.parse_gap for s in summaries)
        total_transform_gap = sum(s.transform_gap for s in summaries)
        years_with_issues = sum(
            1 for s in summaries
            if s.parse_gap > 0 or s.transform_gap > 0 or (s.landing_rows > 0 and s.clean_rows == 0)
        )

        metadata: dict[str, Any] = {
            "cross_partition_summary": MetadataValue.md(table_md),
            "total_partitions": len(summaries),
            "total_clean_rows": total_clean,
            "total_parse_gap": total_parse_gap,
            "total_transform_gap": total_transform_gap,
            "partitions_with_issues": years_with_issues,
        }

        if years_with_issues > 0:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                metadata=metadata,
                description=f"{years_with_issues} partition(s) have row accounting issues.",
            )

        return AssetCheckResult(passed=True, metadata=metadata)

    return _check
