# opendata_framework/dagster/assets/sql_assets.py
# Updated to use the Stateless JIT execution model.
# Removes all references to 'duckdb_warehouse' and persistent DB paths.
# Updated to support Domain-Driven Grouping via SQL frontmatter.

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from collections.abc import Sequence

import duckdb
import polars as pl
import dagster as dg
from dagster import (
    AssetKey,
    AutomationCondition,
    Backoff,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
)

# QueryStation requests are per-asset (not per-page), so a single transient
# 502 should not fail a multi-partition backfill. Mirrors the Socrata landing
# asset's retry policy with a shorter base delay — QueryStation queries are
# typically seconds, not minutes.
_QUERYSTATION_RETRY = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)

from opendata_framework.core.sql.discovery import discover_sql_specs
from opendata_framework.core.sql.parser import (
    extract_qualified_table_names,
    extract_table_names,
)
from opendata_framework.core.sql.runner_duckdb import run_sql_in_duckdb
from opendata_framework.core.sql.runner_querystation import render_sql
from opendata_framework.dagster.partitions import monthly_partitions, yearly_partitions

# Known remote backends that legitimately reference fully-qualified names.
_REMOTE_SOURCES = {"querystation", "ducklake"}


def _parse_partitions_spec(spec: dict[str, Any] | None):
    """Build a PartitionsDefinition from a frontmatter ``partitions:`` block.

    Supported shapes::

        partitions:
          type: yearly
          start: "2020"
          end_offset: 1

        partitions:
          type: monthly
          start: "2024-01-01"
          end_offset: 1
    """
    if not spec:
        return None
    kind = spec.get("type")
    tz = spec.get("tz", "America/New_York")
    if kind == "yearly":
        return yearly_partitions(
            start=spec["start"],
            end=spec.get("end"),
            end_offset=int(spec.get("end_offset", 0)),
            tz=tz,
        )
    if kind == "monthly":
        start = spec.get("start_date") or spec.get("start")
        if start is None:
            raise ValueError("monthly partitions require 'start' (or 'start_date')")
        end = spec.get("end_date") or spec.get("end")
        end_offset = spec.get("end_offset")
        return monthly_partitions(
            start_date=start,
            end_date=end,
            end_offset=int(end_offset) if end_offset is not None else None,
            tz=tz,
        )
    raise ValueError(f"Unknown partitions.type: {kind!r}")


def _markdown_table(df: pl.DataFrame, rows: int = 5) -> str:
    head = df.head(rows)
    cols = head.columns
    md_lines: list[str] = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for row in head.iter_rows():
        md_lines.append("| " + " | ".join("" if x is None else str(x) for x in row) + " |")
    return "\n".join(md_lines)


def _build_asset_core(
    *,
    asset_name: str,
    sql: str,
    meta: dict[str, Any],
    extra_deps: Sequence[str] | None,
    group: str,
    partitions_def: Any | None = None,
) -> list[tuple[str, Any]]:
    """
    Constructs a SQL asset. Uses the private remote execution hook when its
    source is selected, otherwise stateless JIT over Parquet files.
    """
    source = meta.get("source")
    is_ducklake = source == "ducklake"
    is_querystation = source == "querystation"

    # 1. Calculate Dependencies
    declared = set(meta.get("deps", []))
    extras   = set(extra_deps or [])

    if is_ducklake or is_querystation:
        # Remote backends: deps are scheduling-only (no implicit SQL parsing).
        # SQL references remote qualified names (e.g. lake.schema.table).
        run_deps = sorted(declared | extras)
    else:
        # Parquet: implicit deps via SQL table-name parsing
        implicit = {t for t in extract_table_names(sql) if t != asset_name}
        run_deps = sorted(declared | extras | implicit)

    # 2. Define Dagster Deps (Upstream Assets only)
    dagster_deps = [AssetKey(k) for k in run_deps]

    if is_ducklake:
        execution_mode = "ducklake"
    elif is_querystation:
        execution_mode = "querystation_remote"
    else:
        execution_mode = "stateless_jit"

    static_md = {
        "sql": MetadataValue.md(f"```sql\n{sql}\n```"),
        "schema": MetadataValue.json(meta),
        "execution_mode": execution_mode,
    }
    tags = {str(k): str(v) for k, v in (meta.get("tags", {}) or {}).items()}

    # Resolve Group: Prefer frontmatter group, fallback to passed default
    assigned_group = meta.get("group", group)

    if is_ducklake:
        resource_keys = {"ducklake", "analytics_io_manager"}
        asset_kinds = {"ducklake", "sql", "parquet"}
    elif is_querystation:
        resource_keys = {"querystation", "analytics_io_manager"}
        asset_kinds = {"querystation", "remote", "sql"}
    else:
        resource_keys = {"clean_io_manager", "analytics_io_manager", "raw_large_io_manager"}
        asset_kinds = {"duckdb", "sql", "parquet"}

    @dg.asset(
        name=asset_name,
        group_name=assigned_group,
        kinds=asset_kinds,
        io_manager_key="analytics_io_manager",
        deps=dagster_deps,
        required_resource_keys=resource_keys,
        metadata=static_md,
        tags=tags,
        partitions_def=partitions_def,
        retry_policy=_QUERYSTATION_RETRY if is_querystation else None,
        automation_condition=AutomationCondition.eager(),
    )
    def _asset(context) -> MaterializeResult:
        if is_ducklake:
            from opendata_private.ducklake.runner import run_sql_in_ducklake

            df = run_sql_in_ducklake(
                asset_name=asset_name,
                sql=sql,
                ducklake_resource=context.resources.ducklake,
            )
        elif is_querystation:
            op = context.op_execution_context
            partition_key = op.partition_key if op.has_partition_key else None
            partition_start = None
            partition_end = None
            if op.has_partition_key:
                try:
                    window = op.partition_time_window
                    partition_start = window.start
                    partition_end = window.end
                except Exception as exc:
                    # Static/string partitions don't have a time window.
                    # If the SQL uses {{partition_start}}/{{partition_end}}/*_ts
                    # tokens, render_sql will raise — which is what we want.
                    context.log.debug(f"No partition_time_window for {partition_key}: {exc}")
            rendered = render_sql(
                sql,
                partition_key=partition_key,
                partition_start=partition_start,
                partition_end=partition_end,
            )
            context.log.info(
                f"[QueryStation] {asset_name}"
                f"{' partition=' + partition_key if partition_key else ''} "
                f"({len(rendered)} chars)"
            )
            df = context.resources.querystation.query(rendered)
        else:
            io_managers = [
                context.resources.analytics_io_manager,
                context.resources.clean_io_manager,
                context.resources.raw_large_io_manager
            ]
            df = run_sql_in_duckdb(
                asset_name=asset_name,
                sql=sql,
                deps=run_deps,
                io_managers=io_managers
            )

        context.log.debug(f"{asset_name}: deps={', '.join(run_deps) or '∅'} | rows={df.height}")

        return MaterializeResult(
            value=df,
            metadata={
                "row_count": df.height,
                "sample": MetadataValue.md(_markdown_table(df))
            }
        )

    check_name = f"{asset_name}_row_count_check"

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(asset_name),
        blocking=True,
        required_resource_keys={"analytics_io_manager"},
        description="Fails if 0 rows are produced."
    )
    def _row_count_check(context) -> dg.AssetCheckResult:
        io_manager = context.resources.analytics_io_manager
        glob_pattern = io_manager.get_glob_pattern(AssetKey(asset_name), recursive=True)
        asset_root = io_manager.get_path_for_asset(AssetKey(asset_name))

        # For partitioned Hive-layout assets, the flat-file path never exists —
        # only subdirectories (year=YYYY/...) do. Fall back to checking the
        # containing directory, which is what the glob actually scans.
        if not asset_root.exists() and not asset_root.parent.exists():
             return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={"row_count": 0, "reason": "No parquet files written."},
            )

        # Quick DuckDB check on the output
        con = duckdb.connect()
        try:
            cnt = con.execute(
                f"""
                SELECT COUNT(*) FROM parquet_scan(
                  '{glob_pattern}',
                  hive_partitioning=true,
                  union_by_name=true
                )
                """
            ).fetchone()[0]
        except Exception as e:
            return dg.AssetCheckResult(
                passed=False, 
                severity=dg.AssetCheckSeverity.ERROR, 
                metadata={"reason": f"Query failed: {str(e)}"}
            )

        return dg.AssetCheckResult(passed=cnt > 0, metadata={"row_count": int(cnt)})

    return [
        (asset_name, _asset),
        (check_name, _row_count_check),
    ]


def discover_sql_assets(
    *,
    root: Path,
    extra_deps: dict[str, Sequence[str]] | None = None,
    group: str = "Analytics",
) -> dict[str, Any]:
    """Scan ``root`` for *.sql files with YAML frontmatter and build Dagster assets.

    Each ``.sql`` file becomes a stateless DuckDB asset that reads dependencies
    from Parquet via JIT views. Dependencies are auto-detected from SQL table
    references and can be supplemented via ``extra_deps``.

    Args:
        root: Directory to scan recursively for ``.sql`` files.
        extra_deps: Per-asset dependency overrides ``{asset_name: [dep1, dep2]}``.
        group: Default Dagster group name (overridable per-asset via frontmatter).

    Returns:
        Dict mapping asset names to Dagster definitions. Merge into
        ``globals()`` for auto-discovery by Dagster's component scanner.
    """
    extra_deps = extra_deps or {}
    sql_specs = discover_sql_specs(root=root, extra_deps=extra_deps)
    known_assets = {spec.name for spec in sql_specs}

    registry: dict[str, Any] = {}
    for spec in sql_specs:
        # Footgun guard: a SQL file referencing fully-qualified names like
        # ``lake.schema.table`` but lacking a remote ``source:`` will silently
        # fall back to local JIT, which can't resolve the qualified name and
        # emits a generic "Could not locate data" warning. Raise clearly
        # instead so the author fixes the frontmatter at discovery time.
        qualified = extract_qualified_table_names(spec.sql)
        declared_source = spec.meta.get("source")
        if qualified and declared_source not in _REMOTE_SOURCES:
            raise RuntimeError(
                f"SQL asset {spec.name!r} references fully-qualified remote "
                f"tables {sorted(qualified)} but has no remote "
                f"'source:' set in frontmatter. "
                f"Add 'source: querystation' (or another remote backend) "
                f"to execute this query against the remote service, "
                f"or replace the qualified names with local asset names."
            )

        partitions_def = _parse_partitions_spec(spec.meta.get("partitions"))
        generated_assets = _build_asset_core(
            asset_name=spec.name,
            sql=spec.sql,
            meta=spec.meta,
            extra_deps=list(spec.extra_deps),
            group=group,
            partitions_def=partitions_def,
        )

        for key, fn in generated_assets:
            if key in registry:
                raise RuntimeError(f"Duplicate definition detected: '{key}'")
            registry[key] = fn

    return registry
