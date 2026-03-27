# opendata_framework/enrichments/catalog.py
"""Column metadata companions for enrichment functions.

Each function mirrors its enrichment counterpart's parameter names
but returns list[TableColumn] instead of modifying a LazyFrame.
Use these to auto-build dagster/column_schema data dictionaries.

Usage:
    from opendata_framework.enrichments.catalog import (
        borough_key_columns, temporal_columns, completeness_flag_columns,
    )

    derived = [
        *borough_key_columns(),
        *temporal_columns(year=True, month=True, quarter=True),
        *completeness_flag_columns(date_col="crash_date"),
    ]
    schema = build_table_schema(SCHEMA_DEF, derived_columns=derived)
"""

from __future__ import annotations

from dagster import TableColumn


# ---------------------------------------------------------------------------
# Geographic
# ---------------------------------------------------------------------------

def borough_key_columns(
    *, key: bool = True, code: bool = False, canonical_name: bool = False,
) -> list[TableColumn]:
    """Columns added by add_borough_key()."""
    cols: list[TableColumn] = []
    if key:
        cols.append(TableColumn(
            name="borough_key", type="Utf8",
            description="[Derived] Standardized lowercase borough join key (e.g. 'manhattan', 'staten_island').",
        ))
    if code:
        cols.append(TableColumn(
            name="borough_code", type="Int32",
            description="[Derived] Numeric borough code (1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island).",
        ))
    if canonical_name:
        cols.append(TableColumn(
            name="borough_name", type="Utf8",
            description="[Derived] Canonical borough name (e.g. 'Manhattan', 'Staten Island').",
        ))
    return cols


def location_flag_columns(*, alias: str = "has_location") -> list[TableColumn]:
    """Columns added by add_location_flag()."""
    return [TableColumn(
        name=alias, type="Boolean",
        description="[Derived] True when latitude and longitude are present and within NYC bounds.",
    )]


def community_district_key_columns(*, alias: str = "community_district_key") -> list[TableColumn]:
    """Columns added by add_community_district_key()."""
    return [TableColumn(
        name=alias, type="Utf8",
        description="[Derived] 3-digit community district key (borough_code * 100 + district_number).",
    )]


def nyc_bbl_columns(*, alias: str = "bbl") -> list[TableColumn]:
    """Columns added by add_nyc_bbl()."""
    return [TableColumn(
        name=alias, type="Utf8",
        description="[Derived] 10-digit Borough-Block-Lot identifier.",
    )]


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------

def record_timestamp_columns(*, tz: str = "America/New_York") -> list[TableColumn]:
    """Columns added by add_record_timestamp()."""
    return [
        TableColumn(
            name="record_timestamp", type=f"Datetime(us, {tz})",
            description="[Derived] Standardized timezone-aware timestamp from the primary date column.",
        ),
        TableColumn(
            name="record_date_precision", type="Utf8",
            description="[Derived] Precision of record_timestamp: 'year', 'month', 'day', or 'datetime'.",
        ),
    ]


def temporal_columns(
    *,
    year: bool = True,
    month: bool = True,
    quarter: bool = True,
    day_of_week: bool = False,
    fiscal_year: bool = False,
    season: bool = False,
    year_month_key: bool = False,
    hour: bool = False,
    is_overnight: bool = False,
    prefix: str = "",
) -> list[TableColumn]:
    """Columns added by add_temporal_columns()."""
    cols: list[TableColumn] = []
    if year:
        cols.append(TableColumn(name=f"{prefix}year", type="Int32",
                                description="[Derived] Year extracted from record_timestamp."))
    if month:
        cols.append(TableColumn(name=f"{prefix}month", type="Int32",
                                description="[Derived] Month (1–12) extracted from record_timestamp."))
    if quarter:
        cols.append(TableColumn(name=f"{prefix}quarter", type="Int32",
                                description="[Derived] Quarter (1–4) extracted from record_timestamp."))
    if day_of_week:
        cols.append(TableColumn(name=f"{prefix}day_of_week", type="Utf8",
                                description="[Derived] Day of week name (e.g. 'Monday')."))
    if fiscal_year:
        cols.append(TableColumn(name=f"{prefix}fiscal_year", type="Int32",
                                description="[Derived] NYC fiscal year (Jul–Jun). July 2024 → FY 2025."))
    if season:
        cols.append(TableColumn(name=f"{prefix}season", type="Utf8",
                                description="[Derived] Meteorological season: winter, spring, summer, fall."))
    if year_month_key:
        cols.append(TableColumn(name=f"{prefix}year_month", type="Utf8",
                                description="[Derived] Year-month key in YYYY-MM format."))
    if hour:
        cols.append(TableColumn(name=f"{prefix}hour", type="Int32",
                                description="[Derived] Hour (0–23) extracted from record_timestamp."))
    if is_overnight:
        cols.append(TableColumn(name=f"{prefix}is_overnight", type="Boolean",
                                description="[Derived] True if hour is between 22:00 and 05:00."))
    return cols


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

def completeness_flag_columns(
    *,
    date_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    geo_id_cols: list[str] | None = None,
    custom_flags: dict[str, list[str]] | None = None,
) -> list[TableColumn]:
    """Columns added by add_completeness_flags()."""
    cols: list[TableColumn] = []
    if date_col:
        cols.append(TableColumn(name="has_date", type="Boolean",
                                description=f"[Derived] True when {date_col} is non-null."))
    if lat_col and lon_col:
        cols.append(TableColumn(name="has_location", type="Boolean",
                                description=f"[Derived] True when both {lat_col} and {lon_col} are non-null."))
    if geo_id_cols:
        cols.append(TableColumn(name="has_geo_ids", type="Boolean",
                                description=f"[Derived] True when at least one of {', '.join(geo_id_cols)} is non-null."))
    if custom_flags:
        for flag_name, required_cols in custom_flags.items():
            cols.append(TableColumn(name=flag_name, type="Boolean",
                                    description=f"[Derived] True when all of {', '.join(required_cols)} are non-null."))
    return cols


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def duplicate_marker_columns(
    *, count_col: str = "_dedup_count", rank_out_col: str = "_dedup_rank",
) -> list[TableColumn]:
    """Columns added by add_duplicate_markers()."""
    return [
        TableColumn(name=count_col, type="Int64",
                    description="[Derived] Number of rows sharing the same key columns."),
        TableColumn(name=rank_out_col, type="Int32",
                    description="[Derived] Ordinal rank within duplicate group (1 = preferred row)."),
    ]
