# opendata_framework/core/polars_utils.py
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl


def multi_parse_date(
    col: str,
    formats: Sequence[str] = (
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
        "%m/%d/%Y",
    ),
    output_timezone: str = "America/New_York",
) -> pl.Expr:
    """
    Attempts multiple timestamp formats and ensures the result is a timezone-aware
    Datetime in the target timezone, handling DST gaps gracefully.
    """
    base = pl.col(col).cast(pl.Utf8, strict=False)
    attempts: list[pl.Expr] = []

    for fmt in formats:
        is_utc = "Z" in fmt or "%z" in fmt
        parsed = base.str.strptime(pl.Datetime, format=fmt, strict=False)

        if is_utc:
            parsed = parsed.dt.convert_time_zone(output_timezone)
        else:
            parsed = parsed.dt.replace_time_zone(
                output_timezone,
                ambiguous="latest",
                non_existent="null",
            )
        attempts.append(parsed)

    return pl.coalesce(attempts)


def safe_bool(col: str) -> pl.Expr:
    """
    Maps various string and numeric representations of boolean values
    to a strict Polars Boolean type.
    """
    as_utf8 = pl.col(col).cast(pl.Utf8, strict=False).str.to_lowercase().str.strip_chars()
    truthy = as_utf8.is_in(["1", "t", "true", "y", "yes"])
    falsy = as_utf8.is_in(["0", "f", "false", "n", "no"])
    return (
        pl.when(truthy)
        .then(True)
        .when(falsy)
        .then(False)
        .otherwise(None)
        .alias(col)
    )


def safe_float(col: str) -> pl.Expr:
    """
    Cleans string-based currency or formatted numbers (e.g. $1,234.56)
    and casts them to Float64.
    """
    return (
        pl.col(col)
        .cast(pl.Utf8)
        .str.replace_all(r"[$,\s]", "")
        .cast(pl.Float64, strict=False)
    )


def _smart_coerce(expr: pl.Expr, target_type: Any, col_name: str, timezone: str) -> pl.Expr:
    """
    Applies logic-heavy coercion based on the requested target type.
    """
    # Datetime coercion
    if target_type == pl.Datetime or isinstance(target_type, pl.Datetime):
        return multi_parse_date(col_name, output_timezone=timezone)

    # Date coercion — parse as datetime first, then extract the date component.
    # Direct str→Date cast silently fails on ISO timestamps like "2014-01-21T00:00:00.000".
    if target_type == pl.Date:
        return multi_parse_date(col_name, output_timezone=timezone).dt.date()

    # Boolean coercion
    if target_type == pl.Boolean:
        return safe_bool(col_name)

    # Float coercion
    if target_type in (pl.Float32, pl.Float64):
        return safe_float(col_name).cast(target_type)

    # Standard numeric coercion
    if target_type in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    ):
        return pl.col(col_name).cast(pl.Utf8).str.replace_all(r"[,]", "").cast(target_type, strict=False)

    # Fallback
    return pl.col(col_name).cast(target_type, strict=False)


SchemaContract = dict[str, tuple[str, Any]]
"""Column mapping from source to output.

Maps source column names to ``(output_name, polars_dtype)`` tuples.
The factory functions also accept an extended 3-tuple format
``(output_name, polars_dtype, description)`` which auto-generates
Dagster data dictionary metadata.

2-tuple example (minimal)::

    schema = {
        "unique_key": ("unique_key", pl.Utf8),
        "created_date": ("created_date", pl.Datetime),
        "borough": ("borough", pl.Utf8),
    }

3-tuple example (with data dictionary)::

    schema = {
        "unique_key": ("unique_key", pl.Utf8, "Unique service request ID"),
        "created_date": ("created_date", pl.Datetime, "Date request was filed"),
        "borough": ("borough", pl.Utf8, "NYC borough name"),
    }
"""


def apply_schema_contract(
    lf: pl.LazyFrame,
    contract: SchemaContract,
    *,
    timezone: str = "America/New_York",
    drop_unknown: bool = True,
) -> pl.LazyFrame:
    """
    Enforces a projection and type contract on a LazyFrame.

    Args:
        drop_unknown (bool):
            If True, only columns defined in the contract are kept (Strict Mode).
            If False, columns in the contract are enforced/created, and ALL OTHER
            columns found in the input are passed through untouched (Resilient Mode).
    """
    expressions = []

    # We must collect the schema to know what is physically present
    # This is a metadata operation, usually very fast for Parquet/LazyFrames
    available_cols = set(lf.collect_schema().names())

    # 1. Handle Known Columns (Enforce types, Create missing)
    for raw_col, (target_name, target_type) in contract.items():
        if raw_col not in available_cols:
            # If a core column is missing, create it as NULL to satisfy the contract
            expressions.append(pl.lit(None).cast(target_type).alias(target_name))
            continue

        expr = _smart_coerce(pl.col(raw_col), target_type, raw_col, timezone)

        if raw_col != target_name:
            expr = expr.alias(target_name)

        expressions.append(expr)

    # 2. Handle Unknown Columns (Pass through if resilient)
    if not drop_unknown:
        known_raw_cols = set(contract.keys())
        # Identify columns present in data but NOT in contract
        extra_cols = available_cols - known_raw_cols

        for col in sorted(extra_cols):
            # Pass through untouched
            expressions.append(pl.col(col))

    return lf.select(expressions)


def _colnames(df: pl.DataFrame | pl.LazyFrame) -> list[str]:
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema().names()
    return list(df.columns)


def load_partitioned_scans(
    context,
    source_asset_key,
    partition_keys: list[str],
    io_manager_key: str = "clean_large_io_manager",
    cast_overrides: dict[str, pl.DataType] | None = None,
) -> pl.LazyFrame | None:
    """Load and concat multiple partitions of a partitioned asset.

    Returns None if no partitions found.
    """
    from dagster import AssetKey

    if isinstance(source_asset_key, str):
        source_asset_key = AssetKey(source_asset_key)
    io_manager = getattr(context.resources, io_manager_key)
    scans = []
    for pk in partition_keys:
        path = io_manager.get_path_for_asset(source_asset_key, partition_key=pk)
        if not path.exists():
            context.log.warning(f"Partition {pk} not found at {path}. Skipping.")
            continue
        lf = pl.scan_parquet(str(path), hive_partitioning=True)
        if cast_overrides:
            existing = lf.collect_schema().names()
            lf = lf.with_columns([
                pl.col(col).cast(dtype) for col, dtype in cast_overrides.items() if col in existing
            ])
        scans.append(lf)
        context.log.info(f"Queued partition {pk} for merge.")
    if not scans:
        return None
    return pl.concat(scans, how="diagonal")


__all__ = [
    "apply_schema_contract",
    "multi_parse_date",
    "safe_bool",
    "safe_float",
    "load_partitioned_scans",
]
