from __future__ import annotations

from typing import Literal

import polars as pl

SEASON_MAP = {1: "winter", 2: "winter", 3: "spring", 4: "spring", 5: "spring",
              6: "summer", 7: "summer", 8: "summer", 9: "fall", 10: "fall",
              11: "fall", 12: "winter"}

DOW_LABELS = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
              5: "Friday", 6: "Saturday", 7: "Sunday"}

RecordDatePrecision = Literal["year", "month", "day", "datetime"]


def parse_string_date(
    col: str,
    fmt: str,
    *,
    alias: str | None = None,
    pre_replace: dict[str, str] | None = None,
    suffix: str | None = None,
) -> pl.Expr:
    """Parse a string column into pl.Date.

    Args:
        col: Source column name.
        fmt: strftime format string (e.g., ``"%Y-%m-%d"``).
        alias: Output column name. Defaults to ``col`` (overwrite in place).
        pre_replace: Optional literal replacements applied before parsing
            (e.g., ``{" / ": "-"}`` to fix ``"2026 / 02"`` → ``"2026-02"``).
        suffix: String appended after replacements but before parsing.
            Use ``"-01"`` to turn ``"2026-02"`` into ``"2026-02-01"``
            for year-month strings that need a day component.
    """
    expr = pl.col(col).cast(pl.Utf8)
    if pre_replace:
        for old, new in pre_replace.items():
            expr = expr.str.replace_all(old, new, literal=True)
    if suffix:
        expr = expr + pl.lit(suffix)
    return expr.str.strptime(pl.Date, fmt, strict=False).alias(alias or col)


def add_record_timestamp(
    lf: pl.LazyFrame,
    source_expr: pl.Expr,
    precision: RecordDatePrecision,
    *,
    tz: str = "America/New_York",
) -> pl.LazyFrame:
    """Add standardized ``record_timestamp`` and ``record_date_precision`` columns.

    Converts any date, datetime, or pre-parsed expression into a timezone-aware
    ``Datetime("us", tz)`` column named ``record_timestamp``. A companion
    ``record_date_precision`` column records the true granularity of the source
    so downstream consumers know what level of temporal analysis is valid.

    Imputation rules for sub-precision components:
    - ``"year"``: source is an integer year → Jan 1 midnight.
    - ``"month"``: source is a Date with day=01 → midnight.
    - ``"day"``: source is a Date → midnight.
    - ``"datetime"``: source is already datetime-precision.

    Args:
        lf: Input LazyFrame.
        source_expr: A Polars expression that evaluates to a Date or Datetime.
            For example ``pl.col("crash_date")`` or the result of
            ``parse_string_date(...)``.
        precision: The true granularity of the source data.
        tz: Target timezone. Defaults to ``"America/New_York"``.
    """
    # Step 1: Materialise into a temp column so we can inspect the schema.
    _tmp = "__record_ts_staging"
    lf = lf.with_columns(source_expr.alias(_tmp))
    dtype = lf.collect_schema()[_tmp]

    # Step 2: Build the tz-aware Datetime expression.
    col = pl.col(_tmp)
    if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
        # Already tz-aware — convert to target tz.
        ts_aware = col.dt.convert_time_zone(tz)
    elif isinstance(dtype, pl.Datetime):
        # Naive datetime — attach target tz.
        ts_aware = col.dt.replace_time_zone(tz, ambiguous="latest", non_existent="null")
    else:
        # Date, Int, or other — cast to naive Datetime then attach tz.
        ts_aware = (
            col.cast(pl.Datetime("us"), strict=False)
            .dt.replace_time_zone(tz, ambiguous="latest", non_existent="null")
        )

    return lf.with_columns(
        ts_aware.alias("record_timestamp"),
        pl.lit(precision).alias("record_date_precision"),
    ).drop(_tmp)


def add_temporal_columns(
    lf: pl.LazyFrame,
    date_col: str = "record_timestamp",
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
) -> pl.LazyFrame:
    """Add derived temporal columns from a date or datetime column.

    All new columns are named ``{prefix}{name}`` (e.g., ``"flood_year"``
    when ``prefix="flood_"``). Only requested columns are added.

    Args:
        lf: Input LazyFrame.
        date_col: Source date/datetime column.
        year: Add ``{prefix}year`` (Int32).
        month: Add ``{prefix}month`` (Int32).
        quarter: Add ``{prefix}quarter`` (Int32).
        day_of_week: Add ``{prefix}day_of_week`` (Utf8, e.g., "Monday").
        fiscal_year: Add ``{prefix}fiscal_year`` (Int32). NYC fiscal year
            runs Jul 1 – Jun 30, so Jul–Dec → current year + 1.
        season: Add ``{prefix}season`` (Utf8: winter/spring/summer/fall).
        year_month_key: Add ``{prefix}year_month`` (Utf8, "YYYY-MM").
        hour: Add ``{prefix}hour`` (Int32). Only meaningful for Datetime columns.
        is_overnight: Add ``{prefix}is_overnight`` (Boolean, hour >= 22 or <= 5).
    """
    src = pl.col(date_col)
    exprs: list[pl.Expr] = []

    if year:
        exprs.append(src.dt.year().cast(pl.Int32).alias(f"{prefix}year"))
    if month:
        exprs.append(src.dt.month().cast(pl.Int32).alias(f"{prefix}month"))
    if quarter:
        exprs.append(src.dt.quarter().cast(pl.Int32).alias(f"{prefix}quarter"))
    if day_of_week:
        exprs.append(
            src.dt.weekday().replace_strict(DOW_LABELS, default="Unknown")
            .alias(f"{prefix}day_of_week")
        )
    if fiscal_year:
        m = src.dt.month()
        y = src.dt.year()
        exprs.append(
            pl.when(m >= 7).then(y + 1).otherwise(y)
            .cast(pl.Int32).alias(f"{prefix}fiscal_year")
        )
    if season:
        exprs.append(
            src.dt.month().replace_strict(SEASON_MAP, default="unknown")
            .alias(f"{prefix}season")
        )
    if year_month_key:
        exprs.append(src.dt.strftime("%Y-%m").alias(f"{prefix}year_month"))
    if hour:
        exprs.append(src.dt.hour().cast(pl.Int32).alias(f"{prefix}hour"))
    if is_overnight:
        h = src.dt.hour()
        exprs.append(((h >= 22) | (h <= 5)).alias(f"{prefix}is_overnight"))

    if not exprs:
        return lf
    return lf.with_columns(exprs)


_DURATION_EXTRACTORS: dict[str, str] = {
    "hours": "total_hours",
    "minutes": "total_minutes",
    "seconds": "total_seconds",
    "milliseconds": "total_milliseconds",
    "microseconds": "total_microseconds",
}


def compute_duration(
    start_col: str,
    end_col: str,
    *,
    unit: Literal["hours", "minutes", "seconds", "milliseconds", "microseconds"] = "hours",
    alias: str | None = None,
    round_digits: int | None = 2,
    flag_negative: bool = False,
    flag_alias: str = "is_temporal_error",
) -> pl.Expr | list[pl.Expr]:
    """Compute elapsed time between two datetime columns.

    Returns the duration as a Float64 in the requested unit.  When
    ``flag_negative`` is True, a companion boolean column is emitted that is
    True when ``end_col < start_col`` (i.e. a logically impossible ordering).

    Args:
        start_col: Column with the earlier timestamp.
        end_col: Column with the later timestamp.
        unit: Time unit for the result.
        alias: Output column name.  Defaults to
            ``"{start_col}_to_{end_col}_{unit}"``.
        round_digits: Decimal places to round to.  ``None`` to skip rounding.
        flag_negative: If True, return a **list** of two expressions —
            the duration and a boolean error flag.
        flag_alias: Name for the error-flag column (only used when
            ``flag_negative`` is True).

    Returns:
        A single ``pl.Expr`` when ``flag_negative`` is False, or a
        ``list[pl.Expr]`` containing [duration_expr, flag_expr] when True.
    """
    extractor = _DURATION_EXTRACTORS.get(unit)
    if extractor is None:
        msg = f"unit must be one of {list(_DURATION_EXTRACTORS)}, got {unit!r}"
        raise ValueError(msg)

    out_alias = alias or f"{start_col}_to_{end_col}_{unit}"

    diff = pl.col(end_col) - pl.col(start_col)
    duration_expr = getattr(diff.dt, extractor)().cast(pl.Float64)
    if round_digits is not None:
        duration_expr = duration_expr.round(round_digits)
    duration_expr = duration_expr.alias(out_alias)

    if not flag_negative:
        return duration_expr

    flag_expr = (
        (pl.col(end_col) < pl.col(start_col))
        .fill_null(False)
        .alias(flag_alias)
    )
    return [duration_expr, flag_expr]


def enforce_timezone(
    lf: pl.LazyFrame,
    cols: list[str],
    *,
    target_tz: str = "America/New_York",
) -> pl.LazyFrame:
    """Ensure datetime columns are timezone-aware in target timezone.

    Handles three cases per column:
    - Already aware in target TZ → no-op.
    - Aware in different TZ → ``dt.convert_time_zone()``.
    - Naive or string → cast to naive Datetime then ``dt.replace_time_zone()``.
    """
    schema = lf.collect_schema()
    existing = schema.names()
    casts: list[pl.Expr] = []

    for col in cols:
        if col not in existing:
            continue
        dtype = schema[col]
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
            casts.append(pl.col(col).dt.convert_time_zone(target_tz).alias(col))
        else:
            casts.append(
                pl.col(col)
                .cast(pl.Datetime("us"), strict=False)
                .dt.replace_time_zone(target_tz, ambiguous="latest", non_existent="null")
                .alias(col)
            )

    if not casts:
        return lf
    return lf.with_columns(casts)
