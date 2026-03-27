from __future__ import annotations

import polars as pl


def add_completeness_flags(
    lf: pl.LazyFrame,
    *,
    date_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    geo_id_cols: list[str] | None = None,
    custom_flags: dict[str, list[str]] | None = None,
) -> pl.LazyFrame:
    """Add boolean data-quality flags for key column groups.

    Each flag is True when the required columns are non-null.

    Args:
        lf: Input LazyFrame.
        date_col: If set, adds ``has_date`` (True when date column is not null).
        lat_col: Combined with ``lon_col``, adds ``has_location``.
        lon_col: Combined with ``lat_col``, adds ``has_location``.
        geo_id_cols: If set, adds ``has_geo_ids`` (True when at least one
            geo ID column is non-null, e.g., BBL, BIN, NTA).
        custom_flags: Dict of ``{flag_name: [required_col_names]}``.
            Each flag is True when ALL listed columns are non-null.
    """
    exprs: list[pl.Expr] = []

    if date_col is not None:
        exprs.append(pl.col(date_col).is_not_null().alias("has_date"))

    if lat_col is not None and lon_col is not None:
        exprs.append(
            (pl.col(lat_col).is_not_null() & pl.col(lon_col).is_not_null())
            .alias("has_location")
        )

    if geo_id_cols:
        any_present = pl.col(geo_id_cols[0]).is_not_null()
        for c in geo_id_cols[1:]:
            any_present = any_present | pl.col(c).is_not_null()
        exprs.append(any_present.alias("has_geo_ids"))

    if custom_flags:
        for flag_name, cols in custom_flags.items():
            all_present = pl.col(cols[0]).is_not_null()
            for c in cols[1:]:
                all_present = all_present & pl.col(c).is_not_null()
            exprs.append(all_present.alias(flag_name))

    if not exprs:
        return lf
    return lf.with_columns(exprs)
