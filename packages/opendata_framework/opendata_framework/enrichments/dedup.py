from __future__ import annotations

import polars as pl


def add_duplicate_markers(
    lf: pl.LazyFrame,
    *,
    key_cols: list[str],
    rank_col: str,
    count_col: str = "_dedup_count",
    rank_out_col: str = "_dedup_rank",
    descending: bool = True,
) -> pl.LazyFrame:
    """Flag duplicate groups and rank rows within each group.

    Adds two columns:
    - ``count_col``: number of rows sharing the same ``key_cols`` values.
    - ``rank_out_col``: ordinal rank within each group, ordered by
      ``rank_col`` (descending by default so rank 1 = "best" row).
    """
    return lf.with_columns(
        pl.len().over(key_cols).alias(count_col),
        pl.col(rank_col)
        .rank("ordinal", descending=descending)
        .over(key_cols)
        .cast(pl.Int32)
        .alias(rank_out_col),
    )
