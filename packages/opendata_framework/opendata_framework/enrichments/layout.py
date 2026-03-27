from __future__ import annotations

import polars as pl


def reorder_columns(
    lf: pl.LazyFrame,
    priority_cols: list[str],
) -> pl.LazyFrame:
    """Put specified columns first, keeping all others in their original order.

    Columns in ``priority_cols`` that do not exist in the frame are silently
    skipped (no error).

    Args:
        lf: Input LazyFrame.
        priority_cols: Column names to move to the front.
    """
    existing = set(lf.collect_schema().names())
    front = [c for c in priority_cols if c in existing]
    rest = [c for c in lf.collect_schema().names() if c not in set(front)]
    return lf.select(front + rest)
