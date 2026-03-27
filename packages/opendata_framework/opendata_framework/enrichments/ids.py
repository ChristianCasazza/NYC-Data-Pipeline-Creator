from __future__ import annotations

from collections.abc import Iterable

import polars as pl

ExprLike = str | pl.Expr


def _as_expr(item: ExprLike) -> pl.Expr:
    if isinstance(item, str):
        return pl.col(item)
    return item


def stable_string_hash(
    cols: Iterable[ExprLike],
    *,
    seed: int = 42,
    separator: str = "|",
) -> pl.Expr:
    """Build a stable hash from stringified columns, returned as Utf8.

    Concatenates the columns with ``separator``, hashes the result, and
    casts to string for portability across engines.
    """
    parts = [_as_expr(c).fill_null("") for c in cols]
    return pl.concat_str(parts, separator=separator).hash(seed=seed).cast(pl.Utf8)
