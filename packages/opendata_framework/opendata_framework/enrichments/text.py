from __future__ import annotations

import polars as pl


def clean_text_expr(
    col: str,
    *,
    uppercase: bool = True,
    alias: str | None = None,
) -> pl.Expr:
    """Strip whitespace, collapse internal runs, optionally uppercase.

    Produces a single cleaned text expression suitable for use in
    ``.with_columns()``. Returns null when input is null.
    """
    expr = pl.col(col).str.strip_chars().str.replace_all(r"\s+", " ")
    if uppercase:
        expr = expr.str.to_uppercase()
    return expr.alias(alias or col)


def apply_literal_replacements(
    expr: pl.Expr,
    replacements: dict[str, str],
    *,
    replace_all: bool = True,
) -> pl.Expr:
    """Apply ordered literal string replacements to a Polars expression.

    Each key-value pair in ``replacements`` is applied sequentially.
    """
    out = expr
    for old, new in replacements.items():
        if replace_all:
            out = out.str.replace_all(old, new, literal=True)
        else:
            out = out.str.replace(old, new, literal=True)
    return out


def split_and_clean_list(
    expr: pl.Expr,
    *,
    delimiter: str = ",",
) -> pl.Expr:
    """Split delimited text into a trimmed, non-empty list.

    Splits on ``delimiter``, strips whitespace from each element,
    and filters out empty strings.
    """
    return (
        expr.str.split(delimiter)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0))
    )
