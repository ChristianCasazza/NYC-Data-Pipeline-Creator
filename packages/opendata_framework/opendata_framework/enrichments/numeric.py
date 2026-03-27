from __future__ import annotations

import polars as pl


def sum_components(
    cols: list[str],
    alias: str,
    *,
    fill_null: float = 0.0,
) -> pl.Expr:
    """Sum multiple numeric columns into a total, filling nulls.

    Useful for computing totals from component breakdowns (e.g.,
    ``refuse_tons + paper_tons + mgp_tons``).
    """
    result = pl.col(cols[0]).fill_null(fill_null)
    for c in cols[1:]:
        result = result + pl.col(c).fill_null(fill_null)
    return result.alias(alias)


def compute_rate(
    numerator_col: str,
    denominator_col: str,
    alias: str,
    *,
    scale: float = 1.0,
    null_on_zero: bool = True,
    round_digits: int | None = 4,
) -> pl.Expr:
    """Compute a rate or ratio between two numeric columns.

    Args:
        numerator_col: Column for the numerator.
        denominator_col: Column for the denominator.
        alias: Output column name.
        scale: Multiplier for unit conversion (e.g., ``1/60`` for
            seconds → minutes).
        null_on_zero: Return null instead of inf when denominator is zero.
        round_digits: Round result to this many decimal places.
            Set to None to skip rounding.
    """
    num = pl.col(numerator_col) * scale
    den = pl.col(denominator_col)

    if null_on_zero:
        den = pl.when(den == 0).then(None).otherwise(den)

    result = num / den
    if round_digits is not None:
        result = result.round(round_digits)
    return result.alias(alias)
