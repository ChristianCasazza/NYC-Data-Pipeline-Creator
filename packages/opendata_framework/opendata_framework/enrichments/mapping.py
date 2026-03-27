from __future__ import annotations

import polars as pl

Rule = tuple[str, str]


def map_with_default(
    expr: pl.Expr,
    mapping: dict[str, str],
    *,
    default: str,
) -> pl.Expr:
    """Map string values via a dict with a fallback default.

    Wraps ``pl.Expr.replace()`` for consistent semantics.
    """
    return expr.replace(mapping, default=default)


def ordered_regex_classification(
    text_expr: pl.Expr,
    rules: list[Rule],
    *,
    default: str,
) -> pl.Expr:
    """Apply ordered regex rules, returning the label of the first match.

    Rules are evaluated top-to-bottom. The first ``(label, pattern)``
    where ``text_expr.str.contains(pattern)`` is True wins. If nothing
    matches, ``default`` is returned.

    Args:
        text_expr: Polars expression producing a string to test.
        rules: Ordered list of ``(label, regex_pattern)`` tuples.
        default: Fallback label when no rule matches.
    """
    if not rules:
        return pl.lit(default)

    label0, pattern0 = rules[0]
    expr = pl.when(text_expr.str.contains(pattern0)).then(pl.lit(label0))
    for label, pattern in rules[1:]:
        expr = expr.when(text_expr.str.contains(pattern)).then(pl.lit(label))
    return expr.otherwise(pl.lit(default))


def collect_matching_labels(
    text_expr: pl.Expr,
    rules: list[Rule],
    *,
    separator: str = "; ",
) -> pl.Expr:
    """Return all matching labels as a separator-joined string (null if none match).

    Complement to :func:`ordered_regex_classification`: where that function
    returns the *first* match, this function collects *all* matches.  Same
    ``(label, pattern)`` input format.

    Args:
        text_expr: Polars expression producing a string to test.
        rules: Ordered list of ``(label, regex_pattern)`` tuples.
        separator: Delimiter between matched labels.
    """
    if not rules:
        return pl.lit(None)

    parts = [
        pl.when(text_expr.str.contains(pattern))
        .then(pl.lit(label + separator))
        .otherwise(pl.lit(""))
        for label, pattern in rules
    ]
    result = pl.concat_str(parts, separator="").str.strip_chars_end(separator.rstrip())
    # Strip trailing separator chars, then return null for empty strings
    result = result.str.strip_chars()
    return pl.when(result.str.len_chars() == 0).then(pl.lit(None)).otherwise(result)


def assemble_annotations(
    conditions: list[tuple[pl.Expr, str]],
    *,
    separator: str = "; ",
) -> pl.Expr:
    """Collect messages for all true conditions into a separator-joined string.

    Returns null when no conditions are true.  Each ``(condition_expr, message)``
    pair is evaluated independently; all matching messages are included.

    This is the general-purpose building block for audit trails, compliance
    annotations, and human-readable flag descriptions.

    Args:
        conditions: List of ``(boolean_expr, message_string)`` pairs.
        separator: Delimiter between collected messages.
    """
    if not conditions:
        return pl.lit(None)

    parts = [
        pl.when(cond).then(pl.lit(msg + separator)).otherwise(pl.lit(""))
        for cond, msg in conditions
    ]
    result = pl.concat_str(parts, separator="").str.strip_chars_end(separator.rstrip())
    result = result.str.strip_chars()
    return pl.when(result.str.len_chars() == 0).then(pl.lit(None)).otherwise(result)
