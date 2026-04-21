# opendata_framework/core/sql/parser.py
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp


def _extract_via_sqlglot(sql: str) -> set[str] | None:
    """Parse SQL with sqlglot. Returns None if parsing fails.

    CTE names are locally-bound inside the query and must not be treated as
    external deps — otherwise ``WITH foo AS (...) SELECT * FROM foo`` wires
    a phantom ``foo`` dep into the Dagster graph.
    """
    parsed = sqlglot.parse_one(sql, error_level="ignore")
    if parsed is None:
        return None
    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    tables = parsed.find_all(exp.Table)
    names = {t.this.name for t in tables if t.this is not None}
    return names - cte_names


def _extract_via_regex(sql: str) -> set[str]:
    """Fallback regex extraction for malformed SQL."""
    return {m.group(1) for m in re.finditer(r"\bfrom\s+([\w\.]+)", sql, flags=re.I)}


def extract_table_names(sql: str) -> set[str]:
    """
    Return base identifiers of tables referenced in SQL.
    Uses sqlglot parser with regex fallback for malformed SQL.
    """
    result = _extract_via_sqlglot(sql)
    if result is not None:
        return result
    return _extract_via_regex(sql)


def extract_qualified_table_names(sql: str) -> set[str]:
    """Return three-part qualified names (``catalog.schema.table``) from SQL.

    Only includes references with both ``catalog`` and ``schema`` set — these
    are unambiguously remote/external. Excludes CTE names. Used to detect
    accidental remote references in files that forgot to declare
    ``source: querystation``.
    """
    parsed = sqlglot.parse_one(sql, error_level="ignore")
    if parsed is None:
        return set()
    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    qualified: set[str] = set()
    for t in parsed.find_all(exp.Table):
        if t.this is None:
            continue
        name = t.this.name
        if name in cte_names:
            continue
        catalog = t.catalog
        db = t.db
        if catalog and db:
            qualified.add(f"{catalog}.{db}.{name}")
    return qualified
