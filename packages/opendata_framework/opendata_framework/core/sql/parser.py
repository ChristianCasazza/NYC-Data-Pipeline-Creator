# opendata_framework/core/sql/parser.py
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp


def _extract_via_sqlglot(sql: str) -> set[str] | None:
    """Parse SQL with sqlglot. Returns None if parsing fails."""
    parsed = sqlglot.parse_one(sql, error_level="ignore")
    if parsed is None:
        return None
    tables = parsed.find_all(exp.Table)
    return {t.this.name for t in tables if t.this is not None}


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
