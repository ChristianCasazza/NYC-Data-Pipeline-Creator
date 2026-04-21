"""SQL templating and execution helper for QueryStation remote assets.

Supported tokens:

* ``{{partition_start}}`` / ``{{partition_end}}`` — date literal
  (``'YYYY-MM-DD'``). Correct for DATE columns where the source stores
  calendar dates; partition-local timezone determines the date boundary.
* ``{{partition_start_ts}}`` / ``{{partition_end_ts}}`` — timestamp literal
  with timezone offset (``'YYYY-MM-DD HH:MM:SS+HH:MM'``). Use for
  ``TIMESTAMP WITH TIME ZONE`` columns to avoid cross-midnight off-by-one
  errors when the partition timezone differs from the source timezone.
* ``{{partition_key}}`` — the raw partition key as a single-quoted string.
  Rejects keys containing unescaped single quotes to prevent SQL injection
  via user-constructed ``StaticPartitionsDefinition`` entries.
"""
from __future__ import annotations

import re
from datetime import datetime

_TOKEN_NAMES = (
    "partition_start_ts",
    "partition_end_ts",
    "partition_start",
    "partition_end",
    "partition_key",
)
_TOKEN_RE = re.compile(
    r"\{\{\s*(" + "|".join(_TOKEN_NAMES) + r")\s*\}\}"
)

# Partition keys that substitute cleanly as SQL string literals.
# Restricts to characters that appear in Dagster's own date/string partition
# formats — blocks quotes, semicolons, parentheses, and other SQL metacharacters.
_SAFE_PARTITION_KEY = re.compile(r"^[A-Za-z0-9_\-:. ]+$")


def _fmt_ts_with_offset(dt: datetime) -> str:
    """Format a timezone-aware datetime as 'YYYY-MM-DD HH:MM:SS+HH:MM'.

    DuckDB parses this into ``TIMESTAMP WITH TIME ZONE`` unambiguously,
    so filtering TZ-aware source columns against these literals is safe
    regardless of the session timezone.
    """
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        return base
    offset = dt.strftime("%z")  # e.g. "-0500"
    if len(offset) == 5:
        offset = f"{offset[:3]}:{offset[3:]}"  # → "-05:00"
    return f"{base}{offset}"


def render_sql(
    sql: str,
    *,
    partition_key: str | None = None,
    partition_start: datetime | None = None,
    partition_end: datetime | None = None,
    date_fmt: str = "%Y-%m-%d",
) -> str:
    """Substitute partition tokens in SQL.

    Fails loudly if the SQL references a token without matching context —
    prevents silent fall-throughs that would fire full-table queries per
    partition.
    """

    def _sub(match: re.Match) -> str:
        token = match.group(1)
        if token == "partition_key":
            if partition_key is None:
                msg = "SQL references {{partition_key}} but asset has no partition context"
                raise ValueError(msg)
            if not _SAFE_PARTITION_KEY.match(partition_key):
                msg = (
                    f"partition_key {partition_key!r} contains characters disallowed "
                    "in SQL substitution. Allowed: alphanumerics, _-:. and space."
                )
                raise ValueError(msg)
            return f"'{partition_key}'"
        if token.endswith("_ts"):
            dt = partition_start if token == "partition_start_ts" else partition_end
            if dt is None:
                msg = f"SQL references {{{{{token}}}}} but asset is not time-partitioned"
                raise ValueError(msg)
            return f"'{_fmt_ts_with_offset(dt)}'"
        dt = partition_start if token == "partition_start" else partition_end
        if dt is None:
            msg = f"SQL references {{{{{token}}}}} but asset is not time-partitioned"
            raise ValueError(msg)
        return f"'{dt.strftime(date_fmt)}'"

    return _TOKEN_RE.sub(_sub, sql)
