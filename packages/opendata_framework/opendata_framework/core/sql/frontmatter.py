# opendata_framework/core/sql/frontmatter.py
from __future__ import annotations

import re
import textwrap
import yaml
from typing import Any

_FM_RE = re.compile(r"""\A\s*/\*---(.*?)---\*/""", re.S)

def split_front_matter(raw: str) -> tuple[dict[str, Any], str]:
    """
    Parse YAML front-matter from a SQL file.
    Returns (meta, sql) where:
      - meta: dict of front-matter keys/values
      - sql: the SQL body after the /*--- ... ---*/ block
    """
    m = _FM_RE.match(raw)
    if not m:
        return {}, raw.lstrip()
    meta = yaml.safe_load(textwrap.dedent(m.group(1))) or {}
    sql = raw[m.end():].lstrip()
    return meta, sql
