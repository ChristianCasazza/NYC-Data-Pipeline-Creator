# opendata_framework/core/sql/discovery.py
from __future__ import annotations

import logging
from pathlib import Path
from collections.abc import Sequence

import yaml

from opendata_framework.core.sql.frontmatter import split_front_matter
from opendata_framework.core.sql.specs import SqlAssetSpec

_logger = logging.getLogger(__name__)


def discover_sql_specs(
    *,
    root: Path,
    extra_deps: dict[str, Sequence[str]] | None = None,
) -> list[SqlAssetSpec]:
    """
    Walk `root` for *.sql files and produce neutral SqlAssetSpec objects.

    Files that cannot be read or have malformed YAML frontmatter are
    logged and skipped rather than crashing the entire Dagster instance.
    """
    registry: list[SqlAssetSpec] = []
    extra_deps = extra_deps or {}

    for p in root.rglob("*.sql"):
        try:
            raw = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            _logger.warning("Skipping unreadable SQL file %s: %s", p, exc)
            continue

        try:
            meta, sql = split_front_matter(raw)
        except yaml.YAMLError as exc:
            _logger.warning("Skipping SQL file with malformed frontmatter %s: %s", p, exc)
            continue

        # name precedence: front-matter "name" else file stem
        name = meta.get("name", p.stem)
        tags = {str(k): str(v) for k, v in (meta.get("tags", {}) or {}).items()}
        declared = list(meta.get("deps", []) or [])
        xtra = list(extra_deps.get(p.stem) or extra_deps.get(name) or [])
        registry.append(SqlAssetSpec(name=name, sql=sql, tags=tags,
                                     declared_deps=declared, extra_deps=xtra, meta=meta))
    return registry
