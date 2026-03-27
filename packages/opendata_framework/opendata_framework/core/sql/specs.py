# opendata_framework/core/sql/specs.py
from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from typing import Any

@dataclass(frozen=True)
class SqlAssetSpec:
    name: str
    sql: str
    tags: Mapping[str, str]
    declared_deps: Sequence[str]
    extra_deps: Sequence[str]
    meta: Mapping[str, Any]
