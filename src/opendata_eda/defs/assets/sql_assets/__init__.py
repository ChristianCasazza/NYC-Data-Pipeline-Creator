from pathlib import Path

from opendata_framework.dagster.assets.sql_assets import discover_sql_assets

_sql_registry = discover_sql_assets(root=Path(__file__).parent, group="nyc__sanitation")

# Expose assets and checks at module level for Dagster discovery
_sql_assets = [v for v in _sql_registry.values() if hasattr(v, "node_def")]
_sql_checks = [v for v in _sql_registry.values() if not hasattr(v, "node_def")]
