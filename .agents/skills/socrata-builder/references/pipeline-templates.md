# Pipeline Code Templates for OpenDataWeek-API

Each pipeline is its own module in `src/opendata_eda/defs/assets/`. `load_from_defs_folder()` discovers them automatically — no manual registration in `definitions.py`.

Related assets can be grouped into domain subpackages (e.g., `floodnet/` with `_shared.py` for shared schemas).

## Template A: Unpartitioned (most common)

```python
# src/opendata_eda/defs/assets/{asset_name}.py
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

{var}_schema: SchemaContract = {
{schema_entries}
}

{var}_pipeline = create_socrata_pipeline(
    name="{asset_name}",
    socrata_config=SocrataIngestConfig(
        endpoint="{dataset_id}",
        time_col="{time_col}",
        base_domain="{base_domain}",
    ),
    schema={var}_schema,
    description="{description}",
)

{asset_name} = {var}_pipeline.clean
```

## Template B: Staged (monthly landing → yearly clean)

```python
# src/opendata_eda/defs/assets/{asset_name}.py
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
    monthly_partitions,
    yearly_partitions,
)

{var}_schema: SchemaContract = {
{schema_entries}
}

{var}_pipeline = create_socrata_pipeline(
    name="{asset_name}",
    socrata_config=SocrataIngestConfig(
        endpoint="{dataset_id}",
        time_col="{time_col}",
        base_domain="{base_domain}",
    ),
    schema={var}_schema,
    description="{description}",
    partitions_def=monthly_partitions("{start_date}", end_offset=1),
    clean_partitions_def=yearly_partitions("{start_year}", end_offset=1),
)

{asset_name} = {var}_pipeline.clean
```

## Schema Entry Formats

3-tuple (with description — preferred):
```python
    "{api_field}": ("{target_name}", pl.Utf8, "Description from Socrata."),
```

2-tuple (without description — fallback):
```python
    "{api_field}": ("{target_name}", pl.Utf8),
```

## Key Rules

- Schema type alias: `SchemaContract` (imported from `opendata_framework.dagster`)
- Always include `time_col` in `SocrataIngestConfig` (required field)
- Include `base_domain="data.cityofnewyork.us"` for NYC datasets (default is `data.ny.gov`)
- Factory auto-builds data dictionary from 3-tuple schemas
- Factory auto-sets `allow_nonexistent_upstream_partitions=True` for staged pipelines
- Export the clean asset at module level for Dagster discovery
- Each module only needs its own imports — no resource/IO manager imports needed

## Validation and Materialization

```bash
# Validate definitions load (use resolve_asset_graph, NOT get_asset_graph)
uv run python -c "from opendata_eda.definitions import defs; print(f'Assets: {len(list(defs.resolve_asset_graph().get_all_asset_keys()))}')"

# Materialize (always include -m flag)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select {asset_name}_landing
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select {asset_name}
```
