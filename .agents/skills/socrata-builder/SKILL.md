---
name: socrata-builder
description: >
  Generates Dagster Socrata pipeline assets from a dataset URL. Use when user
  provides a Socrata URL (data.cityofnewyork.us, data.ny.gov, or any Socrata
  domain) and wants to add a new dataset. Triggers on: "add socrata asset",
  "ingest this dataset", "create pipeline for this", "add this dataset", or
  when user pastes a Socrata open data link. Runs a metadata script, then
  generates pipeline code as a new module in defs/assets/.
---

# Socrata Asset Builder

Generates a complete Dagster Socrata pipeline (landing, clean) from a single dataset URL. Each pipeline is created as its own module in `src/opendata_eda/defs/assets/` using the `create_socrata_pipeline()` factory from the `opendata_framework` package.

### Before reaching for Socrata

QueryStation's `lake` catalog already hosts 17+ pre-cleaned NYC datasets (311, payroll, capital/expense budgets, restaurant inspections, FloodNet, MTA, checkbook spending, housing). If the dataset the user wants is already in that catalog, **prefer a QueryStation remote SQL asset** — it skips the ingestion stage entirely and gets you the same local Parquet output. See the **asset-builder** skill (Part 3) and the **querystation** skill.

Use this Socrata builder when:
- The dataset isn't in QueryStation's lake, or
- You explicitly need to own the ingestion (custom schema contract, enrichments, private tokens), or
- You want a full landing → clean lineage for auditing.

## Repo Layout

This repo uses Dagster's `load_from_defs_folder()` for automatic asset discovery:

```
src/opendata_eda/
├── definitions.py              ← Entry point — loads defs/ and merges resources
└── defs/
    └── assets/
        ├── nyc_311_sample.py           ← Each pipeline is its own module
        ├── nyc_film_permits.py
        ├── nyc_dsny_monthly_tonnage.py
        ├── nyc_motor_vehicle_collisions.py
        ├── floodnet/                   ← Domain subpackage with shared schemas
        │   ├── _shared.py
        │   ├── sensor_metadata.py
        │   ├── flooding_events.py
        │   └── events_joined.py
        └── sql_assets/                 ← SQL analytics (auto-discovered)
packages/opendata_framework/            ← Factory, IO managers, enrichments, SQL asset framework
packages/data_consumers/                ← Query client library
```

New Socrata pipelines are added as new modules in `defs/assets/` — `load_from_defs_folder()` picks them up automatically. SQL analytics assets go in `defs/assets/sql_assets/` and are auto-discovered — see the **asset-builder** skill for details.

## Partitioning Logic

The metadata script applies this decision tree based on row count:

```
< 5M rows       → No partitions. Small enough for a single pull.
5M–500M rows    → Monthly landing, yearly clean (staged).
> 500M rows     → Monthly all stages.
equality column → Yearly all stages (fiscal_year, etc.).
no date column  → Unpartitioned regardless of size.
```

**Date column scoring** (higher = better partition candidate):
- 90: Known event columns (`created_date`, `arrest_date`, `inspection_date`, etc.)
- 75: Suffix `_date` or `_datetime`
- 70: Suffix `_time` or `_timestamp`
- 65: Contains "date"
- 40: Generic calendar_date
- 20: Non-event dates (`hire_date`, `birth_date`, etc.)
- 10: System columns (`:created_at`, `:updated_at`)

## Instructions

### Step 1: Fetch Metadata

```bash
uv run .agents/skills/socrata-builder/scripts/fetch_socrata_metadata.py "<SOCRATA_URL>"
```

### Step 2: Review with User

Present the metadata summary and confirm:
1. **Asset name** — accept or override
2. **Partitioning** — accept recommendation or override
3. **Column renames** — normalize to `lower_snake_case`
4. **System columns** — skip `:id`, `:created_at`, etc. by default

### Step 3: Create a new asset module

Create a new file in `src/opendata_eda/defs/assets/`:

#### Unpartitioned example (most common for this repo):

```python
# src/opendata_eda/defs/assets/nyc_new_dataset.py
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

new_dataset_schema: SchemaContract = {
    "field_one": ("field_one", pl.Utf8, "Description from Socrata."),
    "field_two": ("field_two", pl.Float64, "Another description."),
}

new_dataset_pipeline = create_socrata_pipeline(
    name="nyc_new_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="created_date",
        base_domain="data.cityofnewyork.us",
    ),
    schema=new_dataset_schema,
    description="Brief description of the dataset.",
)

nyc_new_dataset = new_dataset_pipeline.clean
```

No changes to `definitions.py` needed — `load_from_defs_folder()` picks it up automatically.

#### Staged example (monthly landing → yearly clean):

```python
# src/opendata_eda/defs/assets/nyc_new_staged.py
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
    monthly_partitions,
    yearly_partitions,
)

new_staged_schema: SchemaContract = {
    "created_date": ("created_date", pl.Datetime, "Date created."),
    # ... more columns
}

new_staged_pipeline = create_socrata_pipeline(
    name="nyc_new_staged",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="created_date",
        base_domain="data.cityofnewyork.us",
    ),
    schema=new_staged_schema,
    description="Large dataset with monthly/yearly staging.",
    partitions_def=monthly_partitions("2020-01-01", end_offset=1),
    clean_partitions_def=yearly_partitions("2020", end_offset=1),
)

nyc_new_staged = new_staged_pipeline.clean
```

### Step 4: Validate

```bash
uv run python -c "from opendata_eda.definitions import defs; print(f'Assets: {len(list(defs.resolve_asset_graph().get_all_asset_keys()))}')"
```

**NOTE:** Use `resolve_asset_graph().get_all_asset_keys()` — the older `get_asset_graph().all_asset_keys` is removed in the current Dagster version.

### Step 5: Materialize and Test

**IMPORTANT:** Always include `-m opendata_eda.definitions` in materialize commands. Without it, Dagster cannot find the code location.

```bash
# Unpartitioned
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_dataset_landing
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_dataset

# Partitioned (pick one recent partition)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_staged_landing --partition "2025-01-01"
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_staged --partition "2025"
```

Or use `make dev` and materialize via the Dagster UI.

### Step 6: Verify Output

```bash
uv run python -c "
import polars as pl
df = pl.read_parquet('data/clean/nyc_new_dataset/nyc_new_dataset.parquet')
print(f'Rows: {len(df)}, Columns: {len(df.columns)}')
print(df.schema)
print(df.head(3))
"
```

## Column Name Normalization

Target format: `lower_snake_case` with human-readable words.

Rules:
1. **ALLCAPS/smashed names** → break into words: `COMMUNITYDISTRICT` → `community_district`
2. **Cryptic abbreviations** → expand when clear: `cmplnt_num` → `complaint_number`
3. **Already clean** → leave unchanged: `borough`, `latitude`
4. **Geo columns** → standardize: `x_coord_cd` → `x_coordinate`

In the schema dict, the key stays as the API field name, the target changes:
```python
"communitydistrict": ("community_district", pl.Utf8, "One of NYC's 59 community districts."),
```

## Existing Assets (for dedup)

Check `src/opendata_eda/defs/assets/` before creating — these Socrata pipelines are already built:

| Dataset ID | Asset Name | Module | Type |
|---|---|---|---|
| erm2-nwe9 | nyc_311_sample | `nyc_311_sample.py` | Partitioned |
| tg4x-b46p | nyc_film_permits | `nyc_film_permits.py` | Unpartitioned |
| kb2e-tjy3 | nyc_floodnet_sensor_metadata | `floodnet/sensor_metadata.py` | Unpartitioned |
| aq7i-eu5q | nyc_floodnet_flooding_events | `floodnet/flooding_events.py` | Unpartitioned |
| — | nyc_floodnet_events_joined | `floodnet/events_joined.py` | Derived |
| ebb7-mvp5 | nyc_dsny_monthly_tonnage | `nyc_dsny_monthly_tonnage.py` | Unpartitioned |
| h9gi-nx95 | nyc_motor_vehicle_collisions | `nyc_motor_vehicle_collisions.py` | Unpartitioned |

SQL analytics assets also exist in `src/opendata_eda/defs/assets/sql_assets/` — check there before creating downstream assets. This directory contains both local DuckDB-JIT analytics (e.g. `dsny_tonnage_annual_summary`, `collisions_annual_summary`) and QueryStation-backed remote assets (e.g. `mta_ridership_yearly`, `nyc_air_quality_annual`, `nyc_311_top_heat_bbls_by_cb`, `transit_vs_air_quality`).

## Batch Mode: Catalog CSV

```bash
uv run .agents/skills/socrata-builder/scripts/fetch_catalog.py --max 150 -o catalog.csv
```

Use the CSV to prioritize datasets by `views_total`, filter out `already_exists = True`, and batch by `category`.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| 404 on metadata fetch | Wrong dataset ID or private dataset | Verify URL and dataset ID |
| `time_col` required | `SocrataIngestConfig` requires it | Use the best date column from metadata output |
| Import error | Package not installed | Run `make sync` to reinstall deps |
| No data in parquet | Wrong endpoint or column mismatch | Check Socrata endpoint is correct, schema keys match API field names |
| `AttributeError: get_asset_graph` | Dagster API renamed | Use `resolve_asset_graph().get_all_asset_keys()` |
| `Invalid set of CLI arguments` | Missing module flag | Add `-m opendata_eda.definitions` to materialize command |
| Empty parquet after clean | Column names don't match API | Schema dict keys must be exact Socrata API field names (case-sensitive) |
