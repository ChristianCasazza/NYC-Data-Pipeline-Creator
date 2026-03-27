# OpenDataWeek-API

NYC open data EDA and pipeline toolkit. Ingest datasets from Socrata, materialize them as parquet via Dagster, and query them locally or remotely via QueryStation.

## Repo Structure

```
OpenDataWeek-API/
├── src/opendata_eda/
│   ├── definitions.py              # Entry point — loads defs/ via load_from_defs_folder()
│   └── defs/
│       └── assets/
│           ├── nyc_311_sample.py              # Each pipeline is its own module
│           ├── nyc_film_permits.py
│           ├── nyc_dsny_monthly_tonnage.py
│           ├── nyc_motor_vehicle_collisions.py
│           ├── floodnet/                      # Domain subpackage with shared schemas
│           │   ├── _shared.py
│           │   ├── sensor_metadata.py
│           │   ├── flooding_events.py
│           │   └── events_joined.py
│           └── sql_assets/                    # SQL analytics (YAML frontmatter + SQL)
├── packages/
│   ├── opendata_framework/         # Dagster factories, IO managers, enrichments, schema contracts
│   │   └── opendata_framework/     # (43 Python modules: core/, dagster/, enrichments/, integrations/)
│   └── data_consumers/             # Query client library
│       └── data_consumers/         # RemoteDuckDBWrapper, DuckDBWrapper, PolarsWrapper
├── scripts/
│   └── query_remote.py             # CLI for remote DuckDB queries via QueryStation
├── notebooks/
│   ├── query_local.ipynb           # Query local parquet files (Polars + DuckDB)
│   └── query_remote.ipynb          # Query remote DuckLake via Arrow IPC API
├── reports/                        # Investigation reports
├── data/                           # Materialized output (gitignored)
│   ├── landing/                    # Landing stage (gzipped CSV)
│   ├── clean/                      # Clean stage (typed parquet) — also holds SQL asset output
│   └── exports/                    # Exported query results
├── logs/                           # DAGSTER_HOME (gitignored)
├── .agents/skills/                 # Agent skills (see below)
├── pyproject.toml                  # uv workspace root
├── Makefile                        # install, dev, sync, notebook
└── .env                            # API keys (gitignored)
```

## Quick Start

```bash
make install                        # Create venv, install deps, create .env, create data dirs
source .venv/bin/activate
make dev                            # Launch Dagster UI (DAGSTER_HOME=./logs)
```

## Key Commands

| Command | What it does |
|---|---|
| `make install` | One-shot setup: venv + deps + dirs + .env |
| `make dev` | Launch Dagster with persistent run history in `logs/` |
| `make sync` | Reinstall deps without full setup |
| `make notebook` | Install notebook extras |
| `uv run python scripts/query_remote.py --catalog` | Browse remote QueryStation tables |
| `uv run python scripts/query_remote.py --sql "SELECT ..."` | Run remote SQL query |

## Pipeline Architecture

The project uses Dagster's `load_from_defs_folder()` for automatic asset discovery. Each pipeline is its own module under `src/opendata_eda/defs/assets/`. The entry point (`definitions.py`) loads all assets from `defs/` and merges them with shared resources — no manual asset registration needed. The factory `create_socrata_pipeline()` produces a 2-stage pipeline: **Landing** (CSV) → **Clean** (Parquet). SQL analytics assets live in `src/opendata_eda/defs/assets/sql_assets/` and are auto-discovered.

### Current Assets

#### Socrata Pipeline Assets

| Asset | Dataset ID | Type | Description |
|---|---|---|---|
| `nyc_311_sample` | erm2-nwe9 | Partitioned (monthly→yearly) | 311 service requests |
| `nyc_film_permits` | tg4x-b46p | Unpartitioned | Film/TV shooting permits |
| `nyc_floodnet_sensor_metadata` | kb2e-tjy3 | Unpartitioned | FloodNet sensor locations |
| `nyc_floodnet_flooding_events` | aq7i-eu5q | Unpartitioned | Flood event measurements |
| `nyc_floodnet_events_joined` | — | Unpartitioned (`@asset`) | Events enriched with metadata, severity, hydro metrics |
| `nyc_dsny_monthly_tonnage` | ebb7-mvp5 | Unpartitioned | DSNY monthly collection tonnage by community district |

#### SQL Analytics Assets

| Asset | Upstream | Description |
|---|---|---|
| `dsny_tonnage_annual_summary` | `nyc_dsny_monthly_tonnage` | Citywide annual totals + diversion rates |
| `dsny_tonnage_borough_monthly` | `nyc_dsny_monthly_tonnage` | Borough-level monthly aggregates |
| `dsny_tonnage_district_rankings` | `nyc_dsny_monthly_tonnage` | District rankings by refuse + recycling rate |
| `dsny_tonnage_organics_rollout` | `nyc_dsny_monthly_tonnage` | Organics adoption tracking by borough/year |

### Adding New Socrata Assets

See the **asset-builder** skill (`.agents/skills/asset-builder/`) for the full workflow. Quick version: create a new file in `src/opendata_eda/defs/assets/`:

```python
# src/opendata_eda/defs/assets/nyc_new_dataset.py
import polars as pl
from opendata_framework.dagster import create_socrata_pipeline, SocrataIngestConfig, SchemaContract

new_schema: SchemaContract = {
    "field": ("field", pl.Utf8, "Description."),
}

new_pipeline = create_socrata_pipeline(
    name="nyc_new_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="date_col",
        base_domain="data.cityofnewyork.us",
    ),
    schema=new_schema,
    description="...",
)

nyc_new_dataset = new_pipeline.clean
```

No changes to `definitions.py` needed — `load_from_defs_folder()` picks it up automatically.

### Adding New SQL Analytics Assets

See the **asset-builder** skill for the full workflow. Quick version: create a `.sql` file in `src/opendata_eda/defs/assets/sql_assets/` with YAML frontmatter:

```sql
/*---
name: my_analytics_asset
description: What this asset computes.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
---*/

SELECT ... FROM nyc_dsny_monthly_tonnage ...
```

SQL assets are auto-discovered by `discover_sql_assets()` in `defs/assets/sql_assets/__init__.py` — no manual wiring needed.

## Data Paths

| Stage | Path Pattern |
|---|---|
| Landing | `data/landing/{asset_name}/...` |
| Clean (single) | `data/clean/{asset_name}/{asset_name}.parquet` |
| Clean (partitioned) | `data/clean/{asset_name}/year={YYYY}/{asset_name}.parquet` |
| SQL Analytics | `data/clean/{asset_name}/{asset_name}.parquet` |
| Exports | `data/exports/{name}.{format}` |

## Workspace Packages

This is a uv workspace with two internal packages:

| Package | Location | Purpose |
|---|---|---|
| `opendata-framework` | `packages/opendata_framework/` | Dagster factories, Socrata resources, IO managers, enrichments |
| `data-consumers` | `packages/data_consumers/` | RemoteDuckDBWrapper (Arrow IPC), DuckDBWrapper, PolarsWrapper |

Both are resolved via `[tool.uv.sources]` as workspace dependencies.

## Environment Variables

Set in `.env` (loaded automatically by Dagster and python-dotenv):

| Variable | Required For | Description |
|---|---|---|
| `SOCRATA_API_TOKEN` | Pipeline materialization | NYC Open Data API token |
| `QUERYSTATION_API_KEY` | Remote queries | QueryStation `sk_` API key |
| `AUTH_URL` | Remote queries | Auth endpoint (e.g., `http://localhost:3100`) |

## Dagster Commands

**Always set `DAGSTER_HOME`** so run history persists in `logs/` (same location `make dev` uses). Without it, Dagster creates a temp directory and the run is lost.

**IMPORTANT:** All `dagster asset materialize` commands require the `-m` flag to specify the module:

```bash
-m opendata_eda.definitions
```

Without it, Dagster cannot find the definitions and will error with "Invalid set of CLI arguments".

### Materialize assets

```bash
# Unpartitioned asset (full ingest) — landing then clean
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_floodnet_sensor_metadata_landing
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_floodnet_sensor_metadata

# Partitioned asset (pick one partition)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_311_sample_landing --partition "2026-01-01"
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_311_sample --partition "2026"

# SQL analytics asset (no landing step needed — reads from upstream parquet)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select dsny_tonnage_annual_summary
```

Landing must complete before clean — run them sequentially. SQL assets depend on clean assets.

### Validate definitions load

```bash
uv run python -c "from opendata_eda.definitions import defs; print(f'Assets: {len(list(defs.resolve_asset_graph().get_all_asset_keys()))}')"
```

**NOTE:** The Dagster API for this has changed across versions. Use `resolve_asset_graph().get_all_asset_keys()` — NOT the older `get_asset_graph().all_asset_keys` which will raise `AttributeError`.

### Launch Dagster UI

```bash
make dev
# Or manually:
DAGSTER_HOME=$(pwd)/logs uv run dagster dev
```

### Check materialized output

```bash
uv run python -c "
import polars as pl
df = pl.read_parquet('data/clean/nyc_floodnet_sensor_metadata/nyc_floodnet_sensor_metadata.parquet')
print(f'Rows: {len(df)}, Cols: {len(df.columns)}')
print(df.head(3))
"
```

For partitioned assets:
```bash
uv run python -c "
import polars as pl
df = pl.read_parquet('data/clean/nyc_311_sample/year=2026/nyc_311_sample.parquet')
print(f'Rows: {len(df)}, Cols: {len(df.columns)}')
print(df.head(3))
"
```

## Known Gotchas

Lessons learned from building assets — read these before starting work:

### 1. Dagster CLI requires `-m` flag
`dagster asset materialize --select foo` will fail. Always include `-m opendata_eda.definitions`.

### 2. Dagster API version drift
The validation one-liner uses `defs.resolve_asset_graph().get_all_asset_keys()`. Older docs may reference `get_asset_graph()` or `.all_asset_keys` — both are renamed in the current Dagster version.

### 3. Use `TRY_CAST` not `CAST` in DuckDB SQL
When parsing string fields (like the `month` column in DSNY tonnage which is `"YYYY / MM"`), always use `TRY_CAST(trim(...) AS INT)` instead of `CAST(... AS INT)`. `CAST` throws a hard `ConversionException` on any edge-case empty or malformed string. `TRY_CAST` returns `NULL` instead.

### 4. SQL asset CTE names become phantom dependencies
The SQL parser (`extract_table_names()` in `opendata_framework.core.sql.parser`) uses sqlglot which does not filter out CTE names. A `WITH foo AS (...)` will register `foo` as an upstream dependency. The runner handles this gracefully (creates an empty fallback view, logs a warning), but to keep the asset graph clean, **prefix CTE names with an underscore** (e.g., `_parsed`, `_recent`) so they're obviously not real assets.

### 5. SQL asset IO manager names
SQL assets (via `discover_sql_assets`) expect these resource keys in `defs`: `analytics_io_manager`, `clean_io_manager`, `raw_large_io_manager`. All must be registered in the `resources={}` dict of `dg.Definitions`. Currently they all point to `PolarsParquetIOManager` instances.

## Agent Skills

Four skills are available in `.agents/skills/`:

### querystation
**Trigger:** "query remote", "remote duckdb", "lake.", "iceberg.", "what tables are available remotely"

Remote DuckDB query tool via QueryStation Arrow IPC API. Query DuckLake and Iceberg catalogs containing 17+ NYC datasets (311, capital budget, payroll, restaurant inspections, FloodNet, MTA, etc.).

- **SKILL.md** — Full API reference, table catalog, query patterns, troubleshooting
- **prompts/** — 12 pre-built query prompt templates by domain (311, finance, FloodNet, cross-dataset, data quality, export)

**Usage:** `uv run python scripts/query_remote.py --catalog`

### duckdb-analyst
**Trigger:** "check the data", "run a query", "data quality", "how many rows", "profile this dataset", "null rates"

Read-only DuckDB queries against local parquet files in `data/clean/`. For profiling, schema inspection, data quality checks, and cross-dataset joins on materialized assets.

- **SKILL.md** — Data locations, query patterns, available assets, DuckDB functions

**Usage:** Open `notebooks/query_local.ipynb` or run DuckDB SQL directly.

### socrata-builder
**Trigger:** "add socrata asset", "ingest this dataset", "create pipeline for", Socrata URL pasted

Generates new Dagster Socrata pipelines from a dataset URL. Fetches metadata, recommends partitioning, generates code for `definitions.py`.

- **SKILL.md** — Full workflow (fetch metadata → review → generate → validate → materialize)
- **scripts/fetch_socrata_metadata.py** — Extracts schema, row count, partitioning recommendation from any Socrata URL
- **scripts/fetch_catalog.py** — Fetches NYC Open Data catalog ranked by popularity as CSV
- **references/pipeline-templates.md** — Code templates for unpartitioned and staged pipelines

**Usage:** `uv run .agents/skills/socrata-builder/scripts/fetch_socrata_metadata.py "<URL>"`

### asset-builder
**Trigger:** "add asset", "build pipeline", "create sql asset", "add sql analytics", "new downstream asset"

Comprehensive guide for building both Socrata pipeline assets AND SQL analytics assets. Covers the end-to-end workflow: metadata fetch → code generation → validation → materialization → testing, with full reference on the SQL asset framework (frontmatter schema, CTE naming, dependency resolution).

- **SKILL.md** — Complete reference for both asset types, known pitfalls, and working examples

**Usage:** Read the SKILL.md, then follow the step-by-step workflow.
