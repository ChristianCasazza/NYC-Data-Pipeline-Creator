# OpenDataWeek-API

Open source local data pipeline creator for NYC open-data pipelines. Built with [Dagster](https://dagster.io), [Polars](https://pola.rs), and [DuckDB](https://duckdb.org). Ingest datasets from [Socrata](https://dev.socrata.com/), materialize them as typed Parquet, run SQL analytics on top. 

To query analytics-ready NYC datasets with the [QueryStation API](https://querystation.app).

## What's Inside

```
src/opendata_eda/
  definitions.py            Entry point — loads defs/ via load_from_defs_folder()
  defs/
    assets/
      nyc_311_sample.py     Each pipeline is its own module
      nyc_film_permits.py
      nyc_dsny_monthly_tonnage.py
      nyc_motor_vehicle_collisions.py
      floodnet/             Domain subpackage with shared schemas
        _shared.py
        sensor_metadata.py
        flooding_events.py
        events_joined.py
      sql_assets/           SQL analytics (YAML frontmatter + SQL, auto-discovered)

packages/
  opendata_framework/       Reusable framework: Dagster factories, IO managers,
                            Socrata resource, schema contracts, enrichments
  data_consumers/           Query clients: local DuckDB, Polars, remote Arrow IPC

scripts/query_remote.py     CLI for remote DuckDB queries
notebooks/                  Jupyter notebooks for local & remote exploration
reports/                    Dataset investigation write-ups
examples/                   Cross-dataset query examples
```

## Quick Start

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
make install          # venv, deps, data dirs, .env from template
make dev              # Dagster UI at http://localhost:3000
```
If you don't have Make installed, just ask your LLM to help you. If oy don't want to use make, just open the Make file and run the commands directly.

Edit `.env` with your API keys before materializing:

```env
SOCRATA_API_TOKEN=uHoP8dT0q1BTcacXLCcxrDp8z
QUERYSTATION_API_KEY=sk_your_key_here       # optional, for remote queries
AUTH_URL=https://api.querystation.app
```

The Socrata token above is a shared community key you can use to get started. For production use or heavy workloads, get your own free token at [dev.socrata.com](https://dev.socrata.com/) -- the community key may be rate-limited.

## Datasets

Six Socrata ingestion pipelines ship out of the box, each producing a **landing** (CSV) and **clean** (Parquet) stage:

| Asset | Socrata ID | Description |
|---|---|---|
| `nyc_311_sample` | erm2-nwe9 | 311 service requests (partitioned monthly/yearly) |
| `nyc_film_permits` | tg4x-b46p | Film/TV shooting permits |
| `nyc_floodnet_sensor_metadata` | kb2e-tjy3 | FloodNet sensor locations |
| `nyc_floodnet_flooding_events` | aq7i-eu5q | Street flooding event measurements |
| `nyc_floodnet_events_joined` | -- | Events enriched with sensor metadata, severity, hydro metrics |
| `nyc_dsny_monthly_tonnage` | ebb7-mvp5 | DSNY waste collection by community district |
| `nyc_motor_vehicle_collisions` | h9gi-nx95 | NYPD-reported motor vehicle crashes (2012--present) |

Seven SQL analytics assets are auto-discovered from `src/opendata_eda/defs/assets/sql_assets/`, covering DSNY tonnage trends, borough breakdowns, district rankings, organics rollout tracking, and collision summaries.

## Pipeline Architecture

```
Socrata API  ──>  Landing (gzipped CSV shards)
                       │
                       ▼
                  Clean (typed Parquet, schema-validated, enriched)
                       │
                       ▼
                  SQL Analytics (DuckDB queries over clean Parquet)
```

The project uses Dagster's `load_from_defs_folder()` for automatic asset discovery. The entry point (`definitions.py`) loads all assets from the `defs/` folder and merges them with shared resources -- no manual asset registration needed.

Each pipeline is its own module under `defs/assets/`. Related assets can be grouped into domain subpackages (e.g., `floodnet/` with shared schemas in `_shared.py`). All pipelines are built with `create_socrata_pipeline()` from the `opendata_framework` package, which handles pagination, retry with backoff, schema contracts, and enrichment application. SQL analytics assets use YAML frontmatter for metadata and are auto-wired into the Dagster graph via SQLGlot AST parsing.

### Adding a New Socrata Pipeline

Create a new file in `src/opendata_eda/defs/assets/`:

```python
# src/opendata_eda/defs/assets/nyc_my_dataset.py
import polars as pl
from opendata_framework.dagster import create_socrata_pipeline, SocrataIngestConfig, SchemaContract

my_schema: SchemaContract = {
    "api_field": ("clean_name", pl.Utf8, "Description."),
}

my_pipeline = create_socrata_pipeline(
    name="nyc_my_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="date_col",
        base_domain="data.cityofnewyork.us",
    ),
    schema=my_schema,
    description="What this dataset contains.",
)

nyc_my_dataset = my_pipeline.clean
```

That's it -- `load_from_defs_folder()` picks it up automatically. No changes to `definitions.py` needed.

### Adding a SQL Analytics Asset

Create a `.sql` file in `src/opendata_eda/defs/assets/sql_assets/`:

```sql
/*---
name: my_analytics_view
description: What this computes.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
---*/

SELECT borough, round(sum(refuse_tons_collected), 1) AS total_refuse
FROM nyc_dsny_monthly_tonnage
GROUP BY borough
```

Assets are discovered automatically on Dagster startup.

## Remote Queries via QueryStation

[QueryStation](https://querystation.app) is a hosted service providing curated, analytics-ready NYC Open Data through a remote DuckDB endpoint. It serves 17+ datasets (311, capital budget, payroll, restaurant inspections, FloodNet, MTA, and more) over an Arrow IPC interface for fast, zero-copy result transfer.

QueryStation access is **optional** -- all local ingestion and analytics work without it.

```bash
# Browse available catalogs and tables
uv run python scripts/query_remote.py --catalog

# Describe a table's schema
uv run python scripts/query_remote.py --describe lake.nyc_operations.service_requests_311

# Run a query
uv run python scripts/query_remote.py --sql "SELECT COUNT(*) FROM lake.nyc_operations.service_requests_311"

# Export results
uv run python scripts/query_remote.py --sql "SELECT * FROM ..." --export mydata csv
```

The `notebooks/query_remote.ipynb` notebook provides an interactive exploration environment with pre-built query templates.

## Workspace Packages

This is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) with two internal packages:

| Package | Purpose |
|---|---|
| **opendata-framework** | Dagster factories, Socrata resource, IO managers (Parquet/CSV/JSON), schema contracts, enrichments (temporal, geographic, text, numeric, dedup), SQL asset discovery |
| **data-consumers** | `RemoteDuckDBWrapper` (QueryStation Arrow IPC), `DuckDBWrapper` (local), `PolarsWrapper` |

## Key Commands

| Command | Description |
|---|---|
| `make install` | One-shot setup: venv + deps + dirs + `.env` |
| `make dev` | Launch Dagster UI (`DAGSTER_HOME=./logs`) |
| `make sync` | Reinstall dependencies |
| `make notebook` | Install Jupyter extras |

### Materializing Assets

```bash
# Unpartitioned -- landing then clean
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select nyc_dsny_monthly_tonnage_landing

DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select nyc_dsny_monthly_tonnage

# SQL analytics (reads from upstream Parquet)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select dsny_tonnage_annual_summary
```

## Tech Stack

- **Orchestration:** [Dagster](https://dagster.io) 1.12+
- **DataFrames:** [Polars](https://pola.rs) 1.8+
- **Analytics Engine:** [DuckDB](https://duckdb.org) 1.4+
- **Data Source:** [Socrata SODA v2 API](https://dev.socrata.com/)
- **Remote Queries:** [QueryStation](https://querystation.app) Arrow IPC
- **SQL Parsing:** [sqlglot](https://github.com/tobymao/sqlglot)
- **Package Management:** [uv](https://docs.astral.sh/uv/)

## Data Storage

All materialized data lives under `data/` (gitignored):

| Stage | Path |
|---|---|
| Landing | `data/landing/{asset}/` |
| Clean | `data/clean/{asset}/{asset}.parquet` |
| Clean (partitioned) | `data/clean/{asset}/year={YYYY}/{asset}.parquet` |
| Exports | `data/exports/` |

## License

[MIT](LICENSE)
