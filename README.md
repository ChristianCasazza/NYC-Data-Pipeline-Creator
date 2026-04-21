# Starter Repo for Building NYC Data Pipelines

This is a local data pipeline creator for NYC open data pipelines. Built with [Dagster](https://dagster.io), [Polars](https://pola.rs), and [DuckDB](https://duckdb.org).

Three ways to get data:

- **Ingest from [Socrata](https://dev.socrata.com/)** — pull CSVs from NYC Open Data, land as typed Parquet.
- **Query remote via [QueryStation](https://querystation.app)** — SQL against a curated DuckDB lake of 17+ NYC datasets over Arrow IPC, with results cached as local Parquet.
- **Run SQL analytics on top of either** — DuckDB-JIT views over the combined local Parquet.

All three are first-class Dagster assets. Fork this repo to build pipelines for whatever you care about.

## What's Inside

```
src/opendata_eda/
  definitions.py            Entry point — loads defs/ + wires Socrata and
                            QueryStation resources
  defs/
    assets/
      nyc_311_sample.py            Socrata pipelines (one file per dataset)
      nyc_film_permits.py
      nyc_dsny_monthly_tonnage.py
      nyc_motor_vehicle_collisions.py
      floodnet/                    Domain subpackage with shared schemas
        _shared.py
        sensor_metadata.py
        flooding_events.py
        events_joined.py
      sql_assets/                  Auto-discovered .sql files with YAML
                                   frontmatter. Two documented execution
                                   backends: local DuckDB-JIT (default) and
                                   remote QueryStation (source: querystation).

packages/
  opendata_framework/       Reusable framework: Dagster factories, IO managers,
                            Socrata + QueryStation resources, SQL frontmatter
                            discovery, templating, schema contracts, enrichments
  data_consumers/           Query clients: local DuckDB, Polars, remote Arrow IPC

scripts/query_remote.py     CLI for ad-hoc remote DuckDB queries
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
If you do not have Make installed, open the Makefile and run the equivalent `uv` commands directly.

Edit `.env` with your API keys before materializing:

```env
SOCRATA_API_TOKEN=uHoP8dT0q1BTcacXLCcxrDp8z
QUERYSTATION_API_KEY=sk_your_key_here       # optional, for remote queries
AUTH_URL=https://api.querystation.app
```

The Socrata token above is a shared community key you can use to get started. For production use or heavy workloads, get your own free token at [dev.socrata.com](https://dev.socrata.com/); the community key may be rate-limited.

## Datasets

### Socrata ingestion pipelines

Six Socrata pipelines ship out of the box, each producing a **landing** (CSV) and **clean** (Parquet) stage. The repo also includes one derived FloodNet clean asset:

| Asset | Socrata ID | Description |
|---|---|---|
| `nyc_311_sample` | erm2-nwe9 | 311 service requests (partitioned monthly/yearly) |
| `nyc_film_permits` | tg4x-b46p | Film/TV shooting permits |
| `nyc_floodnet_sensor_metadata` | kb2e-tjy3 | FloodNet sensor locations |
| `nyc_floodnet_flooding_events` | aq7i-eu5q | Street flooding event measurements |
| `nyc_floodnet_events_joined` | — | Derived `@asset`: events enriched with sensor metadata, severity, hydro metrics |
| `nyc_dsny_monthly_tonnage` | ebb7-mvp5 | DSNY waste collection by community district |
| `nyc_motor_vehicle_collisions` | h9gi-nx95 | NYPD-reported motor vehicle crashes (2012–present) |

### QueryStation remote assets

Four remote SQL assets pull from the [QueryStation](https://querystation.app) lake and cache the result as local Parquet:

| Asset | Upstream table | Type | Description |
|---|---|---|---|
| `mta_ridership_by_mode` | `lake.nys_transportation.mta_daily_ridership` | Unpartitioned | Per-mode MTA ridership totals (all years). One-shot remote pull. |
| `mta_ridership_yearly` | `lake.nys_transportation.mta_daily_ridership` | Yearly-partitioned | Per-mode annual aggregates. One remote query per partition. |
| `nyc_air_quality_annual` | `lake.nyc_environment.air_quality` | Unpartitioned | Annual NO2 / PM2.5 / Ozone means across NYC neighborhoods. |
| `nyc_311_top_heat_bbls_by_cb` | `lake.nyc_operations.service_requests_311` | Unpartitioned | Top 10 properties (BBLs) with most 311 heat complaints per community board. |

### SQL analytics assets

Eight local DuckDB-JIT analytics assets are auto-discovered from `src/opendata_eda/defs/assets/sql_assets/` — DSNY trends, collision summaries, organics rollout tracking, and one cross-backend join (`transit_vs_air_quality`) that combines QueryStation-sourced MTA and air-quality Parquet.

## Pipeline Architecture

```
Socrata API  ──>  Landing (gzipped CSV shards)                ┐
                       │                                       │
                       ▼                                       │
                  Clean (typed Parquet, schema-validated)      │
                       │                                       │ Everything lives
                       └──────────┐                            │ in data/clean/*.parquet
                                  │                            │ and is queryable
QueryStation API (Arrow IPC) ──>  Clean (Parquet, one file    │ as a unified lake
                                  per partition)               │
                                  │                            │
                                  └──────────┐                 │
                                             ▼                 │
                                SQL Analytics (DuckDB-JIT      │
                                views over clean Parquet)      ┘
```

The project uses Dagster's `load_from_defs_folder()` for automatic asset discovery. The entry point (`definitions.py`) loads all assets from the `defs/` folder and merges them with shared resources — no manual asset registration needed.

**Three execution backends**, chosen per-asset:

1. **Socrata pipelines** — built with `create_socrata_pipeline()`. Handles pagination, retry with backoff, schema contracts, and enrichment application. Lives in `defs/assets/*.py` (one module per dataset).
2. **QueryStation remote** — author a `.sql` file with `source: querystation` in frontmatter. The framework executes the SQL against QueryStation's Arrow IPC endpoint and writes the Polars result to Parquet. Retries on transient network failures.
3. **Local DuckDB JIT** — default backend for `.sql` files without a `source:` field. Builds ephemeral views over upstream Parquet (Socrata- or QueryStation-produced) and runs the query statelessly. No persistent warehouse.

All three write to the same IO manager (`PolarsParquetIOManager` at `./data/clean/`), so a local JIT asset can join a Socrata-ingested Parquet with a QueryStation-ingested Parquet without caring where either came from.

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

That's it: `load_from_defs_folder()` picks it up automatically. No changes to `definitions.py` needed.

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

### Adding a QueryStation Remote Asset

Same filesystem location, same frontmatter grammar — add `source: querystation` and reference a remote fully-qualified table:

```sql
/*---
name: nyc_restaurant_inspections_recent
source: querystation
description: Recent restaurant inspections from QueryStation.
group: querystation__health
tags:
  domain: health
  geographic_scope: nyc
---*/

SELECT camis, dba, borough, grade, inspection_date, score
FROM lake.nyc_health.restaurant_inspections
WHERE inspection_date >= '2025-01-01'
```

The framework executes the query against QueryStation, receives Arrow IPC, and writes the result to `data/clean/nyc_restaurant_inspections_recent/…parquet` — just like a Socrata-ingested asset. Retry policy (3 attempts, exponential backoff) is automatic.

**Partitioned remote assets.** Add a `partitions:` block in frontmatter and use `{{partition_start}}` / `{{partition_end}}` tokens in the SQL:

```sql
/*---
name: mta_ridership_yearly
source: querystation
description: Per-year MTA ridership aggregates.
group: querystation__transportation
partitions:
  type: yearly
  start: "2020"
  end_offset: 1
---*/

SELECT
    extract(year FROM date)::INT AS year,
    mode,
    sum(count) AS total_riders
FROM lake.nys_transportation.mta_daily_ridership
WHERE date >= {{partition_start}}
  AND date <  {{partition_end}}
GROUP BY 1, 2
```

Each partition fires one templated query and writes its own Hive-layout shard (`year=2023/…parquet`). Backfill any partition individually; DuckDB partition pruning works over the result.

**Supported templating tokens:**

| Token | Renders as | Use for |
|---|---|---|
| `{{partition_start}}` | `'YYYY-MM-DD'` | DATE columns (calendar day, partition-local) |
| `{{partition_end}}` | `'YYYY-MM-DD'` | DATE columns |
| `{{partition_start_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns |
| `{{partition_end_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns |
| `{{partition_key}}` | `'key'` (alphanumerics + `_-:. ` only) | Static/string partitions |

Missing tokens raise at render time; injection attempts in `partition_key` are rejected. Full details in `packages/opendata_framework/opendata_framework/core/sql/runner_querystation.py`.

**Cross-backend pipelines.** A local SQL asset can join Parquet produced by any backend:

```sql
/*---
name: transit_vs_air_quality
description: Join QueryStation-sourced MTA ridership with air-quality aggregates.
deps:
  - mta_ridership_yearly
  - nyc_air_quality_annual
group: querystation__analytics
---*/

SELECT r.year, sum(r.total_riders) AS total, max(a.avg_value) AS pm25
FROM mta_ridership_yearly r
LEFT JOIN nyc_air_quality_annual a ON a.year = r.year
GROUP BY r.year
```

The DuckDB JIT runner mounts each upstream as a view, runs the join, writes the result. Dagster shows the full lineage regardless of backend.

**Asset group conventions:**

| Prefix | Meaning |
|---|---|
| `nyc__{domain}` | Socrata-ingested or local-JIT analytics (e.g., `nyc__sanitation`) |
| `querystation__{domain}` | QueryStation-sourced remote assets (e.g., `querystation__transportation`) |

## Remote Queries via QueryStation

[QueryStation](https://querystation.app) is a hosted service providing curated, analytics-ready NYC Open Data through a remote DuckDB endpoint. It serves 17+ datasets (311, capital budget, payroll, restaurant inspections, FloodNet, MTA, and more) over an Arrow IPC interface for fast, zero-copy result transfer.

Two ways to use it:

1. **As Dagster assets** (preferred for repeatable work) — see [Adding a QueryStation Remote Asset](#adding-a-querystation-remote-asset) above. Results cached as versioned Parquet, participating in the asset graph with lineage, row-count checks, and scheduled refresh.
2. **Ad-hoc via the CLI** (for exploration) — skips materialization, just prints or exports.

### CLI (ad-hoc exploration)

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

### When to promote an ad-hoc query to a Dagster asset

| You're running the query… | …do this |
|---|---|
| Once, just exploring | CLI / notebook |
| Regularly, or committing results | Make it a `.sql` asset with `source: querystation` |
| Once but expensive, results will be reused | Same — materialize once, re-read Parquet freely |
| As input to other SQL analytics | Definitely asset — downstream assets auto-wire via SQL parsing |

## Workspace Packages

This is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) with two internal packages:

| Package | Purpose |
|---|---|
| **opendata-framework** | Dagster factories, Socrata + QueryStation resources, IO managers (Parquet/CSV/JSON), schema contracts, enrichments, SQL asset discovery (local JIT + remote QueryStation), partition templating with timezone-safe rendering, retry policies, frontmatter-time footgun validation |
| **data-consumers** | `RemoteDuckDBWrapper` (QueryStation Arrow IPC), `DuckDBWrapper` (local), `PolarsWrapper`. No Dagster dependency — usable from notebooks, CLIs, and external scripts. |

The framework depends on `data-consumers` (one direction only), which is how QueryStation assets get executed without coupling `data-consumers` to Dagster.

## Key Commands

| Command | Description |
|---|---|
| `make install` | One-shot setup: venv + deps + dirs + `.env` |
| `make dev` | Launch Dagster UI (`DAGSTER_HOME=./logs`) |
| `make sync` | Reinstall dependencies |
| `make notebook` | Install Jupyter extras |

### Materializing Assets

```bash
# Socrata: landing then clean (sequential)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select nyc_dsny_monthly_tonnage_landing

DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select nyc_dsny_monthly_tonnage

# Local SQL analytics (reads from upstream Parquet)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select dsny_tonnage_annual_summary

# QueryStation remote (one shot)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select mta_ridership_by_mode

# QueryStation partitioned (one partition at a time)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select mta_ridership_yearly --partition 2024
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
| Clean (yearly partitioned) | `data/clean/{asset}/year={YYYY}/{asset}_{YYYY}.parquet` |
| Exports | `data/exports/` |

## License

[MIT](LICENSE)
