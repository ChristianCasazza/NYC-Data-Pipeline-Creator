---
name: duckdb-analyst
description: >
  Read-only DuckDB query tool for data analysis. Use when an agent needs to
  inspect, profile, or validate data in parquet files, CSVs, or any DuckDB-
  readable format. Triggers on: "check the data", "run a query", "data quality",
  "how many rows", "what does the schema look like", "profile this dataset",
  "null rates", "check for duplicates", or when any analysis of materialized
  pipeline output is needed.
---

# DuckDB Analyst Tool

Read-only SQL query tool for agents. Runs DuckDB in-memory against parquet, CSV, and other files. Supports single ad-hoc queries and bulk batch queries for data quality checks.

## Data Locations

Materialized Dagster assets live under `data/` in two directories:

| Stage | Path Pattern | Format |
|---|---|---|
| Landing | `data/landing/{asset_name}/...` | Gzipped CSV shards |
| Clean (single file) | `data/clean/{asset_name}/{asset_name}.parquet` | Parquet |
| Clean (yearly partitioned) | `data/clean/{asset_name}/year={YYYY}/{asset_name}_{YYYY}.parquet` | Hive-partitioned Parquet |
| Exports | `data/exports/{name}.{format}` | CSV, Parquet, or JSON |

## Quick Start

```bash
# Row count
uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM 'data/clean/nyc_floodnet_sensor_metadata/nyc_floodnet_sensor_metadata.parquet'\").pl())"

# Schema
uv run python -c "import duckdb; print(duckdb.sql(\"DESCRIBE SELECT * FROM 'data/clean/nyc_floodnet_sensor_metadata/nyc_floodnet_sensor_metadata.parquet'\").pl())"
```

## Querying Files

DuckDB reads files directly in SQL — no setup needed:

| File Type | SQL Pattern |
|---|---|
| **Parquet (single file)** | `SELECT * FROM 'data/clean/foo/foo.parquet'` |
| **Parquet (partitioned, hive)** | `SELECT * FROM 'data/clean/foo/*/*.parquet'` |
| **Parquet (all partitions, recursive)** | `SELECT * FROM 'data/clean/foo/**/*.parquet'` |
| **CSV** | `SELECT * FROM 'data/exports/my_data.csv'` |

### Partitioned datasets

Some assets produce hive-partitioned parquet (e.g., `year=2026/nyc_311_sample_2026.parquet`). Both Socrata staged pipelines (`nyc_311_sample`) and yearly-partitioned QueryStation assets (`mta_ridership_yearly`) use this layout. DuckDB auto-detects hive partitions with glob patterns:

```sql
-- All years (Socrata-backed, staged)
SELECT year, count(*) as n
FROM 'data/clean/nyc_311_sample/*/*.parquet'
GROUP BY year ORDER BY year

-- Single year
SELECT * FROM 'data/clean/nyc_311_sample/year=2026/*.parquet' LIMIT 5

-- Yearly-partitioned QueryStation asset (same layout, different source)
SELECT year, mode, total_riders
FROM 'data/clean/mta_ridership_yearly/*/*.parquet'
ORDER BY year, mode
```

For recursive scans (mixed depths), use `**`:

```sql
SELECT * FROM 'data/clean/mta_ridership_yearly/**/*.parquet' LIMIT 5
```

## Available Assets

All materialized assets — regardless of backend (Socrata, QueryStation remote, or local DuckDB-JIT) — land under `data/clean/{asset_name}/...`. The examples below treat them uniformly.

### Socrata pipeline assets

| Asset | Type | Description |
|---|---|---|
| `nyc_311_sample` | Partitioned (monthly → yearly) | 311 service requests |
| `nyc_film_permits` | Unpartitioned | NYC film/TV shooting permits |
| `nyc_floodnet_sensor_metadata` | Unpartitioned | FloodNet sensor locations |
| `nyc_floodnet_flooding_events` | Unpartitioned | Flood event measurements |
| `nyc_floodnet_events_joined` | Unpartitioned | Events enriched with sensor metadata, severity, hydro metrics |
| `nyc_dsny_monthly_tonnage` | Unpartitioned | DSNY monthly collection tonnage by community district (1990–present) |
| `nyc_motor_vehicle_collisions` | Unpartitioned | NYPD-reported motor vehicle crashes (2012–present) |

### QueryStation remote assets (cached locally as Parquet)

These are authored as `.sql` files with `source: querystation` — QueryStation executes them remotely, but results land in the same `data/clean/` tree and query identically.

| Asset | Layout | Description |
|---|---|---|
| `mta_ridership_by_mode` | Single parquet | All-years per-mode MTA totals |
| `mta_ridership_yearly` | Hive (`year=YYYY/`) | Per-year per-mode ridership aggregates |
| `nyc_air_quality_annual` | Single parquet | Annual NO2 / PM2.5 / Ozone means across NYC neighborhoods |
| `nyc_311_top_heat_bbls_by_cb` | Single parquet | Top 10 BBLs per community board by 311 heat complaints |

### Local SQL analytics assets (downstream)

| Asset | Upstream | Description |
|---|---|---|
| `dsny_tonnage_annual_summary` | `nyc_dsny_monthly_tonnage` | Citywide annual totals + diversion rates |
| `dsny_tonnage_borough_monthly` | `nyc_dsny_monthly_tonnage` | Borough-level monthly aggregates |
| `dsny_tonnage_district_rankings` | `nyc_dsny_monthly_tonnage` | District rankings by refuse + recycling rate |
| `dsny_tonnage_organics_rollout` | `nyc_dsny_monthly_tonnage` | Organics adoption tracking by borough/year |
| `collisions_annual_summary` | `nyc_motor_vehicle_collisions` | Year-over-year collision counts |
| `collisions_borough_monthly` | `nyc_motor_vehicle_collisions` | Borough × month breakdowns |
| `collisions_contributing_factors` | `nyc_motor_vehicle_collisions` | Top contributing factors |
| `transit_vs_air_quality` | `mta_ridership_yearly`, `nyc_air_quality_annual` | Cross-backend join — MTA ridership vs. air-quality aggregates |

## Common Query Patterns

### Data profiling

```sql
-- Row count
SELECT count(*) as n FROM 'data/clean/{asset}/{asset}.parquet'

-- Schema
DESCRIBE SELECT * FROM 'data/clean/{asset}/{asset}.parquet'

-- Sample
SELECT * FROM 'data/clean/{asset}/{asset}.parquet' LIMIT 5

-- Null rates
SELECT count(*) as total,
       count(*) - count(col_a) as null_a,
       count(*) - count(col_b) as null_b
FROM 'data/clean/{asset}/{asset}.parquet'

-- Summary statistics
SELECT * FROM (SUMMARIZE SELECT * FROM 'data/clean/{asset}/{asset}.parquet')
```

### Data quality checks

```sql
-- Duplicates
SELECT id_col, count(*) as n
FROM 'data/clean/{asset}/{asset}.parquet'
GROUP BY id_col HAVING count(*) > 1 LIMIT 10

-- Enum values
SELECT borough, count(*) as n
FROM 'data/clean/{asset}/{asset}.parquet'
GROUP BY borough ORDER BY n DESC

-- Completeness
SELECT round(100.0 * count(col) / count(*), 1) as pct_filled
FROM 'data/clean/{asset}/{asset}.parquet'
```

### Cross-dataset joins

```sql
-- FloodNet: sensors with most events
SELECT s.sensor_name, s.borough, COUNT(e.sensor_id) AS events
FROM 'data/clean/nyc_floodnet_sensor_metadata/nyc_floodnet_sensor_metadata.parquet' s
LEFT JOIN 'data/clean/nyc_floodnet_flooding_events/nyc_floodnet_flooding_events.parquet' e
  ON s.sensor_id = e.sensor_id
GROUP BY 1, 2
ORDER BY 3 DESC
```

### DuckDB-specific functions

```sql
-- Approximate distinct count (fast for large datasets)
SELECT approx_count_distinct(borough) FROM '...'

-- Histogram
SELECT histogram(borough) FROM '...'

-- Summary statistics
SELECT * FROM (SUMMARIZE SELECT * FROM '...')

-- String similarity
SELECT jaro_winkler_similarity('MANHATTAN', borough) FROM '...'
```

## Notebooks

Two notebooks are available for interactive exploration:

- **`notebooks/query_local.ipynb`** — Query local parquet files with Polars and DuckDB
- **`notebooks/query_remote.ipynb`** — Query remote QueryStation tables via Arrow IPC API

## Best Practices

### 1. Always start with schema + row count
Before writing analysis queries, know what you're working with.

### 2. Reference files by relative path
Use paths relative to the project root:
```sql
-- Good
SELECT * FROM 'data/clean/foo/foo.parquet'

-- Bad (unnecessary absolute path)
SELECT * FROM '/home/user/OpenDataWeek-API/data/clean/foo/foo.parquet'
```

### 3. Cap your result sizes
For large datasets, always use `LIMIT`, `WHERE`, or aggregations.

### 4. Use `TRY_CAST` not `CAST` for string-to-number conversions
When parsing string fields (e.g., extracting year from `"2026 / 02"`), always use `TRY_CAST(trim(...) AS INT)` instead of `CAST`. `CAST` throws a hard `ConversionException` on any empty or malformed string. `TRY_CAST` returns `NULL` gracefully.

```sql
-- Good
SELECT TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year FROM ...

-- Bad — will crash on edge cases
SELECT CAST(split_part(month, ' / ', 1) AS INT) AS year FROM ...
```

### 5. Use the local notebook for interactive work
The `notebooks/query_local.ipynb` has convenience functions (`scan()`, `sql()`, `register()`, `show()`) pre-built for local data exploration.
