---
name: querystation
description: >
  Remote DuckDB query tool for querying remote catalogs via the
  QueryStation Arrow IPC API. Use when an agent needs to query remote data,
  browse remote catalogs, describe remote tables, or export remote query results.
  Triggers on: "query remote", "remote duckdb", "querystation", "lake.", "iceberg.",
  "what tables are available remotely", "query the lake", "check remote data",
  "run sql against the server", "export from remote".
---

# QueryStation Remote DuckDB Tool

## What is this?

QueryStation gives you SQL access to NYC open data hosted behind a remote DuckDB server. You send SQL over HTTP, the server executes it, and you get results back as Polars DataFrames via Apache Arrow IPC.

The data includes 311 service requests, capital budgets, city payroll, restaurant inspections, flood sensors, MTA ridership, and more — all pre-cleaned and enriched by the pipeline.

All logic lives in `data_consumers.RemoteDuckDBWrapper` at `data_consumers/remote_duckdb_wrapper.py`. The CLI script and notebook are thin presentation layers over this class.

## Three ways to use QueryStation in this repo

This skill primarily documents **ad-hoc exploration** via the CLI, notebook, or `RemoteDuckDBWrapper`. For **repeatable** queries that should live in the Dagster asset graph — e.g. a query you'll re-run on a schedule, commit a result of, or use as input to downstream SQL — make it a Dagster asset instead.

| You're running the query… | …do this |
|---|---|
| Once, just exploring | CLI / notebook / wrapper — this skill |
| Regularly, or committing results | `.sql` file with `source: querystation` in `src/opendata_eda/defs/assets/sql_assets/` — see **asset-builder** skill |
| Once but expensive, results will be reused | Same — materialize once, re-read parquet freely |
| As input to other SQL analytics | Definitely an asset — downstream local SQL can reference it by bare name |

The asset path gives you: parquet caching under `data/clean/`, partition templating (`{{partition_start}}`), retry policy (3× exponential backoff starting at 30s), auto row-count checks, and Dagster lineage. Same wire protocol underneath — it's the same `RemoteDuckDBWrapper` the wrapper section below describes, wrapped in a Dagster `ConfigurableResource`.

## Setup (one time)

### 1. Get an API key

You need a `QUERYSTATION_API_KEY` (starts with `sk_`). This key gets exchanged automatically for a short-lived JWT token — you never deal with the JWT directly.

### 2. Configure `.env`

Add these two lines to `.env` at the repo root:

```env
QUERYSTATION_API_KEY=sk_your_key_here
AUTH_URL=https://api.querystation.app
```

- `QUERYSTATION_API_KEY` — **required**. Your opaque API key.
- `AUTH_URL` — **required for production**. The auth endpoint that exchanges your key for a JWT. The code defaults to `https://auth-dev.querystation.app` but production is `https://api.querystation.app`.

`python-dotenv` reads this file automatically when the script, notebook, or wrapper starts. If these vars are missing you'll get either "No API key" or a DNS resolution error.

### 3. Install dependencies

```bash
# Install all workspace dependencies
uv sync

# For notebooks, install the notebook extra
uv sync --extra notebook
```

## How to use it

### Option A: Terminal (CLI script)

Run from workspace root:

```bash
# Browse all available tables
uv run python scripts/query_remote.py --catalog

# Describe a table's columns
uv run python scripts/query_remote.py --describe lake.nyc_operations.service_requests_311

# Run a SQL query
uv run python scripts/query_remote.py --sql "SELECT borough, COUNT(*) AS cnt FROM lake.nyc_operations.service_requests_311 WHERE year = 2025 GROUP BY 1 ORDER BY 2 DESC"

# Positional SQL (no --sql flag needed)
uv run python scripts/query_remote.py "SELECT 42 AS answer"

# Export results to data/exports/
uv run python scripts/query_remote.py --sql "SELECT * FROM lake.nyc_finance.city_payroll LIMIT 100" --export payroll csv

# Run a folder of .sql files, export each as parquet
uv run python scripts/query_remote.py --run-sql-folder ./my_queries --export-format parquet
```

**CLI flags:**

| Flag | Default | Description |
|---|---|---|
| `--catalog` | | Show all catalogs, schemas, and tables |
| `--describe TABLE` | | Show column names and types for a table |
| `--sql "SQL"` | | Execute SQL query |
| `--limit N` | 50 | Max rows to print |
| `--export NAME FORMAT` | | Export results (csv, parquet, json) to `data/exports/` |
| `--run-sql-folder DIR` | | Run all .sql files in a directory |
| `--export-format` | parquet | Format for `--run-sql-folder` exports |

### Option B: Jupyter Notebook

Open `notebooks/query_remote.ipynb`, run the first cell, then use these functions in any cell:

```python
catalog()                                              # browse tables
describe("lake.nyc_operations.service_requests_311")   # column names + types
df = sql("SELECT * FROM lake.nyc_finance.city_payroll LIMIT 10")  # query
show(df)                                               # scrollable HTML table
export(df, "my_data", "csv")                           # save to data/exports/
```

| Function | Returns | Description |
|---|---|---|
| `sql("SELECT ...")` | `pl.DataFrame` | Execute SQL, get a DataFrame |
| `show(df)` | | Scrollable HTML table with sticky headers |
| `catalog()` | | Browse all remote tables |
| `describe("catalog.schema.table")` | | Column names and types |
| `export(df, "name", "csv")` | | Save to `data/exports/` (csv, parquet, json) |

### Option C: Python (any script or REPL)

```python
from dotenv import load_dotenv
from data_consumers import RemoteDuckDBWrapper

load_dotenv()

db = RemoteDuckDBWrapper()
df = db.sql("SELECT COUNT(*) FROM lake.nyc_operations.service_requests_311")
print(df)

db.show_tables()
db.describe("lake.nyc_finance.capital_budget")
db.export(df, "my_data", "parquet")
db.close()
```

## Table Naming

All remote tables use fully-qualified three-part names: `catalog.schema.table_name`

| Catalog | Description |
|---|---|
| `lake` | Primary curated data catalog |
| `iceberg` | Apache Iceberg tables |
| `main_db` | User scratch space (temporary tables from previous sessions) |

Examples:

```sql
SELECT * FROM lake.nyc_operations.service_requests_311 LIMIT 5
SELECT * FROM lake.nyc_finance.capital_budget LIMIT 5
SELECT * FROM iceberg.default.people
```

## Available Datasets

### lake catalog

| Schema | Table | Columns | Domain |
|---|---|---|---|
| `nyc_operations` | `service_requests_311` | 58 | 311 complaints (2024-present) |
| `nyc_finance` | `capital_budget` | 45 | City council capital budget with D10 classifier |
| `nyc_finance` | `city_payroll` | 27 | NYC employee payroll |
| `nyc_finance` | `expense_budget` | 17 | Expense budget |
| `nyc_checkbook` | `checkbook_spending` | 40 | NYC spending records |
| `nyc_checkbook` | `checkbook_budget` | 21 | Budget data |
| `nyc_checkbook` | `checkbook_contracts_reg_expense` | 54 | Contracts |
| `nyc_checkbook` | `checkbook_revenue` | 25 | Revenue |
| `nyc_checkbook` | `checkbook_revenue_nycha` | 23 | NYCHA revenue |
| `nyc_health` | `restaurant_inspections` | 40 | Restaurant inspection grades |
| `nyc_housing` | `housing_connect` | 45 | Affordable housing lotteries |
| `nyc_environment` | `floodnet_sensor_metadata` | 16 | Flood sensor locations |
| `nyc_environment` | `floodnet_flooding_events` | 13 | Flood events |
| `nyc_environment` | `floodnet_events_joined` | 43 | Enriched flood events |
| `nys_transportation` | `mta_daily_ridership` | 3 | MTA daily ridership |
| `nys_transportation` | `mta_operations_statement` | 10 | MTA financials |
| `nys_transportation` | `mta_subway_hourly_ridership` | 24 | Subway hourly counts |

### iceberg catalog

| Schema | Table | Columns | Notes |
|---|---|---|---|
| `default` | `people` | 3 | Test table (Alice, Bob, Charlie) |

## Common Query Patterns

### Row counts

```sql
SELECT COUNT(*) FROM lake.nyc_operations.service_requests_311
SELECT COUNT(*) FROM lake.nyc_finance.capital_budget
```

### Aggregations

```sql
-- Complaints by borough
SELECT borough, COUNT(*) AS cnt
FROM lake.nyc_operations.service_requests_311
WHERE year = 2025
GROUP BY 1 ORDER BY 2 DESC

-- Top agencies by payroll
SELECT agency_name, COUNT(*) AS employees, ROUND(SUM(regular_gross_paid), 2) AS total_pay
FROM lake.nyc_finance.city_payroll
GROUP BY 1 ORDER BY 3 DESC LIMIT 10
```

### Distinct values

```sql
SELECT COUNT(DISTINCT borough) AS boroughs,
       COUNT(DISTINCT agency_name) AS agencies,
       COUNT(DISTINCT complaint_type) AS complaint_types
FROM lake.nyc_operations.service_requests_311
```

### Cross-catalog queries

```sql
SELECT s.borough, s.sensors, c.complaints
FROM (SELECT borough, COUNT(*) AS sensors FROM lake.nyc_environment.floodnet_sensor_metadata GROUP BY 1) s
JOIN (SELECT borough, COUNT(*) AS complaints FROM lake.nyc_operations.service_requests_311 WHERE year = 2025 GROUP BY 1) c
ON s.borough = c.borough
ORDER BY c.complaints DESC
```

## Partition Pruning — write filters the planner can use

The `lake.*` tables are Hive-partitioned on `year`. Partition pruning (skipping entire directories before reading any Parquet) only fires when the WHERE clause **references the partition column directly**. The planner does not infer `year = 2025` from a date range on a different column.

Empirical evidence (load tests):

| Query | Latency |
|---|---|
| `WHERE year = 2025 AND month = 6` (311 count) | 0.15s |
| `WHERE year = 2024 AND month = 1` (MTA hourly count) | 0.17s |
| no WHERE — full-table aggregate (MTA hourly) | 29.85s cold (would time out at the wrapper's 30s ceiling) |

That ~200× difference is partition pruning. Always include `WHERE year = …` (or `IN (…)` / `BETWEEN`) when scanning year-partitioned tables.

```sql
-- GOOD: prunes to one partition directory
SELECT * FROM lake.nyc_operations.service_requests_311 WHERE year = 2025

-- WORSE: scans every year's files, relies on row-group min/max stats
SELECT * FROM lake.nyc_operations.service_requests_311
WHERE created_date >= '2025-01-01' AND created_date < '2026-01-01'

-- BEST: partition prune + row-group prune (combine both)
SELECT * FROM lake.nyc_operations.service_requests_311
WHERE year = 2025
  AND created_date >= '2025-06-01' AND created_date < '2025-07-01'
```

**Practical rules:**
1. Touching one year? `WHERE year = 2025`.
2. Touching a year range? `WHERE year BETWEEN 2024 AND 2025` (or `IN (…)`).
3. Touching a sub-year window? Combine: `WHERE year = 2025 AND <date_col> BETWEEN '…' AND '…'`.
4. Aggregating across all years? At minimum, materialize once with the full scan and reuse — don't re-pay the cost on every query.

`RemoteDuckDBWrapper.sql()` hardcodes a 30s HTTP timeout. An unfiltered aggregate on a large table (MTA hourly especially) will hit that ceiling cold. If you genuinely need a full scan, use the load-test script's pattern: send the request via `httpx` directly with a longer timeout, or push the query into a Dagster QueryStation asset (which has retry + longer timeout built in).

## Catalog Discovery

The `/catalog` JSON endpoint is used instead of `information_schema` or `DESCRIBE` because the remote service exposes richer catalog metadata there. The wrapper handles this automatically:

- `db.catalog()` — DataFrame of all tables with column counts
- `db.describe("table")` — DataFrame of column names and types
- `db.describe("table", with_comments=True)` — same, plus a `comment` column joined in from `duckdb_columns()`
- `db.fetch_catalog()` — raw JSON dict for advanced use

### Comments (table & column descriptions)

DuckLake-backed tables can carry per-table and per-column comments (set during materialization from the schema-contract 3-tuples and pipeline `description=` kwargs). The wrapper exposes them via DuckDB's introspection functions (`duckdb_tables()` / `duckdb_columns()`):

```python
db.table_comments()                   # every commented table in catalog 'lake'
db.table_comments("nyc_checkbook")    # one schema's tables only
db.column_comments("lake.nyc_checkbook.checkbook_contracts_reg_expense")
db.describe("lake.nyc_finance.capital_budget", with_comments=True)
```

CLI equivalents:

```bash
uv run python scripts/query_remote.py --table-comments              # all schemas
uv run python scripts/query_remote.py --table-comments nyc_checkbook
uv run python scripts/query_remote.py --column-comments lake.nyc_checkbook.checkbook_contracts_reg_expense
uv run python scripts/query_remote.py --describe lake.nyc_finance.capital_budget --with-comments
```

Implementation notes:
- The metadata physically lives in the DuckLake catalog's `__ducklake_metadata_<catalog>.public.ducklake_tag` / `ducklake_column_tag` tables (snapshot-versioned, `key='comment'`, filter `end_snapshot IS NULL`). **Querying that schema directly through the QueryStation SQL gateway returns 500** — the gateway exposes the metadata DB in `SHOW DATABASES` but blocks reads from it. The wrapper deliberately uses `duckdb_columns()` / `duckdb_tables()` instead because those go through DuckDB's stable introspection layer, which is permitted.
- `column_comments(table, include_empty=True)` (the default) returns one row per declared column — columns without a comment have NULL, so you can immediately spot which raw-API columns are missing descriptions.
- Comments only exist for columns whose schema contract was declared as a 3-tuple `(target, dtype, description)`. 2-tuple `(target, dtype)` declarations produce no comment. Same for table-level comments — the `description=` kwarg on `create_socrata_pipeline()` / `create_checkbook_pipeline()` is what populates `duckdb_tables().comment`.

## Arrow IPC Notes

- The server returns Arrow IPC streams with LZ4 compression
- Polars' native `read_ipc_stream` has a known incompatibility with this server's compression variant
- The wrapper uses `pyarrow.ipc.open_stream()` then `pl.from_arrow()` as the reliable path
- This is why `pyarrow` is a required dependency in the `[remote]` extra
- Remote timestamps arrive as `Datetime("us", "UTC")` — they are already properly typed, not strings

## Post-Fetch Transforms

Remote query results are standard Polars DataFrames. You can chain `opendata_framework` enrichments on them:

```python
from opendata_framework.core import apply_schema_contract

contract = {
    "borough": ("borough", pl.Utf8),
    "created_date": ("created_date", pl.Datetime("us", "UTC")),
}
clean = apply_schema_contract(df.lazy(), contract, drop_unknown=True).collect()
```

Use `pl.Datetime("us", "UTC")` for timestamp columns — bare `pl.Datetime` will try to re-parse and produce `NaT`.

## Architecture

```
.env (QUERYSTATION_API_KEY, AUTH_URL)
  │
  ▼
QueryStationAuth (_auth.py)
  │  API key → JWT exchange
  │  Token caching + auto-refresh
  ▼
RemoteDuckDBWrapper (remote_duckdb_wrapper.py)
  │  .sql()            → POST Arrow IPC → pl.DataFrame
  │  .catalog()        → GET /catalog   → pl.DataFrame
  │  .describe()       → GET /catalog   → pl.DataFrame
  │  .export()         → write parquet/csv/json
  │  .run_sql_folder() → batch .sql files
  ▼
┌─────────────────┬──────────────────────┐
│ CLI              │ Notebook              │
│ query_remote.py  │ query_remote.ipynb    │
│ rich tables      │ scrollable HTML       │
└─────────────────┴──────────────────────┘
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `No API key: pass api_key= or set QUERYSTATION_API_KEY` | `.env` missing or key not set | Add `QUERYSTATION_API_KEY=sk_...` to `.env` |
| `Name or service not known` | DNS can't resolve `AUTH_URL` | Check `AUTH_URL` in `.env` — production is `https://api.querystation.app` |
| `No remoteUrl returned` | API key doesn't have active billing | Contact QueryStation for access |
| `Malformed IPC file` | Using `pl.read_ipc_stream` instead of pyarrow | Already fixed in wrapper — make sure you're using latest `RemoteDuckDBWrapper` |
| `ModuleNotFoundError: data_consumers` | Package not installed | Run `uv sync` from the workspace root |
| `HTTP 403` (~80ms fast-fail) | SQL begins with a comment (`/* … */ SELECT …` or `-- …\nSELECT …`). Endpoint has a SQLi defense at the proxy layer. | Move the comment to the end (`SELECT … -- comment`) or inline it after the SELECT. |
| `Read timed out` (~30s) | `RemoteDuckDBWrapper.sql()` uses a hardcoded 30s timeout, and your query is doing a full-table scan or unfiltered aggregate on a large table | Add `WHERE year = …` to trigger partition pruning (see "Partition Pruning" above). For genuinely-large pulls, send via `httpx` directly with a larger timeout, or use a Dagster QueryStation asset (retry + longer timeout built in). |

## Related Skills

- **asset-builder** — Build a `.sql` file with `source: querystation` to materialize the query as a cached Dagster asset under `data/clean/`
- **duckdb-analyst** — Query local parquet (including QueryStation assets cached locally) without hitting the remote server
- **socrata-builder** — Ingest NYC Open Data from Socrata when a dataset isn't already in the QueryStation lake
