---
name: querystation
description: >
  Remote DuckDB query tool for querying DuckLake and Iceberg catalogs via the
  QueryStation Arrow IPC API. Use when an agent needs to query remote data,
  browse remote catalogs, describe remote tables, or export remote query results.
  Triggers on: "query remote", "remote duckdb", "querystation", "lake.", "iceberg.",
  "what tables are available remotely", "query the lake", "check remote data",
  "run sql against the server", "export from remote".
---

# QueryStation Remote DuckDB Tool

## What is this?

QueryStation gives you SQL access to NYC open data hosted in DuckLake and Apache Iceberg catalogs through a remote DuckDB server. You send SQL over HTTP, the server executes it, and you get results back as Polars DataFrames via Apache Arrow IPC.

The data includes 311 service requests, capital budgets, city payroll, restaurant inspections, flood sensors, MTA ridership, and more — all pre-cleaned and enriched by the pipeline.

All logic lives in `data_consumers.RemoteDuckDBWrapper` at `data_consumers/remote_duckdb_wrapper.py`. The CLI script and notebook are thin presentation layers over this class.

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
# Install all dependencies
uv pip install -e .

# For notebooks, also install the Jupyter kernel
uv pip install -e ".[notebook]"
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
| `lake` | DuckLake tables (the primary data catalog) |
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

## Catalog Discovery

The `/catalog` JSON endpoint is used instead of `information_schema` or `DESCRIBE` because DuckLake catalogs don't expose those reliably. The wrapper handles this automatically:

- `db.catalog()` — DataFrame of all tables with column counts
- `db.describe("table")` — DataFrame of column names and types
- `db.fetch_catalog()` — raw JSON dict for advanced use

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
| `ModuleNotFoundError: data_consumers` | Package not installed | Run `uv pip install -e .` |

## Related Skills

- **duckdb-analyst** — Local DuckDB queries against parquet files on disk
- **project-guide** — Index of all available skills and nah permissions
