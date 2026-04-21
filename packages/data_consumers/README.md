# data-consumers

Dagster-free query client library for Parquet/DuckDB data warehouses, local and remote. Designed to be used from notebooks, CLIs, ad-hoc scripts, and downstream apps — no orchestration dependency.

Inside this workspace, `opendata-framework` wraps `RemoteDuckDBWrapper` in a Dagster `ConfigurableResource` so QueryStation assets can use the same client without pulling Dagster into this package.

## Install

From the workspace root:

```bash
uv sync
```

Or from another uv-managed project:

```bash
uv add --editable path/to/packages/data_consumers
```

## Public import surface

```python
from data_consumers import (
    DuckDBWrapper,        # local DuckDB (attached .duckdb file + in-memory)
    PolarsWrapper,        # polars.SQLContext with lazy scans
    RemoteDuckDBWrapper,  # QueryStation Arrow IPC client
)
```

`DuckDBWrapper` and `RemoteDuckDBWrapper` expose `.sql(query) -> pl.DataFrame`, `.show_tables()`, and `.close()`. `PolarsWrapper` uses `.run_query(sql)` plus `.lazy(name)` for direct Polars access.

## `RemoteDuckDBWrapper` — QueryStation Arrow IPC

Query a remote DuckDB server that serves 17+ NYC datasets via [QueryStation](https://querystation.app). Results come back as Polars DataFrames over Apache Arrow IPC.

```python
from dotenv import load_dotenv
from data_consumers import RemoteDuckDBWrapper

load_dotenv()  # reads QUERYSTATION_API_KEY + AUTH_URL from .env

db = RemoteDuckDBWrapper()
df = db.sql("SELECT COUNT(*) FROM lake.nyc_operations.service_requests_311")

db.show_tables()                                         # rich-printed catalog
db.describe("lake.nyc_finance.capital_budget")           # column names + types
db.export(df, "my_data", "parquet")                      # → data/exports/
db.run_sql_folder("./my_queries", export_fmt="csv")      # batch run .sql files

db.close()
```

### Env vars

| Variable | Required | Default | Notes |
|---|---|---|---|
| `QUERYSTATION_API_KEY` | yes | — | Opaque `sk_...` key; exchanged for a short-lived JWT |
| `AUTH_URL` | production | `https://auth-dev.querystation.app` | Set to `https://api.querystation.app` for prod |

The wrapper caches JWTs and auto-refreshes on 401s. You never deal with tokens directly.

### Why pyarrow?

Polars' native `read_ipc_stream` has a known incompatibility with the server's LZ4 variant. The wrapper uses `pyarrow.ipc.open_stream()` + `pl.from_arrow()` as the reliable path. That's why `pyarrow` is a hard dependency.

Remote timestamps arrive typed as `Datetime("us", "UTC")` — don't re-parse them (bare `pl.Datetime` will produce NaT).

## `DuckDBWrapper` — local DuckDB

Attach a `.duckdb` warehouse file read-only and run SQL against it, with in-memory scratch space and rich-printed table output.

```python
from data_consumers import DuckDBWrapper

db = DuckDBWrapper(duckdb_path="data/warehouse.duckdb")  # or WAREHOUSE_DB_PATH env var
db.register_data_view(
    paths=["data/clean/nyc_dsny_monthly_tonnage/*.parquet"],
    table_names=["dsny"],
)
df = db.sql("SELECT borough, sum(refuse_tons_collected) FROM dsny GROUP BY 1")
db.close()
```

## `PolarsWrapper` — polars.SQLContext

Lazy-scan single-file or Hive-partitioned datasets and run SQL without a DuckDB engine.

```python
from data_consumers import PolarsWrapper

pw = PolarsWrapper()
pw.register_data_view(
    paths=["data/clean/nyc_dsny_monthly_tonnage/*.parquet"],
    table_names=["dsny"],
)
df = pw.run_query("SELECT * FROM dsny LIMIT 5")
lazy = pw.lazy("dsny")         # raw LazyFrame for chained polars ops
```

## Package layout

```
data_consumers/
├── __init__.py              # re-exports the three wrappers
├── _auth.py                 # QueryStationAuth — API-key → JWT exchange + caching
├── duckdb_wrapper.py        # DuckDBWrapper
├── polars_wrapper.py        # PolarsWrapper (polars.SQLContext)
└── remote_duckdb_wrapper.py # RemoteDuckDBWrapper (Arrow IPC over HTTP)
```

## Dependencies

Minimal on purpose — no Dagster, no orchestration libs:

```
duckdb        # local engine + remote IPC decoder
polars        # DataFrames + SQLContext
pyarrow       # Arrow IPC stream decode (required, not optional)
httpx         # Remote HTTP transport
rich          # Pretty-printed tables in terminal
python-dotenv # .env loader for API keys
```

## Usage outside this workspace

The wrappers are self-contained and only need their pinned dependencies. Use `uv add --editable path/to/packages/data_consumers` from another project, or copy the package if you are intentionally vendoring it. `RemoteDuckDBWrapper` will work against any QueryStation deployment as long as `QUERYSTATION_API_KEY` and `AUTH_URL` are set.

## Related

- **[../opendata_framework](../opendata_framework/README.md)** — Dagster layer that uses `RemoteDuckDBWrapper` via `QueryStationResource`
- **[../../scripts/query_remote.py](../../scripts/query_remote.py)** — Thin CLI over `RemoteDuckDBWrapper`
- **[../../notebooks/query_remote.ipynb](../../notebooks/query_remote.ipynb)** — Notebook front-end

## License

MIT (inherits from the workspace root).
