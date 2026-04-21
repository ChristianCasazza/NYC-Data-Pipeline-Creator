# opendata-framework

Reusable Dagster factories, IO managers, resources, enrichments, and SQL asset discovery for NYC open data pipelines. Lives in the `NYC-Data-Pipeline-Creator` uv workspace; depends on the `data-consumers` workspace package.

Designed so a single call (`create_socrata_pipeline`, `create_querystation_sql_asset`, or a `.sql` file in `sql_assets/`) produces a fully-wired Dagster asset with landing/clean/analytics layers, schema contracts, retry policy, and row-count checks.

## Install

From the workspace root:

```bash
uv sync                      # installs all workspace packages
```

Or from another uv-managed project:

```bash
uv add --editable path/to/packages/opendata_framework
```

## Public import surface

```python
from opendata_framework.dagster import (
    # Factories
    create_socrata_pipeline,
    create_checkbook_pipeline,
    create_querystation_sql_asset,
    discover_sql_assets,
    # Config models
    SocrataIngestConfig,
    CheckbookIngestConfig,
    HttpIngestConfig,
    PolarsTransformConfig,
    # Partitions
    yearly_partitions,
    monthly_partitions,
    # Resources
    SocrataResource,
    CheckbookNYCResource,
    QueryStationResource,
    # IO managers
    LandingIOManager,
    PolarsParquetIOManager,
    JsonIOManager,
    # Schema
    SchemaContract,
)

from opendata_framework.core import (
    apply_schema_contract,
    multi_parse_date,
    build_table_schema,
    extract_polars_contract,
)

from opendata_framework.enrichments import (
    StandardEnrichments,
    TemporalConfig,
    BoroughConfig,
    LocationConfig,
    CompletenessConfig,
    TimestampConfig,
)
```

## Three asset factories

### Socrata — landing (CSV) → clean (Parquet)

```python
from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)
import polars as pl

schema: SchemaContract = {
    "api_field": ("clean_name", pl.Utf8, "Description."),
}

pipeline = create_socrata_pipeline(
    name="nyc_my_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="date_col",
        base_domain="data.cityofnewyork.us",
    ),
    schema=schema,
    description="…",
)

nyc_my_dataset = pipeline.clean
```

Handles Socrata SODA v2 pagination, gzip CSV landing, schema-contract application (rename + type), enrichment pipelines, and retry with exponential backoff. Returns `landing`, `clean`, and an auto-generated row-accounting asset check.

### QueryStation remote SQL

Frontmatter-authored — drop a `.sql` file anywhere that `discover_sql_assets()` walks:

```sql
/*---
name: mta_ridership_yearly
source: querystation
group: querystation__transportation
partitions:
  type: yearly
  start: "2020"
  end_offset: 1
---*/
SELECT ... FROM lake.nys_transportation.mta_daily_ridership
WHERE date >= {{partition_start}} AND date < {{partition_end}}
```

Or, for cases needing a custom `PartitionsDefinition` / `AutomationCondition`, use the Python factory:

```python
from opendata_framework.dagster import (
    create_querystation_sql_asset,
    yearly_partitions,
)

items = create_querystation_sql_asset(
    name="mta_ridership_yearly",
    sql="SELECT ... FROM lake.nys_transportation.mta_daily_ridership WHERE date >= {{partition_start}}",
    partitions_def=yearly_partitions("2020", end_offset=1),
    group="querystation__transportation",
)
```

Both return `[asset, row_count_check]` and share the same `render_sql()` templating core.

### Local DuckDB-JIT SQL

Default `.sql` backend — no `source:` field. `discover_sql_assets()` walks a directory, parses YAML frontmatter, and at materialization opens an ephemeral in-memory DuckDB, mounts each upstream as a `parquet_scan` view (via registered IO managers), and runs the query:

```python
from pathlib import Path
from opendata_framework.dagster import discover_sql_assets

_sql_registry = discover_sql_assets(
    root=Path(__file__).parent,
    group="nyc__sanitation",
)
```

Implicit deps are extracted from SQL via sqlglot (CTE names are excluded). Fully-qualified three-part names (`catalog.schema.table`) in a local SQL asset raise at discovery time — an explicit guard against accidentally shipping a remote reference through the local runner.

## Package layout

```
opendata_framework/
├── core/
│   ├── polars_utils.py      # apply_schema_contract, safe_bool/float, load_partitioned_scans
│   ├── duckdb_utils.py
│   ├── schema/              # schema contracts + catalog column metadata
│   └── sql/
│       ├── discovery.py     # walks .sql files → SqlAssetSpec[]
│       ├── frontmatter.py   # /*--- ... ---*/ YAML parser
│       ├── parser.py        # sqlglot-based table-name extraction (CTE-aware)
│       ├── runner_duckdb.py         # stateless JIT runner for local SQL
│       └── runner_querystation.py   # partition-templated render_sql()
├── dagster/
│   ├── factories.py                 # create_socrata_pipeline, create_checkbook_pipeline
│   ├── querystation_factory.py      # create_querystation_sql_asset
│   ├── partitions.py                # monthly_partitions, yearly_partitions (tz-aware)
│   ├── standards.py                 # Pydantic config models
│   ├── assets/
│   │   ├── ingestors.py             # landing fetch ops (Socrata, Checkbook, HTTP)
│   │   ├── checks.py                # asset-check builders
│   │   └── sql_assets.py            # discover_sql_assets + _build_asset_core
│   └── resources/
│       ├── socrata_resource.py
│       ├── checkbook_resource.py
│       ├── querystation_resource.py # wraps data_consumers.RemoteDuckDBWrapper
│       └── io/                       # Landing, JSON, Polars/Parquet IO managers
├── enrichments/                     # Declarative StandardEnrichments + individual helpers
└── integrations/                    # NYC-specific + weather (Open-Meteo) helpers
```

## Resources expected by factory-built assets

The factories wire assets that require the following resource keys — register them on your `dg.Definitions(...)` merge:

| Key | Type | Needed by |
|---|---|---|
| `socrata` | `SocrataResource` | Socrata landing ops |
| `querystation` | `QueryStationResource` | QueryStation SQL assets + factory |
| `landing_io_manager` | `LandingIOManager` | Socrata landing assets |
| `clean_io_manager` | `PolarsParquetIOManager` | Socrata clean assets, local SQL upstream resolution |
| `analytics_io_manager` | `PolarsParquetIOManager` | All SQL assets (output + asset checks) |
| `raw_large_io_manager` | `PolarsParquetIOManager` | Local SQL runner fallback when a dep lives in landing |

See `src/opendata_eda/definitions.py` in the workspace root for a worked-out example.

## Partition templating

`render_sql()` supports five tokens. Missing context raises instead of silently producing a full-table scan per partition:

| Token | Renders as | Use for |
|---|---|---|
| `{{partition_start}}` | `'YYYY-MM-DD'` | DATE columns |
| `{{partition_end}}` | `'YYYY-MM-DD'` | DATE columns |
| `{{partition_start_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns |
| `{{partition_end_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns |
| `{{partition_key}}` | `'key'` (alphanumerics + `_-:. ` only) | Static/string partitions |

`{{partition_key}}` rejects SQL-unsafe characters to block injection via user-constructed `StaticPartitionsDefinition` entries.

## Enrichments

Declarative config passed into a factory:

```python
from opendata_framework.enrichments import (
    StandardEnrichments, TemporalConfig, BoroughConfig, LocationConfig,
)

enrichments = StandardEnrichments(
    temporal=TemporalConfig(year=True, month=True, hour=True, overnight=True),
    borough=BoroughConfig(source_col="borough"),
    location=LocationConfig(lat_col="latitude", lng_col="longitude"),
)

create_socrata_pipeline(..., enrichments=enrichments)
```

For transforms beyond the declarative surface, pass `post_transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame]`.

## Testing a factory-built asset

```python
from opendata_eda.definitions import defs

# Asset count (sanity)
len(list(defs.resolve_asset_graph().get_all_asset_keys()))
```

Use `resolve_asset_graph().get_all_asset_keys()` — the older `get_asset_graph().all_asset_keys` is removed in Dagster 1.12+.

## License

MIT (inherits from the workspace root).
