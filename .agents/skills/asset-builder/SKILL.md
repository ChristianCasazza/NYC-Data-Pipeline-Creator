---
name: asset-builder
description: >
  Comprehensive guide for building the three asset types in this repo: Socrata
  pipelines, local DuckDB-JIT SQL analytics, and QueryStation remote SQL assets.
  Use when an agent needs to add a new dataset, create a pipeline, build
  downstream SQL analytics, or add new asset modules to defs/assets/. Triggers on:
  "add asset", "build pipeline", "create sql asset", "add sql analytics",
  "new downstream asset", "add querystation asset", "remote sql asset",
  or ambiguous requests to add/ingest a dataset. For explicit Socrata URLs,
  use this skill to choose the backend, then delegate Socrata implementation
  details to socrata-builder if ingestion is still the right path.
---

# Asset Builder

End-to-end guide for building the three asset types in this repo:

1. **Socrata Pipeline Assets** — Ingest raw data from NYC Open Data (Socrata API) into Landing (CSV) → Clean (Parquet).
2. **Local SQL Analytics Assets** — DuckDB-JIT transformations that read clean parquet (produced by any backend) and write new parquet.
3. **QueryStation Remote SQL Assets** — `.sql` files with `source: querystation` in frontmatter. Executed against QueryStation's Arrow IPC endpoint at materialization time; result cached as Parquet so downstream local assets can reference them.

All three share the same IO manager (`./data/clean/`), so a local SQL asset can join across backends transparently. The project uses Dagster's `load_from_defs_folder()` for automatic asset discovery. Each Socrata pipeline is its own module under `src/opendata_eda/defs/assets/`. Related assets can be grouped into domain subpackages (e.g., `floodnet/` with shared schemas in `_shared.py`). SQL assets (both local and remote) are auto-discovered from `src/opendata_eda/defs/assets/sql_assets/*.sql`.

---

## Part 1: Socrata Pipeline Assets

### Overview

The `create_socrata_pipeline()` factory from `opendata_framework` produces a 2-stage pipeline:
- **Landing** — Fetches CSV from Socrata API, stores as gzipped CSV shards in `data/landing/`
- **Clean** — Reads landing CSV, applies schema contract (renames + types), writes typed Parquet to `data/clean/`
- **Row accounting check** — Auto-generated asset check that verifies landing rows == clean rows

### Step 1: Fetch Metadata

```bash
uv run .agents/skills/socrata-builder/scripts/fetch_socrata_metadata.py "<SOCRATA_URL>"
```

This outputs: dataset ID, column schema with Polars types, row count, partitioning recommendation, and a suggested asset name.

### Step 2: Review Metadata

Before generating code, review and confirm:
1. **Asset name** — accept the suggested name or override
2. **Partitioning** — accept the recommendation or override (see Partitioning Logic below)
3. **Column renames** — normalize to `lower_snake_case` (see Column Name Normalization below)
4. **System columns** — skip `:id`, `:created_at`, etc. by default

### Step 3: Write the Schema and Pipeline

Create a new file in `src/opendata_eda/defs/assets/`:

```python
# src/opendata_eda/defs/assets/nyc_new_dataset.py
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

new_schema: SchemaContract = {
    "api_field_name": ("clean_snake_case_name", pl.Utf8, "Description from Socrata."),
    "numeric_field":  ("numeric_field", pl.Float64, "Another description."),
}

new_pipeline = create_socrata_pipeline(
    name="nyc_new_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="date_col",
        base_domain="data.cityofnewyork.us",
    ),
    schema=new_schema,
    description="Brief description of the dataset.",
)

nyc_new_dataset = new_pipeline.clean
```

No changes to `definitions.py` needed — `load_from_defs_folder()` picks it up automatically.

#### For staged/partitioned pipelines:

```python
new_pipeline = create_socrata_pipeline(
    name="nyc_large_dataset",
    socrata_config=SocrataIngestConfig(
        endpoint="xxxx-yyyy",
        time_col="created_date",
        base_domain="data.cityofnewyork.us",
    ),
    schema=new_schema,
    description="Large dataset with monthly/yearly staging.",
    partitions_def=monthly_partitions("2020-01-01", end_offset=1),
    clean_partitions_def=yearly_partitions("2020", end_offset=1),
)
```

### Partitioning Logic

The metadata script recommends partitioning based on row count:

```
< 5M rows       → No partitions (most common in this repo)
5M–500M rows    → Monthly landing, yearly clean (staged)
> 500M rows     → Monthly all stages
equality column → Yearly all stages (fiscal_year, etc.)
no date column  → Unpartitioned regardless of size
```

**Date column scoring** (higher = better partition candidate):
- 90: Known event columns (`created_date`, `arrest_date`, `inspection_date`, etc.)
- 75: Suffix `_date` or `_datetime`
- 70: Suffix `_time` or `_timestamp`
- 65: Contains "date"
- 40: Generic calendar_date
- 20: Non-event dates (`hire_date`, `birth_date`, etc.)
- 10: System columns (`:created_at`, `:updated_at`)

### Schema Key Rules

- Schema dict key = Socrata API field name (as-is, case-sensitive)
- Value tuple = `(target_name, polars_type, description)`
- Target names should be `lower_snake_case`
- Always include `time_col` in `SocrataIngestConfig` (required)
- Include `base_domain="data.cityofnewyork.us"` for NYC datasets

### Column Name Normalization

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

### Required Imports

Each asset module only needs its own imports — no need to import resource or IO manager classes:

```python
import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
    monthly_partitions,    # only if partitioned
    yearly_partitions,     # only if partitioned
)
```

### Step 4: Validate

```bash
uv run python -c "from opendata_eda.definitions import defs; print(f'Assets: {len(list(defs.resolve_asset_graph().get_all_asset_keys()))}')"
```

**CRITICAL:** Use `resolve_asset_graph().get_all_asset_keys()`. The older `get_asset_graph().all_asset_keys` is removed in the current Dagster version and will throw `AttributeError`.

### Step 5: Materialize

**CRITICAL:** Always include `-m opendata_eda.definitions`. Without it, Dagster errors with "Invalid set of CLI arguments".

```bash
# Landing first, then clean (sequential — landing must finish before clean)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_dataset_landing
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select nyc_new_dataset
```

### Step 6: Verify Output

```bash
uv run python -c "
import polars as pl
df = pl.read_parquet('data/clean/nyc_new_dataset/nyc_new_dataset.parquet')
print(f'Rows: {len(df)}, Cols: {len(df.columns)}')
print(df.schema)
print(df.head(3))
"
```

---

## Part 2: SQL Analytics Assets

### Overview

SQL analytics assets are downstream DuckDB transformations. They:
- Live as `.sql` files in `src/opendata_eda/defs/assets/sql_assets/`
- Are auto-discovered by `discover_sql_assets()` — no manual wiring into `defs` needed
- Use YAML frontmatter (`/*--- ... ---*/`) for metadata
- Execute via stateless in-memory DuckDB with JIT parquet views
- Output to `data/clean/{asset_name}/{asset_name}.parquet`
- Get an automatic row count check (blocks on 0 rows)

### How It Works (Framework Flow)

1. `discover_sql_assets()` walks `src/opendata_eda/defs/assets/sql_assets/` for `*.sql` files
2. Each file's YAML frontmatter is parsed for name, deps, group, tags
3. `extract_table_names()` (sqlglot) finds implicit table references in the SQL
4. At materialization time, `run_sql_in_duckdb()`:
   - Opens an ephemeral in-memory DuckDB connection
   - Creates JIT `parquet_scan` views for each dependency (resolved via IO managers)
   - Executes the SQL
   - Returns a Polars DataFrame, written to parquet by the IO manager

### SQL File Format

```sql
/*---
name: my_analytics_asset
description: >
  What this asset computes. Be specific — this shows in the Dagster UI.
deps:
  - upstream_asset_name
group: nyc__sanitation
tags:
  domain: sanitation
  geographic_scope: nyc
  stage: analytics
---*/

SELECT
    col_a,
    round(sum(col_b), 0) AS total_b
FROM upstream_asset_name
GROUP BY 1
ORDER BY 1
```

### Frontmatter Fields

| Field | Required | Description |
|---|---|---|
| `name` | No | Asset name (defaults to file stem if omitted) |
| `description` | Yes | Shows in Dagster UI metadata |
| `deps` | No | List of upstream asset names. For local DuckDB-JIT assets, implicit deps are also parsed from the SQL body via sqlglot; for `source: querystation` deps must be declared explicitly (remote FQNs don't resolve to local assets). |
| `group` | No | Dagster group name (default: `Analytics`). Convention: `nyc__{domain}` for local/Socrata-backed analytics, `querystation__{domain}` for remote-backed. |
| `tags` | No | Key-value tags for filtering |
| `source` | No | Execution backend. Omit → local DuckDB-JIT. `querystation` → remote Arrow IPC. |
| `partitions` | No | Partition spec (see QueryStation section below). Only meaningful when `source` points at a remote backend that can template per-partition SQL. |

### Dependency Resolution

Dependencies come from three sources (merged):
1. **`deps` in frontmatter** — explicit, always used
2. **`extract_table_names()` from SQL** — implicit via sqlglot parsing
3. **`extra_deps` passed to `discover_sql_assets()`** — programmatic override

At runtime, each dependency is resolved by checking IO managers in priority order:
`analytics_io_manager` → `clean_io_manager` → `raw_large_io_manager`

If a dependency can't be found, the runner creates an empty fallback view and logs a warning. Because the parser now subtracts CTE names from extracted tables, this warning should only fire when a real upstream asset is missing — investigate rather than ignore.

### CTE Naming (no longer required, but still readable)

`extract_table_names()` in `opendata_framework.core.sql.parser` subtracts CTE names from extracted tables (`names - cte_names`), so `WITH foo AS (...) SELECT * FROM foo` no longer registers a phantom `foo` dep. The underscore-prefix convention (`WITH _yearly AS ...`) is still a nice readability signal but is no longer needed for correctness.

```sql
-- Both are now safe — neither registers 'yearly' as a Dagster dep
WITH _yearly AS (
    SELECT ... FROM nyc_dsny_monthly_tonnage
)
SELECT * FROM _yearly
```

### CRITICAL: Footgun guard for fully-qualified names

`discover_sql_assets` also runs `extract_qualified_table_names()` and raises at Dagster startup if it finds three-part names (`catalog.schema.table`, e.g. `lake.nyc_operations.service_requests_311`) in a file that does NOT declare `source: querystation`. The error message is actionable: add `source: querystation` or replace the FQN with a local asset name.

### CRITICAL: Use TRY_CAST not CAST

When converting string fields to numbers in DuckDB, always use `TRY_CAST`:

```sql
-- GOOD: returns NULL on bad input
TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT)

-- BAD: throws ConversionException on empty strings or malformed data
CAST(split_part(month, ' / ', 1) AS INT)
```

### Wiring

SQL assets are wired via `src/opendata_eda/defs/assets/sql_assets/__init__.py`:

```python
from pathlib import Path
from opendata_framework.dagster.assets.sql_assets import discover_sql_assets

_sql_registry = discover_sql_assets(root=Path(__file__).parent, group="nyc__sanitation")

_sql_assets = [v for v in _sql_registry.values() if hasattr(v, "node_def")]
_sql_checks = [v for v in _sql_registry.values() if not hasattr(v, "node_def")]
```

`load_from_defs_folder()` picks these up automatically along with all other assets.

**To add a new SQL asset:** Just create a new `.sql` file in `src/opendata_eda/defs/assets/sql_assets/`. No changes needed anywhere else — discovery is automatic.

### Required IO Manager Resources

SQL assets (via `_build_asset_core`) require these three resource keys to be registered:

| Resource Key | Points To | Purpose |
|---|---|---|
| `analytics_io_manager` | `PolarsParquetIOManager(base_path="./data/clean")` | Write output + resolve analytics deps |
| `clean_io_manager` | `PolarsParquetIOManager(base_path="./data/clean")` | Resolve clean pipeline deps |
| `raw_large_io_manager` | `PolarsParquetIOManager(base_path="./data/landing")` | Resolve landing deps (fallback) |

If any are missing, materialization will fail with a missing resource error.

### Materialize SQL Assets

```bash
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize -m opendata_eda.definitions --select my_analytics_asset
```

No landing step needed — SQL assets read directly from upstream parquet.

---

## Part 3: QueryStation Remote SQL Assets

QueryStation remote SQL assets live in the **same directory** as local SQL assets (`src/opendata_eda/defs/assets/sql_assets/`) and share the same frontmatter grammar. The only differences: `source: querystation` switches the execution backend, and the SQL references fully-qualified three-part names (`catalog.schema.table`).

### How it works

1. Discovery finds the `.sql` file and sees `source: querystation` in frontmatter.
2. `extract_qualified_table_names()` detects any `lake.*.*` references — if found without `source: querystation`, discovery raises at Dagster startup (see footgun guard).
3. At materialization, `render_sql()` substitutes any `{{partition_*}}` tokens.
4. `QueryStationResource.query(rendered)` executes over Arrow IPC → `pl.DataFrame`.
5. Dagster's `analytics_io_manager` writes the result to `data/clean/{asset_name}/...parquet` (Hive layout if partitioned).
6. A row-count asset check runs automatically.
7. A local SQL asset downstream can reference `{asset_name}` by bare name — the DuckDB-JIT runner will mount the written parquet as a view.

Retry policy is automatic: `RetryPolicy(max_retries=3, delay=30, backoff=EXPONENTIAL)` — 0s, 30s, 90s, 210s. A materialization that exhausts retries takes ~6 minutes; don't wrap with shorter timeouts.

### Unpartitioned (simplest)

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

### Partitioned (one remote query per partition)

Add a `partitions:` block and use `{{partition_start}}` / `{{partition_end}}` tokens in the SQL. Each partition renders and fires its own remote query and writes its own Hive shard (`year=2024/...parquet`).

```sql
/*---
name: mta_ridership_yearly
source: querystation
description: Per-year MTA ridership aggregates.
group: querystation__transportation
partitions:
  type: yearly              # or: monthly
  start: "2020"             # "YYYY" for yearly, "YYYY-MM-DD" for monthly
  end_offset: 1             # optional; include current/future partition
  tz: America/New_York      # optional; default America/New_York
tags:
  domain: transportation
  stage: analytics
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

### Templating tokens

`render_sql()` lives in `packages/opendata_framework/opendata_framework/core/sql/runner_querystation.py`. Missing context raises at render time — no silent fallthrough to a full-table scan per partition.

| Token | Renders as | Use for |
|---|---|---|
| `{{partition_start}}` | `'YYYY-MM-DD'` | DATE columns (partition-local date boundary) |
| `{{partition_end}}` | `'YYYY-MM-DD'` | DATE columns |
| `{{partition_start_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns — avoids cross-midnight off-by-one |
| `{{partition_end_ts}}` | `'YYYY-MM-DD HH:MM:SS±HH:MM'` | `TIMESTAMP WITH TIME ZONE` columns |
| `{{partition_key}}` | `'key'` (alphanumerics + `_-:. ` only) | Static/string partitions; injection attempts are rejected |

### Python factory alternative

Use when you need a custom `PartitionsDefinition`, `AutomationCondition`, or anything the frontmatter YAML doesn't expose. Lives in `opendata_framework.dagster.querystation_factory.create_querystation_sql_asset`.

```python
# src/opendata_eda/defs/assets/my_remote.py
from opendata_framework.dagster import (
    create_querystation_sql_asset,
    yearly_partitions,
)

my_remote_items = create_querystation_sql_asset(
    name="my_remote_asset",
    sql="""
        SELECT ... FROM lake.foo.bar
        WHERE date >= {{partition_start}}
          AND date <  {{partition_end}}
    """,
    partitions_def=yearly_partitions("2020", end_offset=1),
    group="querystation__foo",
    tags={"domain": "foo"},
)
```

The factory returns `[asset, row_count_check]` — both must be at module scope so `load_from_defs_folder()` picks them up. Both code paths (frontmatter + factory) share the same `render_sql()` core.

### Cross-backend joins

A local SQL asset (default `source`, no `source:` field) can join upstream parquet written by any backend. It's just a parquet read:

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

### Materialize QueryStation assets

```bash
# Unpartitioned: single remote query
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select mta_ridership_by_mode

# Partitioned: one partition at a time (one remote query each)
DAGSTER_HOME=$(pwd)/logs uv run dagster asset materialize \
  -m opendata_eda.definitions --select mta_ridership_yearly --partition 2024
```

Requires `QUERYSTATION_API_KEY` and `AUTH_URL` in `.env`. See the **querystation** skill for troubleshooting auth / Arrow IPC issues.

---

## Part 4: Working Examples

### Example: DSNY Monthly Tonnage (Socrata → SQL Analytics)

This is a real end-to-end example from this repo.

**Socrata asset** (`defs/assets/nyc_dsny_monthly_tonnage.py`):
```python
dsny_tonnage_schema: SchemaContract = {
    "month": ("month", pl.Utf8, "Year and month of collection."),
    "borough": ("borough", pl.Utf8, "One of the 5 boroughs within NYC."),
    "communitydistrict": ("community_district", pl.Utf8, "Sanitation district."),
    "refusetonscollected": ("refuse_tons_collected", pl.Float64, "Tons of refuse."),
    "papertonscollected": ("paper_tons_collected", pl.Float64, "Tons of paper."),
    "mgptonscollected": ("mgp_tons_collected", pl.Float64, "Tons of MGP."),
    # ... more columns
}

dsny_tonnage_pipeline = create_socrata_pipeline(
    name="nyc_dsny_monthly_tonnage",
    socrata_config=SocrataIngestConfig(
        endpoint="ebb7-mvp5",
        time_col="month",
        base_domain="data.cityofnewyork.us",
    ),
    schema=dsny_tonnage_schema,
    description="DSNY monthly collection tonnage by community district.",
)
```

**SQL analytics asset** (`src/opendata_eda/defs/assets/sql_assets/dsny_tonnage_annual_summary.sql`):
```sql
/*---
name: dsny_tonnage_annual_summary
description: Citywide annual summary with diversion rates.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
---*/

SELECT
    TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year,
    round(sum(refuse_tons_collected), 0) AS total_refuse_tons,
    round(sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0)), 0) AS total_recycling_tons,
    round(100.0 * sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0), 1) AS recycling_diversion_pct
FROM nyc_dsny_monthly_tonnage
GROUP BY 1
ORDER BY 1
```

**Asset lineage:**
```
nyc_dsny_monthly_tonnage_landing (Socrata CSV)
  └── nyc_dsny_monthly_tonnage (Clean Parquet)
        ├── dsny_tonnage_annual_summary (SQL)
        ├── dsny_tonnage_borough_monthly (SQL)
        ├── dsny_tonnage_district_rankings (SQL)
        └── dsny_tonnage_organics_rollout (SQL)
```

---

## Existing Assets (for dedup)

Check `src/opendata_eda/defs/assets/` before creating.

### Socrata pipeline assets

| Dataset ID | Asset Name | Module | Type |
|---|---|---|---|
| erm2-nwe9 | nyc_311_sample | `nyc_311_sample.py` | Partitioned |
| tg4x-b46p | nyc_film_permits | `nyc_film_permits.py` | Unpartitioned |
| kb2e-tjy3 | nyc_floodnet_sensor_metadata | `floodnet/sensor_metadata.py` | Unpartitioned |
| aq7i-eu5q | nyc_floodnet_flooding_events | `floodnet/flooding_events.py` | Unpartitioned |
| — | nyc_floodnet_events_joined | `floodnet/events_joined.py` | Derived |
| ebb7-mvp5 | nyc_dsny_monthly_tonnage | `nyc_dsny_monthly_tonnage.py` | Unpartitioned |
| h9gi-nx95 | nyc_motor_vehicle_collisions | `nyc_motor_vehicle_collisions.py` | Unpartitioned |

### QueryStation remote assets (in `sql_assets/`, `source: querystation`)

| Asset | Upstream table | Type |
|---|---|---|
| `mta_ridership_by_mode` | `lake.nys_transportation.mta_daily_ridership` | Unpartitioned |
| `mta_ridership_yearly` | `lake.nys_transportation.mta_daily_ridership` | Yearly-partitioned |
| `nyc_air_quality_annual` | `lake.nyc_environment.air_quality` | Unpartitioned |
| `nyc_311_top_heat_bbls_by_cb` | `lake.nyc_operations.service_requests_311` | Unpartitioned |

### Local SQL analytics assets (in `sql_assets/`, default backend)

- `dsny_tonnage_annual_summary`, `dsny_tonnage_borough_monthly`, `dsny_tonnage_district_rankings`, `dsny_tonnage_organics_rollout`
- `collisions_annual_summary`, `collisions_borough_monthly`, `collisions_contributing_factors`
- `transit_vs_air_quality` — cross-backend join of QueryStation-sourced MTA + air quality

### Dagster group conventions

| Prefix | Contents |
|---|---|
| `nyc__{domain}` | Socrata-ingested assets and their local-JIT analytics (e.g. `nyc__sanitation`, `nyc__public_safety`, `nyc__operations`, `nyc__environment`) |
| `querystation__{domain}` | QueryStation-sourced remote assets (e.g. `querystation__transportation`, `querystation__environment`, `querystation__operations`, `querystation__analytics`) |

---

## Batch Mode: Catalog CSV

To browse available NYC Open Data datasets and prioritize new ones to ingest:

```bash
uv run .agents/skills/socrata-builder/scripts/fetch_catalog.py --max 150 -o catalog.csv
```

Use the CSV to prioritize datasets by `views_total`, filter out `already_exists = True`, and batch by `category`.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `AttributeError: get_asset_graph` | Dagster API renamed | Use `resolve_asset_graph().get_all_asset_keys()` |
| `Invalid set of CLI arguments` | Missing module flag | Add `-m opendata_eda.definitions` to materialize command |
| `ConversionException: Could not convert string '' to INT32` | `CAST` on dirty string data | Use `TRY_CAST(trim(...))` instead of `CAST` |
| `RuntimeError: SQL asset 'x' references fully-qualified remote tables ...` | SQL has `lake.a.b` but no remote `source:` | Add `source: querystation`, or replace FQN with a local asset name |
| `SQL references {{partition_start}} but asset is not time-partitioned` | Token used without partition context | Add a `partitions:` block, or remove the token |
| `partition_key '...' contains characters disallowed in SQL substitution` | `{{partition_key}}` value has quotes/semicolons | Restrict `StaticPartitionsDefinition` values to alphanumerics + `_-:. ` + space |
| QueryStation materialization takes ~6 min before failing | Exhausting `RetryPolicy(max_retries=3, delay=30, backoff=EXPONENTIAL)` | Expected — attempts at 0s/30s/90s/210s. Don't wrap with shorter timeouts |
| `Could not locate data for dependency 'X'` | Missing upstream | Materialize upstream first. Since the parser now subtracts CTE names, this is almost always a real missing dep |
| Phantom CTE deps in graph | Previously a known bug | Fixed — `parser.py` now returns `names - cte_names` |
| Missing resource key error | SQL asset needs `analytics_io_manager`, `clean_io_manager`, `raw_large_io_manager` (local) or `querystation` (remote) | Check `definitions.py` registers the right resource for your `source:` |
| 404 on Socrata metadata fetch | Wrong dataset ID or private dataset | Verify URL and dataset ID |
| `time_col` required error | `SocrataIngestConfig` needs `time_col` | Use best date column from metadata output |
| Empty parquet after materialize | Wrong endpoint or column names | Check Socrata endpoint matches, column names are API field names |
| `No API key: pass api_key= or set QUERYSTATION_API_KEY` | `.env` missing QueryStation creds | Add `QUERYSTATION_API_KEY=sk_...` and `AUTH_URL=https://api.querystation.app` |
| Shell env vars ignored when testing overrides | Dagster auto-loads `.env`, overriding shell env | Rename `.env` during the test or spawn a child process that skips dotenv loading |
