from dagster import TableSchema, TableColumn
from opendata_framework.core.schema.catalog import polars_type_to_catalog_type


def normalize_schema(schema: dict) -> tuple[dict, bool]:
    """Detect 2-tuple vs 3-tuple schema and normalize to 2-tuple contract.

    Returns ``(contract, is_3tuple)`` where *contract* always has the form
    ``{source_col: (target_name, polars_type)}``.

    Raises ``ValueError`` if entries have inconsistent tuple lengths.
    """
    sample = next(iter(schema.values()))
    expected = len(sample)
    bad = {k for k, v in schema.items() if len(v) != expected}
    if bad:
        msg = f"Schema has mixed tuple lengths (expected {expected}-tuple): {bad}"
        raise ValueError(msg)
    if expected == 3:
        return {k: (v[0], v[1]) for k, v in schema.items()}, True
    return schema, False


def extract_polars_contract(schema_def: dict) -> dict:
    """Extract {source: (target, type)} from a 3-tuple schema definition."""
    return {k: (v[0], v[1]) for k, v in schema_def.items()}


def build_catalog_columns_metadata(
    schema_def: dict,
    derived_columns: list[dict] | None = None,
    insert_after: dict[str, list[dict]] | None = None,
) -> list[dict]:
    """Build catalog column metadata from a 3-tuple schema definition.

    Args:
        schema_def: {source_col: (target_name, polars_type, description)}
        derived_columns: Extra columns to append at the end.
        insert_after: {target_col_name: [columns_to_insert_after_it]}
    """
    columns = []
    for src_key, (dst_col, dtype, desc) in schema_def.items():
        columns.append({
            "name": dst_col,
            "type": polars_type_to_catalog_type(dtype),
            "api_name": src_key,
            "description": desc,
        })
        if insert_after and dst_col in insert_after:
            columns.extend(insert_after[dst_col])
    if derived_columns:
        columns.extend(derived_columns)
    return columns


def build_table_schema(
    schema_def: dict,
    derived_columns: list[TableColumn] | None = None,
    insert_after: dict[str, list[TableColumn]] | None = None,
) -> TableSchema:
    """Build Dagster TableSchema from a 3-tuple schema definition."""
    cols = []
    for _, (dst_col, dtype, desc) in schema_def.items():
        cols.append(TableColumn(name=dst_col, type=str(dtype), description=desc))
        if insert_after and dst_col in insert_after:
            cols.extend(insert_after[dst_col])
    if derived_columns:
        cols.extend(derived_columns)
    return TableSchema(columns=cols)


def build_table_schema_from_contract(
    schema_contract: dict,
    derived_columns: list[TableColumn] | None = None,
) -> TableSchema:
    """Build Dagster TableSchema from a 2-tuple or 3-tuple schema.

    Handles both 2-tuple (target, type) and 3-tuple (target, type, description)
    formats. Use this when the schema may or may not include descriptions.
    """
    cols = []
    for _, v in schema_contract.items():
        dst_col, dtype = v[0], v[1]
        desc = v[2] if len(v) > 2 else ""
        cols.append(TableColumn(name=dst_col, type=str(dtype), description=desc))
    if derived_columns:
        cols.extend(derived_columns)
    return TableSchema(columns=cols)
