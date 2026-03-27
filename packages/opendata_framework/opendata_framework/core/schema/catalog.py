import polars as pl


def polars_type_to_catalog_type(dtype: type) -> str:
    """Map Polars types to catalog-friendly type names."""
    type_map = {
        pl.Utf8: "string",
        pl.String: "string",
        pl.Datetime: "date",
        pl.Date: "date",
        pl.Float64: "number",
        pl.Float32: "number",
        pl.Int64: "number",
        pl.Int32: "number",
        pl.Int16: "number",
        pl.Int8: "number",
        pl.Boolean: "boolean",
    }
    return type_map.get(dtype, "unknown")
