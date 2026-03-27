"""Standardized enrichment building blocks for open data pipelines.

For pipeline factories (``create_socrata_pipeline``, ``create_checkbook_pipeline``),
use the **declarative** ``StandardEnrichments`` config::

    from opendata_framework.enrichments import StandardEnrichments, TemporalConfig

    enrichments = StandardEnrichments(
        temporal=TemporalConfig(year=True, month=True),
    )
    # Pass to: create_socrata_pipeline(..., enrichments=enrichments)

For custom transforms beyond what StandardEnrichments covers, use
``post_transform_fn`` which runs **after** standard enrichments::

    def my_custom_logic(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(...)

    create_socrata_pipeline(..., enrichments=enrichments, post_transform_fn=my_custom_logic)

Individual enrichment functions (``add_borough_key``, ``add_temporal_columns``, etc.)
are exported below for advanced standalone use outside of factories.
"""

# --- Temporal ---
from opendata_framework.enrichments.temporal import (
    add_record_timestamp as add_record_timestamp,
    add_temporal_columns as add_temporal_columns,
    compute_duration as compute_duration,
    enforce_timezone as enforce_timezone,
    parse_string_date as parse_string_date,
)

# --- Geographic ---
from opendata_framework.enrichments.geographic import (
    add_borough_key as add_borough_key,
    add_community_district_key as add_community_district_key,
    add_location_flag as add_location_flag,
    add_nyc_bbl as add_nyc_bbl,
    borough_code_expr as borough_code_expr,
    borough_key_expr as borough_key_expr,
    borough_name_expr as borough_name_expr,
)

# --- Text ---
from opendata_framework.enrichments.text import (
    apply_literal_replacements as apply_literal_replacements,
    clean_text_expr as clean_text_expr,
    split_and_clean_list as split_and_clean_list,
)

# --- Data Quality Flags ---
from opendata_framework.enrichments.flags import (
    add_completeness_flags as add_completeness_flags,
)

# --- Numeric ---
from opendata_framework.enrichments.numeric import (
    compute_rate as compute_rate,
    sum_components as sum_components,
)

# --- Mapping / Classification ---
from opendata_framework.enrichments.mapping import (
    assemble_annotations as assemble_annotations,
    collect_matching_labels as collect_matching_labels,
    map_with_default as map_with_default,
    ordered_regex_classification as ordered_regex_classification,
)

# --- IDs ---
from opendata_framework.enrichments.ids import stable_string_hash as stable_string_hash

# --- Layout ---
from opendata_framework.enrichments.layout import reorder_columns as reorder_columns

# --- Deduplication ---
from opendata_framework.enrichments.dedup import (
    add_duplicate_markers as add_duplicate_markers,
)

# --- Column Metadata Catalog (for data dictionaries) ---
from opendata_framework.enrichments import catalog as catalog

# --- Declarative Enrichment Builder ---
from opendata_framework.enrichments.builder import (
    BoroughConfig as BoroughConfig,
    CompletenessConfig as CompletenessConfig,
    LocationConfig as LocationConfig,
    StandardEnrichments as StandardEnrichments,
    TemporalConfig as TemporalConfig,
    TimestampConfig as TimestampConfig,
)

__all__ = [
    # Temporal
    "add_record_timestamp",
    "add_temporal_columns",
    "compute_duration",
    "enforce_timezone",
    "parse_string_date",
    # Geographic
    "add_borough_key",
    "add_community_district_key",
    "add_location_flag",
    "add_nyc_bbl",
    "borough_code_expr",
    "borough_key_expr",
    "borough_name_expr",
    # Text
    "apply_literal_replacements",
    "clean_text_expr",
    "split_and_clean_list",
    # Flags
    "add_completeness_flags",
    # Numeric
    "compute_rate",
    "sum_components",
    # Mapping
    "assemble_annotations",
    "collect_matching_labels",
    "map_with_default",
    "ordered_regex_classification",
    # Layout
    "reorder_columns",
    # IDs
    "stable_string_hash",
    # Dedup
    "add_duplicate_markers",
]
