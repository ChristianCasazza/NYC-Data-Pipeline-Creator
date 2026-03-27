# opendata_framework/__init__.py
"""
opendata-framework: Dagster resources, IO managers, and ingestion factories for open data pipelines.

Import paths::

    from opendata_framework.dagster import (
        create_socrata_pipeline,
        create_checkbook_pipeline,
        SocrataIngestConfig,
        CheckbookIngestConfig,
        SchemaContract,
        yearly_partitions,
        monthly_partitions,
        discover_sql_assets,
    )

    from opendata_framework.enrichments import (
        StandardEnrichments,
        TemporalConfig,
        BoroughConfig,
    )

    from opendata_framework.core import (
        apply_schema_contract,
        SchemaContract,
    )
"""
