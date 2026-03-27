# opendata_framework/enrichments/builder.py
"""Declarative enrichment builder.

Generates both a ``post_transform_fn`` and ``derived_columns`` list from a
single config object, ensuring they never diverge.

Usage::

    from opendata_framework.enrichments.builder import (
        StandardEnrichments, BoroughConfig, TimestampConfig,
        TemporalConfig, CompletenessConfig, LocationConfig,
    )

    enrichments = StandardEnrichments(
        borough=BoroughConfig(source_col="boro"),
        location=LocationConfig(),
        timestamp=TimestampConfig(source_col="inspection_date", precision="datetime"),
        temporal=TemporalConfig(year=True, month=True, quarter=True, fiscal_year=True),
        completeness=CompletenessConfig(date_col="inspection_date"),
    )

    # Single source of truth — these two always agree:
    transform_fn = enrichments.build_transform_fn()
    derived_cols = enrichments.build_derived_columns()
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import polars as pl
from dagster import TableColumn

from opendata_framework.enrichments.temporal import RecordDatePrecision
from opendata_framework.enrichments import (
    add_borough_key,
    add_completeness_flags,
    add_location_flag,
    add_record_timestamp,
    add_temporal_columns,
    enforce_timezone,
)
from opendata_framework.enrichments.catalog import (
    borough_key_columns,
    completeness_flag_columns,
    location_flag_columns,
    record_timestamp_columns,
    temporal_columns,
)


@dataclass(frozen=True)
class BoroughConfig:
    """Config for ``add_borough_key()``."""

    source_col: str
    key: bool = True
    code: bool = False
    canonical_name: bool = False


@dataclass(frozen=True)
class LocationConfig:
    """Config for ``add_location_flag()``."""

    lat_col: str = "latitude"
    lon_col: str = "longitude"


@dataclass(frozen=True)
class TimestampConfig:
    """Config for ``add_record_timestamp()``."""

    source_col: str
    precision: RecordDatePrecision = "day"


@dataclass(frozen=True)
class TemporalConfig:
    """Config for ``add_temporal_columns()``."""

    year: bool = True
    month: bool = True
    quarter: bool = True
    day_of_week: bool = False
    fiscal_year: bool = False
    season: bool = False
    year_month_key: bool = False
    hour: bool = False
    is_overnight: bool = False


@dataclass(frozen=True)
class CompletenessConfig:
    """Config for ``add_completeness_flags()``.

    Note: ``lat_col`` / ``lon_col`` are deliberately omitted.  When a
    ``LocationConfig`` is present, ``has_location`` is already produced by
    ``location_flag_columns()``.  Including them here would create a
    duplicate column.
    """

    date_col: str | None = None
    geo_id_cols: list[str] | None = None
    custom_flags: dict[str, list[str]] | None = None


@dataclass(frozen=True)
class StandardEnrichments:
    """Declarative config for standard enrichment pipeline.

    ``build_derived_columns()`` and ``build_transform_fn()`` use the same
    parameters, so their outputs can never diverge.
    """

    borough: BoroughConfig | None = None
    location: LocationConfig | None = None
    timestamp: TimestampConfig | None = None
    temporal: TemporalConfig | None = None
    completeness: CompletenessConfig | None = None
    timezone_columns: list[str] | None = None
    extra_columns: list[TableColumn] = field(default_factory=list)

    def build_derived_columns(self) -> list[TableColumn]:
        """Generate the ``derived_columns`` list for ``build_table_schema``."""
        cols: list[TableColumn] = []
        if self.borough:
            cols.extend(borough_key_columns(
                key=self.borough.key,
                code=self.borough.code,
                canonical_name=self.borough.canonical_name,
            ))
        if self.location:
            cols.extend(location_flag_columns())
        if self.timestamp:
            cols.extend(record_timestamp_columns())
        if self.temporal:
            cols.extend(temporal_columns(
                year=self.temporal.year,
                month=self.temporal.month,
                quarter=self.temporal.quarter,
                day_of_week=self.temporal.day_of_week,
                fiscal_year=self.temporal.fiscal_year,
                season=self.temporal.season,
                year_month_key=self.temporal.year_month_key,
                hour=self.temporal.hour,
                is_overnight=self.temporal.is_overnight,
            ))
        if self.completeness:
            cols.extend(completeness_flag_columns(
                date_col=self.completeness.date_col,
                geo_id_cols=self.completeness.geo_id_cols,
                custom_flags=self.completeness.custom_flags,
            ))
        cols.extend(self.extra_columns)
        names = [c.name for c in cols]
        dupes = [n for n in set(names) if names.count(n) > 1]
        if dupes:
            msg = (
                f"Duplicate derived column names: {dupes}. "
                "Check for overlap between LocationConfig and "
                "CompletenessConfig custom_flags."
            )
            raise ValueError(msg)
        return cols

    def build_transform_fn(self) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        """Generate a ``post_transform_fn`` that applies all configured enrichments."""
        def _enrich(lf: pl.LazyFrame) -> pl.LazyFrame:
            if self.timezone_columns:
                schema_names = lf.collect_schema().names()
                existing = [c for c in self.timezone_columns if c in schema_names]
                if existing:
                    lf = enforce_timezone(lf, existing)
            if self.borough:
                lf = add_borough_key(
                    lf, self.borough.source_col,
                    key=self.borough.key,
                    code=self.borough.code,
                    canonical_name=self.borough.canonical_name,
                )
            if self.location:
                lf = add_location_flag(lf, self.location.lat_col, self.location.lon_col)
            if self.timestamp:
                lf = add_record_timestamp(
                    lf, pl.col(self.timestamp.source_col), self.timestamp.precision,
                )
            if self.temporal:
                lf = add_temporal_columns(
                    lf,
                    year=self.temporal.year,
                    month=self.temporal.month,
                    quarter=self.temporal.quarter,
                    day_of_week=self.temporal.day_of_week,
                    fiscal_year=self.temporal.fiscal_year,
                    season=self.temporal.season,
                    year_month_key=self.temporal.year_month_key,
                    hour=self.temporal.hour,
                    is_overnight=self.temporal.is_overnight,
                )
            if self.completeness:
                lf = add_completeness_flags(
                    lf,
                    date_col=self.completeness.date_col,
                    geo_id_cols=self.completeness.geo_id_cols,
                    custom_flags=self.completeness.custom_flags,
                )
            return lf
        return _enrich
