"""Dagster resource wrapping RemoteDuckDBWrapper for QueryStation Arrow IPC queries."""
from __future__ import annotations

import polars as pl
from dagster import ConfigurableResource, EnvVar
from pydantic import PrivateAttr

from data_consumers import RemoteDuckDBWrapper


class QueryStationResource(ConfigurableResource):
    """Exposes QueryStation as a Dagster resource.

    Holds a RemoteDuckDBWrapper in a private attribute so the underlying
    auth token is cached across calls within a run. One public method:
    ``query(sql) -> pl.DataFrame``.
    """

    api_key: str = EnvVar("QUERYSTATION_API_KEY")
    auth_url: str = "https://api.querystation.app"

    _wrapper: RemoteDuckDBWrapper | None = PrivateAttr(default=None)

    def _client(self) -> RemoteDuckDBWrapper:
        if self._wrapper is None:
            self._wrapper = RemoteDuckDBWrapper(
                api_key=self.api_key,
                auth_url=self.auth_url,
            )
        return self._wrapper

    def query(self, sql: str) -> pl.DataFrame:
        return self._client().sql(sql)
