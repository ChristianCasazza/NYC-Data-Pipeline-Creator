# opendata_framework/dagster/resources/socrata_resource.py

from __future__ import annotations

import itertools
from collections.abc import Generator, Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from pydantic import PrivateAttr

_TIMEOUT_S = 90
_BACKOFF_FACTOR_S = 1
_STREAM_CHUNK_SIZE = 8192
_RETRY = Retry(
    total=5,
    backoff_factor=_BACKOFF_FACTOR_S,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)


def _peek_stream(stream_iter: Iterator[bytes]) -> tuple[bytes | None, Iterator[bytes]]:
    """
    Peek at first chunk of stream without consuming it.
    Returns (first_chunk_or_none, reconstituted_iterator).
    """
    first_chunk = next(stream_iter, None)
    if first_chunk is None:
        return None, iter([])
    return first_chunk, itertools.chain([first_chunk], stream_iter)


class SocrataResource(ConfigurableResource):
    """
    Reusable resource for hitting Socrata endpoints with robust retries.
    Uses the stable SODA v2 API for CSV streaming and pagination.
    """

    api_token: str = EnvVar("SOCRATA_API_TOKEN")
    base_domain: str = "data.ny.gov"

    _session: requests.Session | None = PrivateAttr(default=None)

    def _get_session(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"X-App-Token": self.api_token})
            s.mount("http://", HTTPAdapter(max_retries=_RETRY))
            s.mount("https://", HTTPAdapter(max_retries=_RETRY))
            self._session = s
        return self._session

    def fetch_data(
        self,
        endpoint_identifier: str,
        query: dict[str, Any] | str,
        base_domain: str | None = None,
    ) -> list[dict]:
        """Fetch JSON data from Socrata endpoint."""
        domain = base_domain or self.base_domain
        sess = self._get_session()

        url = f"https://{domain}/resource/{endpoint_identifier}.json"
        resp = sess.get(url, params=query, timeout=_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()

    def get_csv_page_generator(
        self,
        endpoint: str,
        *,
        where_clause: str = "1=1",
        base_domain: str | None = None,
        limit: int = 50_000,
        order_field: str = ":id",
    ) -> Generator[tuple[int, Iterator[bytes]], None, None]:
        """
        Yields pages of CSV data using the SODA v2 API.
        Returns: Generator yielding (batch_index, byte_stream_iterator)

        Termination Logic:
        1. SODA v2 returns a header row even if the offset is beyond the record count.
        2. A batch is considered "Final" if it contains fewer than 'limit' rows of data.
        """
        logger = get_dagster_logger()
        domain = base_domain or self.base_domain
        sess = self._get_session()
        
        url = f"https://{domain}/resource/{endpoint}.csv"

        batch_idx = 0
        should_continue = True

        while should_continue:
            offset = batch_idx * limit
            params = {
                "$select": "*",
                "$limit": str(limit),
                "$offset": str(offset),
                "$order": order_field,
            }
            
            if where_clause and where_clause != "1=1":
                params["$where"] = where_clause

            logger.debug(f"[Socrata] Fetching batch {batch_idx} (Offset {offset}) from {url}...")

            response = sess.get(url, params=params, stream=True, timeout=_TIMEOUT_S)
            response.raise_for_status()

            # We wrap the response in a counter to detect partial pages
            raw_iter = response.iter_content(chunk_size=_STREAM_CHUNK_SIZE)
            
            # Peek to check for empty response
            first_chunk, reconstituted_iter = _peek_stream(raw_iter)
            if first_chunk is None:
                break

            # To avoid loading into memory, we create a wrapper that counts newlines
            # as it streams to the IO Manager.
            row_count = [0] 

            def counting_wrapper(inner_iter: Iterator[bytes]) -> Generator[bytes, None, None]:
                for chunk in inner_iter:
                    row_count[0] += chunk.count(b"\n")
                    yield chunk

            # Yield the counting stream to be written to disk
            yield (batch_idx, counting_wrapper(reconstituted_iter))

            # Socrata v2 CSV includes 1 header row. 
            # If total rows (newlines) - 1 is less than the limit, we've hit the end.
            data_rows = row_count[0] - 1
            
            logger.info(f"[Socrata] Batch {batch_idx} processed. Received {data_rows} data rows.")

            if data_rows < limit:
                logger.info("[Socrata] Received partial or empty page. Pagination complete.")
                should_continue = False
            
            batch_idx += 1
            if batch_idx > 1000:
                logger.warning("Safety limit reached. Stopping.")
                break