# opendata_framework/dagster/resources/checkbook_resource.py

from __future__ import annotations

import csv
import io
import time
import xml.etree.ElementTree as ET
from collections.abc import Generator, Iterator
from xml.sax.saxutils import escape as xml_escape

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import PrivateAttr

_TIMEOUT_S = 90
_BATCH_SIZE = 20_000
_RETRY = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[403, 429, 500, 502, 503, 504],
    allowed_methods=["POST"],
    raise_on_status=False,
)


class CheckbookNYCResource(ConfigurableResource):
    """
    Resource for fetching data from the Checkbook NYC XML API.

    Uses HTTP POST with XML request bodies as required by the API.
    Handles pagination via records_from/max_records (API cap: 20,000 per call).
    Yields (batch_index, bytes_iterator) tuples compatible with LandingIOManager.
    """

    base_url: str = "https://www.checkbooknyc.com/api"
    timeout: int = _TIMEOUT_S
    batch_size: int = _BATCH_SIZE

    _session: requests.Session | None = PrivateAttr(default=None)

    def _get_session(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers["Content-Type"] = "application/xml"
            s.mount("http://", HTTPAdapter(max_retries=_RETRY))
            s.mount("https://", HTTPAdapter(max_retries=_RETRY))
            self._session = s
        return self._session

    @staticmethod
    def _build_request_xml(
        type_of_data: str,
        records_from: int,
        max_records: int,
        criteria: list[dict[str, str]],
        response_columns: list[str],
    ) -> str:
        """Build the XML POST body for a Checkbook NYC API request."""
        criteria_xml = ""
        for c in criteria:
            if c.get("type") == "range":
                criteria_xml += (
                    f"    <criteria>\n"
                    f"      <name>{xml_escape(c['name'])}</name>\n"
                    f"      <type>range</type>\n"
                    f"      <start>{xml_escape(c['start'])}</start>\n"
                    f"      <end>{xml_escape(c['end'])}</end>\n"
                    f"    </criteria>\n"
                )
            else:
                criteria_xml += (
                    f"    <criteria>\n"
                    f"      <name>{xml_escape(c['name'])}</name>\n"
                    f"      <type>value</type>\n"
                    f"      <value>{xml_escape(c['value'])}</value>\n"
                    f"    </criteria>\n"
                )

        cols_xml = "\n".join(f"    <column>{col}</column>" for col in response_columns)

        return (
            f"<request>\n"
            f"  <type_of_data>{type_of_data}</type_of_data>\n"
            f"  <records_from>{records_from}</records_from>\n"
            f"  <max_records>{max_records}</max_records>\n"
            f"  <search_criteria>\n"
            f"{criteria_xml}"
            f"  </search_criteria>\n"
            f"  <response_columns>\n"
            f"{cols_xml}\n"
            f"  </response_columns>\n"
            f"</request>"
        )

    @staticmethod
    def _parse_response(text: str) -> tuple[list[dict[str, str]], int]:
        """Parse XML response into (records, total_record_count).

        Handles both flat responses (Spending) and nested responses (Budget)
        where amounts are grouped under <budget_amounts> / <expenditure_amounts>.
        Nested sub-elements are flattened into the top-level record dict.
        """
        root = ET.fromstring(text)

        status_el = root.find(".//status/result")
        if status_el is not None and status_el.text != "success":
            msg_el = root.find(".//status/messages/message/description")
            desc = msg_el.text if msg_el is not None else "unknown error"
            raise RuntimeError(f"Checkbook API failure: {desc}")

        count_el = root.find(".//result_records/record_count")
        total_count = int(count_el.text) if count_el is not None else 0

        records: list[dict[str, str]] = []
        for txn in root.findall(".//transaction"):
            record: dict[str, str] = {}
            for child in txn:
                if len(child) > 0:
                    # Nested group (budget_amounts, expenditure_amounts) — flatten
                    for sub in child:
                        record[sub.tag] = (sub.text or "").strip()
                else:
                    record[child.tag] = (child.text or "").strip()
            records.append(record)

        return records, total_count

    @staticmethod
    def _records_to_csv_bytes(
        records: list[dict[str, str]],
        columns: list[str],
        include_header: bool = True,
    ) -> bytes:
        """Serialize records to CSV bytes with consistent column ordering."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        if include_header:
            writer.writeheader()
        writer.writerows(records)
        return buf.getvalue().encode("utf-8")

    def get_page_generator(
        self,
        type_of_data: str,
        criteria: list[dict[str, str]],
        response_columns: list[str],
    ) -> Generator[tuple[int, Iterator[bytes]], None, None]:
        """
        General paginated fetcher for any Checkbook NYC data type.

        Yields (batch_index, csv_bytes_iterator) tuples compatible with
        LandingIOManager.handle_output().

        Parameters
        ----------
        type_of_data:
            API domain (e.g. "Spending", "Budget", "Contracts").
        criteria:
            Search criteria dicts. Each must have "name" and "type" keys.
            Range: {"name": ..., "type": "range", "start": ..., "end": ...}
            Value: {"name": ..., "type": "value", "value": ...}
        response_columns:
            Column names to request from the API.
        """
        logger = get_dagster_logger()
        session = self._get_session()

        batch_idx = 0
        records_from = 1
        total_count: int | None = None

        while True:
            xml_body = self._build_request_xml(
                type_of_data=type_of_data,
                records_from=records_from,
                max_records=self.batch_size,
                criteria=criteria,
                response_columns=response_columns,
            )

            logger.info(
                f"[Checkbook] Batch {batch_idx}: records_from={records_from}, "
                f"type_of_data={type_of_data}"
            )

            # Retry loop for transient errors (403 rate limit, 5xx)
            resp = None
            for attempt in range(6):
                resp = session.post(self.base_url, data=xml_body, timeout=self.timeout)
                if resp.status_code == 200:
                    break
                if resp.status_code in (403, 429, 500, 502, 503, 504):
                    wait = 2 ** attempt * 5  # 5, 10, 20, 40, 80, 160s
                    logger.warning(
                        f"[Checkbook] HTTP {resp.status_code} on batch {batch_idx}, "
                        f"attempt {attempt + 1}/6. Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    resp.raise_for_status()
            else:
                resp.raise_for_status()

            try:
                records, resp_total = self._parse_response(resp.text)
            except (ET.ParseError, RuntimeError) as exc:
                logger.error(
                    f"[Checkbook] Failed to parse batch {batch_idx} "
                    f"(type_of_data={type_of_data}, records_from={records_from}): {exc}\n"
                    f"Response preview: {resp.text[:500]}"
                )
                raise

            if total_count is None:
                total_count = resp_total
                logger.info(f"[Checkbook] Total records for query: {total_count}")

            if not records:
                if batch_idx == 0:
                    logger.info("[Checkbook] No records found for query.")
                break

            logger.info(f"[Checkbook] Batch {batch_idx}: parsed {len(records)} records")

            csv_bytes = self._records_to_csv_bytes(
                records, columns=response_columns, include_header=True
            )
            yield (batch_idx, iter([csv_bytes]))

            records_from += self.batch_size
            batch_idx += 1

            # Throttle to avoid 403 rate limiting from the API
            time.sleep(2.0)

            if len(records) < self.batch_size or records_from > total_count:
                logger.info("[Checkbook] Pagination complete.")
                break

            if batch_idx > 500:
                logger.warning("[Checkbook] Safety limit reached (500 batches). Stopping.")
                break

    def get_spending_page_generator(
        self,
        issue_date_start: str,
        issue_date_end: str,
        response_columns: list[str],
        type_of_data: str = "Spending",
    ) -> Generator[tuple[int, Iterator[bytes]], None, None]:
        """Convenience wrapper for spending data with issue_date range filtering."""
        criteria = [
            {
                "name": "issue_date",
                "type": "range",
                "start": issue_date_start,
                "end": issue_date_end,
            },
        ]
        yield from self.get_page_generator(
            type_of_data=type_of_data,
            criteria=criteria,
            response_columns=response_columns,
        )
