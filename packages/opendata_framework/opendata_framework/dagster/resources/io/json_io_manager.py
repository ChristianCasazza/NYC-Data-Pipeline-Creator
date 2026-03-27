# opendata_framework/dagster/resources/io/json_io_manager.py
"""
IO Manager for JSON landing data from REST APIs.

Stores API response pages as individual .json files and reads them back
as list[dict] for downstream processing.

Write protocol (handle_output):
    - list[dict]         → single page, written as one file
    - list[list[dict]]   → multiple pages, one file each
    - Generator/Iterator of list[dict] → lazy pages, one file each
    - dict               → single record, written as one file

Read protocol (load_input):
    - Returns list[dict] with all pages concatenated in file-name order

Directory layout mirrors LandingIOManager / PolarsParquetIOManager:
    - Unpartitioned: {base}/{asset}/page_0000.json
    - Yearly:        {base}/{asset}/year=2025/page_0000.json
    - Monthly:       {base}/{asset}/year=2025/month=01/page_0000.json
"""

from __future__ import annotations

import json
from collections.abc import Generator, Iterator
from typing import Any

from dagster import ConfigurableIOManager, InputContext, MetadataValue, OutputContext
from upath import UPath


class JsonIOManager(ConfigurableIOManager):
    """IO Manager that stores and retrieves JSON pages for REST API landing data."""

    base_path: str

    @property
    def _base_path(self) -> UPath:
        return UPath(self.base_path)

    def _resolve_dir(self, asset_name: str, partition_key: str | None) -> UPath:
        """Resolve the target directory using Hive-style partitioning."""
        root = self._base_path / asset_name

        if not partition_key:
            return root

        if "-" in partition_key:
            parts = partition_key.split("-")
            if (
                len(parts) >= 2
                and len(parts[0]) == 4
                and parts[0].isdigit()
                and parts[1].isdigit()
            ):
                return root / f"year={parts[0]}" / f"month={parts[1]}"

        if len(partition_key) == 4 and partition_key.isdigit():
            return root / f"year={partition_key}"

        return root / partition_key

    def _iter_pages(self, obj: Any) -> Generator[list[dict], None, None]:
        """Normalize any accepted input type into an iterator of pages."""
        if isinstance(obj, dict):
            yield [obj]
        elif isinstance(obj, (Generator, Iterator)):
            yield from obj
        elif isinstance(obj, list):
            if not obj:
                return
            if isinstance(obj[0], dict):
                # list[dict] — single page of records
                yield obj
            elif isinstance(obj[0], list):
                # list[list[dict]] — multiple pages
                yield from obj
            else:
                raise TypeError(
                    f"JsonIOManager expected list[dict] or list[list[dict]], "
                    f"got list[{type(obj[0]).__name__}]"
                )
        else:
            raise TypeError(
                f"JsonIOManager expected dict, list, or Generator. "
                f"Got {type(obj).__name__}"
            )

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Write JSON pages to individual files."""
        partition_key = context.partition_key if context.has_partition_key else None
        asset_name = context.asset_key.path[-1]
        target_dir = self._resolve_dir(asset_name, partition_key)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Clear previous pages to avoid stale data from prior materializations
        for old_file in target_dir.glob("page_*.json"):
            old_file.unlink()

        total_pages = 0
        total_records = 0

        for page_idx, page in enumerate(self._iter_pages(obj)):
            filename = f"page_{page_idx:04d}.json"
            path = target_dir / filename
            path.write_text(
                json.dumps(page, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            total_pages += 1
            total_records += len(page) if isinstance(page, list) else 1
            context.log.info(f"Wrote {len(page)} records to {filename}")

        context.add_output_metadata({
            "path": MetadataValue.path(str(target_dir)),
            "pages": total_pages,
            "records": total_records,
            "format": "json",
        })

    def load_input(self, context: InputContext) -> list[dict]:
        """Read all JSON pages and return a concatenated list of records."""
        asset_name = context.asset_key.path[-1]
        partition_key = context.partition_key if context.has_partition_key else None
        target_dir = self._resolve_dir(asset_name, partition_key)

        if not target_dir.exists():
            context.log.warning(f"Directory not found: {target_dir}. Returning empty list.")
            return []

        files = sorted(target_dir.glob("page_*.json"))

        if not files:
            context.log.warning(f"No page_*.json files in {target_dir}.")
            return []

        context.log.info(f"Loading {len(files)} JSON pages from {target_dir}")

        all_records: list[dict] = []
        for path in files:
            page = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(page, list):
                all_records.extend(page)
            elif isinstance(page, dict):
                all_records.append(page)

        context.log.info(f"Loaded {len(all_records)} total records")
        return all_records
