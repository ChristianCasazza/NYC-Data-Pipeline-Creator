# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///

"""
fetch_catalog.py — Fetch NYC Open Data catalog ranked by popularity.

Usage:
    uv run .agents/skills/socrata-builder/scripts/fetch_catalog.py
    uv run .agents/skills/socrata-builder/scripts/fetch_catalog.py --max 500
    uv run .agents/skills/socrata-builder/scripts/fetch_catalog.py --max 150 -o catalog.csv

Queries the Socrata Discovery API for datasets on data.cityofnewyork.us,
sorted by total page views (descending). Outputs a CSV with metadata useful
for prioritizing which datasets to build pipelines for.

Only includes tabular datasets (skips maps, charts, external links, filtered views).
Cross-references against already-built assets in this repo.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys

import requests

DISCOVERY_API = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.cityofnewyork.us"
PAGE_SIZE = 100

# Socrata dataset IDs already built in this repo.
# Used to flag rows so you know what's already covered.
EXISTING_DATASET_IDS = {
    "erm2-nwe9",  # nyc_311_sample
    "tg4x-b46p",  # nyc_film_permits
    "aq7i-eu5q",  # nyc_floodnet_flooding_events
    "kb2e-tjy3",  # nyc_floodnet_sensor_metadata
    "ebb7-mvp5",  # nyc_dsny_monthly_tonnage
    "h9gi-nx95",  # nyc_motor_vehicle_collisions
}

CSV_COLUMNS = [
    "rank",
    "dataset_id",
    "name",
    "description",
    "category",
    "tags",
    "views_total",
    "views_last_month",
    "column_count",
    "owner",
    "updated_at",
    "created_at",
    "url",
    "already_exists",
]


def fetch_page(offset: int) -> list[dict]:
    """Fetch one page of catalog results from the Discovery API."""
    resp = requests.get(
        DISCOVERY_API,
        params={
            "domains": DOMAIN,
            "search_context": DOMAIN,
            "only": "datasets",
            "order": "page_views_total",
            "limit": PAGE_SIZE,
            "offset": offset,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def parse_result(result: dict, rank: int) -> dict:
    """Extract flat CSV row from a Discovery API result."""
    resource = result.get("resource", {})
    classification = result.get("classification", {})
    owner = result.get("owner", {})
    page_views = resource.get("page_views", {})

    dataset_id = resource.get("id", "")
    description = resource.get("description", "") or ""
    # Truncate long descriptions for CSV readability
    if len(description) > 300:
        description = description[:297] + "..."

    tags = classification.get("domain_tags") or []

    return {
        "rank": rank,
        "dataset_id": dataset_id,
        "name": resource.get("name", ""),
        "description": description.replace("\n", " ").replace("\r", ""),
        "category": classification.get("domain_category", ""),
        "tags": "; ".join(tags) if tags else "",
        "views_total": page_views.get("page_views_total", 0),
        "views_last_month": page_views.get("page_views_last_month", 0),
        "column_count": len(resource.get("columns_field_name") or []),
        "owner": owner.get("display_name", ""),
        "updated_at": resource.get("updatedAt", ""),
        "created_at": resource.get("createdAt", ""),
        "url": result.get("link", ""),
        "already_exists": dataset_id in EXISTING_DATASET_IDS,
    }


def fetch_catalog(max_datasets: int) -> list[dict]:
    """Fetch up to max_datasets from the catalog, paginating as needed."""
    rows: list[dict] = []
    offset = 0
    rank = 1

    while len(rows) < max_datasets:
        page = fetch_page(offset)
        if not page:
            break

        for result in page:
            if len(rows) >= max_datasets:
                break
            rows.append(parse_result(result, rank))
            rank += 1

        offset += PAGE_SIZE
        print(
            f"  Fetched {min(len(rows), max_datasets)}/{max_datasets} datasets...",
            file=sys.stderr,
        )

    return rows


def write_csv(rows: list[dict], output: io.TextIOBase) -> None:
    """Write rows as CSV to the given file-like object."""
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NYC Open Data catalog ranked by popularity",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=150,
        help="Maximum number of datasets to fetch (default: 150)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file path (default: stdout)",
        default=None,
    )
    args = parser.parse_args()

    print(f"Fetching top {args.max} datasets from {DOMAIN}...", file=sys.stderr)
    rows = fetch_catalog(args.max)

    existing_count = sum(1 for r in rows if r["already_exists"])
    print(
        f"Done. {len(rows)} datasets fetched, {existing_count} already built.",
        file=sys.stderr,
    )

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            write_csv(rows, f)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        write_csv(rows, sys.stdout)


if __name__ == "__main__":
    main()
