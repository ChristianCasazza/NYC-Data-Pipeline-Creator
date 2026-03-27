# /// script
# requires-python = ">=3.10"
# dependencies = ["requests", "beautifulsoup4"]
# ///

"""
fetch_socrata_metadata.py — Fetch and structure Socrata dataset metadata for pipeline generation.

Usage:
    uv run .agents/skills/socrata-builder/scripts/fetch_socrata_metadata.py <SOCRATA_URL>
    uv run .agents/skills/socrata-builder/scripts/fetch_socrata_metadata.py <SOCRATA_URL> -o output.json

Accepts any Socrata URL format:
    - https://data.cityofnewyork.us/Environment/FloodNet-.../aq7i-eu5q
    - https://data.cityofnewyork.us/Environment/FloodNet-.../aq7i-eu5q/about_data
    - https://data.cityofnewyork.us/resource/aq7i-eu5q.json
    - https://data.ny.gov/resource/aq7i-eu5q.geojson

Outputs structured JSON with everything needed to generate a create_socrata_pipeline() call.

Partitioning logic (from first principles):
    < 5M rows       → no partitions on any stage
    5M–500M rows    → monthly landing/raw, yearly clean (staged)
    > 500M rows     → monthly landing/raw, monthly clean
    equality column → yearly all stages (when only a year column is available)
    no date column  → unpartitioned regardless of size
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PARTITION_THRESHOLD = 5_000_000        # Below this: no partitions
MONTHLY_CLEAN_THRESHOLD = 500_000_000  # Above this: monthly clean too

# Field name patterns that suggest integer rather than float
_INTEGER_FIELD_PATTERNS = (
    "_cd", "_code", "_id", "_key", "precinct", "district",
    "bin", "zipcode", "zip_code", "census_tract", "block", "lot",
)

# Preferred date column names — ranked by how likely they represent "when the
# event happened" vs. a system/audit timestamp.  Higher score = better candidate.
_DATE_COLUMN_SCORES: dict[str, int] = {
    "created_date": 90,
    "reported_date": 90,
    "inspection_date": 90,
    "occur_date": 90,
    "arrest_date": 90,
    "issue_date": 90,
    "incident_date": 85,
    "violation_date": 85,
    "complaint_date": 85,
    "record_date": 60,
    "closed_date": 50,
    "resolution_date": 50,
}

# Column name fragments that indicate a non-event attribute date (e.g., an
# employee's hire date, an agency founding date).  These are poor partition
# candidates because they don't spread rows evenly across time windows.
_NON_EVENT_DATE_FRAGMENTS = (
    "agency_start", "employee_start", "hire_date", "birth_date",
    "termination_date", "end_date", "expiration",
    "start_date",  # ambiguous — often an entity attribute, not an event
)

# Patterns that suggest a year-only equality column (not a real date)
_YEAR_COLUMN_PATTERNS = ("fiscal_year", "violation_year", "calendar_year", "budget_year")


# --------------------------------------------------------------------------- #
# Socrata type -> Polars type mapping
# --------------------------------------------------------------------------- #

def _socrata_type_to_polars(field_name: str, data_type: str, fmt: dict | None) -> str:
    """Map a Socrata dataTypeName to a Polars type string."""
    if data_type == "text":
        return "pl.Utf8"

    if data_type == "number":
        lower = field_name.lower()
        if any(pat in lower for pat in _INTEGER_FIELD_PATTERNS):
            return "pl.Int64"
        return "pl.Float64"

    if data_type == "calendar_date":
        view = (fmt or {}).get("view")
        if isinstance(view, dict):
            vtype = view.get("type")
            if vtype == "date":
                return "pl.Date"
        elif isinstance(view, str):
            if view == "date":
                return "pl.Date"
        return "pl.Datetime"

    if data_type == "checkbox":
        return "pl.Boolean"

    if data_type in ("money", "percent"):
        return "pl.Float64"

    # Geometry and other types -> string
    return "pl.Utf8"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _strip_html(txt: str | None) -> str:
    """Remove HTML tags from text."""
    return BeautifulSoup(txt or "", "html.parser").get_text(" ", strip=True)


def _extract_id(url: str) -> str | None:
    """Extract the four-by-four dataset identifier from any Socrata URL."""
    m = re.search(r"/([A-Za-z0-9]{4}-[A-Za-z0-9]{4})(?:[.?/]|$)", url)
    return m[1].lower() if m else None


def _slugify(text: str) -> str:
    """Create a Python-safe snake_case name from a title."""
    s = text.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _get_row_count_from_cached(columns: list[dict]) -> int | None:
    """Extract approximate row count from column cachedContents.

    Socrata stores per-column statistics.  We take the first column that has
    cachedContents and compute: non_null + null = total rows.
    """
    for col in columns:
        cached = col.get("cachedContents")
        if not cached:
            continue
        non_null = cached.get("non_null")
        null_count = cached.get("null")
        if non_null is not None:
            total = int(non_null)
            if null_count is not None:
                total = int(non_null) + int(null_count)
            return total
    return None


def _get_row_count_from_api(base_domain: str, dataset_id: str, scheme: str = "https") -> int | None:
    """Fallback: query the SODA API directly for an exact row count.

    Uses: GET /resource/{id}.json?$select=count(*)
    This is a lightweight query that Socrata handles server-side.
    """
    url = f"{scheme}://{base_domain}/resource/{dataset_id}.json?$select=count(*)"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Response is [{"count": "12345"}] or [{"count_1": "12345"}]
        if data and isinstance(data, list) and len(data) > 0:
            row = data[0]
            for v in row.values():
                return int(v)
    except (requests.RequestException, ValueError, KeyError):
        pass
    return None


def _get_row_count(columns: list[dict], base_domain: str, dataset_id: str, scheme: str = "https") -> tuple[int | None, str]:
    """Get row count, trying cachedContents first then falling back to API.

    Returns (count, source) where source is "cached" | "api" | "unknown".
    """
    count = _get_row_count_from_cached(columns)
    if count is not None:
        return count, "cached"

    count = _get_row_count_from_api(base_domain, dataset_id, scheme)
    if count is not None:
        return count, "api"

    return None, "unknown"


# --------------------------------------------------------------------------- #
# Partition column scoring
# --------------------------------------------------------------------------- #

def _score_date_column(field_name: str) -> int:
    """Score a calendar_date column as a partition candidate.

    Higher score = better candidate for the WHERE clause.
    We prefer columns that represent "when the event occurred" over system
    timestamps, audit fields, or entity attribute dates (hire date, etc.).
    """
    lower = field_name.lower()

    # System columns are last-resort — they represent when Socrata ingested
    # the row, not when the event happened.
    if lower in (":created_at", ":updated_at"):
        return 10

    # Non-event attribute dates (employee hire date, agency start date, etc.)
    # These don't spread rows evenly across time — most rows cluster around a
    # few values.  Score them below year-equality columns so the engine prefers
    # fiscal_year over agency_start_date.
    if any(frag in lower for frag in _NON_EVENT_DATE_FRAGMENTS):
        return 20

    # Exact match on known good column names
    if lower in _DATE_COLUMN_SCORES:
        return _DATE_COLUMN_SCORES[lower]

    # Suffix heuristics
    if lower.endswith("_date") or lower.endswith("_datetime"):
        return 75
    if lower.endswith("_time") or lower.endswith("_timestamp"):
        return 70
    if "date" in lower:
        return 65
    if "time" in lower:
        return 60

    # Generic calendar_date column without date/time in the name
    return 40


def _get_date_range_from_api(
    base_domain: str,
    dataset_id: str,
    field_name: str,
    scheme: str = "https",
) -> tuple[str | None, str | None]:
    """Fallback: query the SODA API for min/max of a date or year column.

    Uses: GET /resource/{id}.json?$select=min(col),max(col)
    """
    url = (
        f"{scheme}://{base_domain}/resource/{dataset_id}.json"
        f"?$select=min({field_name}),max({field_name})"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data and isinstance(data, list) and len(data) > 0:
            row = data[0]
            values = list(row.values())
            date_min = values[0] if len(values) > 0 else None
            date_max = values[1] if len(values) > 1 else None
            return date_min, date_max
    except (requests.RequestException, ValueError, KeyError, IndexError):
        pass
    return None, None


def _extract_year(date_str: str | None) -> str | None:
    """Extract a 4-digit year from a date string."""
    if not date_str or not isinstance(date_str, str):
        return None
    m = re.match(r"(\d{4})", date_str)
    return m[1] if m else None


def _is_plausible_date_range(date_min: str | None, date_max: str | None) -> bool:
    """Check if a date range looks plausible for partitioning.

    Rejects ranges that span implausible periods (e.g., 1901-9999 from
    default/sentinel values) which indicate the column is an entity attribute,
    not an event timestamp.
    """
    min_year = _extract_year(date_min)
    max_year = _extract_year(date_max)
    if not min_year or not max_year:
        return True  # Can't check — assume plausible
    try:
        span = int(max_year) - int(min_year)
        # A 200+ year span is almost certainly sentinel values
        if span > 200:
            return False
        # Min year before 1970 is suspicious for modern datasets
        if int(min_year) < 1970 and span > 100:
            return False
    except ValueError:
        pass
    return True


def _find_year_equality_column(
    columns: list[dict],
    base_domain: str,
    dataset_id: str,
    scheme: str = "https",
) -> dict | None:
    """Find a year-based equality partition column (e.g., fiscal_year).

    These are number or text columns whose name matches a known year pattern
    and whose cached values look like years (4-digit integers).
    Falls back to API query if cachedContents is missing.
    """
    for col in columns:
        field = col.get("fieldName", "")
        dtype = col.get("dataTypeName", "")
        if dtype not in ("number", "text"):
            continue
        if not any(pat in field.lower() for pat in _YEAR_COLUMN_PATTERNS):
            continue

        # Try cachedContents first
        cached = col.get("cachedContents") or {}
        smallest = str(cached.get("smallest", ""))
        largest = str(cached.get("largest", ""))

        if re.match(r"^(19|20)\d{2}", smallest):
            return {"column": field, "min": smallest, "max": largest}

        # Fallback: query API for min/max
        api_min, api_max = _get_date_range_from_api(base_domain, dataset_id, field, scheme)
        if api_min and re.match(r"^(19|20)\d{2}", str(api_min)):
            return {"column": field, "min": str(api_min), "max": str(api_max)}

    return None


def _find_best_date_column(
    columns: list[dict],
    base_domain: str,
    dataset_id: str,
    scheme: str = "https",
) -> dict | None:
    """Find the best calendar_date column for time-based partitioning.

    Scores all calendar_date columns, resolves date ranges from cachedContents
    or API fallback, penalizes implausible ranges, and returns the best candidate.
    """
    candidates: list[tuple[int, dict]] = []

    for col in columns:
        if col.get("dataTypeName") != "calendar_date":
            continue
        field = col.get("fieldName", "")
        score = _score_date_column(field)
        cached = col.get("cachedContents") or {}

        date_min = cached.get("smallest")
        date_max = cached.get("largest")
        range_source = "cached" if date_min else None

        # Fallback: query API for date range if cachedContents is empty
        if not date_min:
            date_min, date_max = _get_date_range_from_api(
                base_domain, dataset_id, field, scheme,
            )
            range_source = "api" if date_min else None

        # Penalize implausible date ranges (e.g., 1901-9999 sentinel values).
        # This catches columns like agency_start_date even if the name scoring
        # didn't already demote them.
        if date_min and not _is_plausible_date_range(date_min, date_max):
            score = min(score, 15)

        candidates.append((score, {
            "column": field,
            "min": date_min,
            "max": date_max,
            "score": score,
            "range_source": range_source,
        }))

    if not candidates:
        return None

    # Return highest-scoring candidate
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _suggest_partitioning(
    row_count: int | None,
    columns: list[dict],
    base_domain: str = "",
    dataset_id: str = "",
    scheme: str = "https",
) -> dict:
    """Recommend a partitioning strategy from first principles.

    Decision tree:
        1. Find the best partition column (date or year-equality).
        2. If no column found → unpartitioned (warn user).
        3. If row_count < 5M → unpartitioned.
        4. If row_count >= 5M and time-based column:
            a. < 500M  → monthly landing/raw, yearly clean (staged).
            b. >= 500M → monthly landing/raw, monthly clean.
        5. If row_count >= 5M and equality column only:
            → yearly all stages.
    """
    result: dict = {
        "recommended": "none",
        "reason": "",
        "partition_col": None,
        "filter_type": None,
        "start_year": None,
        "start_date": None,
        "raw_granularity": None,
        "clean_granularity": None,
        "limit": 50_000,
    }

    # --- Find candidates ---
    best_date = _find_best_date_column(columns, base_domain, dataset_id, scheme)
    year_equality = _find_year_equality_column(columns, base_domain, dataset_id, scheme)

    # Also check for text columns with date-like names — these are common in
    # datasets where Socrata stores dates as text (e.g., parking violations
    # has issue_date as text).  We can't use them for time-range filtering, but
    # we note them for the user.
    text_date_hints = [
        col.get("fieldName") for col in columns
        if col.get("dataTypeName") == "text"
        and ("date" in (col.get("fieldName") or "").lower()
             or "time" in (col.get("fieldName") or "").lower())
        and not (col.get("fieldName") or "").startswith(":@")
    ]

    # Pick the primary partition column.
    #
    # Decision: prefer a strong event-date column (score >= 50) over an
    # equality column, because time-based supports monthly granularity.
    # But if the best date column is weak (non-event attribute like
    # agency_start_date, score < 50) AND a year-equality column exists,
    # prefer the equality column — it directly represents the business
    # partitioning of the data.
    use_date = False
    use_equality = False

    if best_date and year_equality:
        if best_date.get("score", 0) >= 50:
            use_date = True
        else:
            use_equality = True
    elif best_date:
        use_date = True
    elif year_equality:
        use_equality = True

    if use_date:
        result["partition_col"] = best_date["column"]
        result["filter_type"] = "time"
        result["start_year"] = _extract_year(best_date.get("min"))
        result["start_date"] = best_date.get("min")
        date_min_display = best_date.get("min", "?")
        date_max_display = best_date.get("max", "?")
    elif use_equality:
        result["partition_col"] = year_equality["column"]
        result["filter_type"] = "equality"
        result["start_year"] = _extract_year(year_equality.get("min"))
        date_min_display = year_equality.get("min", "?")
        date_max_display = year_equality.get("max", "?")
    else:
        # No usable partition column found.
        hints = ""
        if text_date_hints:
            hints = (
                f" Text columns with date-like names found: {text_date_hints}. "
                "These may contain parseable dates but Socrata types them as text."
            )
        result["reason"] = (
            "No calendar_date or year column found in metadata. "
            "Cannot auto-partition."
            f"{hints} "
            "Note: Socrata system column ':created_at' is always available as a "
            "last-resort partition column (use filter_type='time') but is not "
            "listed in dataset metadata."
        )
        result["text_date_hints"] = text_date_hints
        return result

    # --- Apply row-count thresholds ---
    if row_count is None:
        result["reason"] = (
            "Could not determine row count from metadata. Defaulting to unpartitioned. "
            f"Partition column '{result['partition_col']}' available if needed "
            f"(range: {date_min_display} to {date_max_display})."
        )
        return result

    if row_count < PARTITION_THRESHOLD:
        result["reason"] = (
            f"Small dataset ({row_count:,} rows, threshold {PARTITION_THRESHOLD:,}). "
            f"No partitions needed. Partition column '{result['partition_col']}' available "
            f"if dataset grows (range: {date_min_display} to {date_max_display})."
        )
        return result

    # --- Dataset is large enough to partition ---
    result["limit"] = 500_000

    if result["filter_type"] == "equality":
        # Year-equality columns can only do yearly
        result["recommended"] = "yearly"
        result["raw_granularity"] = "yearly"
        result["clean_granularity"] = "yearly"
        result["reason"] = (
            f"Large dataset ({row_count:,} rows). "
            f"Column '{result['partition_col']}' is year-based (equality filter). "
            f"Yearly partitions on all stages "
            f"(range: {date_min_display} to {date_max_display})."
        )
        return result

    # Time-based column — can support monthly granularity
    if row_count >= MONTHLY_CLEAN_THRESHOLD:
        result["recommended"] = "monthly_all"
        result["raw_granularity"] = "monthly"
        result["clean_granularity"] = "monthly"
        result["reason"] = (
            f"Very large dataset ({row_count:,} rows, above {MONTHLY_CLEAN_THRESHOLD:,}). "
            f"Monthly partitions on all stages. "
            f"Column '{result['partition_col']}' "
            f"(range: {date_min_display} to {date_max_display})."
        )
    else:
        result["recommended"] = "monthly_to_yearly"
        result["raw_granularity"] = "monthly"
        result["clean_granularity"] = "yearly"
        result["reason"] = (
            f"Large dataset ({row_count:,} rows, above {PARTITION_THRESHOLD:,}). "
            f"Monthly landing/raw, yearly clean (staged). "
            f"Column '{result['partition_col']}' "
            f"(range: {date_min_display} to {date_max_display})."
        )

    return result


# --------------------------------------------------------------------------- #
# Inferences
# --------------------------------------------------------------------------- #

def _infer_geographic_scope(domain: str) -> str:
    """Infer geographic scope from the Socrata domain hostname."""
    lower = domain.lower()
    if "cityofnewyork" in lower:
        return "nyc"
    if "data.ny.gov" in lower:
        return "nys"
    return "unknown"


_CATEGORY_TO_DOMAIN = {
    "public safety": "public_safety",
    "health": "health",
    "transportation": "transportation",
    "housing & development": "housing",
    "housing": "housing",
    "city government": "finance",
    "government & finance": "finance",
    "education": "education",
    "social services": "social_services",
    "environment": "environment",
    "recreation": "recreation",
    "business": "business",
}


def _infer_domain(category: str | None) -> str:
    """Map Socrata category to our domain string."""
    if not category:
        return "unknown"
    return _CATEGORY_TO_DOMAIN.get(category.lower(), "unknown")


def _frequency(meta: dict, base_domain: str) -> str | None:
    """Retrieve update/posting frequency from Socrata metadata."""
    md = meta.get("metadata") or meta.get("metaData") or {}
    custom = md.get("custom_fields") or md.get("customFields") or {}
    label, fallback = (
        ("posting frequency", md.get("postingFrequency"))
        if "data.ny.gov" in base_domain.lower()
        else ("update frequency", md.get("updateFrequency"))
    )
    for panel in custom.values():
        if isinstance(panel, dict):
            for k, v in panel.items():
                if label in k.lower():
                    return v
    return fallback


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def fetch_metadata(url: str) -> dict:
    """Fetch and structure Socrata metadata from a dataset URL."""
    dataset_id = _extract_id(url)
    if not dataset_id:
        return {"error": f"Cannot parse dataset ID from: {url}"}

    parsed = urlparse(url)
    base_domain = parsed.netloc

    meta_url = f"{parsed.scheme}://{base_domain}/api/views/{dataset_id}.json"
    resp = requests.get(meta_url, timeout=30)
    resp.raise_for_status()
    meta = resp.json()

    raw_columns = meta.get("columns", [])

    # Filter columns
    columns = []
    for c in raw_columns:
        field = c.get("fieldName", "")
        if field.startswith(":@computed_region_"):
            continue
        polars_type = _socrata_type_to_polars(
            field,
            c.get("dataTypeName", "text"),
            c.get("format"),
        )
        columns.append({
            "field_name": field,
            "display_name": c.get("name", ""),
            "description": _strip_html(c.get("description")),
            "socrata_type": c.get("dataTypeName", ""),
            "polars_type": polars_type,
            "is_system_column": field.startswith(":"),
        })

    row_count, row_count_source = _get_row_count(raw_columns, base_domain, dataset_id, parsed.scheme)
    title = meta.get("name", "")
    category = meta.get("category", "")
    geographic_scope = _infer_geographic_scope(base_domain)

    # Partitioning uses raw columns (needs cachedContents + API fallback)
    partitioning = _suggest_partitioning(row_count, raw_columns, base_domain, dataset_id, parsed.scheme)

    return {
        "dataset_id": dataset_id,
        "base_domain": base_domain,
        "title": title,
        "description": _strip_html(meta.get("description")),
        "category": category,
        "owner": (meta.get("tableAuthor") or {}).get("displayName", ""),
        "update_frequency": _frequency(meta, base_domain),
        "source_url": url.split("/about_data")[0],

        # Inferences
        "suggested_asset_name": f"{geographic_scope}_{_slugify(title)}",
        "geographic_scope": geographic_scope,
        "domain": _infer_domain(category),

        # Data stats
        "row_count": row_count,
        "row_count_source": row_count_source,
        "total_columns": len([c for c in columns if not c["is_system_column"]]),

        # Partitioning recommendation
        "partitioning": partitioning,

        # Full column list
        "columns": columns,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Socrata metadata for pipeline generation",
    )
    parser.add_argument("url", help="Socrata dataset URL")
    parser.add_argument(
        "-o", "--output",
        help="Output file path (default: stdout)",
        default=None,
    )
    args = parser.parse_args()

    result = fetch_metadata(args.url)

    output = json.dumps(result, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Wrote metadata to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
