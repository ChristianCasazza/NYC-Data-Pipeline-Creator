from __future__ import annotations

import polars as pl

# Master borough mapping — covers every variant found across NYC datasets.
# Keys are UPPERCASE for case-insensitive lookup.
# Values: (canonical_name, borough_code)
_BOROUGH_LOOKUP: dict[str, tuple[str, int]] = {
    # Full names (various casings handled by uppercasing input)
    "MANHATTAN": ("Manhattan", 1),
    "BRONX": ("Bronx", 2),
    "BROOKLYN": ("Brooklyn", 3),
    "QUEENS": ("Queens", 4),
    "STATEN ISLAND": ("Staten Island", 5),
    # Single-letter codes (arrest data)
    "M": ("Manhattan", 1),
    "B": ("Bronx", 2),
    "K": ("Brooklyn", 3),
    "Q": ("Queens", 4),
    "S": ("Staten Island", 5),
    # Capital budget single-letter codes
    "X": ("Bronx", 2),
    "R": ("Staten Island", 5),
    # Two-letter codes (PLUTO)
    "MN": ("Manhattan", 1),
    "BX": ("Bronx", 2),
    "BK": ("Brooklyn", 3),
    "QN": ("Queens", 4),
    "SI": ("Staten Island", 5),
    # Numeric string codes
    "1": ("Manhattan", 1),
    "2": ("Bronx", 2),
    "3": ("Brooklyn", 3),
    "4": ("Queens", 4),
    "5": ("Staten Island", 5),
    # Historical / alternate names
    "NEW YORK": ("Manhattan", 1),
    "KINGS": ("Brooklyn", 3),
    "RICHMOND": ("Staten Island", 5),
    # EMS dispatch variant
    "RICHMOND / STATEN ISLAND": ("Staten Island", 5),
}

# Pre-built dicts for Polars .replace()
_TO_NAME: dict[str, str] = {k: v[0] for k, v in _BOROUGH_LOOKUP.items()}
_TO_CODE: dict[str, int] = {k: v[1] for k, v in _BOROUGH_LOOKUP.items()}
_TO_KEY: dict[str, str] = {k: v[0].lower().replace(" ", "_") for k, v in _BOROUGH_LOOKUP.items()}


def borough_name_expr(col: str, *, alias: str = "borough_name") -> pl.Expr:
    """Map any known borough variant to canonical Title Case name."""
    return (
        pl.col(col).str.strip_chars().str.to_uppercase()
        .replace_strict(_TO_NAME, default=None)
        .alias(alias)
    )


def borough_code_expr(col: str, *, alias: str = "borough_code") -> pl.Expr:
    """Map any known borough variant to numeric code (1-5)."""
    return (
        pl.col(col).str.strip_chars().str.to_uppercase()
        .replace_strict(_TO_CODE, default=None)
        .cast(pl.Int32)
        .alias(alias)
    )


def borough_key_expr(col: str, *, alias: str = "borough_key") -> pl.Expr:
    """Map any known borough variant to lowercase join key (e.g., ``"staten_island"``)."""
    return (
        pl.col(col).str.strip_chars().str.to_uppercase()
        .replace_strict(_TO_KEY, default=None)
        .alias(alias)
    )


def add_borough_key(
    lf: pl.LazyFrame,
    source_col: str = "borough",
    *,
    key: bool = True,
    code: bool = False,
    canonical_name: bool = False,
) -> pl.LazyFrame:
    """Add standardized borough columns from any known variant format.

    Args:
        lf: Input LazyFrame.
        source_col: Column containing raw borough values.
        key: Add ``borough_key`` (lowercase, underscore-separated).
        code: Add ``borough_code`` (1–5 integer).
        canonical_name: Add ``borough_name`` (Title Case).
    """
    exprs: list[pl.Expr] = []
    if key:
        exprs.append(borough_key_expr(source_col))
    if code:
        exprs.append(borough_code_expr(source_col))
    if canonical_name:
        exprs.append(borough_name_expr(source_col))

    if not exprs:
        return lf
    return lf.with_columns(exprs)


def add_location_flag(
    lf: pl.LazyFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    *,
    validate_nyc_bounds: bool = True,
    alias: str = "has_location",
) -> pl.LazyFrame:
    """Add a boolean flag indicating whether lat/lon are present and valid.

    When ``validate_nyc_bounds`` is True, also checks that coordinates fall
    within the NYC bounding box (lat 40.4–40.95, lon -74.3 to -73.65).
    """
    lat = pl.col(lat_col).cast(pl.Float64, strict=False)
    lon = pl.col(lon_col).cast(pl.Float64, strict=False)

    valid = lat.is_not_null() & lon.is_not_null()
    if validate_nyc_bounds:
        valid = valid & lat.is_between(40.4, 40.95) & lon.is_between(-74.3, -73.65)

    return lf.with_columns(valid.alias(alias))


def add_nyc_bbl(
    lf: pl.LazyFrame,
    boro_col: str = "boro",
    block_col: str = "block",
    lot_col: str = "lot",
    *,
    alias: str = "bbl",
) -> pl.LazyFrame:
    """Compute 10-digit Borough-Block-Lot (BBL) string with correct zero-padding.

    BBL format: ``B`` (1 digit) + ``BLOCK`` (5 digits, zero-padded) +
    ``LOT`` (4 digits, zero-padded). Result is a string to preserve
    leading zeros.
    """
    bbl_expr = (
        pl.col(boro_col).cast(pl.Utf8).str.slice(0, 1)
        + pl.col(block_col).cast(pl.Int64).cast(pl.Utf8).str.pad_start(5, "0")
        + pl.col(lot_col).cast(pl.Int64).cast(pl.Utf8).str.pad_start(4, "0")
    ).alias(alias)
    return lf.with_columns(bbl_expr)


def add_community_district_key(
    lf: pl.LazyFrame,
    district_col: str = "community_district",
    borough_col: str | None = "borough",
    *,
    alias: str = "community_district_key",
) -> pl.LazyFrame:
    """Normalize community district to a 3-digit key: ``"{boro_code}{district:02d}"``.

    If ``borough_col`` is provided, the borough code is derived from it.
    Otherwise the district column must already contain a numeric code
    that encodes the borough (e.g., ``"101"`` for Manhattan CD 01).
    """
    district = pl.col(district_col).cast(pl.Utf8).str.strip_chars()

    if borough_col is not None:
        boro_code = (
            pl.col(borough_col).str.strip_chars().str.to_uppercase()
            .replace_strict(_TO_CODE, default=None)
            .cast(pl.Utf8)
        )
        cd_key = boro_code + district.str.pad_start(2, "0")
    else:
        cd_key = district.str.pad_start(3, "0")

    return lf.with_columns(cd_key.alias(alias))
