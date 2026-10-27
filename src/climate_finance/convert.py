"""Currency and price conversion with the factors a release ships, applied last.

USD at current prices returns the plan unchanged. Every other target joins one factor
per provider and year from the release's `conversion_factors`. The factor is the DAC
exchange rate for current prices, or the inverse of the DAC deflator for constant
prices. A provider without its own DAC series, such as a multilateral agency,
takes the DAC countries' total, the same rule pydeflate applies.

The build materialises the factors from pydeflate's OECD DAC data when it fetches, so
a converted figure changes only with a new release. `provenance()` gives the sha256 of
the release's table and a hash of the rows a conversion read, which a result carries in
its metadata.
"""

import hashlib
import struct
from collections.abc import Sequence
from functools import partial
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.errors import InvalidInputError, SourceUnavailableError
from climate_finance.spec import Currency

DAC_TOTAL = 20001


class Conversion(BaseModel):
    """The conversion inputs behind a converted result.

    `table_sha256` is the sha256 of the release's `conversion_factors` table, and
    `factors_sha256` hashes the sorted rows of it the conversion read, so two results
    converted with different factors differ in it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    table_sha256: str
    factors_sha256: str


def target_factors(
    table: pl.LazyFrame,
    *,
    release: str,
    currency: Currency,
    prices: int | Literal["current"],
) -> pl.DataFrame | None:
    """The rows of a release's `conversion_factors` `table` for `currency` at
    `prices`, as columns `entity_code`, `year` and `factor`, or None for USD at
    current prices, which needs none.

    Raises:
        InvalidInputError: for a base year outside those the release ships for
            `currency`, naming their range.
    """
    if currency == "USD" and prices == "current":
        return None
    table = table.filter(pl.col("currency") == currency)
    if prices == "current":
        rows = table.filter(pl.col("base_year").is_null())
    else:
        first, last = (
            table.select(pl.min("base_year"), pl.max("base_year").alias("last"))
            .collect()
            .row(0)
        )
        if first is None or not first <= prices <= last:
            shipped = "none" if first is None else f"{first} to {last}"
            raise InvalidInputError(
                f"prices={prices}: release {release} ships {currency} conversion "
                f"factors for base years {shipped}"
            )
        rows = table.filter(pl.col("base_year") == prices)
    return rows.select("entity_code", "year", "factor").collect()


# The little-endian `struct` code of each factor column's dtype, so the hash reads
# the same bytes as the columns' memory without importing numpy into a base install.
_PACKED = {pl.Int16: "h", pl.Int32: "i", pl.Float64: "d"}


def factors_hash(factors: pl.DataFrame) -> str:
    """A SHA-256 over `factors` sorted by entity code, year and factor."""
    ordered = factors.sort("entity_code", "year", "factor")
    digest = hashlib.sha256()
    for column in ordered.iter_columns():
        values = column.to_list()
        digest.update(struct.pack(f"<{len(values)}{_PACKED[column.dtype]}", *values))
    return digest.hexdigest()


def provenance(factors: pl.DataFrame | None, *, table_sha256: str) -> Conversion | None:
    """What `convert` reads from `factors`, the rows `target_factors` gives from the
    table of sha256 `table_sha256`, or None for USD at current prices."""
    if factors is None:
        return None
    return Conversion(table_sha256=table_sha256, factors_sha256=factors_hash(factors))


def _require_factors(keys: pl.Series, *, release: str) -> pl.Series:
    """The factors, or an error naming every provider and year without one."""
    frame = keys.struct.unnest()
    missing = frame.filter(pl.col("factor").is_null())
    if missing.height:
        pairs = sorted(set(zip(missing["provider_code"], missing["year"], strict=True)))
        raise SourceUnavailableError(
            f"release {release} ships no conversion factor for (provider, year) "
            f"{pairs[:10]}{' and more' if len(pairs) > 10 else ''}"
        )
    return frame.get_column("factor")


def convert(
    frame: pl.LazyFrame,
    *,
    columns: Sequence[str],
    factors: pl.DataFrame | None,
    release: str,
) -> pl.LazyFrame:
    """Convert the USD current-price `columns` of `frame` with `factors`, the rows
    `target_factors` gives from `release`; None leaves `frame` unchanged.

    `frame` needs `provider_code` (Int32) and `year` (Int16).

    Raises:
        SourceUnavailableError: on collection, when a provider and year has neither
            its own DAC series nor the DAC total.
    """
    if factors is None:
        return frame
    own = factors.lazy().rename({"entity_code": "provider_code", "factor": "_own"})
    total = (
        factors.lazy()
        .filter(pl.col("entity_code") == DAC_TOTAL)
        .select("year", _total="factor")
    )
    factor = pl.struct(
        "provider_code", "year", factor=pl.coalesce("_own", "_total")
    ).map_batches(partial(_require_factors, release=release), return_dtype=pl.Float64)
    return (
        frame.join(own, on=["provider_code", "year"], how="left")
        .join(total, on="year", how="left")
        .with_columns(_factor=factor)
        .with_columns(pl.col(name) * pl.col("_factor") for name in columns)
        .drop("_own", "_total", "_factor")
    )
