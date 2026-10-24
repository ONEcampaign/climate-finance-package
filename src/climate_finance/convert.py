"""Currency and price conversion through pydeflate's OECD DAC data, applied last.

USD at current prices returns the plan unchanged and skips importing pydeflate, which
imports pandas. Every other target joins one factor per provider and year: the DAC
exchange rate for current prices, or the inverse of the DAC deflator for constant
prices. A provider without its own DAC series, such as a multilateral agency, takes
the DAC countries' total, the same rule pydeflate applies.

The factors come from pydeflate's DAC data at query time, and pydeflate refreshes that
data every 30 days. `provenance()` gives the pydeflate version and a hash of the factors
used, which a result carries in its metadata to identify them.
"""

import hashlib
from collections.abc import Sequence
from functools import cache
from importlib.metadata import version
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.errors import SourceUnavailableError
from climate_finance.spec import Currency

DAC_TOTAL = 20001


class Conversion(BaseModel):
    """The conversion inputs behind a converted result.

    `factors_sha256` hashes the sorted rows of the factor table the conversion read,
    so two results converted with different factors differ in it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    pydeflate_version: str
    factors_sha256: str


@cache
def dac_factors(
    *, currency: Currency, prices: int | Literal["current"]
) -> pl.DataFrame:
    """Multipliers from current USD to `currency` at `prices`, by DAC code and year.

    Columns `entity_code` (Int32), `year` (Int16) and `factor` (Float64).
    """
    import pydeflate  # imports pandas, so only conversion loads it

    if prices == "current":
        rates = pydeflate.get_oecd_dac_exchange_rates(
            target_currency=currency, use_source_codes=True
        )
        factor = pl.col("exchange_rate")
    else:
        rates = pydeflate.get_oecd_dac_deflators(
            base_year=prices, target_currency=currency, use_source_codes=True
        )
        factor = 1 / pl.col("deflator")
    return (
        pl.from_pandas(rates)
        .select(
            pl.col("entity_code").cast(pl.Int32),
            pl.col("year").cast(pl.Int16),
            factor.cast(pl.Float64).alias("factor"),
        )
        .drop_nulls()
    )


def factors_hash(factors: pl.DataFrame) -> str:
    """A SHA-256 over `factors` sorted by entity code, year and factor."""
    ordered = factors.sort("entity_code", "year", "factor")
    digest = hashlib.sha256()
    for name in ordered.columns:
        digest.update(ordered.get_column(name).to_numpy().tobytes())
    return digest.hexdigest()


def provenance(
    *, currency: Currency, prices: int | Literal["current"]
) -> Conversion | None:
    """What `convert` reads for this target, or None for USD at current prices."""
    if currency == "USD" and prices == "current":
        return None
    return Conversion(
        pydeflate_version=version("pydeflate"),
        factors_sha256=factors_hash(dac_factors(currency=currency, prices=prices)),
    )


def _require_factors(keys: pl.Series) -> pl.Series:
    """The factors, or an error naming every provider and year without one."""
    frame = keys.struct.unnest()
    missing = frame.filter(pl.col("factor").is_null())
    if missing.height:
        pairs = sorted(set(zip(missing["provider_code"], missing["year"], strict=True)))
        raise SourceUnavailableError(
            f"pydeflate's DAC data has no conversion factor for (provider, year) "
            f"{pairs[:10]}{' and more' if len(pairs) > 10 else ''}"
        )
    return frame.get_column("factor")


def convert(
    frame: pl.LazyFrame,
    *,
    columns: Sequence[str],
    currency: Currency,
    prices: int | Literal["current"],
) -> pl.LazyFrame:
    """Convert the USD current-price `columns` of `frame` to `currency` at `prices`.

    `frame` needs `provider_code` (Int32) and `year` (Int16).

    Raises:
        SourceUnavailableError: on collection, when a provider and year has neither
            its own DAC series nor the DAC total.
    """
    if currency == "USD" and prices == "current":
        return frame
    factors = dac_factors(currency=currency, prices=prices).lazy()
    own = factors.rename({"entity_code": "provider_code", "factor": "_own"})
    total = factors.filter(pl.col("entity_code") == DAC_TOTAL).select(
        "year", _total="factor"
    )
    factor = pl.struct(
        "provider_code", "year", factor=pl.coalesce("_own", "_total")
    ).map_batches(_require_factors, return_dtype=pl.Float64)
    return (
        frame.join(own, on=["provider_code", "year"], how="left")
        .join(total, on="year", how="left")
        .with_columns(_factor=factor)
        .with_columns(pl.col(name) * pl.col("_factor") for name in columns)
        .drop("_own", "_total", "_factor")
    )
