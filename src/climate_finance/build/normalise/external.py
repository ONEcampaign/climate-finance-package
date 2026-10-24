"""IATI and GCF rows: amounts in US dollars and recipients as DAC codes."""

import logging
from functools import cache

import polars as pl
from resolvekit import Resolver
from resolvekit.core.api.output_view import OutputView

from climate_finance.schema import EURO_ISSUER, UNSPECIFIED_RECIPIENT

logger = logging.getLogger(__name__)

# The DAC code for "Developing countries, unspecified", given to a row that names
# several countries.
MULTI_COUNTRY = UNSPECIFIED_RECIPIENT
_EURO = {"EUR": EURO_ISSUER}
_ISO_SYSTEMS = {2: "iso2", 3: "iso3"}
# Alpha-2 codes IATI uses that resolvekit 0.1.13 does not map to a DAC code:
# South Sudan, and Kosovo's user-assigned code.
_ALPHA2_GAPS = {"SS": 279, "XK": 57}


def normalise_external(
    external: pl.DataFrame, *, exchange: pl.DataFrame
) -> pl.DataFrame:
    """Fill `amount_usd` and `recipient_code` on the readers' `external` rows.

    Amounts are converted to US dollars by `to_usd`. Those with no rate for their
    currency and year keep a null `amount_usd`, and are counted in the log.

    `recipient_iso` is mapped code by code: an ISO 3166 alpha-2 or alpha-3 code to
    its DAC recipient code, and a number (an IATI region in the DAC vocabulary) as
    it stands. A value naming several countries is multi-country (`MULTI_COUNTRY`).
    Codes with no DAC recipient code leave `recipient_code` null, and are logged.

    Args:
        external: Rows in the `external` schema, as the IATI and GCF readers give.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
    """
    recipients = pl.DataFrame(
        {"recipient_iso": external["recipient_iso"].drop_nulls().unique().sort()}
    ).with_columns(
        recipient_code=pl.col("recipient_iso").map_elements(
            _recipient_code, return_dtype=pl.Int32
        )
    )
    converted = (
        to_usd(external, exchange=exchange)
        .drop("recipient_code")
        .join(recipients, on="recipient_iso", how="left", maintain_order="left")
    )
    unconverted = converted.filter(pl.col("amount_usd").is_null())
    if unconverted.height:
        logger.warning(
            "External amounts with no DAC exchange rate, left without a USD value: %s",
            dict(unconverted.group_by("currency").len().sort("currency").iter_rows()),
        )
    codes = recipients["recipient_iso"].str.split(",").explode().unique()
    if unmapped := sorted(code for code in codes if _dac_code(code) is None):
        logger.warning("External recipient codes with no DAC code: %s", unmapped)
    return converted.select(external.columns)


def to_usd(frame: pl.DataFrame, *, exchange: pl.DataFrame) -> pl.DataFrame:
    """Fill a null `amount_usd` from `amount`, `currency` and `year`.

    A non-USD amount is converted at the DAC annual rate (local currency per US
    dollar) of the country that issues its currency: the euro's for EUR, else the
    country whose ISO 3166 code begins the ISO 4217 code. An amount with no rate for
    its currency and year keeps a null `amount_usd`.
    """
    rates = exchange.select(
        issuer=pl.col("iso_code").cast(pl.String),
        year=pl.col("year").cast(pl.Int16),
        exchange_rate=pl.col("exchange_rate").cast(pl.Float64),
    )
    issuers = pl.DataFrame(
        {"currency": frame["currency"].unique().sort()}, schema={"currency": pl.String}
    ).with_columns(
        issuer=pl.col("currency").map_elements(
            _issuer, return_dtype=pl.String, skip_nulls=True
        )
    )
    return (
        frame.join(issuers, on="currency", how="left", maintain_order="left")
        .join(rates, on=["issuer", "year"], how="left", maintain_order="left")
        .with_columns(
            amount_usd=pl.coalesce(
                "amount_usd", pl.col("amount") / pl.col("exchange_rate")
            )
        )
        .select(frame.columns)
    )


def _issuer(currency: str) -> str | None:
    """The ISO 3166 alpha-3 code of the country that issues `currency`."""
    if currency in _EURO:
        return _EURO[currency]
    return _resolver("iso3").resolve(currency[:2], from_system="iso2")


def _recipient_code(recipient_iso: str) -> int | None:
    if "," in recipient_iso:
        return MULTI_COUNTRY
    return _dac_code(recipient_iso)


@cache
def _dac_code(code: str) -> int | None:
    if code.isdigit():
        return int(code)
    if code in _ALPHA2_GAPS:
        return _ALPHA2_GAPS[code]
    system = _ISO_SYSTEMS.get(len(code))
    if system is None:
        return None
    found = _resolver("oecd:recipient").resolve(code, from_system=system)
    return None if found is None else int(found)


@cache
def _resolver(output: str) -> OutputView:
    """Exact code lookups that give null for an entity without an `output` code."""
    return Resolver.lite().to(output, on_missing="null")
