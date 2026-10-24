"""Bank ledgers as one `ledger` table, with amounts in US dollars."""

import logging
from collections.abc import Collection
from pathlib import Path

import polars as pl

from climate_finance.build.fetch import GCF_FILE
from climate_finance.build.normalise.external import to_usd
from climate_finance.build.readers.ledgers.finances_one import (
    PUBLISHERS as FINANCES_ONE_PUBLISHERS,
)
from climate_finance.build.readers.ledgers.finances_one import read_finances_one
from climate_finance.build.readers.ledgers.gcf import (
    PUBLISHER as GCF_PUBLISHER,
)
from climate_finance.build.readers.ledgers.gcf import (
    READINESS_PUBLISHER,
    read_gcf_disbursements,
    read_gcf_readiness,
)
from climate_finance.build.readers.ledgers.iati import read_iati_ledger
from climate_finance.build.readers.ledgers.imf import read_sdr_rates
from climate_finance.build.sources import finances_one, iati
from climate_finance.build.sources.gcf import READINESS_FILE
from climate_finance.build.sources.imf import SDR_FILE, XDR_PUBLISHERS

logger = logging.getLogger(__name__)


def normalise_ledgers(
    raw: Path,
    *,
    publishers: Collection[str],
    years: tuple[int, int],
    exchange: pl.DataFrame,
) -> pl.DataFrame:
    """Read the ledger of each of `publishers` from `raw` and convert it to US
    dollars.

    World Bank Finances One gives flows for `years`; the other ledgers give
    every transaction their files hold. Amounts are converted by `to_usd`, and the
    AfDB's SDR amounts, which have no DAC rate, at the IMF's annual average US dollar
    rate of the SDR for their year, which fetch saves when the AfDB is named. Amounts
    with no rate for their currency and year keep a null `amount_usd` and are
    counted in the log.

    Args:
        raw: The raw directory fetch wrote.
        publishers: The ledgers the build config names.
        years: The first and last years of the ledgers, as fetch recorded them.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
    """
    first, last = years
    frames = [
        _read(raw, publisher=publisher, years=range(first, last + 1))
        for publisher in publishers
    ]
    ledger = to_usd(pl.concat(frames), exchange=exchange)
    if XDR_PUBLISHERS & set(publishers):
        ledger = _sdr_to_usd(ledger, rates=read_sdr_rates(raw / SDR_FILE))
    unconverted = ledger.filter(pl.col("amount_usd").is_null())
    if unconverted.height:
        logger.warning(
            "Ledger amounts with no DAC exchange rate, left without a USD value: %s",
            dict(
                unconverted.group_by("publisher", "currency")
                .len()
                .sort("publisher", "currency")
                .select(pl.concat_str("publisher", "currency", separator=" "), "len")
                .iter_rows()
            ),
        )
    return ledger


def _sdr_to_usd(ledger: pl.DataFrame, *, rates: pl.DataFrame) -> pl.DataFrame:
    """Fill a null `amount_usd` of an XDR amount at its year's US dollar rate."""
    return (
        ledger.join(rates, on="year", how="left", maintain_order="left")
        .with_columns(
            amount_usd=pl.when(pl.col("currency") == "XDR")
            .then(pl.coalesce("amount_usd", pl.col("amount") * pl.col("usd_per_xdr")))
            .otherwise("amount_usd")
        )
        .select(ledger.columns)
    )


def _read(raw: Path, *, publisher: str, years: range) -> pl.DataFrame:
    if publisher in FINANCES_ONE_PUBLISHERS:
        return read_finances_one(
            raw / finances_one.FOLDER,
            dataset=FINANCES_ONE_PUBLISHERS[publisher],
            years=years,
        )
    if publisher == GCF_PUBLISHER:
        return read_gcf_disbursements(raw / GCF_FILE)
    if publisher == READINESS_PUBLISHER:
        return read_gcf_readiness(raw / READINESS_FILE)
    return read_iati_ledger(raw / iati.FOLDER / publisher, publisher=publisher)
