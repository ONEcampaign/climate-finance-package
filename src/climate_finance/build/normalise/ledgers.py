"""Bank ledgers as one `ledger` table, with amounts in US dollars."""

import logging
from pathlib import Path

import polars as pl

from climate_finance.build.config import Ledgers
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
from climate_finance.build.sources import finances_one, iati
from climate_finance.build.sources.gcf import READINESS_FILE

logger = logging.getLogger(__name__)


def normalise_ledgers(
    raw: Path, *, ledgers: Ledgers, exchange: pl.DataFrame
) -> pl.DataFrame:
    """Read every ledger `ledgers` names from `raw` and convert it to US dollars.

    World Bank Finances One gives flows for `ledgers.years`; the other ledgers give
    every transaction their files hold. Amounts are converted by `to_usd`. Those with
    no DAC rate for their currency and year, such as the AfDB's XDR, keep a null
    `amount_usd` and are counted in the log.

    Args:
        raw: The raw directory fetch wrote.
        ledgers: The ledger sources of the build config.
        exchange: DAC exchange rates with `iso_code`, `year` and `exchange_rate`.
    """
    first, last = ledgers.years
    frames = [
        _read(raw, publisher=publisher, years=range(first, last + 1))
        for publisher in ledgers.publishers
    ]
    ledger = to_usd(pl.concat(frames), exchange=exchange)
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
