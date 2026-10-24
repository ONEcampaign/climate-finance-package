"""The ledger check of the archive build against World Bank Finances One. Needs
CF_E2E_DATA pointing at the scoping `data/`."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance.build.config import Ledgers
from climate_finance.build.fetch import DAC_EXCHANGE_FILE
from climate_finance.build.link.ledger import check_ledgers
from climate_finance.build.normalise.ledgers import normalise_ledgers
from climate_finance.build.readers.ledgers.finances_one import FOLDER
from climate_finance.build.stages import raw_dir, table_path

pytestmark = pytest.mark.slow

LEDGERS = Ledgers(years=(2021, 2022), publishers=("ibrd", "ida"))
# The archive keeps the year-end snapshots with the bank-route prototypes.
SNAPSHOTS = Path("raw/banks/bank-routes-a")


def test_world_bank_loans_confirm_within_five_percent(
    release: Path, tmp_path: Path
) -> None:
    """At least 99% of the IBRD and IDA loan-years of 2021 and 2022 that the trace
    links reach and the ledger lists confirm within 5%."""
    release_dir = release.parent
    archive = Path(os.environ["CF_E2E_DATA"]) / SNAPSHOTS
    raw = tmp_path / "raw"
    (raw / FOLDER).mkdir(parents=True)
    for snapshot in archive.glob("DS0097*_31-Dec-*.json"):
        (raw / FOLDER / snapshot.name).symlink_to(snapshot)
    ledger = normalise_ledgers(
        raw,
        publishers=LEDGERS.publishers,
        years=LEDGERS.years,
        exchange=pl.read_parquet(raw_dir(release_dir) / DAC_EXCHANGE_FILE),
    )

    links, labels = check_ledgers(
        pl.scan_parquet(table_path(release_dir, "link", "links")),
        units=pl.scan_parquet(table_path(release_dir, "link", "units")),
        ledger=ledger.lazy(),
        years=LEDGERS.years,
    )

    units = pl.scan_parquet(table_path(release_dir, "link", "units")).select(
        "unit_id", "donor_code", "project_number", "year"
    )
    checked = (
        links.filter(
            pl.col("ledger_check").is_in(["confirmed", "contradicted"]), task="trace"
        )
        .join(units, left_on="right_id", right_on="unit_id")
        .join(units.select(left_id="unit_id", d_year="year"), on="left_id")
        .group_by(
            "donor_code",
            "d_year",
            loan=pl.col("project_number").str.split(".").list.get(1),
        )
        .agg(confirmed=(pl.col("ledger_check") == "confirmed").all())
        .group_by("donor_code", "d_year")
        .agg(loans=pl.len(), share=pl.col("confirmed").mean())
        .sort("donor_code", "d_year")
        .collect()
    )
    assert checked.height == 4
    assert checked["loans"].min() > 500
    assert checked["share"].min() >= 0.99, checked
    assert labels.collect().height > 1_000
