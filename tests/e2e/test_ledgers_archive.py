"""The ledger readers over the archived raw files. Needs CF_E2E_DATA pointing at the
scoping `data/`."""

import os
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.readers.ledgers import iati
from climate_finance.build.readers.ledgers.finances_one import (
    Dataset,
    read_finances_one,
)
from climate_finance.build.readers.ledgers.iati import read_iati_ledger

pytestmark = pytest.mark.slow

DATASETS: tuple[tuple[Dataset, str], ...] = (("DS00975", "IBRD"), ("DS00976", "IDA"))


def _data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.mark.parametrize(("dataset", "bank"), DATASETS)
def test_finances_one_flows_match_the_prototype_for_2021_and_2022(
    dataset: Dataset, bank: str
) -> None:
    """The prototype's `gap-fill/ledger_wb.parquet` holds these flows, which agree
    with the CRS at a ratio of 1.000 for 2021 to 2024."""
    flows = read_finances_one(
        _data() / "raw" / "banks" / "bank-routes-a",
        dataset=dataset,
        years=[2021, 2022],
    )
    prototype = (
        pl.read_parquet(_data() / "gap-fill" / "ledger_wb.parquet")
        .filter((pl.col("bank") == bank) & pl.col("year").is_in([2021, 2022]))
        .select("key", "year", "usd")
    )

    schema.validate(schema.LEDGER, flows)
    joined = flows.select("key", "year", "amount").join(
        prototype, on=["key", "year"], how="full", coalesce=True
    )
    # The prototype also lists loans first seen in 2023 or 2024, at zero.
    gap = (pl.col("amount").fill_null(0) - pl.col("usd").fill_null(0)).abs()
    assert joined.filter(gap > 1e-3).is_empty()


def test_gef_disbursements_for_2021_to_2024_under_each_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GEF's 2021 to 2024 disbursements are USD 4.52bn as published, 2.24bn as the
    rise of the running peak (the reader's reading) and 1.61bn as year-end differences,
    whose negative corrections reach USD 1.3bn. The CRS shows USD 1.91bn, in rows that
    carry agency ids and so allow no comparison by activity."""
    folder = _data() / "raw" / "external" / "iati" / "gef"

    def disbursements(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(pl.col("transaction_type") == "disbursement")

    peaks = disbursements(read_iati_ledger(folder, publisher="gef"))
    monkeypatch.setattr(iati, "_to_period_flows", lambda frame: frame)
    published = disbursements(read_iati_ledger(folder, publisher="gef"))

    in_window = pl.col("year").is_between(2021, 2024)
    assert peaks.filter(in_window)["amount"].sum() == pytest.approx(2.2426e9, rel=1e-3)
    assert published.filter(in_window)["amount"].sum() == pytest.approx(
        4.5163e9, rel=1e-3
    )

    # Year-end differences: each activity's last figure in each year, carried
    # forward, minus the year before.
    last = published.group_by("activity_id", "year").agg(
        pl.col("amount").sort_by("date").last()
    )
    grid = (
        last.select("activity_id")
        .unique()
        .join(
            pl.DataFrame({"year": list(range(2012, 2027))}, schema={"year": pl.Int16}),
            how="cross",
        )
    )
    differences = (
        grid.join(last, on=["activity_id", "year"], how="left")
        .sort("activity_id", "year")
        .with_columns(pl.col("amount").forward_fill().over("activity_id").fill_null(0))
        .with_columns(
            flow=pl.col("amount")
            - pl.col("amount").shift(1).over("activity_id").fill_null(0)
        )
        .filter(in_window)
    )
    assert differences["flow"].sum() == pytest.approx(1.6074e9, rel=1e-3)
    assert differences.filter(pl.col("flow") < 0)["flow"].sum() == pytest.approx(
        -1.3114e9, rel=1e-3
    )
