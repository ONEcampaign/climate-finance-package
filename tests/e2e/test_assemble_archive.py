"""A full offline build of the archived sources, fetch through assemble, opened with
`cf.load`. The `release` fixture in conftest.py builds it.
"""

from pathlib import Path

import polars as pl
import pytest

import climate_finance as cf
from climate_finance import schema
from climate_finance.schema import RELEASE_TABLES

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def tables(release: Path) -> dict[str, pl.DataFrame]:
    return {
        name: pl.read_parquet(release / f"{name}.parquet") for name in RELEASE_TABLES
    }


def test_every_release_table_holds_its_schema_and_the_invariants(
    tables: dict[str, pl.DataFrame],
) -> None:
    for name, frame in tables.items():
        schema.validate(RELEASE_TABLES[name], frame)
        print(f"{name}: {frame.height} rows")

    assert schema.release_invariants(tables) == []
    assert tables["commitments"].height > 200_000
    assert tables["disbursements"].height > 100_000


def test_load_answers_germany_under_each_method(release: Path) -> None:
    snapshot = cf.load(path=release)
    years = range(2015, 2025)
    for method in (cf.methods.ONE, cf.methods.OECD, cf.methods.RANGE):
        table = snapshot.spending(
            providers="Germany", years=years, method=method
        ).table.to_polars()
        totals = table.group_by("method").agg(
            pl.col("value", "low", "high").sum() / 1e9
        )
        print(totals.sort("method"))
        assert totals["value"].min() > 0
