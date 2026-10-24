"""The release tables of a full offline build of the archived sources, fetch
through assemble, against their schemas and the release invariants. The `release`
fixture in conftest.py builds it.
"""

from pathlib import Path

import polars as pl
import pytest

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
