"""The committed synthetic snapshot is schema-valid, hashed and covers the cases."""

from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR, IFC, write_fixture

from climate_finance.schema import (
    LINK_METHODS,
    RELEASE_TABLES,
    STATUSES,
    ReleaseManifest,
    sha256_file,
    validate,
)

MANIFEST = ReleaseManifest.model_validate_json(
    (FIXTURE_DIR / "release.json").read_text()
)


def _read(table: str) -> pl.DataFrame:
    return pl.read_parquet(FIXTURE_DIR / f"{table}.parquet")


@pytest.mark.parametrize("table", RELEASE_TABLES)
def test_table_validates_and_matches_its_hash(table: str) -> None:
    entry = MANIFEST.tables[table]
    frame = _read(table)

    validate(RELEASE_TABLES[table], frame)
    assert sha256_file(FIXTURE_DIR / f"{table}.parquet") == entry.sha256
    assert frame.height == entry.rows


def test_fixture_covers_the_cases_the_analysis_layer_needs() -> None:
    commitments = _read("commitments")
    disbursements = _read("disbursements")

    assert commitments["provider_code"].n_unique() == 6
    assert set(commitments["status"]) == set(STATUSES)
    assert set(disbursements["link_method"].drop_nulls()) == set(LINK_METHODS)
    assert {2, 4} <= set(commitments["bi_multi"])
    id_less_multilateral = commitments.filter(
        pl.col("crs_id").is_null(),
        pl.col("project_number").is_null(),
        pl.col("bi_multi") == 4,
    )
    assert id_less_multilateral.height == 1
    assert set(_read("modelled")["provider_code"]) == {IFC}
    assert IFC not in set(commitments["provider_code"])


def test_regenerating_the_fixture_gives_the_same_tables(tmp_path: Path) -> None:
    write_fixture(tmp_path)

    for table in RELEASE_TABLES:
        assert pl.read_parquet(tmp_path / f"{table}.parquet").equals(_read(table))
