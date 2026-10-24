"""The committed synthetic snapshot is schema-valid, hashed and covers the cases."""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR, IFC, write_fixture
from pydantic import ValidationError

from climate_finance.schema import (
    LINK_METHODS,
    RELEASE_TABLES,
    STATUSES,
    ReleaseManifest,
    release_invariants,
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


def test_fixture_holds_the_release_invariants() -> None:
    tables = {name: _read(name) for name in ("commitments", "disbursements")}

    assert release_invariants(tables) == []


def _break_allocation_bound(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("record_id") == "DEU-1-R1"
    return frame.with_columns(
        allocated_low_usd=pl.when(is_row)
        .then(pl.col("allocated_usd") + 1.0)
        .otherwise(pl.col("allocated_low_usd"))
    )


def _break_disbursed_sum(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("row_id") == "DEU-1"
    return frame.with_columns(
        disbursed_usd=pl.when(is_row)
        .then(pl.col("disbursed_usd") - 1e-3)
        .otherwise(pl.col("disbursed_usd"))
    )


def _break_face_cap(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("row_id") == "DEU-1"
    return frame.with_columns(
        face_usd=pl.when(is_row).then(1.0).otherwise(pl.col("face_usd"))
    )


def _break_record_grant_equiv(frame: pl.DataFrame) -> pl.DataFrame:
    # JPN-SPLIT's second allocation carries its own share instead of the record's.
    is_row = (pl.col("record_id") == "JPN-SPLIT") & (pl.col("row_id") == "JPN-3")
    return frame.with_columns(
        record_grant_equiv_usd=pl.when(is_row)
        .then(pl.col("record_grant_equiv_usd") * 10 / 30)
        .otherwise(pl.col("record_grant_equiv_usd"))
    )


def _break_record_disbursement(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = (pl.col("record_id") == "JPN-SPLIT") & (pl.col("row_id") == "JPN-3")
    return frame.with_columns(
        record_disbursement_usd=pl.when(is_row)
        .then(pl.col("allocated_usd"))
        .otherwise(pl.col("record_disbursement_usd"))
    )


def _overdraw_record(frame: pl.DataFrame) -> pl.DataFrame:
    # JPN-SPLIT allocates 22m; its disbursement is cut to 20m on every row.
    return frame.with_columns(
        record_disbursement_usd=pl.when(pl.col("record_id") == "JPN-SPLIT")
        .then(20e6)
        .otherwise(pl.col("record_disbursement_usd"))
    )


def _negative_record_grant_equiv(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        record_grant_equiv_usd=pl.when(pl.col("record_id") == "JPN-SPLIT")
        .then(-1.0)
        .otherwise(pl.col("record_grant_equiv_usd"))
    )


def _orphan_allocation(frame: pl.DataFrame) -> pl.DataFrame:
    orphan = frame.head(1).with_columns(
        row_id=pl.lit("NONE-1"), record_id=pl.lit("NONE-1-R0")
    )
    return pl.concat([frame, orphan])


@pytest.mark.parametrize(
    ("table", "edit", "row_id"),
    [
        ("disbursements", _break_allocation_bound, "DEU-1"),
        ("commitments", _break_disbursed_sum, "DEU-1"),
        ("commitments", _break_face_cap, "DEU-1"),
        ("disbursements", _orphan_allocation, "NONE-1"),
        ("disbursements", _break_record_grant_equiv, "JPN-SPLIT"),
        ("disbursements", _break_record_disbursement, "JPN-SPLIT"),
        ("disbursements", _overdraw_record, "JPN-SPLIT"),
        ("disbursements", _negative_record_grant_equiv, "JPN-SPLIT"),
    ],
)
def test_release_invariants_name_the_row_that_breaks_one(
    table: str, edit: Callable[[pl.DataFrame], pl.DataFrame], row_id: str
) -> None:
    tables = {name: _read(name) for name in ("commitments", "disbursements")}
    tables[table] = edit(tables[table])

    problems = release_invariants(tables)

    assert problems
    assert all(row_id in problem for problem in problems)


@pytest.mark.parametrize(
    "column", ["record_grant_equiv_usd", "record_disbursement_usd"]
)
@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_release_invariants_report_a_non_finite_amount_once(
    column: str, value: float
) -> None:
    # AFDB-1-R1 is a one-row record, so no row of it can differ from another.
    tables = {name: _read(name) for name in ("commitments", "disbursements")}
    tables["disbursements"] = tables["disbursements"].with_columns(
        pl.when(pl.col("record_id") == "AFDB-1-R1")
        .then(value)
        .otherwise(pl.col(column))
        .alias(column)
    )

    problems = release_invariants(tables)

    assert len(problems) == 1
    assert "AFDB-1-R1" in problems[0]
    assert problems[0].endswith(f"{column} is not finite")


def test_grant_equivalent_coverage_weights_records_by_allocated_value() -> None:
    # AFDB-1 (AfDB, 2021) allocates three records of 11.67m, the first without a
    # grant equivalent, and a 1m ledger fill without one: half its records, but
    # 23.3m of its 36m allocated, carry a grant equivalent.
    rows = _read("disbursements").filter(pl.col("row_id") == "AFDB-1")
    valued = rows.filter(pl.col("record_grant_equiv_usd").is_not_null())
    coverage = (
        _read("quality")
        .filter(pl.col("provider_code") == 913, pl.col("year") == 2021)[
            "grant_equiv_coverage"
        ]
        .item()
    )

    assert valued.height / rows.height == 0.5
    assert coverage == pytest.approx(
        valued["allocated_usd"].sum() / rows["allocated_usd"].sum()
    )


def test_release_json_records_the_build_settings() -> None:
    without = MANIFEST.model_dump(exclude={"settings"})

    assert MANIFEST.settings["key_repair"] is True
    with pytest.raises(ValidationError, match="settings"):
        ReleaseManifest.model_validate(without)
