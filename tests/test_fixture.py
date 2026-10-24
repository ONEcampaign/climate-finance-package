"""The committed synthetic snapshot is schema-valid, hashed and covers the cases."""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR, IFC, write_fixture

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


INVARIANT_TABLES = ("commitments", "disbursements", "agency_shares")


def test_fixture_holds_the_release_invariants() -> None:
    tables = {name: _read(name) for name in INVARIANT_TABLES}

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


def _revise_face_below_zero(frame: pl.DataFrame) -> pl.DataFrame:
    # A downward revision may keep its row, but not the disbursement against it.
    is_row = pl.col("row_id") == "DEU-1"
    return frame.with_columns(
        face_usd=pl.when(is_row).then(-1.0).otherwise(pl.col("face_usd"))
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


def _orphan_allocation(frame: pl.DataFrame) -> pl.DataFrame:
    orphan = frame.head(1).with_columns(
        row_id=pl.lit("NONE-1"), record_id=pl.lit("NONE-1-R0")
    )
    return pl.concat([frame, orphan])


def _keep_a_parent_beside_its_child(frame: pl.DataFrame) -> pl.DataFrame:
    # DEU-2 becomes a child of DEU-1, which the release still holds.
    return frame.with_columns(
        parent_row_id=pl.when(pl.col("row_id") == "DEU-2")
        .then(pl.lit("DEU-1"))
        .otherwise(pl.col("parent_row_id"))
    )


def _code_unspecified_as_the_bulk_crs_does(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        recipient_code=pl.when(pl.col("recipient_code") == 998)
        .then(9998)
        .otherwise(pl.col("recipient_code"))
    )


def _split_a_climate_only_share(frame: pl.DataFrame) -> pl.DataFrame:
    # AfDB's 2020 OECD share is climate-only; a mitigation row joins it.
    split = frame.filter(objective="climate").with_columns(
        objective=pl.lit("mitigation")
    )
    return pl.concat([frame, split])


def _null_disbursed(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("row_id") == "DEU-1"
    return frame.with_columns(
        disbursed_usd=pl.when(~is_row).then(pl.col("disbursed_usd"))
    )


def _null_allocation_bound(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("record_id") == "DEU-1-R1"
    return frame.with_columns(
        allocated_low_usd=pl.when(~is_row).then(pl.col("allocated_low_usd"))
    )


def _null_record_disbursement(frame: pl.DataFrame) -> pl.DataFrame:
    is_record = pl.col("record_id") == "JPN-SPLIT"
    return frame.with_columns(
        record_disbursement_usd=pl.when(~is_record).then(
            pl.col("record_disbursement_usd")
        )
    )


def _unlink_an_allocation(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = pl.col("record_id") == "DEU-1-R1"
    return frame.with_columns(link_method=pl.when(~is_row).then(pl.col("link_method")))


def _repeat_an_allocated_row(frame: pl.DataFrame) -> pl.DataFrame:
    # DEU-1 holds allocations; as a repeat of DEU-2 its money drops out of totals.
    return frame.with_columns(
        repeat_of=pl.when(pl.col("row_id") == "DEU-1")
        .then(pl.lit("DEU-2"))
        .otherwise(pl.col("repeat_of"))
    )


@pytest.mark.parametrize(
    ("table", "edit", "row_id"),
    [
        ("disbursements", _break_allocation_bound, "DEU-1"),
        ("commitments", _null_disbursed, "DEU-1"),
        ("disbursements", _null_allocation_bound, "DEU-1"),
        ("disbursements", _null_record_disbursement, "JPN-SPLIT"),
        ("disbursements", _unlink_an_allocation, "DEU-1"),
        ("commitments", _repeat_an_allocated_row, "DEU-1"),
        ("commitments", _break_disbursed_sum, "DEU-1"),
        ("commitments", _break_face_cap, "DEU-1"),
        ("commitments", _revise_face_below_zero, "DEU-1"),
        ("disbursements", _orphan_allocation, "NONE-1"),
        ("commitments", _keep_a_parent_beside_its_child, "DEU-1"),
        ("disbursements", _code_unspecified_as_the_bulk_crs_does, "disbursements"),
        ("disbursements", _break_record_grant_equiv, "JPN-SPLIT"),
        ("disbursements", _break_record_disbursement, "JPN-SPLIT"),
        ("disbursements", _overdraw_record, "JPN-SPLIT"),
        ("agency_shares", _split_a_climate_only_share, "46002 2020 oecd"),
    ],
)
def test_release_invariants_name_the_row_that_breaks_one(
    table: str, edit: Callable[[pl.DataFrame], pl.DataFrame], row_id: str
) -> None:
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    tables[table] = edit(tables[table])

    problems = release_invariants(tables)

    assert problems
    assert all(row_id in problem for problem in problems)
