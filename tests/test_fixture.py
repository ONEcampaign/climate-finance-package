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
    (FIXTURE_DIR / "release.json").read_text(encoding="utf-8")
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
    pytest.importorskip("climate_finance.build")
    write_fixture(tmp_path)

    for table in RELEASE_TABLES:
        assert pl.read_parquet(tmp_path / f"{table}.parquet").equals(_read(table))


INVARIANT_TABLES = (
    "commitments",
    "disbursements",
    "untraced",
    "unallocated",
    "agency_shares",
    "agency_spending",
    "core_contributions",
    "btr",
    "btr_links",
    "btr_totals",
    "provider_coefficients",
    "btr_coefficients",
    "reflows",
    "jev_flags",
)


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


def _publish_more_climate_than_face(frame: pl.DataFrame) -> pl.DataFrame:
    # DEU-1's Rio markers count its 40m face value at most, not 80m.
    return frame.with_columns(
        climate_usd=pl.when(pl.col("row_id") == "DEU-1")
        .then(pl.col("climate_usd") * 2)
        .otherwise(pl.col("climate_usd"))
    )


def _keep_an_aggregate_channel(frame: pl.DataFrame) -> pl.DataFrame:
    # 44000, the World Bank Group, holds IDA's 44002.
    aggregate = frame.filter(channel_code=44002).with_columns(
        channel_code=pl.lit(44000, pl.Int32)
    )
    return pl.concat([frame, aggregate])


def _mix_share_sources(frame: pl.DataFrame) -> pl.DataFrame:
    # One of IDA's 2021 spending rows is held, the others computed.
    first = (
        (pl.col("channel_code") == 44002)
        & (pl.col("year") == 2021)
        & (pl.col("rio_adaptation") == 2)
        & (pl.col("rio_mitigation") == 0)
    )
    return frame.with_columns(
        share_source=pl.when(first).then(pl.lit("held")).otherwise("share_source")
    )


def _measure_ida_twice(frame: pl.DataFrame) -> pl.DataFrame:
    # IDA's 2021 share is measured in agency_spending; a `one` row would count it
    # twice.
    one = frame.filter(channel_code=44002, year=2021, source="oecd").with_columns(
        source=pl.lit("one")
    )
    return pl.concat([frame, one])


def _give_a_marker_row_components(frame: pl.DataFrame) -> pl.DataFrame:
    is_row = (pl.col("channel_code") == 44002) & (pl.col("year") == 2021)
    is_row &= (pl.col("rio_adaptation") == 2) & (pl.col("rio_mitigation") == 0)
    return frame.with_columns(
        adaptation_share=pl.when(is_row).then(0.06).otherwise("adaptation_share")
    )


def _repeat_a_components_row(frame: pl.DataFrame) -> pl.DataFrame:
    return pl.concat([frame, frame.filter(channel_code=46002, year=2022)])


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


def _move_a_fill_to_another_recipient(frame: pl.DataFrame) -> pl.DataFrame:
    # The bank-ledger fill on AFDB-1 has no CRS record; its facts are AFDB-1's.
    is_fill = pl.col("disbursement_source") == "bank_ledger"
    return frame.with_columns(
        recipient_code=pl.when(is_fill).then(1).otherwise(pl.col("recipient_code"))
    )


def _ship_a_gcf_fill(frame: pl.DataFrame) -> pl.DataFrame:
    # The bank-ledger fill on AFDB-1 becomes a GCF record, which releases leave out.
    return frame.with_columns(
        record_id=pl.when(pl.col("disbursement_source") == "bank_ledger")
        .then(pl.lit("ledger:gcf:FP001:2021"))
        .otherwise(pl.col("record_id"))
    )


def _withhold_a_shipped_fill(frame: pl.DataFrame) -> pl.DataFrame:
    # AFDB-1 states its shipped bank-ledger fill as withheld too, so counts it twice.
    return frame.with_columns(
        withheld_usd=pl.when(pl.col("row_id") == "AFDB-1")
        .then(pl.col("disbursed_usd"))
        .otherwise(pl.col("withheld_usd"))
    )


def _repeat_an_allocated_row(frame: pl.DataFrame) -> pl.DataFrame:
    # DEU-1 holds allocations; as a repeat of DEU-2 its money drops out of totals.
    return frame.with_columns(
        repeat_of=pl.when(pl.col("row_id") == "DEU-1")
        .then(pl.lit("DEU-2"))
        .otherwise(pl.col("repeat_of"))
    )


def _edit_untraced(
    record: str, /, **changes: object
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """An edit setting `changes` on `untraced` row `record`."""

    def edit(frame: pl.DataFrame) -> pl.DataFrame:
        is_row = pl.col("record_id") == record
        return frame.with_columns(
            pl.when(is_row)
            .then(pl.lit(value, frame.schema[name]))
            .otherwise(pl.col(name))
            .alias(name)
            for name, value in changes.items()
        )

    return edit


def _edit_reflow(**values: object) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """An edit setting `values` on Japan's repayment of JPN-1."""

    def edit(frame: pl.DataFrame) -> pl.DataFrame:
        repayment = pl.col("record_id") == "JPN-1-REPAY"
        return frame.with_columns(
            pl.when(repayment).then(pl.lit(value)).otherwise(pl.col(name)).alias(name)
            for name, value in values.items()
        )

    return edit


def _overstate_excess(frame: pl.DataFrame) -> pl.DataFrame:
    # Japan's 2023 excess is JPN-SPLIT's 8m.
    is_row = (pl.col("provider_code") == 701) & (pl.col("year") == 2023)
    return frame.with_columns(
        excess_usd=pl.when(is_row).then(9e6).otherwise(pl.col("excess_usd"))
    )


def _remainder(frame: pl.DataFrame, usd: float) -> pl.DataFrame:
    # A 40000 row holds the part of a provider's total no leaf itemises.
    first = frame.head(1).with_columns(
        channel_code=pl.lit(40000, pl.Int32), contribution_usd=pl.lit(usd)
    )
    return pl.concat([frame, first])


def test_a_top_channel_remainder_breaks_no_invariant() -> None:
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    tables["core_contributions"] = _remainder(tables["core_contributions"], 5e6)

    assert release_invariants(tables) == []


def _zero_remainder(frame: pl.DataFrame) -> pl.DataFrame:
    return _remainder(frame, 0.0)


def _restate_a_btr_total(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        face_usd=pl.when(pl.col("party") == "NOR")
        .then(pl.col("face_usd") * 1.01)
        .otherwise("face_usd")
    )


def _unlink_a_btr_total(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(linked=pl.col("linked") & (pl.col("party") != "DEU"))


def _drop_a_btr_row(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("party") != "DEU")


def _repeat_a_btr_link(frame: pl.DataFrame) -> pl.DataFrame:
    return pl.concat([frame, frame.filter(record_id="NOR-B-R1")])


def _btr_coefficient_edit(
    provider: int, marker: str, **changes: object
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """An edit setting `changes` on the provider's `btr_coefficients` row of
    `marker`."""
    is_row = (pl.col("provider_code") == provider) & (pl.col("marker") == marker)
    return lambda frame: frame.with_columns(
        pl.when(is_row).then(pl.lit(value)).otherwise(pl.col(name)).alias(name)
        for name, value in changes.items()
    )


def _coefficient_edit(
    provider: int, first_year: int | None, **changes: object
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """An edit setting `changes` on the provider's general row from `first_year`."""
    first = pl.col("first_year")
    is_row = (
        (pl.col("provider_code") == provider)
        & pl.col("agency_code").is_null()
        & (first.is_null() if first_year is None else first == first_year)
    )

    def edit(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.when(is_row).then(pl.lit(value)).otherwise(pl.col(name)).alias(name)
            for name, value in changes.items()
        )

    return edit


@pytest.mark.parametrize(
    ("table", "edit", "row_id"),
    [
        ("disbursements", _break_allocation_bound, "DEU-1"),
        ("commitments", _null_disbursed, "DEU-1"),
        ("disbursements", _null_allocation_bound, "DEU-1"),
        ("disbursements", _null_record_disbursement, "JPN-SPLIT"),
        ("disbursements", _unlink_an_allocation, "DEU-1"),
        ("disbursements", _move_a_fill_to_another_recipient, "AFDB-1"),
        ("commitments", _repeat_an_allocated_row, "DEU-1"),
        ("commitments", _break_disbursed_sum, "DEU-1"),
        ("commitments", _break_face_cap, "DEU-1"),
        ("commitments", _revise_face_below_zero, "DEU-1"),
        ("disbursements", _orphan_allocation, "NONE-1"),
        ("commitments", _keep_a_parent_beside_its_child, "DEU-1"),
        ("disbursements", _code_unspecified_as_the_bulk_crs_does, "disbursements"),
        ("disbursements", _ship_a_gcf_fill, "disbursements"),
        (
            "untraced",
            _edit_untraced("DEU-UNLINKED", record_id="ledger:gcf:FP001:2022"),
            "untraced",
        ),
        ("commitments", _withhold_a_shipped_fill, "AFDB-1"),
        ("disbursements", _break_record_grant_equiv, "JPN-SPLIT"),
        ("disbursements", _break_record_disbursement, "JPN-SPLIT"),
        ("disbursements", _overdraw_record, "JPN-SPLIT"),
        ("disbursements", _negative_record_grant_equiv, "JPN-SPLIT"),
        ("untraced", _edit_untraced("DEU-UNLINKED", rio_adaptation=0), "DEU-UNLINKED"),
        ("untraced", _edit_untraced("DEU-UNLINKED", bi_multi=2), "DEU-UNLINKED"),
        # An unlinked record whose allocations the release holds.
        ("untraced", _edit_untraced("DEU-UNLINKED", record_id="DEU-1-R1"), "DEU-1-R1"),
        (
            "untraced",
            _edit_untraced("FRA-UNLINKED", record_disbursement_usd=4e6),
            "FRA-UNLINKED",
        ),
        (
            "untraced",
            _edit_untraced("JPN-SPLIT", record_disbursement_usd=31e6),
            "JPN-SPLIT",
        ),
        ("unallocated", _overstate_excess, "701 2023"),
        ("agency_shares", _split_a_climate_only_share, "46002 2020 oecd"),
        ("agency_spending", _mix_share_sources, "44002 2021"),
        ("commitments", _publish_more_climate_than_face, "DEU-1"),
        ("core_contributions", _keep_an_aggregate_channel, "44000"),
        ("core_contributions", _zero_remainder, "40000"),
        ("btr_totals", _restate_a_btr_total, "NOR"),
        ("btr_totals", _unlink_a_btr_total, "DEU 2021"),
        ("btr", _drop_a_btr_row, "DEU"),
        ("btr_links", _repeat_a_btr_link, "NOR-B-R1"),
        ("agency_shares", _measure_ida_twice, "44002 2021"),
        ("agency_spending", _give_a_marker_row_components, "44002 2021 2 0"),
        ("agency_spending", _repeat_a_components_row, "46002 2022 None None"),
        ("reflows", _edit_reflow(received_low_usd=3e6), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(interest_low_usd=-1.0), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(row_id=None), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(basis="excess"), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(row_id="NONE-1"), "JPN-1-REPAY"),
        # The repayment of a loan marked 0 for adaptation weighs at the record's 2.
        ("reflows", _edit_reflow(inherited_rio_adaptation=2), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(received_usd=1e6), "JPN-1-REPAY"),
        ("reflows", _edit_reflow(loan_disbursement_usd=1e6), "crs_id:JPN-1"),
        # Japan's coefficients up to 2021 would also cover 2022.
        (
            "provider_coefficients",
            _coefficient_edit(701, None, last_year=2022),
            "701 None 2022",
        ),
        (
            "provider_coefficients",
            _coefficient_edit(701, 2022, last_year=2021),
            "701 None 2022",
        ),
        # Its `btr_coefficients` row then calls a missing coefficient stated.
        (
            "provider_coefficients",
            _coefficient_edit(5, None, significant=None),
            " 5 None ",
        ),
        (
            "provider_coefficients",
            _coefficient_edit(5, None, both_significant=1.5),
            "5 None None",
        ),
        # Germany states its significant coefficient, so its BTR's cannot stand in.
        (
            "btr_coefficients",
            _btr_coefficient_edit(5, "significant", source="btr"),
            "btr_coefficients 5 None significant",
        ),
        # Norway states no principal coefficient.
        (
            "btr_coefficients",
            _btr_coefficient_edit(8, "principal", source="stated"),
            "btr_coefficients 8 None principal",
        ),
        (
            "btr_coefficients",
            _btr_coefficient_edit(8, "principal", coefficient=1.2),
            "btr_coefficients 8 None principal",
        ),
        (
            "btr_coefficients",
            _btr_coefficient_edit(8, "principal", coefficient=None),
            "btr_coefficients 8 None principal",
        ),
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


def test_release_invariants_refuse_an_untraced_adaptation_marker_before_2010() -> None:
    # DEU-UNLINKED, Germany's only untraced record of 2022, moves to 2009 in both
    # tables and keeps its adaptation marker. Its 2.5m leaves Germany's 2022
    # `unallocated` row for a 2009 row of its own.
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    untraced, unallocated = tables["untraced"], tables["unallocated"]
    tables["untraced"] = untraced.with_columns(
        year=pl.when(pl.col("record_id") == "DEU-UNLINKED")
        .then(pl.lit(2009, pl.Int16))
        .otherwise(pl.col("year"))
    )
    is_row = (pl.col("provider_code") == 5) & (pl.col("year") == 2022)
    moved = unallocated.filter(is_row).with_columns(
        year=pl.lit(2009, pl.Int16),
        marked_usd=pl.col("unlinked_marked_usd"),
        outside_release_usd=pl.lit(0.0),
    )
    kept = unallocated.with_columns(
        pl.when(is_row)
        .then(pl.col(name) - pl.col("unlinked_marked_usd"))
        .otherwise(pl.col(name))
        .alias(name)
        for name in ("marked_usd", "unlinked_marked_usd")
    )
    tables["unallocated"] = pl.concat([kept, moved])

    assert release_invariants(tables) == [
        "untraced DEU-UNLINKED: an adaptation marker before ADAPTATION_FIRST_YEAR"
    ]


def test_release_invariants_refuse_a_dropped_allocation_row() -> None:
    # NOR-1-R1, Norway's only Rio-marked record of 2022, loses its allocation row,
    # and NOR-1's disbursed amounts drop with it, so every per-row and per-record
    # bound still holds. Norway's 2022 Rio-marked disbursement then reaches no table.
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    dropped = tables["disbursements"].filter(pl.col("record_id") == "NOR-1-R1")
    tables["disbursements"] = tables["disbursements"].filter(
        pl.col("record_id") != "NOR-1-R1"
    )
    tables["commitments"] = tables["commitments"].with_columns(
        pl.when(pl.col("row_id") == "NOR-1")
        .then(pl.col(total) - dropped[part].sum())
        .otherwise(pl.col(total))
        .alias(total)
        for total, part in (
            ("disbursed_usd", "allocated_usd"),
            ("disbursed_low_usd", "allocated_low_usd"),
        )
    )

    assert release_invariants(tables) == [
        "unallocated 8 2022: marked_usd 5700000.0 differs from its parts' sum 0.0"
    ]


def test_release_invariants_allow_rounding_in_a_marked_total() -> None:
    # Japan's 64m of 2023 off by 1e-5 USD, above INVARIANT_TOLERANCE_USD and within
    # INVARIANT_RELATIVE_TOLERANCE of it, as rounding parts large totals.
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    is_row = (pl.col("provider_code") == 701) & (pl.col("year") == 2023)
    tables["unallocated"] = tables["unallocated"].with_columns(
        marked_usd=pl.when(is_row)
        .then(pl.col("marked_usd") + 1e-5)
        .otherwise(pl.col("marked_usd"))
    )

    assert release_invariants(tables) == []


@pytest.mark.parametrize(
    "column", ["record_grant_equiv_usd", "record_disbursement_usd", "allocated_usd"]
)
# JPN-SPLIT has a row under JPN-1 and one under JPN-3, so a non-finite amount on
# the JPN-1 row leaves the record's rows unequal and the commitment's sum short.
@pytest.mark.parametrize(
    ("row_id", "record_id"), [("AFDB-1", "AFDB-1-R1"), ("JPN-1", "JPN-SPLIT")]
)
@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_release_invariants_report_a_non_finite_amount_once(
    column: str, row_id: str, record_id: str, value: float
) -> None:
    tables = {name: _read(name) for name in INVARIANT_TABLES}
    tables["disbursements"] = tables["disbursements"].with_columns(
        pl.when(pl.col("row_id") == row_id, pl.col("record_id") == record_id)
        .then(value)
        .otherwise(pl.col(column))
        .alias(column)
    )

    problems = release_invariants(tables)

    assert len(problems) == 1
    assert record_id in problems[0]
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
