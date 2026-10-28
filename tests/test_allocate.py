"""Allocation and status on hand-built CRS records, linked by the link stage's own
`build_units`, `link_by_keys` and `link_within_year`."""

from collections.abc import Callable, Sequence
from datetime import date
from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from stage_runs import run_stage, stage_context, write_fetched_years, write_inputs

from climate_finance import schema
from climate_finance.build.allocate import (
    _fill,
    allocate,
    allocate_to_rows,
    cancellations,
    compute_status,
    counted_rows,
    dated_donors,
    status_basis,
    window_donors,
    years_since_commitment,
)
from climate_finance.build.errors import AllocationInputError
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.keys import as_exact_links, build_units, link_by_keys
from climate_finance.build.stages import table_path

FIRST_YEAR = 2010
TOLERANCE = 1e-6

CRS_COLUMNS = {
    "record_id": pl.String,
    "year": pl.Int16,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "short_description": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "bi_multi": pl.Int8,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
    "commitment_date": pl.Date,
    "expected_start_date": pl.Date,
    "completion_date": pl.Date,
}
CRDF_COLUMNS = {
    "row_id": pl.String,
    "perspective": pl.String,
    "provider_code": pl.Int32,
    "crs_id": pl.String,
    "year": pl.Int16,
    "purpose_code": pl.Int32,
    "face_usd": pl.Float64,
    "climate_usd": pl.Float64,
    "parent_row_id": pl.String,
    "repeat_of": pl.String,
}


def _date(year: int | None) -> date | None:
    return None if year is None else date(year, 6, 30)


def record(
    record_id: str,
    year: int,
    *,
    commit: float | None = None,
    disburse: float | None = None,
    donor: int = 1,
    crs_id: str = "2019000001",
    project_number: str | None = None,
    purpose: int = 230,
    dated: int | None = None,
    start: int | None = None,
    end: int | None = None,
    title: str | None = None,
) -> dict[str, object]:
    """One CRS record; `dated` is its commitment-date year."""
    return {
        "record_id": record_id,
        "year": year,
        "donor_code": donor,
        "agency_code": 10,
        "crs_id": crs_id,
        "project_number": project_number,
        "title": title,
        "short_description": None,
        "recipient_code": 289,
        "purpose_code": purpose,
        "bi_multi": 1,
        "commitment_usd": commit,
        "disbursement_usd": disburse,
        "commitment_date": _date(dated),
        "expected_start_date": _date(start),
        "completion_date": _date(end),
    }


def crs_frame(records: Sequence[dict[str, object]]) -> pl.LazyFrame:
    return pl.DataFrame(list(records), schema=CRS_COLUMNS).lazy()


def crdf_frame(rows: Sequence[dict[str, object]]) -> pl.LazyFrame:
    base = {"perspective": "recipient", "parent_row_id": None, "repeat_of": None}
    return pl.DataFrame([{**base, **row} for row in rows], schema=CRDF_COLUMNS).lazy()


def crdf_row(
    row_id: str,
    year: int,
    face: float,
    *,
    climate: float | None = None,
    donor: int = 1,
    crs_id: str = "2019000001",
    purpose: int = 230,
    parent: str | None = None,
) -> dict[str, object]:
    """One recipient-perspective CRDF row; climate defaults to a quarter of face."""
    return {
        "row_id": row_id,
        "provider_code": donor,
        "crs_id": crs_id,
        "year": year,
        "purpose_code": purpose,
        "face_usd": face,
        "climate_usd": face / 4 if climate is None else climate,
        "parent_row_id": parent,
    }


def no_classes() -> pl.DataFrame:
    return pl.DataFrame(schema=schema.ID_PRACTICE.dtypes)


def link(
    records: Sequence[dict[str, object]], crdf: Sequence[dict[str, object]] = ()
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """(crs, counted CRDF rows, units, links) as the link stage builds them."""
    crs = crs_frame(records)
    rows = counted_rows(crdf_frame(crdf))
    units = build_units(crs).collect()
    trace = link_by_keys(units, first_year=FIRST_YEAR)
    within = link_within_year(rows, units=units.lazy()).collect()
    return crs, rows, units.lazy(), pl.concat([trace, within]).lazy()


def run(
    records: Sequence[dict[str, object]],
    *,
    dated: tuple[int, ...] = (),
    windowed: tuple[int, ...] = (),
) -> pl.DataFrame:
    crs, crdf, units, links = link(records)
    return allocate(
        units, crs=crs, links=links, crdf=crdf, dated=dated, windowed=windowed
    )


def unit_of(units: pl.LazyFrame, record_id: str) -> str:
    match = units.filter(pl.col("record_ids").list.contains(record_id)).collect()
    return match["unit_id"].item()


def by_commitment(pairs: pl.DataFrame) -> dict[str, float]:
    sums = (
        pairs.drop_nulls("commitment_id")
        .group_by("commitment_id")
        .agg(pl.col("allocated_usd").sum())
    )
    return dict(sums.iter_rows())


def excess_of(pairs: pl.DataFrame) -> dict[str, float]:
    excess = pairs.filter(pl.col("rule") == "excess")
    return dict(excess.select("record_id", "allocated_usd").iter_rows())


def pair(pairs: pl.DataFrame, commitment: str, rec: str) -> float:
    match = pairs.filter(
        pl.col("commitment_id") == commitment, pl.col("record_id") == rec
    )
    return float(match["allocated_usd"].sum())


# Two phases under one id: 100 in 2019, 100 in 2021; 60 disbursed in 2020, 100 in 2022.
PHASES = [
    record("c19", 2019, commit=100.0),
    record("d20", 2020, disburse=60.0),
    record("c21", 2021, commit=100.0),
    record("d22", 2022, disburse=100.0),
]


def test_fifo_fills_the_oldest_commitment_first() -> None:
    pairs = run(PHASES)
    assert by_commitment(pairs) == pytest.approx({"c19": 100.0, "c21": 60.0})
    assert pair(pairs, "c19", "d22") == pytest.approx(40.0)


def test_fifo_splits_one_year_pro_rata_to_headroom() -> None:
    pairs = run(
        [
            record("a", 2019, commit=100.0, purpose=140),
            record("b", 2019, commit=300.0, purpose=230),
            record("d", 2020, disburse=200.0, purpose=999),
        ]
    )
    assert by_commitment(pairs) == pytest.approx({"a": 50.0, "b": 150.0})


def test_a_commitment_takes_no_more_than_face_and_the_rest_is_excess() -> None:
    pairs = run(
        [
            record("c21", 2021, commit=100.0, disburse=30.0),
            record("d22", 2022, disburse=50.0),
            record("d23", 2023, disburse=40.0),
        ]
    )
    assert by_commitment(pairs) == pytest.approx({"c21": 100.0})
    assert excess_of(pairs) == pytest.approx({"d23": 20.0})


def test_records_of_one_unit_share_its_allocation_by_disbursement() -> None:
    pairs = run(
        [
            record("c19", 2019, commit=100.0),
            record("d20a", 2020, disburse=90.0),
            record("d20b", 2020, disburse=30.0),
        ]
    )
    assert pair(pairs, "c19", "d20a") == pytest.approx(75.0)
    assert excess_of(pairs) == pytest.approx({"d20a": 15.0, "d20b": 5.0})


def test_earlier_record_years_fill_first() -> None:
    pairs = run(
        [
            record("c19", 2019, commit=100.0),
            record("c20", 2020, commit=100.0),
            record("d20", 2020, disburse=100.0),
            record("d21", 2021, disburse=100.0),
        ]
    )
    assert pair(pairs, "c19", "d20") == pytest.approx(100.0)
    assert pair(pairs, "c20", "d21") == pytest.approx(100.0)


def test_floored_records_fill_before_undated_records_of_their_year() -> None:
    # d_dated continues the 2019 commitment, so it can fill only 2019 on; it goes
    # first and takes the 2019 commitment, and the undated record the 2018 one.
    pairs = run(
        [
            record("c18", 2018, commit=100.0),
            record("c19", 2019, commit=100.0),
            record("d_dated", 2022, disburse=100.0, dated=2019),
            record("d_undated", 2022, disburse=150.0),
        ]
    )
    assert pair(pairs, "c19", "d_dated") == pytest.approx(100.0)
    assert pair(pairs, "c18", "d_undated") == pytest.approx(100.0)
    assert excess_of(pairs) == pytest.approx({"d_undated": 50.0})


def test_methods_run_in_link_method_order_within_a_year() -> None:
    # Both records reach c20: d_purpose by crs_id_purpose, d_id only by crs_id,
    # since no commitment shares its purpose. The crs_id_purpose record goes first.
    pairs = run(
        [
            record("c20", 2020, commit=100.0, purpose=230),
            record("c21", 2021, commit=100.0, purpose=140),
            record("d_purpose", 2022, disburse=100.0, purpose=230),
            record("d_id", 2022, disburse=100.0, purpose=999),
        ]
    )
    assert pair(pairs, "c20", "d_purpose") == pytest.approx(100.0)
    assert pair(pairs, "c21", "d_id") == pytest.approx(100.0)


# A record dated its own year belongs to that year's commitment, not the older one.
DATED_SAME = [
    record("c19", 2019, commit=100.0, dated=2019),
    record("c21", 2021, commit=100.0, dated=2021),
    record("d21", 2021, disburse=50.0, dated=2021),
]


def test_a_dated_donor_fills_its_own_year_first() -> None:
    pairs = run(DATED_SAME, dated=(1,))
    assert by_commitment(pairs) == pytest.approx({"c21": 50.0})
    assert set(pairs["rule"]) == {"dated"}


def test_an_undated_donor_keeps_fifo_for_a_same_year_record() -> None:
    pairs = run(DATED_SAME)
    assert by_commitment(pairs) == pytest.approx({"c19": 50.0})
    assert set(pairs["rule"]) == {"fifo"}


# One crs id reused for a second module, each with its own commitment.
MODULES = [
    record("m1", 2021, commit=100.0, dated=2021, title="Module one: rural water"),
    record("m2", 2022, commit=100.0, dated=2022, title="Module two: feeder roads"),
]


def test_a_record_that_commits_and_disburses_fills_its_own_commitment_first() -> None:
    pairs = run([MODULES[0], {**MODULES[1], "disbursement_usd": 60.0}])
    assert by_commitment(pairs) == pytest.approx({"m2": 60.0})


@pytest.mark.parametrize(
    ("title", "filled"),
    [
        ("Feeder roads, module two", {"m2": 60.0}),
        ("Teacher training", {"m1": 60.0}),
    ],
)
def test_a_disbursement_fills_the_commitment_whose_date_and_title_it_repeats(
    title: str, filled: dict[str, float]
) -> None:
    # d22 carries m2's date; it is m2's own when the titles share half their words.
    pairs = run([*MODULES, record("d22", 2022, disburse=60.0, dated=2022, title=title)])
    assert by_commitment(pairs) == pytest.approx(filled)


@pytest.mark.parametrize(
    ("line", "filled"),
    [
        # CRS3 commits, so its disbursement fills CRS3 first and pools only the
        # remainder over the loan's other line.
        ("CRS3", {"CRS1": 50.0, "CRS3": 100.0}),
        # CRS5 never commits, so its disbursement pools over the loan's lines.
        ("CRS5", {"CRS1": 75.0, "CRS3": 75.0}),
    ],
)
def test_a_world_bank_disbursement_fills_its_own_line_first(
    line: str, filled: dict[str, float]
) -> None:
    def line_record(
        name: str, year: int, purpose: int, **amount: float
    ) -> dict[str, object]:
        return record(
            name,
            year,
            donor=905,
            crs_id="P1",
            project_number=f"P1.IDA2.{name}",
            purpose=purpose,
            **amount,
        )

    crs, crdf, units, links = link(
        [
            line_record("CRS1", 2019, 140, commit=100.0),
            line_record("CRS3", 2019, 230, commit=100.0),
            {**line_record(line, 2020, 230, disburse=150.0), "record_id": "d"},
        ]
    )
    assert set(links.collect()["link_method"]) == {"wb_loan"}
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    assert by_commitment(pairs) == pytest.approx(filled)


@pytest.mark.parametrize(
    ("donor", "crs_id", "filled"),
    [
        # An EU contract record carries the contract's date, not the decision's,
        # so a parent-id link neither floors it nor reads it as a dated donor's
        # year.
        (918, "2019000001001", "c19"),
        # A German lettered record keeps its own date.
        (5, "2019000001a", "c21"),
    ],
)
def test_only_eu_parent_id_links_ignore_the_records_date(
    donor: int, crs_id: str, filled: str
) -> None:
    contract = record(
        "d21", 2021, disburse=50.0, donor=donor, dated=2021, crs_id=crs_id, purpose=999
    )
    crs, crdf, units, links = link(
        [
            record("c19", 2019, commit=100.0, donor=donor, dated=2019),
            record("c21", 2021, commit=100.0, donor=donor, dated=2021),
            contract,
        ]
    )
    assert set(links.collect()["link_method"]) == {"crs_id_parent"}
    pairs = allocate(
        units, crs=crs, links=links, crdf=crdf, dated=(donor,), windowed=()
    )
    assert by_commitment(pairs) == pytest.approx({filled: 50.0})


# A 2023 record falls in the 2022 phase's window, not the 2019 phase's.
WINDOW = [
    record("c19", 2019, commit=100.0, start=2019, end=2020),
    record("c22", 2022, commit=100.0, start=2022, end=2025),
    record("d23", 2023, disburse=50.0),
]


def test_a_window_donor_fills_in_window_commitments_first() -> None:
    pairs = run(WINDOW, windowed=(1,))
    assert by_commitment(pairs) == pytest.approx({"c22": 50.0})
    assert set(pairs["rule"]) == {"window"}


def test_other_donors_ignore_windows() -> None:
    pairs = run(WINDOW)
    assert by_commitment(pairs) == pytest.approx({"c19": 50.0})


def test_a_cancellation_leaves_face_value_whole_and_is_reported() -> None:
    records = [
        record("c19", 2019, commit=100.0),
        record("x", 2020, commit=-60.0),
        record("d21", 2021, disburse=150.0),
    ]
    assert by_commitment(run(records)) == pytest.approx({"c19": 100.0})
    reported = cancellations(crs_frame(records)).collect()
    assert reported.rows() == [(1, 2020, 60.0)]


def test_two_groups_of_one_pass_sharing_a_commitment_are_refused() -> None:
    crs, crdf, units, links = link(
        [*PHASES, record("d22b", 2022, disburse=10.0, dated=2022)]
    )
    # d22b is its own unit (dated its own year) in d22's pass; give it a group of
    # its own that reaches the same commitments.
    d22b = unit_of(units, "d22b")
    split = links.with_columns(
        group_id=pl.when(pl.col("left_id") == d22b)
        .then(pl.lit("crs_id_purpose|other"))
        .otherwise("group_id")
    )
    with pytest.raises(AllocationInputError, match="share a commitment"):
        allocate(units, crs=crs, links=split, crdf=crdf)


def with_links(links: pl.LazyFrame, *added: tuple[str, str, str, str]) -> pl.LazyFrame:
    """`links` plus trace links given as (left unit, right unit, group, method)."""
    rows = pl.LazyFrame(
        added,
        schema=["left_id", "right_id", "group_id", "link_method"],
        orient="row",
    )
    return pl.concat([links, as_exact_links(rows, task="trace")])


# d20 links to c19 by crs id; a repair group adds cB, committed a year earlier.
CASCADE = [
    record("c19", 2019, commit=100.0),
    record("cB", 2018, commit=80.0, crs_id="2018000002"),
    record("d20", 2020, disburse=150.0),
]


def test_a_later_group_takes_only_what_the_earlier_one_left() -> None:
    crs, crdf, units, links = link(CASCADE)
    repair = (
        unit_of(units, "d20"),
        unit_of(units, "cB"),
        "repair|x",
        "repair_new_crs_id",
    )
    pairs = allocate(units, crs=crs, links=with_links(links, repair), crdf=crdf)
    # The exact tier fills c19 first, though cB is older; cB takes the rest.
    assert by_commitment(pairs) == pytest.approx({"c19": 100.0, "cB": 50.0})
    assert excess_of(pairs) == {}


def test_a_row_names_the_link_of_the_first_pass_that_filled_it() -> None:
    # One CRDF row covers c19 and cB. The exact tier fills c19's 20 first; the
    # repair pass then gives cB 80, more, but the row names the exact tier's link.
    records = [{**CASCADE[0], "commitment_usd": 20.0}, *CASCADE[1:]]
    crs, crdf, units, links = link(records)
    rows = (("R", unit_of(units, "c19")), ("R", unit_of(units, "cB")))
    links = pl.concat(
        [
            with_links(
                links,
                (
                    unit_of(units, "d20"),
                    unit_of(units, "cB"),
                    "repair|x",
                    "repair_new_crs_id",
                ),
            ),
            as_exact_links(
                pl.LazyFrame(
                    [(*row, "g", "within_year") for row in rows],
                    schema=["left_id", "right_id", "group_id", "link_method"],
                    orient="row",
                ),
                task="crdf_crs",
            ),
        ]
    )
    crdf = counted_rows(crdf_frame([crdf_row("R", 2019, 100.0)]))
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    table = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    assert pair(pairs, "cB", "d20") == pytest.approx(80.0)
    carried = table.filter(pl.col("row_id") == "R")
    assert carried.select("link_method", "commitment_unit_id").rows() == [
        ("crs_id_purpose", unit_of(units, "c19"))
    ]


def test_a_row_names_the_link_that_carried_the_most_over_its_records() -> None:
    # c19's unit sums two commitment records, which take 30 each from d22; c20's
    # one record takes 50. The row covering both names c19's link, which carried 60.
    records = [
        record("c19a", 2019, commit=30.0),
        record("c19b", 2019, commit=30.0),
        record("c20", 2020, commit=50.0),
        record("d22", 2022, disburse=110.0),
    ]
    crs, crdf, units, links = link(records)
    covered = [
        ("R", unit_of(units, name), "g", "within_year") for name in ("c19a", "c20")
    ]
    links = pl.concat(
        [
            links,
            as_exact_links(
                pl.LazyFrame(
                    covered,
                    schema=["left_id", "right_id", "group_id", "link_method"],
                    orient="row",
                ),
                task="crdf_crs",
            ),
        ]
    )
    crdf = counted_rows(crdf_frame([crdf_row("R", 2019, 110.0)]))
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    table = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    assert table.select("row_id", "allocated_usd", "commitment_unit_id").rows() == [
        ("R", 110.0, unit_of(units, "c19a"))
    ]


@pytest.mark.parametrize(
    ("disagree", "route"),
    [(False, "within_year"), (True, "within_year"), (False, "eib_list")],
)
def test_low_bounds_keep_only_what_certain_links_carried(
    disagree: bool, route: str
) -> None:
    # c19's crs id link is certain unless its records disagree; the repair link to
    # cB never is. s20 commits and disburses in one record, so its link to itself
    # stays certain when flagged. A CRDF row reached by an estimated route, as the
    # EIB list's match on recipient, amount and title, counts nothing in low.
    crs, crdf, units, links = link(
        [*CASCADE, record("s20", 2020, commit=30.0, disburse=30.0, crs_id="S")],
        [
            crdf_row("c19", 2019, 100.0),
            crdf_row("cB", 2018, 80.0, crs_id="2018000002"),
            crdf_row("s20", 2020, 30.0, crs_id="S"),
        ],
    )
    repair = (
        unit_of(units, "d20"),
        unit_of(units, "cB"),
        "repair|x",
        "repair_new_crs_id",
    )
    links = with_links(links, repair).with_columns(
        records_disagree=pl.when(pl.col("task") == "trace").then(disagree),
        link_method=pl.when(pl.col("task") == "crdf_crs")
        .then(pl.lit(route))
        .otherwise("link_method"),
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    routed = route == "within_year"
    got = {
        row: (allocated, low, high)
        for row, allocated, low, high in rows.select(
            "row_id", "allocated_usd", "allocated_low_usd", "allocated_high_usd"
        ).iter_rows()
    }
    assert got == pytest.approx(
        {
            "c19": (100.0, 100.0 if routed and not disagree else 0.0, 100.0),
            "cB": (50.0, 0.0, 50.0),
            "s20": (30.0, 30.0 if routed else 0.0, 30.0),
        }
    )


@pytest.mark.parametrize(
    ("records", "certain"),
    [
        ([record("r1", 2022, commit=100.0, disburse=40.0)], True),
        # Two unrelated id-less records sum into one unit: nothing ties r2 to r1.
        (
            [record("r1", 2022, commit=100.0), record("r2", 2022, disburse=40.0)],
            False,
        ),
    ],
)
def test_a_unit_link_to_itself_is_certain_only_for_one_record(
    records: list[dict[str, object]], certain: bool
) -> None:
    crs, crdf, units, links = link([{**rec, "crs_id": None} for rec in records])
    unit = unit_of(units, "r1")
    links = with_links(links, (unit, unit, "aggregate|x", "aggregate_partition"))
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    assert pairs.select("allocated_usd", "certain").rows() == [(40.0, certain)]


def test_certain_links_fill_before_estimated_links_of_earlier_years() -> None:
    # d22 reaches c20 by crs id, a certain link; e21, a year earlier, only by a
    # repair link. The certain record fills c20 first, and e21 takes the rest.
    crs, crdf, units, links = link(
        [
            record("c20", 2020, commit=10.0),
            record("d22", 2022, disburse=8.0),
            record("e21", 2021, disburse=8.0, crs_id="E"),
        ]
    )
    repair = (
        unit_of(units, "e21"),
        unit_of(units, "c20"),
        "repair|x",
        "repair_new_crs_id",
    )
    pairs = allocate(units, crs=crs, links=with_links(links, repair), crdf=crdf)
    assert pair(pairs, "c20", "d22") == pytest.approx(8.0)
    assert pair(pairs, "c20", "e21") == pytest.approx(2.0)
    assert excess_of(pairs) == pytest.approx({"e21": 6.0})


def test_a_record_with_nothing_left_gives_later_groups_nothing() -> None:
    records = [*CASCADE[:2], record("d20", 2020, disburse=60.0)]
    crs, crdf, units, links = link(records)
    repair = (
        unit_of(units, "d20"),
        unit_of(units, "cB"),
        "repair|x",
        "repair_new_crs_id",
    )
    pairs = allocate(units, crs=crs, links=with_links(links, repair), crdf=crdf)
    assert by_commitment(pairs) == pytest.approx({"c19": 60.0})


def test_the_remainder_passes_through_the_repair_rules_in_order() -> None:
    records = [
        record("cB", 2018, commit=30.0, crs_id="B"),
        record("cC", 2017, commit=30.0, crs_id="C"),
        record("d", 2020, disburse=50.0, crs_id="D"),
    ]
    crs, crdf, units, links = link(records)
    d = unit_of(units, "d")
    pairs = allocate(
        units,
        crs=crs,
        links=with_links(
            links,
            (d, unit_of(units, "cC"), "title|x", "repair_title_recipient"),
            (d, unit_of(units, "cB"), "pn|x", "repair_new_crs_id"),
        ),
        crdf=crdf,
    )
    assert by_commitment(pairs) == pytest.approx({"cB": 30.0, "cC": 20.0})


def test_classifier_links_take_what_the_key_rules_left() -> None:
    records = [
        record("cB", 2018, commit=30.0, crs_id="B"),
        record("cE", 2016, commit=30.0, crs_id="E"),
        record("d", 2020, disburse=40.0, crs_id="D"),
    ]
    crs, crdf, units, links = link(records)
    d = unit_of(units, "d")
    pairs = allocate(
        units,
        crs=crs,
        crdf=crdf,
        links=with_links(
            links,
            (d, unit_of(units, "cE"), "classifier|x", "classifier"),
            (d, unit_of(units, "cB"), "pn|x", "repair_new_crs_id"),
        ),
    )
    # cE is older, but the classifier passes after the repair rule.
    assert by_commitment(pairs) == pytest.approx({"cB": 30.0, "cE": 10.0})


def test_two_groups_of_one_method_on_a_unit_are_refused() -> None:
    crs, crdf, units, links = link(CASCADE)
    d20, cb = unit_of(units, "d20"), unit_of(units, "cB")
    bad = with_links(
        links,
        (d20, cb, "repair|x", "repair_new_crs_id"),
        (d20, cb, "repair|y", "repair_new_crs_id"),
    )
    with pytest.raises(AllocationInputError, match="two groups of one"):
        allocate(units, crs=crs, links=bad, crdf=crdf)


def test_a_group_spanning_two_methods_is_refused() -> None:
    crs, crdf, units, links = link(CASCADE)
    group = links.collect().filter(task="trace")["group_id"].item()
    bad = with_links(
        links, (unit_of(units, "d20"), unit_of(units, "cB"), group, "repair_pn_variant")
    )
    with pytest.raises(AllocationInputError, match="span more than one"):
        allocate(units, crs=crs, links=bad, crdf=crdf)


def test_a_group_whose_units_reach_different_commitments_is_refused() -> None:
    records = [*CASCADE, record("e20", 2020, disburse=5.0, crs_id="2020000009")]
    crs, crdf, units, links = link(records)
    bad = with_links(
        links,
        (unit_of(units, "d20"), unit_of(units, "cB"), "repair|x", "repair_new_crs_id"),
        (unit_of(units, "e20"), unit_of(units, "c19"), "repair|x", "repair_new_crs_id"),
    )
    with pytest.raises(AllocationInputError, match="reach different commitments"):
        allocate(units, crs=crs, links=bad, crdf=crdf)


def test_a_trace_link_from_a_unit_without_disbursement_is_refused() -> None:
    crs, crdf, units, links = link(PHASES)
    c21 = unit_of(units, "c21")
    bad = links.with_columns(
        left_id=pl.when(pl.col("task") == "trace")
        .then(pl.lit(c21))
        .otherwise("left_id")
    )
    with pytest.raises(AllocationInputError, match="not a positive"):
        allocate(units, crs=crs, links=bad, crdf=crdf)


def test_dated_donors_need_sixty_percent_of_dollars_dated_earlier() -> None:
    records = [
        record("a1", 2021, disburse=60.0, donor=1, dated=2019),
        record("a2", 2021, disburse=40.0, donor=1),
        record("b1", 2021, disburse=59.0, donor=2, dated=2019),
        record("b2", 2021, disburse=41.0, donor=2, dated=2021),
        # Commitments and records outside 2020 to 2025 do not count.
        record("b3", 2021, commit=10.0, disburse=500.0, donor=2, dated=2019),
        record("b4", 2019, disburse=500.0, donor=2, dated=2015),
    ]
    assert dated_donors(build_units(crs_frame(records))) == [1]


def test_window_donors_need_eighty_percent_inside_over_100m() -> None:
    def donor(code: int, inside: float, outside: float) -> list[dict[str, object]]:
        return [
            record(f"{code}c", 2018, commit=1e9, donor=code, start=2018, end=2020),
            record(f"{code}i", 2021, disburse=inside, donor=code),
            record(f"{code}o", 2023, disburse=outside, donor=code),
        ]

    crs = crs_frame(
        [*donor(1, 90e6, 10e6), *donor(2, 70e6, 30e6), *donor(3, 45e6, 5e6)]
    )
    assert window_donors(build_units(crs), crs=crs) == [1]


# CRDF rows ---------------------------------------------------------------------


def test_an_aggregate_parent_with_children_is_left_out() -> None:
    rows = counted_rows(
        crdf_frame(
            [
                crdf_row("parent", 2021, 100.0),
                crdf_row("child", 2021, 60.0, parent="parent"),
                crdf_row("lone_parent", 2021, 100.0),
                {**crdf_row("provider_side", 2021, 1.0), "perspective": "provider"},
            ]
        )
    ).collect()
    assert sorted(rows["row_id"]) == ["child", "lone_parent"]


def test_rows_split_a_commitment_by_face_and_never_give_more_than_it_received() -> None:
    # The CRDF rows' faces (80 + 40) exceed the CRS face (100): the record's 100 is
    # split 2:1, so both rows stay under face and the record's total holds.
    crs, crdf, units, links = link(
        [record("c19", 2019, commit=100.0), record("d20", 2020, disburse=100.0)],
        [crdf_row("big", 2019, 80.0), crdf_row("small", 2019, 40.0)],
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    got = dict(rows.select("row_id", "allocated_usd").iter_rows())
    assert got == pytest.approx({"big": 200 / 3, "small": 100 / 3})


def test_rows_below_crs_face_leave_the_rest_to_the_crs_commitment() -> None:
    crs, crdf, units, links = link(
        [record("c19", 2019, commit=100.0), record("d20", 2020, disburse=100.0)],
        [crdf_row("half", 2019, 50.0)],
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    got = dict(rows.select("row_id", "allocated_usd").iter_rows())
    assert got == pytest.approx({"half": 50.0, None: 50.0})


def test_no_row_receives_a_record_from_before_its_year() -> None:
    # Denmark's IFU dates its 2023 top-up under the crs id by the 2017 commitment,
    # so both units sit in 2017. The 2019 record fills the 2017 row alone, and the
    # 2023 record the top-up.
    crs, crdf, units, links = link(
        [
            record("c17", 2017, commit=100.0),
            record("c23", 2023, commit=50.0, dated=2017),
            record("d19", 2019, disburse=120.0),
            record("d23", 2023, disburse=50.0),
        ],
        [crdf_row("r17", 2017, 100.0), crdf_row("r23", 2023, 50.0)],
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    got = {
        (row_id, record_id): amount
        for row_id, record_id, amount in rows.select(
            "row_id", "record_id", "allocated_usd"
        ).iter_rows()
    }
    assert got == pytest.approx(
        {("r17", "d19"): 100.0, (None, "d19"): 20.0, ("r23", "d23"): 50.0}
    )


@pytest.mark.parametrize(
    ("disbursed", "expected"), [(100.0, "disbursed"), (46.0, "partly_disbursed")]
)
def test_a_group_gives_its_allocation_to_its_first_row_and_not_its_repeat(
    disbursed: float, expected: str
) -> None:
    # The 2022 row reports the 2021 commitment again and sits in its group: the
    # first row takes the group's whole allocation, and its status reads that over
    # the whole CRS face.
    crs, crdf, units, links = link(
        [record("c21", 2021, commit=100.0), record("d22", 2022, disburse=disbursed)],
        [
            crdf_row("first", 2021, 100.0),
            {**crdf_row("repeat", 2022, 100.0), "repeat_of": "first"},
        ],
    )
    group = links.filter(pl.col("task") == "crdf_crs", pl.col("left_id") == "first")
    links = pl.concat([links, group.with_columns(left_id=pl.lit("repeat"))])
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    got = dict(rows.select("row_id", "allocated_usd").iter_rows())
    assert got == pytest.approx({"first": disbursed})
    facts = status_basis(
        crdf,
        units=units,
        crs=crs,
        links=links,
        pairs=pairs,
        id_practice=no_classes().lazy(),
    )
    status = statuses(compute_status(commitments=facts, allocations=rows))
    assert status["first"] == expected


@pytest.mark.parametrize(
    ("first_face", "expected"),
    [(100.0, {"first": 100.0}), (60.0, {"first": 60.0, None: 40.0})],
)
def test_a_repeat_hands_its_allocation_to_its_first_row_up_to_face(
    first_face: float, expected: dict[str | None, float]
) -> None:
    # The 2022 row republishes the 2021 commitment under the crs id the bulk
    # records it with in 2022, so it links to the 2022 commitment alone.
    crs, crdf, units, links = link(
        [
            record("c21", 2021, commit=100.0, crs_id="A"),
            record("c22", 2022, commit=100.0, crs_id="B"),
            record("d23", 2023, disburse=100.0, crs_id="B"),
        ],
        [
            crdf_row("first", 2021, first_face, crs_id="A"),
            {**crdf_row("repeat", 2022, 100.0, crs_id="B"), "repeat_of": "first"},
        ],
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    rows = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    got = dict(rows.select("row_id", "allocated_usd").iter_rows())
    assert got == pytest.approx(expected)


# Status ------------------------------------------------------------------------


def basis(**rows: tuple[float, bool, bool]) -> pl.DataFrame:
    """Status inputs as row_id=(face, traceable, id_disbursing); climate is half."""
    return pl.DataFrame(
        [(row, face, face / 2, t, d) for row, (face, t, d) in rows.items()],
        schema={
            "row_id": pl.String,
            "face_usd": pl.Float64,
            "climate_usd": pl.Float64,
            "traceable": pl.Boolean,
            "id_disbursing": pl.Boolean,
        },
        orient="row",
    ).with_columns(crs_face_usd=pl.col("face_usd"))


def allocations(**amounts: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "row_id": list(amounts),
            "record_id": [f"r{row}" for row in amounts],
            "year": pl.Series([2022] * len(amounts), dtype=pl.Int16),
            "allocated_usd": list(amounts.values()),
            "allocated_low_usd": list(amounts.values()),
            "allocated_high_usd": list(amounts.values()),
            "rule": ["fifo"] * len(amounts),
            "disbursement_source": ["crs"] * len(amounts),
        }
    )


def statuses(frame: pl.LazyFrame) -> dict[str, str]:
    return dict(frame.select("row_id", "status").collect().iter_rows())


def test_status_reads_allocated_over_face() -> None:
    status = compute_status(
        commitments=basis(
            full=(100.0, True, False),
            ninety=(100.0, True, False),
            part=(100.0, True, False),
            none=(100.0, True, False),
            lost=(100.0, False, False),
            ledger=(100.0, False, False),
        ),
        allocations=allocations(full=100.0, ninety=90.0, part=89.0, ledger=1.0),
    )
    assert statuses(status) == {
        "full": "disbursed",
        "ninety": "disbursed",
        "part": "partly_disbursed",
        "none": "no_disbursement_record",
        "lost": "not_traceable",
        "ledger": "partly_disbursed",
    }


def test_pending_overrides_every_status() -> None:
    status = compute_status(
        commitments=basis(full=(100.0, True, False), lost=(100.0, False, False)),
        allocations=allocations(full=100.0),
        pending=pl.DataFrame({"row_id": ["full", "lost"]}),
    )
    assert statuses(status) == {"full": "pending_review", "lost": "pending_review"}


def test_project_disbursing_marks_no_record_rows_whose_id_disburses() -> None:
    status = compute_status(
        commitments=basis(
            waiting=(100.0, True, True),
            idle=(100.0, True, False),
            paid=(100.0, True, True),
        ),
        allocations=allocations(paid=50.0),
    ).collect()
    flagged = status.filter(pl.col("project_disbursing"))["row_id"].to_list()
    assert flagged == ["waiting"]


def test_status_basis_reads_links_id_practice_and_earlier_tranches() -> None:
    # Donor 2's agency is classed `neither`; donor 3's keys 0.1 of its disbursement
    # value and donor 4's 0.9.
    crs, crdf, units, links = link(
        [
            record("c19", 2019, commit=100.0),
            record("d20", 2020, disburse=100.0),
            record("c21", 2021, commit=100.0),
            record("n21", 2021, commit=100.0, crs_id="2021000009", donor=2),
            record("u21", 2021, commit=100.0, crs_id="2021000003", donor=3),
            record("p21", 2021, commit=100.0, crs_id="2021000033", donor=3),
            record("p22", 2022, disburse=50.0, crs_id="2021000033", donor=3),
            record("k21", 2021, commit=100.0, crs_id="2021000004", donor=4),
        ],
        [
            crdf_row("r19", 2019, 100.0),
            crdf_row("r21", 2021, 100.0),
            crdf_row("rn", 2021, 100.0, crs_id="2021000009", donor=2),
            crdf_row("r_unlinked", 2021, 100.0, crs_id="2021999999"),
            crdf_row("r_unkeyed", 2021, 100.0, crs_id="2021000003", donor=3),
            crdf_row("r_unkeyed_paid", 2021, 100.0, crs_id="2021000033", donor=3),
            crdf_row("r_keyed", 2021, 100.0, crs_id="2021000004", donor=4),
        ],
    )
    id_practice = pl.LazyFrame(
        {
            "donor_code": [2, 3, 4],
            "agency_code": [10, 10, 10],
            "period": ["2019-2022"] * 3,
            "id_class": ["neither", "mixed", "mixed"],
            "keyed_disbursement_share": [None, 0.1, 0.9],
        },
        schema_overrides={
            "donor_code": pl.Int32,
            "agency_code": pl.Int32,
            "keyed_disbursement_share": pl.Float64,
        },
    )
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    facts = status_basis(
        crdf, units=units, crs=crs, links=links, pairs=pairs, id_practice=id_practice
    )
    rows = {row["row_id"]: row for row in facts.iter_rows(named=True)}
    assert {r: rows[r]["traceable"] for r in rows} == {
        "r19": True,
        "r21": True,
        "rn": False,
        "r_unlinked": False,
        "r_unkeyed": False,
        "r_unkeyed_paid": False,
        "r_keyed": True,
    }
    assert [r for r in rows if rows[r]["id_disbursing"]] == ["r21"]
    status = compute_status(
        commitments=facts,
        allocations=allocate_to_rows(
            pairs, crdf=crdf, units=units, crs=crs, links=links
        ),
    )
    assert statuses(status) == {
        "r19": "disbursed",
        "r21": "no_disbursement_record",
        "rn": "not_traceable",
        "r_unlinked": "not_traceable",
        "r_unkeyed": "not_traceable",
        "r_unkeyed_paid": "partly_disbursed",
        "r_keyed": "no_disbursement_record",
    }
    assert status.collect().filter("project_disbursing")["row_id"].to_list() == ["r21"]


def test_a_row_linked_across_years_reads_status_over_the_crs_face() -> None:
    # A 2021 row of EUR 1,000 converts to USD 1,000; the bulk records it in 2022,
    # when the same euros convert to USD 889, and 2023 disburses all of it. A
    # recovery route links the row to the later commitment.
    crs, crdf, units, links = link(
        [
            record("c22", 2022, commit=889.0, crs_id="EIB1"),
            record("d23", 2023, disburse=889.0, crs_id="EIB1"),
        ],
        [crdf_row("r21", 2021, 1000.0, crs_id="EIB1")],
    )
    recovered = as_exact_links(
        pl.LazyFrame(
            {
                "left_id": ["r21"],
                "right_id": [unit_of(units, "c22")],
                "group_id": ["eib_list|EIB1"],
                "link_method": ["eib_list"],
            }
        ),
        task="crdf_crs",
    )
    links = pl.concat([links, recovered])
    pairs = allocate(units, crs=crs, links=links, crdf=crdf)
    table = allocate_to_rows(pairs, crdf=crdf, units=units, crs=crs, links=links)
    facts = status_basis(
        crdf,
        units=units,
        crs=crs,
        links=links,
        pairs=pairs,
        id_practice=no_classes().lazy(),
    )
    status = (
        compute_status(commitments=facts, allocations=table)
        .collect()
        .row(0, named=True)
    )

    # Status reads the commitment's full disbursement over its CRS face; the
    # amount stays in the 2023 dollars it was disbursed in, below the row's face.
    assert status["status"] == "disbursed"
    assert status["disbursed_usd"] == pytest.approx(889.0)
    assert table["allocated_usd"].sum() == pytest.approx(889.0)


def _status_bits(rng: np.random.Generator) -> bytes:
    rows = rng.integers(0, 500, 2_000)
    amounts = rng.lognormal(12, 2, rows.size)
    table = pl.DataFrame(
        {
            "row_id": rows.astype(str),
            "record_id": np.arange(rows.size).astype(str),
            "year": np.full(rows.size, 2022, dtype=np.int16),
            "allocated_usd": amounts,
            "allocated_low_usd": amounts,
            "allocated_high_usd": amounts,
            "rule": "fifo",
        }
    )
    facts = basis(**{str(row): (1e9, True, False) for row in range(500)})
    status = compute_status(commitments=facts, allocations=table).collect()
    return status.sort("row_id")["disbursed_usd"].to_numpy().tobytes()


def _fill_bits(rng: np.random.Generator) -> bytes:
    size = 400_000
    candidates = pl.DataFrame(
        {
            "pass": np.zeros(size, dtype=np.uint32),
            "group_id": rng.integers(0, 50_000, size).astype(str),
            "c_idx": np.arange(size, dtype=np.uint32),
            "order": rng.integers(2010, 2013, size).astype(np.int32),
        }
    ).with_columns(total=pl.len().over("group_id").cast(pl.Float64) * 3e5)
    filled = _fill(candidates, rng.lognormal(12, 2, size))
    return filled.select("fill", "capacity").to_numpy().tobytes()


def _cancellation_bits(rng: np.random.Generator) -> bytes:
    size = 50_000
    crs = pl.LazyFrame(
        {
            "record_id": np.arange(size).astype(str),
            "donor_code": rng.integers(1, 30, size).astype(np.int32),
            "year": rng.integers(2010, 2025, size).astype(np.int16),
            "commitment_usd": rng.normal(0, 1e6, size),
        }
    )
    reported = cancellations(crs).collect().sort("provider_code", "year")
    return reported["cancelled_usd"].to_numpy().tobytes()


@pytest.mark.parametrize("bits", [_status_bits, _fill_bits, _cancellation_bits])
def test_float_sums_give_the_same_bits_on_every_run(
    bits: Callable[[np.random.Generator], bytes],
) -> None:
    # A polars group sum adds its threads' partial sums in any order; at these
    # sizes a plain sum changed the last bits from one run to the next.
    runs = {bits(np.random.default_rng(0)) for _ in range(6)}
    assert len(runs) == 1


def test_status_sums_a_rows_allocations_in_ascending_order() -> None:
    # Added as listed the amounts sum to 0.6, and in ascending order to
    # 0.6000000000000001, so a sum in input order shows on one thread.
    ascending = 0.1 + 0.2 + 0.3
    assert ascending != 0.3 + 0.2 + 0.1
    table = pl.concat([allocations(r=amount) for amount in (0.3, 0.2, 0.1)])
    status = compute_status(
        commitments=basis(r=(1.0, True, False)), allocations=table
    ).collect()
    amounts = ("disbursed_usd", "disbursed_low_usd", "disbursed_high_usd")
    assert status.select(amounts).row(0) == (ascending,) * 3


def test_years_since_commitment_accumulates_by_record_year() -> None:
    crdf = crdf_frame([crdf_row("r", 2019, 100.0)])
    table = pl.concat([allocations(r=30.0), allocations(r=50.0)]).with_columns(
        year=pl.Series([2020, 2022], dtype=pl.Int16)
    )
    got = years_since_commitment(table, crdf=crdf, last_year=2023).collect()
    assert got["cumulative_disbursed_usd"].to_list() == [0.0, 30.0, 30.0, 80.0, 80.0]
    assert got["years_since"].to_list() == [0, 1, 2, 3, 4]


# Generated cases ---------------------------------------------------------------

DONORS = tuple(range(1, 9))
SEEDS = range(8)


def _random_records(
    rng: np.random.Generator,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """CRS records and CRDF rows for eight donors with four ids each."""
    records, crdf = [], []
    for donor in DONORS:
        for k in range(4):
            crs_id = f"20150000{k:02d}"
            for n in range(rng.integers(1, 5)):
                year = int(rng.integers(2012, 2023))
                purpose = int(rng.choice([140, 230]))
                rid = f"c{donor}{k}{n}"
                both = rng.random() < 0.2
                dated = rng.random() < 0.5
                records.append(
                    record(
                        rid,
                        year,
                        commit=float(rng.integers(1, 500)),
                        disburse=float(rng.integers(1, 100)) if both else None,
                        donor=donor,
                        crs_id=crs_id,
                        purpose=purpose,
                        dated=year - int(rng.integers(0, 3)) if dated else None,
                        start=year,
                        end=year + int(rng.integers(0, 4)),
                    )
                )
                crdf.extend(
                    crdf_row(
                        f"{rid}r{m}",
                        year,
                        float(rng.integers(1, 400)),
                        climate=float(rng.integers(1, 100)),
                        donor=donor,
                        crs_id=crs_id,
                        purpose=purpose,
                    )
                    for m in range(rng.integers(0, 3))
                )
            for n in range(rng.integers(0, 8)):
                year = int(rng.integers(2013, 2026))
                draw = rng.random()
                if draw < 0.4:
                    dated_year = None
                elif draw < 0.6:
                    dated_year = year
                else:
                    dated_year = year - int(rng.integers(1, 5))
                records.append(
                    record(
                        f"d{donor}{k}{n}",
                        year,
                        disburse=float(rng.integers(1, 300)),
                        donor=donor,
                        # A purpose no commitment has links by crs_id, and a
                        # suffixed id by its parent.
                        crs_id=crs_id + "001" if draw < 0.1 else crs_id,
                        purpose=int(rng.choice([140, 230, 999])),
                        dated=dated_year,
                    )
                )
    return records, crdf


def _donor_groups(units: pl.LazyFrame) -> pl.LazyFrame:
    """A key-repair group per donor after the exact tiers: each disbursement unit
    reaches every commitment unit of its donor that its year and floor allow."""
    disbursing = units.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= FIRST_YEAR),
    ).select("unit_id", "donor_code", "year", "floor_year")
    committing = units.filter(pl.col("commitment_usd") > 0).select(
        right_id="unit_id", donor_code="donor_code", placed="placed_year"
    )
    pairs = (
        disbursing.join(committing, on="donor_code")
        .filter(
            pl.col("placed") <= pl.col("year"),
            pl.col("floor_year").is_null() | (pl.col("placed") >= pl.col("floor_year")),
        )
        .select(
            left_id="unit_id",
            right_id="right_id",
            group_id=pl.format("repair_new_crs_id|{}", "donor_code"),
            link_method=pl.lit("repair_new_crs_id"),
        )
    )
    return as_exact_links(pairs, task="trace")


@cache
def _scenario(
    seed: int, *, reorder: bool, cascade: bool = False
) -> dict[str, pl.DataFrame]:
    """A random case linked by the link stage and allocated.

    With `reorder`, odd donors are dated and even ones window donors. With
    `cascade`, every disbursement unit also carries a donor-wide key-repair group
    that takes what its exact group left. Cached, since each property reads the
    same cases; callers must not mutate the frames.
    """
    records, crdf = _random_records(np.random.default_rng(seed))
    crs, rows, units, links = link(records, crdf)
    if cascade:
        links = pl.concat([links, _donor_groups(units)])
    pairs = allocate(
        units,
        crs=crs,
        links=links,
        crdf=rows,
        dated=DONORS[::2] if reorder else (),
        windowed=DONORS[1::2] if reorder else (),
    )
    table = allocate_to_rows(pairs, crdf=rows, units=units, crs=crs, links=links)
    basis = status_basis(
        rows,
        units=units,
        crs=crs,
        links=links,
        pairs=pairs,
        id_practice=no_classes().lazy(),
    )
    unit_frame = units.collect()
    members = unit_frame.select("unit_id", record_id="record_ids").explode("record_id")
    # Record-level trace pairs: (disbursement record, commitment record), and
    # whether the record may fill the commitment, by not preceding its CRDF rows.
    groups = links.filter(pl.col("task") == "crdf_crs").collect()
    row_years = (
        groups.join(rows.collect(), left_on="left_id", right_on="row_id")
        .group_by(right_id="right_id")
        .agg(row_year=pl.col("year").max())
    )
    trace = (
        links.filter(pl.col("task") == "trace")
        .collect()
        .join(members.rename({"unit_id": "left_id"}), on="left_id")
        .join(
            members.rename({"unit_id": "right_id", "record_id": "commitment_id"}),
            on="right_id",
        )
        .join(unit_frame.select(left_id="unit_id", year="year"), on="left_id")
        .join(row_years, on="right_id", how="left")
        .with_columns(
            eligible=pl.col("row_year").is_null()
            | (pl.col("year") >= pl.col("row_year"))
        )
    )
    return {
        "crs": crs.collect(),
        "crdf": rows.collect(),
        "units": unit_frame,
        "trace": trace,
        "pairs": pairs,
        "allocations": table,
        "basis": basis,
    }


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("cascade", [False, True])
@pytest.mark.parametrize("reorder", [False, True])
def test_allocated_plus_excess_equals_each_record(
    seed: int, reorder: bool, cascade: bool
) -> None:
    case = _scenario(seed, reorder=reorder, cascade=cascade)
    linked = case["trace"]["record_id"].unique().implode()
    totals = (
        case["crs"]
        .filter(pl.col("record_id").is_in(linked), pl.col("disbursement_usd") > 0)
        .select("record_id", "disbursement_usd")
        .join(
            case["allocations"]
            .group_by("record_id")
            .agg(pl.col("allocated_usd").sum()),
            on="record_id",
            how="left",
        )
    )
    assert totals.height > 0
    gap = (totals["disbursement_usd"] - totals["allocated_usd"].fill_null(0.0)).abs()
    assert gap.max() < TOLERANCE
    assert case["allocations"]["record_id"].is_in(linked).all()


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("cascade", [False, True])
@pytest.mark.parametrize("reorder", [False, True])
def test_no_commitment_receives_more_than_face(
    seed: int, reorder: bool, cascade: bool
) -> None:
    case = _scenario(seed, reorder=reorder, cascade=cascade)
    received = (
        case["pairs"]
        .drop_nulls("commitment_id")
        .group_by("commitment_id")
        .agg(pl.col("allocated_usd").sum())
        .join(
            case["crs"].select(commitment_id="record_id", face="commitment_usd"),
            on="commitment_id",
        )
    )
    assert received.height > 0
    assert (received["allocated_usd"] - received["face"]).max() < TOLERANCE


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("cascade", [False, True])
@pytest.mark.parametrize("reorder", [False, True])
def test_allocated_plus_undisbursed_equals_climate_value(
    seed: int, reorder: bool, cascade: bool
) -> None:
    case = _scenario(seed, reorder=reorder, cascade=cascade)
    table, commitments = case["allocations"], case["basis"]
    status = compute_status(commitments=commitments, allocations=table).collect()
    share = pl.col("climate_usd") / pl.col("face_usd")
    allocated = (
        table.drop_nulls("row_id")
        .join(commitments, on="row_id")
        .group_by("row_id")
        .agg((pl.col("allocated_usd") * share).sum().alias("allocated_climate"))
    )
    assert allocated.height > 0
    check = status.join(allocated, on="row_id", how="left").with_columns(
        undisbursed=share * (pl.col("face_usd") - pl.col("disbursed_usd")),
        allocated_climate=pl.col("allocated_climate").fill_null(0.0),
    )
    total = check["allocated_climate"] + check["undisbursed"]
    assert (total - check["climate_usd"]).abs().max() < TOLERANCE
    assert (check["disbursed_usd"] - check["face_usd"]).max() < TOLERANCE
    assert (check["disbursed_low_usd"] <= check["disbursed_usd"]).all()
    assert (check["disbursed_usd"] <= check["disbursed_high_usd"]).all()
    assert (check["disbursed_low_usd"] >= 0).all()


@pytest.mark.parametrize("seed", SEEDS)
def test_fifo_fills_an_older_commitment_before_a_newer_one(seed: int) -> None:
    # Whenever a record reached a commitment other than its own, every older
    # commitment it could also fill ended full. A record that commits and
    # disburses fills itself first; the random titles are empty, so no other
    # commitment is a record's own.
    case = _scenario(seed, reorder=False)
    received = (
        case["pairs"]
        .drop_nulls("commitment_id")
        .group_by("commitment_id")
        .agg(pl.col("allocated_usd").sum())
    )
    placed = (
        case["units"]
        .select("placed_year", commitment_id="record_ids")
        .explode("commitment_id")
    )
    left = (
        case["crs"]
        .filter(pl.col("commitment_usd") > 0)
        .select(commitment_id="record_id", face="commitment_usd")
        .join(placed, on="commitment_id")
        .join(received, on="commitment_id", how="left")
        .select(
            "commitment_id",
            cyear="placed_year",
            headroom=pl.col("face") - pl.col("allocated_usd").fill_null(0.0),
        )
    )
    reached = (
        case["pairs"]
        .drop_nulls("commitment_id")
        .filter(
            pl.col("allocated_usd") > 0, pl.col("commitment_id") != pl.col("record_id")
        )
        .join(left.select("commitment_id", reached_year="cyear"), on="commitment_id")
        .select("record_id", "reached_year")
    )
    assert reached.height > 0
    open_older = (
        case["trace"]
        .filter("eligible")
        .select("record_id", "commitment_id")
        .join(left, on="commitment_id")
        .join(reached, on="record_id")
        .filter(
            pl.col("cyear") < pl.col("reached_year"), pl.col("headroom") > TOLERANCE
        )
    )
    assert open_older.is_empty()


# The stage ---------------------------------------------------------------------


def test_run_allocate_writes_tables_that_pass_their_schemas(tmp_path: Path) -> None:
    records, crdf_rows = _random_records(np.random.default_rng(3))
    # One CRDF row becomes an aggregate child, so its parent drops out.
    child = {**crdf_rows[0], "parent_row_id": "parent"}
    parent = crdf_row("parent", 2020, 1e9, crs_id="2015000000")
    crdf = crdf_frame([parent, child, *crdf_rows[1:]]).collect()
    crs, rows, units, links = link(records, crdf.to_dicts())

    release_dir = tmp_path / "r"
    write_fetched_years(release_dir)
    write_inputs(release_dir, "normalise", {"crs": crs.collect(), "crdf": crdf})
    write_inputs(
        release_dir,
        "link",
        {
            "units": units.collect(),
            "links": links.collect(),
            "id_practice": no_classes(),
        },
    )
    written = run_stage("allocate", stage_context(release_dir))
    assert set(written) == set(schema.STAGE_TABLES["allocate"])

    status = pl.read_parquet(table_path(release_dir, "allocate", "commitments"))
    assert sorted(status["row_id"]) == sorted(rows.collect()["row_id"])
    assert "parent" not in status["row_id"]
    # A later stage recomputes status from the stage's own tables.
    table = pl.read_parquet(table_path(release_dir, "allocate", "allocations"))
    again = compute_status(commitments=status, allocations=table).collect()
    assert_frame_equal(again, status, check_row_order=False)
    since = pl.read_parquet(
        table_path(release_dir, "allocate", "years_since_commitment")
    )
    assert since["years_since"].max() == 2024 - crdf["year"].min()
