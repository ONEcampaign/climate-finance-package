"""The recovery routes from CRDF rows to CRS commitments."""

import polars as pl
import pytest
from normalised import crdf_rows, crs_records

from climate_finance import schema
from climate_finance.build.allocate import (
    allocate,
    allocate_to_rows,
    compute_status,
    status_basis,
)
from climate_finance.build.errors import LinkInputError
from climate_finance.build.link.crdf_crs import link_within_year, recover
from climate_finance.build.link.id_practice import classify_id_practice
from climate_finance.build.link.keys import build_units, link_by_keys

EIB = {"provider_code": 918, "agency_code": 3}
BULK = {"donor_code": 918, "agency_code": 3, "purpose_code": 23210}
# Euros per US dollar: one rate everywhere, unless a test sets its own.
RATES = dict.fromkeys(range(2015, 2026), 1.0)


def _row(row_id: str, year: int, face: float, **fields: object) -> dict[str, object]:
    return {"row_id": row_id, "year": year, "climate_usd": face, **EIB, **fields}


def _bulk(record_id: str, year: int, commit: float, **fields: object) -> dict:
    return {
        "record_id": record_id,
        "year": year,
        "commitment_usd": commit,
        "crs_id": f"{year}{record_id}",
        **BULK,
        **fields,
    }


def _external(*activity_ids: str) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "source": ["iati"] * len(activity_ids),
            "publisher": ["eib"] * len(activity_ids),
            "activity_id": list(activity_ids),
        },
        schema={"source": pl.String, "publisher": pl.String, "activity_id": pl.String},
    )


def _exchange(rates: dict[int, float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "iso_code": ["EUI"] * len(rates),
            "year": list(rates),
            "exchange_rate": list(rates.values()),
        }
    )


def _recover(
    rows: list[dict[str, object]],
    records: list[dict[str, object]],
    *,
    activities: tuple[str, ...] = (),
    rates: dict[int, float] = RATES,
    skip: tuple[str, ...] = (),
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(links, units) as the link stage writes them with recoveries on."""
    crdf = crdf_rows(*rows).lazy()
    crs = crs_records(*records).lazy()
    units = build_units(crs).collect()
    links = recover(
        crdf,
        within_year=link_within_year(crdf, units=units.lazy()).collect(),
        units=units.lazy(),
        crs=crs,
        external=_external(*activities),
        exchange=_exchange(rates),
        skip=skip,
    )
    schema.validate(schema.LINKS, links)
    return links, units


def _linked(links: pl.DataFrame, units: pl.DataFrame) -> set[tuple[str, str, str]]:
    """(row, record, method) for each `crdf_crs` link."""
    records = units.select("unit_id", record=pl.col("record_ids").list.first())
    return set(
        links.filter(task="crdf_crs")
        .join(records, left_on="right_id", right_on="unit_id")
        .select("left_id", "record", "link_method")
        .iter_rows()
    )


def test_the_project_list_links_a_row_to_its_projects_bulk_commitment() -> None:
    links, units = _recover(
        [_row("row", 2021, 100.0, project_number="90001")],
        [_bulk("b", 2022, 101.0, project_number="90001")],
    )

    assert _linked(links, units) == {("row", "b", "eib_list")}
    assert set(links["group_id"]) == {"eib_list|row"}


def test_the_project_list_matches_a_specific_title_and_close_amount() -> None:
    # The titles share three words of five: enough for the project list, not for
    # the title route.
    links, units = _recover(
        [_row("row", 2021, 100.0, title="SOLAR PARK KENYA PHASE")],
        [_bulk("b", 2022, 102.0, title="SOLAR PARK KENYA EXTENSION")],
    )

    assert _linked(links, units) == {("row", "b", "eib_list")}


@pytest.mark.parametrize(("commit", "linked"), [(106.0, True), (111.0, False)])
def test_the_crosswalk_takes_a_contract_of_the_rows_serial_within_ten_percent(
    commit: float, linked: bool
) -> None:
    links, units = _recover(
        [_row("row", 2022, 100.0, project_number="20220001", title="ALPHA")],
        [_bulk("b", 2023, commit, project_number="95001", title="BETA")],
        activities=("XM-DAC-918-3-20220001-95001", "XM-DAC-918-3-20220002-95002"),
    )

    assert _linked(links, units) == ({("row", "b", "crosswalk")} if linked else set())


@pytest.mark.parametrize(
    ("skip", "linked"),
    [((), True), (("crosswalk",), False), (("title_recipient",), True)],
)
def test_a_skipped_route_links_nothing(skip: tuple[str, ...], linked: bool) -> None:
    links, units = _recover(
        [_row("row", 2022, 100.0, project_number="20220001", title="ALPHA")],
        [_bulk("b", 2023, 106.0, project_number="95001", title="BETA")],
        activities=("XM-DAC-918-3-20220001-95001",),
        skip=skip,
    )

    assert _linked(links, units) == ({("row", "b", "crosswalk")} if linked else set())


def test_an_unknown_route_to_skip_is_refused() -> None:
    with pytest.raises(LinkInputError, match="cross_walk"):
        _recover([], [], skip=("cross_walk",))


def test_a_projects_amount_counts_commitments_from_the_rows_year_on() -> None:
    links, units = _recover(
        [_row("row", 2022, 100.0, project_number="20220001")],
        [
            _bulk("before", 2021, 50.0, project_number="95001"),
            _bulk("after", 2023, 104.0, project_number="95001"),
        ],
        activities=("XM-DAC-918-3-20220001-95001",),
    )

    assert _linked(links, units) == {("row", "after", "crosswalk")}


def test_amounts_compare_in_euros() -> None:
    # USD 125 at 0.8 and USD 100 at 1.0 are both EUR 100.
    links, units = _recover(
        [_row("row", 2021, 125.0, project_number="20210001")],
        [_bulk("b", 2023, 100.0, project_number="95001")],
        activities=("XM-DAC-918-3-20210001-95001",),
        rates=RATES | {2021: 0.8},
    )

    assert _linked(links, units) == {("row", "b", "crosswalk")}


def test_rows_and_projects_pair_one_to_one_closest_amount_first() -> None:
    # Taken in row order, row "a" would take p1 and leave "b" 4% from p2.
    links, units = _recover(
        [
            _row("a", 2022, 97.0, project_number="20220001"),
            _row("b", 2022, 100.0, project_number="20220001"),
        ],
        [
            _bulk("p1", 2023, 99.0, project_number="95001"),
            _bulk("p2", 2023, 96.0, project_number="95002"),
        ],
        activities=("XM-DAC-918-3-20220001-95001", "XM-DAC-918-3-20220001-95002"),
    )

    assert _linked(links, units) == {
        ("a", "p2", "crosswalk"),
        ("b", "p1", "crosswalk"),
    }


TITLE = "LAKE VICTORIA WATER SUPPLY"


@pytest.mark.parametrize(
    ("bulk_title", "recipient", "copies", "linked"),
    [
        # A trailing tranche letter is not a word, so the titles are the same words.
        (f"{TITLE} B", 248, 1, True),
        (f"{TITLE} B", 249, 1, False),
        (f"{TITLE} B", 248, 11, False),
        # Three words of five in common.
        ("LAKE VICTORIA SEWERAGE SUPPLY", 248, 1, False),
    ],
)
def test_the_title_route_needs_the_recipient_and_a_specific_title(
    bulk_title: str, recipient: int, copies: int, linked: bool
) -> None:
    others = [
        _bulk(f"o{n}", 2016, 5.0, project_number=f"8000{n}", title=bulk_title)
        for n in range(copies - 1)
    ]
    links, units = _recover(
        [_row("row", 2021, 100.0, project_number="93000", title=TITLE)],
        [
            _bulk(
                "b",
                2022,
                107.0,
                project_number="93111",
                title=bulk_title,
                recipient_code=recipient,
            ),
            *others,
        ],
    )

    expected = {("row", "b", "title_recipient")} if linked else set()
    assert _linked(links, units) == expected


def test_a_missing_euro_rate_is_refused() -> None:
    with pytest.raises(LinkInputError, match="2021"):
        _recover(
            [_row("row", 2021, 100.0, project_number="90001")],
            [_bulk("b", 2022, 100.0, project_number="90001")],
            rates={2022: 1.0},
        )


def _repeated(*, repeat_of: str | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    # The 2024 row carries the bulk id and joins it within its year; the 2021 row
    # matches the same commitment by project number.
    return _recover(
        [
            _row("first", 2021, 100.0, project_number="90001"),
            _row(
                "later",
                2024,
                100.0,
                crs_id="2024b",
                purpose_code=23210,
                repeat_of=repeat_of,
            ),
        ],
        [_bulk("b", 2024, 100.0, project_number="90001")],
    )


def test_the_earliest_row_takes_over_its_repeats_commitment() -> None:
    links, units = _repeated(repeat_of="first")

    assert _linked(links, units) == {("first", "b", "eib_list")}
    assert set(links["group_id"]) == {"eib_list|first"}


def test_a_commitment_another_row_holds_is_not_recovered() -> None:
    links, units = _repeated(repeat_of=None)

    assert _linked(links, units) == {("later", "b", "within_year")}


def test_repeat_rows_are_not_recovered() -> None:
    links, units = _recover(
        [
            _row("first", 2020, 300.0, project_number="70001"),
            _row("later", 2021, 100.0, project_number="90001", repeat_of="first"),
        ],
        [_bulk("b", 2022, 100.0, project_number="90001")],
    )

    assert _linked(links, units) == set()


def test_a_taken_over_commitment_allocates_to_the_earliest_row() -> None:
    rows = [
        _row("first", 2021, 100.0, project_number="90001"),
        _row("later", 2024, 100.0, crs_id="2024b", repeat_of="first"),
    ]
    records = [
        _bulk("b", 2024, 100.0, project_number="90001"),
        _bulk("d", 2025, None, crs_id="2024b", disbursement_usd=95.0),
    ]
    crdf = crdf_rows(*rows).lazy()
    crs = crs_records(*records).lazy()
    units = build_units(crs).collect()
    id_practice = classify_id_practice(units)
    trace = link_by_keys(units, first_year=2010)
    recovered = recover(
        crdf,
        within_year=link_within_year(crdf, units=units.lazy()).collect(),
        units=units.lazy(),
        crs=crs,
        external=_external(),
        exchange=_exchange(RATES),
    )
    links = pl.concat([trace, recovered]).lazy()

    pairs = allocate(units.lazy(), crs=crs, links=links, dated=[], windowed=[])
    allocations = allocate_to_rows(
        pairs, crdf=crdf, units=units.lazy(), crs=crs, links=links
    )
    basis = status_basis(
        crdf,
        units=units.lazy(),
        crs=crs,
        links=links,
        pairs=pairs,
        id_practice=id_practice.lazy(),
    )
    status = compute_status(commitments=basis, allocations=allocations).collect()

    assert dict(status.select("row_id", "status").iter_rows()) == {
        "first": "disbursed",
        "later": "not_traceable",
    }


def _aggregate(child_recipient: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    # A Norwegian aggregate row rebuilt from one id-less record to unspecified
    # recipients, which normalise codes 998 in the CRS as in the CRDF.
    aggregate = {"provider_code": 8, "purpose_code": 99810, "recipient_code": 998}
    return _recover(
        [
            _row("parent", 2021, 50.0, **aggregate),
            _row(
                "child",
                2021,
                50.0,
                **aggregate | {"agency_code": 8, "recipient_code": child_recipient},
                parent_row_id="parent",
            ),
        ],
        [
            {
                "record_id": "r",
                "year": 2021,
                "donor_code": 8,
                "agency_code": 8,
                "recipient_code": 998,
                "purpose_code": 99810,
                "commitment_usd": 50.0,
                "disbursement_usd": 50.0,
            }
        ],
    )


def test_an_id_less_aggregate_child_links_to_its_records_commitment() -> None:
    links, units = _aggregate(998)
    unit = units["unit_id"].item()

    assert _linked(links, units) == {("child", "r", "aggregate_partition")}
    # The record's own disbursement fills its own commitment.
    trace = links.filter(task="trace")
    assert trace.select("left_id", "right_id", "link_method").rows() == [
        (unit, unit, "aggregate_partition")
    ]


def test_an_aggregate_child_needs_its_records_recipient() -> None:
    links, units = _aggregate(248)

    assert _linked(links, units) == set()
    assert links.filter(task="trace").is_empty()
