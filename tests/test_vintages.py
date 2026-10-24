"""Rows repeated across CRDF vintages, on hand-built normalised rows."""

import polars as pl
import pytest
from normalised import crdf_rows, crs_records

from climate_finance.build.normalise.crdf import CRDFNormaliseError
from climate_finance.build.normalise.vintages import mark_repeats

# Euros per US dollar. The euro strengthens, so one euro amount is fewer dollars in
# 2021 than in 2024.
RATES = {2019: 0.89, 2020: 0.88, 2021: 0.85, 2022: 0.95, 2023: 0.92, 2024: 0.92}


def _exchange(rates: dict[int, float] = RATES) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "iso_code": "EUI",
            "year": list(rates),
            "exchange_rate": list(rates.values()),
        },
        schema={"iso_code": pl.String, "year": pl.Int32, "exchange_rate": pl.Float64},
    )


def _repeats(
    crdf: pl.DataFrame, crs: pl.DataFrame, exchange: pl.DataFrame | None = None
) -> dict[str, str | None]:
    marked = mark_repeats(
        crdf.lazy(),
        crs=crs.lazy(),
        exchange=_exchange() if exchange is None else exchange,
    ).collect()
    assert marked.height == crdf.height
    return dict(marked.select("row_id", "repeat_of").iter_rows())


@pytest.mark.parametrize(
    ("committed", "repeat_of"),
    [
        ({2019: 1e6}, "a"),
        # A second commitment under the same id is a new commitment, not a repeat.
        ({2019: 1e6, 2021: 1e6}, None),
        # A token first row without a commitment of its own, and new money under the
        # id later: the later row repeats nothing.
        ({2017: 1e6, 2021: 1e6}, None),
    ],
)
def test_same_id_without_a_commitment_repeats(
    committed: dict[int, float], repeat_of: str | None
) -> None:
    crdf = crdf_rows(
        {"row_id": "a", "year": 2019, "crs_id": "2019000001"},
        {"row_id": "b", "year": 2021, "crs_id": "2019000001"},
    )
    records = crs_records(
        *(
            {
                "record_id": f"r{year}",
                "year": year,
                "crs_id": "2019000001",
                "commitment_usd": amount,
            }
            for year, amount in committed.items()
        )
    )

    assert _repeats(crdf, records) == {"a": None, "b": repeat_of}


BACKING = crs_records(
    {"record_id": "r1", "year": 2019, "crs_id": "2019000001", "commitment_usd": 10e6},
    {"record_id": "r3", "year": 2023, "crs_id": "2023000003", "commitment_usd": 10e6},
)


@pytest.mark.parametrize(
    ("field", "value"),
    [("project_number", "P-1234.0"), ("title", "Solar mini-grids for rural Kenya")],
)
@pytest.mark.parametrize(
    ("change", "repeats"),
    [
        ({}, True),
        # Faces 15% apart are two commitments.
        ({"face_usd": 11.5e6}, False),
        # The CRS commits under both ids: a tranche.
        ({"crs_id": "2023000003", "year": 2023}, False),
        # Another recipient.
        ({"recipient_code": 238}, False),
    ],
)
def test_project_id_and_title_pairs_repeat(
    field: str, value: str, change: dict[str, object], repeats: bool
) -> None:
    later = {"row_id": "b", "year": 2021, "crs_id": "2021000002", field: value} | change
    crdf = crdf_rows(
        {
            "row_id": "a",
            "year": 2019,
            "crs_id": "2019000001",
            field: value,
            "face_usd": 10e6,
        },
        {"face_usd": 10.5e6} | later,
    )

    assert _repeats(crdf, BACKING) == {"a": None, "b": "a" if repeats else None}


@pytest.mark.parametrize("placeholder", ["UNKNOWN", "N/A"])
def test_a_placeholder_project_number_pairs_nothing(placeholder: str) -> None:
    crdf = crdf_rows(
        {
            "row_id": "a",
            "year": 2019,
            "crs_id": "2019000001",
            "project_number": placeholder,
            "face_usd": 10e6,
        },
        {
            "row_id": "b",
            "year": 2021,
            "crs_id": "2021000002",
            "project_number": placeholder,
            "face_usd": 10e6,
        },
    )

    assert _repeats(crdf, BACKING) == {"a": None, "b": None}


def test_a_title_four_rows_carry_is_not_specific() -> None:
    title = "Solar mini-grids for rural Kenya"
    crdf = crdf_rows(
        {"row_id": "a", "year": 2019, "crs_id": "2019000001", "title": title},
        {"row_id": "b", "year": 2021, "crs_id": "2021000002", "title": title},
        {
            "row_id": "c",
            "year": 2022,
            "crs_id": "2022000002",
            "title": title,
            "recipient_code": 238,
        },
        {
            "row_id": "d",
            "year": 2022,
            "crs_id": "2022000003",
            "title": title,
            "recipient_code": 238,
        },
    )

    assert set(_repeats(crdf, BACKING).values()) == {None}


def test_a_group_counts_its_earliest_row() -> None:
    crdf = crdf_rows(
        {"row_id": "x", "year": 2023, "crs_id": "2023000009", "project_number": "P-77"},
        {"row_id": "y", "year": 2021, "crs_id": "2021000009", "project_number": "P-77"},
        {"row_id": "z", "year": 2019, "crs_id": "2019000009", "project_number": "P-77"},
    )

    assert _repeats(crdf, BACKING) == {"x": "z", "y": "z", "z": None}


def _eib(row_id: str, year: int, crs_id: str, **fields: object) -> dict[str, object]:
    return {
        "row_id": row_id,
        "year": year,
        "provider_code": 918,
        "agency_code": 3,
        "crs_id": crs_id,
        "recipient_code": 645,
        "purpose_code": 21020,
        "id_scheme": "sequence",
    } | fields


# Agra Metro's shape: a 2021 row under a reporting-sequence id, and the 2024 row
# under the bulk CRS id of the commitment the 2021 row matches. The 2024 row
# publishes no project number, so only the EIB rule pairs the two.
EUROS = 255e6
BULK_ID = "2024392954645"


def _eib_case(
    bulk_number: str = "92954", **earlier: object
) -> tuple[pl.DataFrame, pl.DataFrame]:
    crdf = crdf_rows(
        _eib(
            "early",
            2021,
            "2021000104c",
            project_number="92954",
            title="Agra metro rail project",
            purpose_code=21010,
            face_usd=EUROS / RATES[2021],
            climate_usd=EUROS / RATES[2021],
        )
        | earlier,
        _eib(
            "late",
            2024,
            BULK_ID,
            title="Agra metro rail project A",
            face_usd=EUROS / RATES[2024],
            climate_usd=EUROS / RATES[2024],
            id_scheme="bulk",
        ),
    )
    crs = crs_records(
        {
            "record_id": "bulk",
            "year": 2024,
            "donor_code": 918,
            "agency_code": 3,
            "crs_id": BULK_ID,
            "project_number": bulk_number,
            "title": "AGRA METRO RAIL PROJECT A",
            "recipient_code": 645,
            "purpose_code": 21020,
            "commitment_usd": EUROS / RATES[2024],
        }
    )
    return crdf, crs


@pytest.mark.parametrize(
    ("earlier", "repeats"),
    [
        ({}, True),
        # A title overlap of 0.5 or more stands in for the project number.
        ({"project_number": "90000"}, True),
        # Amount and purpose alone tie on round euro amounts and are dropped.
        (
            {
                "project_number": "90000",
                "title": "Urban transport",
                "purpose_code": 21020,
            },
            False,
        ),
        ({"recipient_code": 798}, False),
    ],
)
def test_eib_rows_repeat_through_a_matched_bulk_commitment(
    earlier: dict[str, object], repeats: bool
) -> None:
    crdf, crs = _eib_case(**earlier)

    assert _repeats(crdf, crs) == {"early": None, "late": "early" if repeats else None}


@pytest.mark.parametrize(
    ("row_number", "bulk_number", "repeats"),
    [
        # Two placeholders name no project, so they are no evidence.
        ("UNKNOWN", "UNKNOWN", False),
        ("92954.0", " 92954 ", True),
    ],
)
def test_eib_project_numbers_compare_normalised(
    row_number: str, bulk_number: str, repeats: bool
) -> None:
    # Title and purpose differ, so only the project number can pair the rows.
    crdf, crs = _eib_case(
        bulk_number, project_number=row_number, title="Urban transport"
    )

    assert _repeats(crdf, crs)["late"] == ("early" if repeats else None)


def test_eib_amounts_compare_in_euros() -> None:
    crdf, crs = _eib_case()
    # At one rate for both years the dollar amounts are 8% apart.
    flat = _exchange(dict.fromkeys(RATES, 0.9))

    assert _repeats(crdf, crs, flat)["late"] is None


def test_eib_matching_refuses_a_year_without_a_euro_rate() -> None:
    crdf, crs = _eib_case()
    rates = {year: rate for year, rate in RATES.items() if year != 2024}

    with pytest.raises(CRDFNormaliseError, match="2024"):
        _repeats(crdf, crs, _exchange(rates))
