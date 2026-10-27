"""Comparison levels between a disbursement unit and a commitment unit."""

from datetime import date

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.link.keys import build_units
from climate_finance.build.link.levels import (
    DATE_FEATURES,
    DESCRIPTION_WORDS,
    LEVELS,
    comparison_levels,
    levels_struct,
    unit_attributes,
)

_RECORD = {
    "donor_code": 5,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": "rural water supply",
    "short_description": None,
    "long_description": None,
    "recipient_code": 238,
    "purpose_code": 14030,
    "channel_code": 11000,
    "flow_code": 11,
    "finance_t": 110,
    "commitment_date": None,
    "expected_start_date": None,
    "completion_date": None,
    "commitment_usd": None,
    "disbursement_usd": None,
}
_SCHEMA = {
    "record_id": pl.String,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "short_description": pl.String,
    "long_description": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "channel_code": pl.Int32,
    "flow_code": pl.Int32,
    "finance_t": pl.Int32,
    "year": pl.Int16,
    "commitment_date": pl.Date,
    "expected_start_date": pl.Date,
    "completion_date": pl.Date,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
}
_STEMS = pl.DataFrame(schema=schema.PROGRAMME_STEMS.dtypes)


def _levels(
    commitment: dict[str, object],
    disbursement: dict[str, object],
    *extra: dict[str, object],
    programme: str | None = None,
) -> dict[str, object]:
    """The levels and features of the pair (disbursement `d`, commitment `c`).

    `c` is a 2020 commitment of 10, `d` a 2022 disbursement of 6, each with
    `_RECORD`'s fields; `extra` records join units by their own fields. With
    `programme`, every unit falls under that programme key.
    """
    records = [
        {**_RECORD, "record_id": "c", "year": 2020, "commitment_usd": 10.0}
        | commitment,
        {**_RECORD, "record_id": "d", "year": 2022, "disbursement_usd": 6.0}
        | disbursement,
        *({**_RECORD, **record} for record in extra),
    ]
    crs = pl.LazyFrame(records, schema=_SCHEMA)
    units = build_units(crs).collect()
    unit = {
        record_id: unit_id
        for unit_id, ids in units.select("unit_id", "record_ids").iter_rows()
        for record_id in ids
    }
    pairs = pl.DataFrame({"left_id": [unit["d"]], "right_id": [unit["c"]]})
    attributes = unit_attributes(units, crs=crs, stems=_STEMS)
    if programme is not None:
        attributes = attributes.with_columns(prog=pl.lit(programme))
    levels = comparison_levels(pairs, attributes=attributes)
    return levels.row(0, named=True)


@pytest.mark.parametrize(
    ("commitment", "disbursement", "level"),
    [
        ("AB-123", None, -1),
        ("AB-123", "12", -1),
        ("AB-123", "N/A", -1),
        ("AB-123", "ab-123", 3),
        ("AB-123", "AB123", 2),
        ("0AB-123", "AB123", 2),
        ("12345.0", "12345", 3),
        # A number a spreadsheet rounded names no project.
        ("2.0002E+12", "2.0002E+12", -1),
        ("ABC12345", "ABC12346", 1),
        ("ABC12345", "XYZ98765", 0),
    ],
)
def test_project_number_level(
    commitment: str, disbursement: str | None, level: int
) -> None:
    pair = _levels({"project_number": commitment}, {"project_number": disbursement})
    assert pair["gamma_project_number"] == level


@pytest.mark.parametrize(
    ("disbursement", "level"),
    [("AB-124", 4), ("12", 4), ("AB-123", 3), (None, -1)],
)
def test_other_numbers_under_one_programme_are_siblings(
    disbursement: str | None, level: int
) -> None:
    pair = _levels(
        {"project_number": "AB-123"},
        {"project_number": disbursement},
        programme="AB",
    )
    assert pair["gamma_project_number"] == level


_FIRST = " ".join(f"first{index:03d}" for index in range(DESCRIPTION_WORDS))
_LATER = " ".join(f"later{index:03d}" for index in range(DESCRIPTION_WORDS))


@pytest.mark.parametrize(
    ("commitment", "disbursement", "level"),
    [
        ({"title": None}, {}, -1),
        ({}, {}, 5),
        (
            {"long_description": "solar pumps for village wells"},
            {"long_description": "cash transfers to households in towns"},
            4,
        ),
        (
            {"long_description": "solar pumps for village wells"},
            {"long_description": "solar pumps for village wells"},
            5,
        ),
        ({"title": "rural water supply phase"}, {}, 3),
        ({"title": "rural water supply and sanitation works in the north"}, {}, 2),
        (
            {"title": "health clinics", "long_description": "aaa bbb ccc ddd eee"},
            {"title": "road works", "long_description": "aaa bbb fff ggg hhh"},
            1,
        ),
        (
            {"title": "health clinics", "long_description": "aaa bbb ccc ddd eee"},
            {"title": "road works", "long_description": "fff ggg hhh iii jjj"},
            0,
        ),
        # Only the first DESCRIPTION_WORDS words compare.
        (
            {"title": "health clinics", "long_description": f"{_FIRST} {_LATER}"},
            {"title": "road works", "long_description": _LATER},
            0,
        ),
    ],
)
def test_text_level(
    commitment: dict[str, object], disbursement: dict[str, object], level: int
) -> None:
    assert _levels(commitment, disbursement)["gamma_text"] == level


def test_a_missing_title_falls_back_to_the_short_description() -> None:
    pair = _levels(
        {"title": None, "short_description": "Rural water supply"},
        {},
    )
    assert pair["gamma_text"] == 5


def test_a_unit_reads_its_largest_record() -> None:
    # A second, larger disbursement record in d's unit carries another title.
    larger = {"record_id": "e", "year": 2022, "disbursement_usd": 50.0}
    pair = _levels({}, {}, larger | {"title": "road works"})
    assert pair["gamma_text"] == 0
    assert pair["log_ratio"] == pytest.approx(0.748188, abs=1e-6)


@pytest.mark.parametrize(
    ("commitment", "disbursement", "level"),
    [(14030, 14030, 2), (14030, 14020, 1), (14030, 41010, 0), (None, 41010, -1)],
)
def test_purpose_level(commitment: int | None, disbursement: int, level: int) -> None:
    pair = _levels({"purpose_code": commitment}, {"purpose_code": disbursement})
    assert pair["gamma_purpose"] == level


@pytest.mark.parametrize(
    ("field", "level"),
    [
        ("recipient_code", "gamma_recipient_code"),
        ("channel_code", "gamma_channel_code"),
        ("flow_code", "gamma_flow_code"),
        ("finance_t", "gamma_finance_type_code"),
    ],
)
def test_equality_levels(field: str, level: str) -> None:
    assert _levels({}, {})[level] == 1
    assert _levels({}, {field: 1})[level] == 0


def test_a_missing_channel_is_its_own_level() -> None:
    assert _levels({"channel_code": None}, {})["gamma_channel_code"] == -1


@pytest.mark.parametrize(
    ("disbursed", "level"),
    [(6.0, 4), (10.0, 4), (2.0, 3), (0.5, 2), (10.5, 1), (12.0, 0)],
)
def test_amount_level(disbursed: float, level: int) -> None:
    assert _levels({}, {"disbursement_usd": disbursed})["gamma_amount"] == level


@pytest.mark.parametrize(
    ("committed", "level", "later_commit"),
    [(None, 2, 0), (6.03, 1, 1), (12.0, 0, 1)],
)
def test_later_commitment_level(
    committed: float | None, level: int, later_commit: int
) -> None:
    pair = _levels({}, {"commitment_usd": committed})
    assert pair["gamma_later_commitment"] == level
    assert pair["later_commit"] == later_commit


@pytest.mark.parametrize(
    ("year", "level"), [(2021, 3), (2022, 2), (2023, 1), (2019, 0)]
)
def test_year_gap_level(year: int, level: int) -> None:
    assert _levels({}, {"year": year})["gamma_year_gap"] == level


def test_log_ratio_is_the_disbursement_over_the_commitment() -> None:
    assert _levels({}, {"disbursement_usd": 100.0})["log_ratio"] == pytest.approx(1.0)


_DATES = {
    "commitment_date": date(2020, 3, 1),
    "expected_start_date": date(2020, 4, 1),
    "completion_date": date(2024, 12, 31),
}


@pytest.mark.parametrize(
    ("disbursement", "features"),
    [
        (_DATES, (1, 1)),
        # A continuation that carries its own commitment date.
        ({**_DATES, "commitment_date": date(2022, 12, 31)}, (1, 1)),
        (
            {
                "commitment_date": date(2022, 12, 31),
                "expected_start_date": date(2023, 1, 1),
                "completion_date": date(2023, 12, 31),
            },
            (0, 0),
        ),
        ({}, (None, None)),
    ],
)
def test_date_features_compare_the_start_and_completion_dates(
    disbursement: dict[str, object], features: tuple[int | None, ...]
) -> None:
    pair = _levels(_DATES, disbursement)
    assert {name: pair[name] for name in DATE_FEATURES} == dict(
        zip(("same_start", "same_completion"), features, strict=True)
    )


def test_levels_pack_into_the_links_struct() -> None:
    pair = pl.DataFrame([_levels({}, {})]).select(levels=levels_struct())
    assert pair.schema["levels"] == schema.COMPARISON_LEVELS
    assert list(pair["levels"].struct.fields) == list(LEVELS)
