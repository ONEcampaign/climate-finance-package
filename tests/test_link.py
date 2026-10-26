"""Units, the exact key tiers, id practice and the within-year CRDF join."""

import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from normalised import named_records
from stage_runs import (
    BUILD_TOML,
    run_stage,
    write_fetched_years,
    write_inputs,
    write_normalised,
)

from climate_finance import schema
from climate_finance.build.config import load_config
from climate_finance.build.errors import StageInputMissingError
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link import run_link
from climate_finance.build.link.crdf_crs import link_within_year
from climate_finance.build.link.decisions import PAIR, drop_pairs, override_links
from climate_finance.build.link.disagreement import flag_disagreement
from climate_finance.build.link.id_practice import (
    classify_id_practice,
    parent_id,
    period_of,
    untraceable,
)
from climate_finance.build.link.jev import JevFitError
from climate_finance.build.link.keys import as_exact_links, build_units, link_by_keys
from climate_finance.build.normalise.project_numbers import normalise_pn
from climate_finance.build.stages import StageContext, table_path

FIRST_YEAR = 2010


def _crs(records: dict[str, dict[str, object]]) -> pl.LazyFrame:
    """CRS records named by their record id."""
    return named_records(
        records, donor_code=5, recipient_code=238, purpose_code=23210, year=2020
    )


def _units(records: dict[str, dict[str, object]]) -> pl.DataFrame:
    return build_units(_crs(records)).collect()


def _trace(
    records: dict[str, dict[str, object]], *, rejected: tuple[tuple[str, str], ...] = ()
) -> dict[str, tuple[str, list[str]]]:
    """Each linked disbursement record's tier and the commitment records it reaches,
    with the (disbursement, commitment) record pairs of `rejected` rejected.

    Every test record is its own unit, so a unit is named by its one record.
    """
    units = _units(records)
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    unit = {name: unit_id for unit_id, name in names.items()}
    pairs = pl.DataFrame(
        [("trace", unit[left], unit[right]) for left, right in rejected],
        schema=dict.fromkeys(PAIR, pl.String),
        orient="row",
    )
    links = link_by_keys(
        units,
        first_year=FIRST_YEAR,
        decide=lambda tier: drop_pairs(tier, pairs=pairs, accepted=pairs.clear()),
    )
    schema.validate(schema.LINKS, links)
    traced: dict[str, tuple[str, list[str]]] = {}
    for row in links.iter_rows(named=True):
        method, reached = traced.setdefault(
            names[row["left_id"]], (row["link_method"], [])
        )
        assert row["link_method"] == method
        reached.append(names[row["right_id"]])
    return {
        name: (method, sorted(reached)) for name, (method, reached) in traced.items()
    }


@pytest.mark.parametrize(
    ("published", "normalised"),
    [
        (" ab 12-c ", "AB12-C"),
        ("4471.0", "4471"),
        ("2.0002E+12", None),
        ("2100150000000", None),
        ("2100150000123", "2100150000123"),
        ("P 1 2 3", "P123"),
        ("not applicable", None),
        ("N/A", None),
        ("0", None),
        ("x", None),
        ("--", None),
        (None, None),
    ],
)
def test_normalise_pn(published: str | None, normalised: str | None) -> None:
    frame = pl.DataFrame({"pn": [published]}, schema={"pn": pl.String})
    assert frame.select(normalise_pn(pl.col("pn"))).item() == normalised


def test_units_sum_records_that_share_every_key() -> None:
    units = _units(
        {
            "b": {
                "crs_id": "A",
                "project_number": "p 1",
                "commitment_date": date(2020, 2, 1),
                "commitment_usd": 2.0,
            },
            "a": {
                "crs_id": "A",
                "project_number": "P1",
                "commitment_date": date(2020, 5, 1),
                "commitment_usd": 3.0,
            },
            # A different commitment-date year is a different unit.
            "c": {
                "crs_id": "A",
                "commitment_date": date(2018, 5, 1),
                "disbursement_usd": 1.0,
            },
            # Amounts that sum to nothing leave no unit.
            "d": {"crs_id": "B", "commitment_usd": 1.0},
            "e": {"crs_id": "B", "commitment_usd": -1.0},
        }
    )
    schema.validate(schema.UNITS, units)
    by_records = {tuple(row["record_ids"]): row for row in units.iter_rows(named=True)}
    assert set(by_records) == {("a", "b"), ("c",)}
    summed = by_records[("a", "b")]
    assert summed["commitment_usd"] == 5.0
    assert summed["disbursement_usd"] == 0.0
    assert summed["project_number"] == "P1"
    # The commitment date is the largest record's.
    assert summed["commitment_date"] == date(2020, 5, 1)
    assert (summed["placed_year"], summed["floor_year"]) == (2020, None)
    # A record dated before its year is placed and floored at its date's year.
    dated = by_records[("c",)]
    assert (dated["placed_year"], dated["floor_year"]) == (2018, 2018)
    # Ids digest the key, so they hold across builds and record order.
    again = _units(
        {
            "c": {
                "crs_id": "A",
                "commitment_date": date(2018, 5, 1),
                "disbursement_usd": 9.0,
            }
        }
    )
    assert again["unit_id"].item() == dated["unit_id"]


def _commitment(**fields: object) -> dict[str, object]:
    return {"year": 2019, "commitment_usd": 10.0, **fields}


def _disbursement(**fields: object) -> dict[str, object]:
    return {"year": 2021, "disbursement_usd": 4.0, **fields}


@pytest.mark.parametrize(
    ("commitment", "disbursement", "tier", "probability"),
    [
        ({"crs_id": "A"}, {"crs_id": "A"}, "crs_id_purpose", 1.0),
        # A line suffix with and without zero padding.
        (
            {"donor_code": 1601, "crs_id": "2021015528_06"},
            {"donor_code": 1601, "crs_id": "2021015528_6"},
            "crs_id_purpose",
            1.0,
        ),
        # Another purpose under the same crs id.
        ({"crs_id": "A"}, {"crs_id": "A", "purpose_code": 41010}, "crs_id", 1.0),
        # An EU contract under its decision id, on another purpose.
        (
            {"donor_code": 918, "crs_id": "2019000001"},
            {"donor_code": 918, "crs_id": "2019000001042", "purpose_code": 41010},
            "crs_id_parent",
            1.0,
        ),
        # A German record under its project's crs id plus a letter.
        (
            {"crs_id": "2019001320"},
            {"crs_id": "2019001320a", "purpose_code": 41010},
            "crs_id_parent",
            1.0,
        ),
        # A new crs id under the same project number: estimated, so unscored.
        (
            {"crs_id": "A", "project_number": "P-1"},
            {"crs_id": "B", "project_number": "p-1"},
            "pn_recipient_purpose",
            None,
        ),
        (
            {"crs_id": "A", "project_number": "P-1"},
            {"crs_id": "B", "project_number": "P-1", "purpose_code": 41010},
            "pn_recipient",
            None,
        ),
        # The same project number written with and without separators.
        (
            {"crs_id": "A", "project_number": "01AB1907A1_L"},
            {"crs_id": "B", "project_number": "01-AB1907A1L"},
            "pn_recipient_purpose",
            None,
        ),
    ],
)
def test_each_tier_links_what_the_tiers_before_it_miss(
    commitment: dict[str, object],
    disbursement: dict[str, object],
    tier: str,
    probability: float | None,
) -> None:
    records = {"c": _commitment(**commitment), "d": _disbursement(**disbursement)}
    assert _trace(records) == {"d": (tier, ["c"])}
    links = link_by_keys(_units(records), first_year=FIRST_YEAR)
    assert links["probability"].to_list() == [probability]


def test_a_rejected_link_leaves_its_unit_to_the_later_tiers() -> None:
    # d continues c19 by crs id and c18 by project number under another crs id.
    records = {
        "c19": _commitment(crs_id="A"),
        "c18": _commitment(crs_id="B", project_number="P-9", year=2018),
        "d": _disbursement(crs_id="A", project_number="P-9"),
    }
    assert _trace(records) == {"d": ("crs_id_purpose", ["c19"])}
    assert _trace(records, rejected=(("d", "c19"),)) == {
        "d": ("pn_recipient_purpose", ["c18"])
    }


@pytest.mark.parametrize(
    ("donor_code", "traced"),
    [
        (905, {"d": ("wb_loan", ["c_loan2"])}),
        (901, {"d": ("wb_loan", ["c_loan2"])}),
        # Other donors' numbers carry no loan, so the crs id holds the project.
        (5, {"d": ("crs_id_purpose", ["c_loan1", "c_loan2"])}),
    ],
)
def test_a_world_bank_disbursement_stays_on_its_own_loan(
    donor_code: int, traced: dict[str, tuple[str, list[str]]]
) -> None:
    project = {"donor_code": donor_code, "crs_id": "A"}
    records = {
        "c_loan1": _commitment(**project, project_number="P1.IDA1.CRS1"),
        "c_loan2": _commitment(**project, project_number="P1.IDA2.CRS1"),
        "d": _disbursement(**project, project_number="P1.IDA2.CRS3"),
    }
    assert _trace(records) == traced


def test_a_disbursement_reaches_every_eligible_commitment_of_its_group() -> None:
    traced = _trace(
        {
            "c2018": _commitment(crs_id="A", year=2018),
            "c2019": _commitment(crs_id="A"),
            # Placed after the record year: not yet committed.
            "c2022": _commitment(crs_id="A", year=2022),
            "d": _disbursement(crs_id="A"),
        }
    )
    assert traced == {"d": ("crs_id_purpose", ["c2018", "c2019"])}


@pytest.mark.parametrize(
    ("ids", "tier"),
    [
        ({"crs_id": "A"}, "crs_id_purpose"),
        # A unit without a crs id reaches itself by project number, and its own
        # records are still certain.
        ({"project_number": "P-1"}, "pn_recipient_purpose"),
    ],
)
def test_a_unit_that_commits_and_disburses_links_to_itself(
    ids: dict[str, object], tier: str
) -> None:
    records = {"cd": _commitment(disbursement_usd=3.0, **ids)}
    assert _trace(records) == {"cd": (tier, ["cd"])}
    links = link_by_keys(_units(records), first_year=FIRST_YEAR)
    assert links["probability"].to_list() == [1.0]


def test_the_floor_holds_outside_the_parent_tier() -> None:
    dated = {"commitment_date": date(2019, 1, 1)}
    traced = _trace(
        {
            "old": _commitment(crs_id="A", year=2017),
            "new": _commitment(crs_id="A"),
            "d": _disbursement(crs_id="A", **dated),
            "eu_old": _commitment(donor_code=918, crs_id="2017000001", year=2017),
            "eu_d": _disbursement(donor_code=918, crs_id="2017000001007", **dated),
        }
    )
    assert traced == {
        "d": ("crs_id_purpose", ["new"]),
        "eu_d": ("crs_id_parent", ["eu_old"]),
    }


@pytest.mark.parametrize(
    ("in_floor_year", "reached"),
    [
        # KfW's shape: the 2020 commitment, and disbursements under its crs id
        # dated 2021, a year with no commitment of that crs id or project.
        ({}, ["c2020"]),
        # A 2021 commitment under the crs id is the one the date names.
        ({"c2021": _commitment(crs_id="A", year=2021)}, ["c2021"]),
    ],
)
def test_a_floor_naming_no_commitment_passes_under_the_commitments_own_crs_id(
    in_floor_year: dict[str, dict[str, object]], reached: list[str]
) -> None:
    traced = _trace(
        {
            "c2020": _commitment(crs_id="A", project_number="P1", year=2020),
            "d": _disbursement(
                crs_id="A",
                project_number="P1",
                year=2022,
                commitment_date=date(2021, 3, 1),
            ),
            **in_floor_year,
        }
    )
    assert traced == {"d": ("crs_id_purpose", reached)}


def test_a_floor_with_no_commitment_on_or_after_it_falls_through() -> None:
    # The crs id's only commitment precedes the floor, and the floor year holds a
    # commitment of the project under a new id, so the crs-id tiers miss and the
    # unique project number reaches it.
    traced = _trace(
        {
            "old": _commitment(crs_id="A", year=2016),
            "renewed": _commitment(crs_id="B", project_number="P1", purpose_code=41010),
            "d": _disbursement(
                crs_id="A", project_number="P1", commitment_date=date(2019, 3, 1)
            ),
        }
    )
    assert traced == {"d": ("pn_recipient", ["renewed"])}


@pytest.mark.parametrize(
    ("commitments", "traced"),
    [
        # Only later commitments: its own lies before the data.
        (
            {
                "c": _commitment(crs_id="A", year=FIRST_YEAR),
                "eu_c": _commitment(
                    donor_code=918, crs_id="2008000001", year=FIRST_YEAR
                ),
            },
            {},
        ),
        # A later record carries the commitment's date, so the build sees it, and
        # the disbursement reaches it and the lines placed after it.
        (
            {
                "c": _commitment(crs_id="A", year=FIRST_YEAR),
                "c_dated": _commitment(
                    crs_id="A",
                    year=FIRST_YEAR,
                    purpose_code=41010,
                    commitment_date=date(FIRST_YEAR - 1, 3, 1),
                ),
                "eu_c": _commitment(
                    donor_code=918,
                    crs_id="2008000001",
                    year=FIRST_YEAR,
                    commitment_date=date(FIRST_YEAR - 2, 3, 1),
                ),
            },
            {
                "d": ("crs_id", ["c", "c_dated"]),
                "eu_d": ("crs_id_parent", ["eu_c"]),
            },
        ),
    ],
)
def test_a_floor_before_the_first_year_links_where_the_build_holds_its_commitment(
    commitments: dict[str, dict[str, object]],
    traced: dict[str, tuple[str, list[str]]],
) -> None:
    floor = {"commitment_date": date(FIRST_YEAR - 1, 6, 1), "purpose_code": 15110}
    assert (
        _trace(
            {
                **commitments,
                "d": _disbursement(crs_id="A", **floor),
                "eu_d": _disbursement(donor_code=918, crs_id="2008000001003", **floor),
            }
        )
        == traced
    )


def test_a_shared_project_number_does_not_link() -> None:
    traced = _trace(
        {
            "c1": _commitment(crs_id="A", project_number="FRAMEWORK"),
            "c2": _commitment(crs_id="B", project_number="FRAMEWORK"),
            "d": _disbursement(crs_id="C", project_number="FRAMEWORK"),
            # The same number in another recipient is another key.
            "c3": _commitment(
                crs_id="D", project_number="FRAMEWORK", recipient_code=289
            ),
            "d3": _disbursement(
                crs_id="E", project_number="FRAMEWORK", recipient_code=289
            ),
        }
    )
    assert traced == {"d3": ("pn_recipient_purpose", ["c3"])}


def test_each_project_number_tier_asks_for_one_crs_id_over_its_own_key() -> None:
    # One number under two crs ids in one recipient, one per purpose: the purpose
    # tier reaches each line, and a disbursement under a third purpose none.
    traced = _trace(
        {
            "c1": _commitment(crs_id="A", project_number="P-1"),
            "c2": _commitment(crs_id="B", project_number="P-1", purpose_code=41010),
            "d1": _disbursement(crs_id="C", project_number="P-1"),
            "d2": _disbursement(crs_id="D", project_number="P-1", purpose_code=41010),
            "d3": _disbursement(crs_id="E", project_number="P-1", purpose_code=15110),
        }
    )
    assert traced == {
        "d1": ("pn_recipient_purpose", ["c1"]),
        "d2": ("pn_recipient_purpose", ["c2"]),
    }


_LINE = {"project_number": "P-1", "commitment_usd": 10.0}


def _line(crs_id: str, day: date, **fields: object) -> dict[str, object]:
    return {
        **_LINE,
        "crs_id": crs_id,
        "year": day.year,
        "commitment_date": day,
        **fields,
    }


@pytest.mark.parametrize(
    ("second", "day", "traced"),
    [
        # The disbursement repeats the 2014 line's date and continues that line.
        (
            _line("B", date(2016, 3, 1)),
            date(2014, 6, 27),
            {"d": ("pn_recipient_date", ["c1"])},
        ),
        # Two lines share the date, so the date names neither.
        (_line("B", date(2014, 6, 27)), date(2014, 6, 27), {}),
        # One line split by purpose under the date: the purpose tier decides.
        (
            _line("A", date(2014, 6, 27), purpose_code=41010),
            date(2014, 6, 27),
            {"d": ("pn_recipient_purpose", ["c1"])},
        ),
        # A year-end date is a placeholder and names no line.
        (_line("B", date(2016, 3, 1)), date(2014, 12, 31), {}),
    ],
)
def test_a_disbursement_continues_the_line_whose_commitment_date_it_repeats(
    second: dict[str, object], day: date, traced: dict[str, tuple[str, list[str]]]
) -> None:
    records = {
        "c1": _line("A", day),
        "c2": second,
        "d": _disbursement(crs_id="C", project_number="P-1", commitment_date=day),
    }
    assert _trace(records) == traced


def test_numbers_that_merge_without_separators_keep_their_own_keys() -> None:
    # `X.1-1-01` and `X.1-10-1` both read `X.1101` without separators, under two
    # crs ids, so each number keys on itself as written.
    traced = _trace(
        {
            "c1": _commitment(crs_id="A", project_number="X.1-1-01"),
            "c2": _commitment(crs_id="B", project_number="X.1-10-1"),
            "d1": _disbursement(crs_id="C", project_number="X.1-1-01"),
            "d": _disbursement(crs_id="D", project_number="X.1101"),
        }
    )
    assert traced == {"d1": ("pn_recipient_purpose", ["c1"])}


def test_project_number_tiers_hold_for_an_agency_whose_ids_persist() -> None:
    # Ten projects continue under their crs ids, which classes the agency
    # id_persistent, and one more continues under a new crs id.
    agency = {"donor_code": 918, "agency_code": 7}
    records = _agency(7, n=10, follow=("id",)) | {
        "c": _commitment(**agency, crs_id="A", project_number="P1", year=2020),
        "d": _disbursement(**agency, crs_id="B", project_number="P1"),
    }
    classes = classify_id_practice(_units(records))
    assert classes["id_class"].to_list() == ["id_persistent"]
    assert _trace(records)["d"] == ("pn_recipient_purpose", ["c"])


@pytest.mark.parametrize(
    ("donor_code", "crs_id", "parent"),
    [
        (918, "2019000001042", "2019000001"),
        (918, "2019000001", None),
        (5, "2019001320a", "2019001320"),
        (12, "2021000329-c", "2021000329"),
        (915, "2015003432/b", "2015003432"),
        (302, "2015018152eqz", "2015018152"),
        # France and 5RDB006 write sequence numbers, which name other projects.
        (4, "20161000088", None),
        (1012, "20230000052", None),
        (5, "20190013201", None),
        # Sweden's letters start new agreements.
        (10, "2016060279A", None),
    ],
)
def test_parent_id(donor_code: int, crs_id: str, parent: str | None) -> None:
    assert pl.select(parent_id(pl.lit(donor_code), pl.lit(crs_id))).item() == parent


@pytest.mark.parametrize(
    ("other_number", "recipient_code", "traced"),
    [
        # The AsDB files an Indian loan under the crs id of a Nauru grant.
        ("TA9143-NAU", 880, {}),
        # One project with lines in two countries: each line is its own.
        ("LN3375-IND", 880, {}),
        # A regional line of the project.
        ("LN3375-IND", 798, {"d": ("crs_id", ["c"])}),
    ],
)
def test_the_crs_id_tier_keeps_an_id_that_spans_countries_in_its_recipient(
    other_number: str, recipient_code: int, traced: dict[str, tuple[str, list[str]]]
) -> None:
    project = {"donor_code": 915, "crs_id": "2018000001"}
    records = {
        "c": _commitment(
            **project, project_number=other_number, recipient_code=recipient_code
        ),
        "d": _disbursement(
            **project,
            project_number="LN3375-IND",
            recipient_code=645,
            purpose_code=41010,
        ),
    }
    assert _trace(records) == traced


_NAURU = {"donor_code": 915, "project_number": "TA9143-NAU", "recipient_code": 880}
_INDIA = {
    "donor_code": 915,
    "project_number": "LN3375-IND",
    "recipient_code": 645,
    "purpose_code": 41010,
}


@pytest.mark.parametrize(
    ("commitment", "disbursement", "traced"),
    [
        # The AsDB files an Indian loan under the crs id of a Nauru grant.
        ({**_NAURU, "crs_id": "2018000001"}, {**_INDIA, "crs_id": "2018000001"}, {}),
        # A missing or placeholder number is no evidence of one project.
        (
            {**_NAURU, "crs_id": "2018000001"},
            {**_INDIA, "crs_id": "2018000001", "project_number": None},
            {},
        ),
        (
            {**_NAURU, "crs_id": "2018000001", "project_number": "N/A"},
            {**_INDIA, "crs_id": "2018000001"},
            {},
        ),
        # A lettered record continues its parent's project in its recipient only.
        ({**_NAURU, "crs_id": "2018000001"}, {**_INDIA, "crs_id": "2018000001a"}, {}),
        (
            {**_INDIA, "crs_id": "2018000001"},
            {**_INDIA, "crs_id": "2018000001a"},
            {"d": ("crs_id_parent", ["c"])},
        ),
        (
            {**_INDIA, "crs_id": "2018000001", "project_number": "LN3376-IND"},
            {**_INDIA, "crs_id": "2018000001a"},
            {},
        ),
        # The floor holds for a lettered record.
        (
            {**_INDIA, "crs_id": "2018000001", "year": 2017},
            {**_INDIA, "crs_id": "2018000001a", "commitment_date": date(2019, 1, 1)},
            {},
        ),
    ],
)
def test_a_crs_id_links_only_within_the_project_it_names(
    commitment: dict[str, object],
    disbursement: dict[str, object],
    traced: dict[str, tuple[str, list[str]]],
) -> None:
    records = {"c": _commitment(**commitment), "d": _disbursement(**disbursement)}
    assert _trace(records) == traced


def test_a_line_passed_to_another_country_links_by_project_number() -> None:
    # Canada files Tanzania under line 7 in 2019 and Guinea under it in 2021, when
    # Guinea's line is 3.
    project = {"donor_code": 301, "project_number": "P007307001"}
    traced = _trace(
        {
            "c_tza": _commitment(**project, crs_id="P-0007", recipient_code=282),
            "c_gin": _commitment(**project, crs_id="P-0003", recipient_code=243),
            "d": _disbursement(**project, crs_id="P-0007", recipient_code=243),
        }
    )
    assert traced == {"d": ("pn_recipient_purpose", ["c_gin"])}


@pytest.mark.parametrize(
    ("commitment", "disbursement", "flags"),
    [
        # The number agrees though the purpose line differs.
        ({"project_number": "PRJ-001"}, {"project_number": "prj 1"}, {"c": False}),
        # A crs id shared by two agreements.
        ({"project_number": "PRJ-001"}, {"project_number": "AGR-002"}, {"c": True}),
        # The same, where the disbursement unit also commits and links to itself.
        (
            {"project_number": "PRJ-001"},
            {"project_number": "AGR-002", "purpose_code": 23210, "commitment_usd": 1.0},
            {"c": True, "d": False},
        ),
        # No number on one side.
        ({"project_number": "PRJ-001"}, {}, {"c": False}),
        # An umbrella decision in another country.
        (
            {"donor_code": 918, "crs_id": "2019000001", "recipient_code": 279},
            {"donor_code": 918, "crs_id": "2019000001042", "recipient_code": 235},
            {"c": True},
        ),
        # A regional decision.
        (
            {"donor_code": 918, "crs_id": "2019000001", "recipient_code": 289},
            {"donor_code": 918, "crs_id": "2019000001042", "recipient_code": 235},
            {"c": False},
        ),
    ],
)
def test_a_trace_link_records_whether_its_records_disagree(
    commitment: dict[str, object],
    disbursement: dict[str, object],
    flags: dict[str, bool],
) -> None:
    units = _units(
        {
            "c": _commitment(**{"crs_id": "A", **commitment}),
            "d": _disbursement(
                **{"crs_id": "A", "purpose_code": 41010, **disbursement}
            ),
            # A unit linked to itself disagrees with nothing.
            "cd": _commitment(crs_id="S", project_number="X-1", disbursement_usd=1.0),
        }
    )
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    crdf = pl.DataFrame(
        {"left_id": ["row"], "right_id": [units["unit_id"][0]]}
    ).with_columns(link_method=pl.lit("within_year"))
    links = pl.concat(
        [
            link_by_keys(units, first_year=FIRST_YEAR),
            as_exact_links(crdf.lazy(), task="crdf_crs").collect(),
        ]
    )

    flagged = flag_disagreement(links, units=units)

    schema.validate(schema.LINKS, flagged)
    assert {
        (names.get(row["left_id"], row["left_id"]), names[row["right_id"]]): row[
            "records_disagree"
        ]
        for row in flagged.iter_rows(named=True)
    } == {
        **{("d", right): flag for right, flag in flags.items()},
        ("cd", "cd"): False,
        ("row", names[units["unit_id"][0]]): None,
    }


@pytest.mark.parametrize(
    ("number", "accepted", "probability"),
    [
        # The numbers agree, so the crs-id link stays certain.
        ("prj 1", False, 1.0),
        # A crs id shared by two agreements: the link is no longer certain.
        ("AGR-002", False, None),
        # A person accepted the link, so it is certain again.
        ("AGR-002", True, 1.0),
    ],
)
def test_a_certain_link_whose_records_disagree_is_certain_only_once_accepted(
    number: str, accepted: bool, probability: float | None
) -> None:
    units = _units(
        {
            "c": _commitment(crs_id="A", project_number="PRJ-001"),
            "d": _disbursement(crs_id="A", project_number=number, purpose_code=41010),
        }
    )
    unit = dict(zip(units["record_ids"].list.first(), units["unit_id"], strict=True))
    labels = pl.DataFrame(
        {
            "task": ["trace"],
            "left_id": [unit["d"]],
            "right_id": [unit["c"]],
            "pair_label_source": ["human"],
            "is_match": [True],
        }
    ).head(int(accepted))

    links = override_links(
        flag_disagreement(link_by_keys(units, first_year=FIRST_YEAR), units=units),
        labels=labels,
    )

    schema.validate(schema.LINKS, links)
    [link] = links.filter(left_id=unit["d"]).iter_rows(named=True)
    assert link["link_method"] in schema.CERTAIN_METHODS
    assert link["probability"] == probability
    assert link["records_disagree"] is (probability is None)


@pytest.mark.parametrize(
    ("year", "period"),
    [
        (1995, "2000-2009"),
        (2009, "2000-2009"),
        (2010, "2010-2014"),
        (2014, "2010-2014"),
        (2015, "2015-2018"),
        (2022, "2019-2022"),
        (2025, "2019-2022"),
    ],
)
def test_period_of(year: int, period: str) -> None:
    assert pl.select(period_of(pl.lit(year))).item() == period


def _agency(
    agency: int, *, n: int, follow: tuple[str, ...], commit_year: int = 2020
) -> dict[str, dict[str, object]]:
    """`n` open commitments of `agency`, each disbursed two years later through every
    route in `follow`: `id`, `parent` (a contract under it) or `pn` (a new crs id
    under its project number)."""
    records: dict[str, dict[str, object]] = {}
    for i in range(n):
        crs_id = f"{agency}{i:09d}"
        common = {
            "donor_code": 918,
            "agency_code": agency,
            "project_number": f"P{agency}-{i}",
        }
        records[f"{agency}c{i}"] = {
            **common,
            "crs_id": crs_id,
            "year": commit_year,
            "commitment_usd": 10.0,
            # 10% disbursed in its own year leaves it open.
            "disbursement_usd": 1.0,
        }
        later = {"id": crs_id, "parent": f"{crs_id}001", "pn": f"{agency}X{i:08d}"}
        for route in follow:
            records[f"{agency}d{i}{route}"] = {
                **common,
                "crs_id": later[route],
                "year": commit_year + 2,
                "disbursement_usd": 5.0,
            }
    return records


@pytest.mark.parametrize(
    ("follow", "n", "id_class"),
    [
        (("id",), 10, "id_persistent"),
        (("parent",), 10, "parent_id_persistent"),
        (("pn",), 10, "pn_persistent"),
        # A commitment continues by the first route that applies.
        (("id", "parent", "pn"), 10, "id_persistent"),
        (("parent", "pn"), 10, "parent_id_persistent"),
        ((), 10, "neither"),
        (("id",), 9, "unclassified"),
    ],
)
def test_id_practice_classes(follow: tuple[str, ...], n: int, id_class: str) -> None:
    classes = classify_id_practice(_units(_agency(7, n=n, follow=follow)))
    schema.validate(schema.ID_PRACTICE, classes)
    row = classes.row(0, named=True)
    assert classes.height == 1
    assert (row["period"], row["id_class"], row["open_commitments"]) == (
        "2019-2022",
        id_class,
        n,
    )


def test_id_practice_mixed_and_shares() -> None:
    records = _agency(7, n=6, follow=("id",)) | {
        f"x{name}": fields for name, fields in _agency(8, n=4, follow=("pn",)).items()
    }
    # Fold the second set into agency 7 with ids that do not collide.
    for fields in records.values():
        fields["agency_code"] = 7
    row = classify_id_practice(_units(records)).row(0, named=True)
    assert row["id_class"] == "mixed"
    assert row["id_share"] == pytest.approx(0.6)
    assert row["pn_share"] == pytest.approx(0.4)
    assert row["parent_share"] == 0.0


@pytest.mark.parametrize(
    ("key", "share"),
    [
        ({"crs_id": "Z", "project_number": None}, 0.1),
        ({"crs_id": "Z", "project_number": "N/A"}, 0.1),
        ({"crs_id": "Z", "project_number": "P9"}, 1.0),
        # A crs id one of the agency's commitments carries.
        ({"crs_id": "7000000003", "project_number": None}, 1.0),
    ],
)
def test_keyed_disbursement_share(key: dict[str, object], share: float) -> None:
    # Ten keyed dollars disbursed in the commitments' own year, and 90 more.
    records = _agency(7, n=10, follow=()) | {
        "d": _disbursement(donor_code=918, agency_code=7, disbursement_usd=90.0, **key)
    }
    row = classify_id_practice(_units(records)).row(0, named=True)
    assert row["keyed_disbursement_share"] == pytest.approx(share)


def test_a_commitment_disbursed_in_its_own_year_is_not_open() -> None:
    records = _agency(7, n=10, follow=("id",))
    records["7c0"]["disbursement_usd"] = 9.6
    row = classify_id_practice(_units(records)).row(0, named=True)
    assert row["open_commitments"] == 9


def test_id_practice_is_per_period() -> None:
    records = _agency(7, n=10, follow=("id",), commit_year=2012) | {
        f"late{name}": fields
        for name, fields in _agency(7, n=10, follow=("pn",), commit_year=2016).items()
    }
    classes = classify_id_practice(_units(records))
    assert dict(classes.select("period", "id_class").iter_rows()) == {
        "2000-2009": "id_persistent",
        "2010-2014": "id_persistent",
        "2015-2018": "pn_persistent",
    }


def test_2000_2009_keys_on_its_own() -> None:
    # Ten keyed 2010-2014 commitments disbursing 60, and 900 unkeyed in 2005.
    records = _agency(7, n=10, follow=("id",), commit_year=2012) | {
        "d": _disbursement(
            donor_code=918,
            agency_code=7,
            year=2005,
            disbursement_usd=900.0,
            crs_id="Z",
            project_number=None,
        )
    }
    classes = classify_id_practice(_units(records))
    assert dict(classes.select("period", untraceable=untraceable()).iter_rows()) == {
        "2000-2009": True,
        "2010-2014": False,
    }


@pytest.mark.parametrize(
    ("id_class", "share", "expected"),
    [
        ("neither", None, True),
        ("neither", 0.9, True),
        # A period with no disbursement to key leaves the class to decide.
        ("mixed", None, False),
        ("mixed", 0.44, True),
        ("mixed", 0.45, False),
    ],
)
def test_untraceable(id_class: str, share: float | None, expected: bool) -> None:
    row = pl.DataFrame(
        {"id_class": [id_class], "keyed_disbursement_share": [share]},
        schema={"id_class": pl.String, "keyed_disbursement_share": pl.Float64},
    )
    assert row.select(untraceable()).item() is expected


def test_2000_2009_takes_the_2010_2014_share_when_it_disburses_nothing() -> None:
    # Ten keyed 2010-2014 commitments disbursing 60, and 140 unkeyed in 2013.
    records = _agency(7, n=10, follow=("id",), commit_year=2012) | {
        "d": _disbursement(
            donor_code=918,
            agency_code=7,
            year=2013,
            disbursement_usd=140.0,
            crs_id="Z",
            project_number=None,
        )
    }
    classes = classify_id_practice(_units(records))
    [row] = (
        classes.filter(pl.col("period") == period_of(pl.lit(2005)))
        .select("keyed_disbursement_share", untraceable=untraceable())
        .rows()
    )
    assert row == (pytest.approx(0.3), True)


def test_2000_2009_takes_the_2010_2014_class() -> None:
    # 2005 commitments that never continue would class the agency `neither`.
    unpersisted = _agency(8, n=10, follow=(), commit_year=2005)
    for fields in unpersisted.values():
        fields["agency_code"] = 7
    records = _agency(7, n=10, follow=("id",), commit_year=2012) | unpersisted
    classes = classify_id_practice(_units(records))
    schema.validate(schema.ID_PRACTICE, classes)
    inherited = ["id_class", "open_commitments", "id_share", "parent_share"]
    early, measured = (
        classes.filter(period=period).select(inherited).row(0)
        for period in ("2000-2009", "2010-2014")
    )
    assert early == measured == ("id_persistent", 10, 1.0, 0.0)


def _crdf_rows(**rows: dict[str, object]) -> pl.LazyFrame:
    base = {"provider_code": 5, "crs_id": "A", "year": 2020, "purpose_code": 23210}
    return pl.LazyFrame(
        [{**base, "row_id": name, **fields} for name, fields in rows.items()],
        schema={
            "row_id": pl.String,
            "provider_code": pl.Int32,
            "crs_id": pl.String,
            "year": pl.Int16,
            "purpose_code": pl.Int32,
        },
    )


def test_within_year_join() -> None:
    units = _units(
        {
            "c_a": {"crs_id": "A", "commitment_usd": 5.0},
            "c_a_other_recipient": {
                "crs_id": "A",
                "recipient_code": 289,
                "commitment_usd": 2.0,
            },
            "d_a": {"crs_id": "A", "year": 2021, "disbursement_usd": 5.0},
            "cancelled": {"crs_id": "Z", "commitment_usd": -1.0},
        }
    )
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    links = link_within_year(
        _crdf_rows(
            hit={},
            other_purpose={"purpose_code": 41010},
            other_year={"year": 2021},
            no_purpose={"purpose_code": None},
            cancelled={"crs_id": "Z"},
        ),
        units=units.lazy(),
    ).collect()
    schema.validate(schema.LINKS, links)
    assert {
        (row["left_id"], names[row["right_id"]]) for row in links.iter_rows(named=True)
    } == {
        ("hit", "c_a"),
        ("hit", "c_a_other_recipient"),
    }
    assert set(links["group_id"]) == {"within_year|5|A|2020|23210"}
    assert set(links["task"]) == {"crdf_crs"}


class _NoJev(StageContext):
    """A context that refuses to make a Jev client."""

    @property
    def jev(self) -> JevClient:
        raise AssertionError("the link stage asked for Jev with jev_links off")


def _context(
    release_dir: Path,
    decisions: Path | None = None,
    *,
    btr_archive: Path | None = None,
    **switches: bool,
) -> StageContext:
    # The fixture has no truth group to set Jev's scale, so the Jev step is off
    # unless a test turns it on. With it off, the context refuses to make a client.
    config = load_config(BUILD_TOML).model_copy(update={"jev_links": False} | switches)
    sources = config.sources.model_copy(
        update={"btr_archive": btr_archive}
        | ({} if decisions is None else {"review_decisions": decisions})
    )
    config = config.model_copy(update={"sources": sources})
    return (StageContext if config.jev_links else _NoJev)(
        release="test", release_dir=release_dir, config=config, offline=True
    )


def test_run_link_writes_the_deterministic_tables_reproducibly(
    tmp_path: Path,
) -> None:
    write_normalised(tmp_path)
    context = _context(tmp_path, key_repair=False, recoveries=False)

    hashes = run_stage("link", context)
    shutil.rmtree(tmp_path / "link")

    assert run_stage("link", context) == hashes
    assert set(hashes) == {
        "links",
        "units",
        "reflow_units",
        "reflow_links",
        "id_practice",
        "programme_stems",
        "candidates",
        "pair_scores",
        "provider_profile",
    }
    links = pl.read_parquet(table_path(tmp_path, "link", "links"))
    crdf = pl.read_parquet(table_path(tmp_path, "normalise", "crdf"))
    crdf_links = links.filter(task="crdf_crs")
    # Only recipient-perspective rows join.
    linked = crdf.filter(pl.col("row_id").is_in(crdf_links["left_id"].implode()))
    assert linked.height
    assert set(linked["perspective"]) == {"recipient"}
    assert set(links["task"]) == {"crdf_crs", "trace"}


def test_btr_rows_link_when_the_build_has_them(tmp_path: Path) -> None:
    write_normalised(tmp_path)
    archive = tmp_path / "archive"
    with pytest.raises(StageInputMissingError):
        run_link(_context(tmp_path, btr_archive=archive, recoveries=False))
    btr = pl.DataFrame(
        {
            "row_id": ["BTR-DEU-2022-00001"],
            "party": ["DEU"],
            "provider_code": [5],
            "year": [2022],
            "title": ["Solar Kenya (2022000102)"],
            "is_aggregate": [False],
            "source_file": ["DEU.xlsx"],
        }
    )
    btr = btr.select(
        pl.col(name).cast(column.dtype)
        if name in btr.columns
        else pl.lit(None, column.dtype).alias(name)
        for name, column in schema.BTR.columns.items()
    )
    path = table_path(tmp_path, "normalise", "btr")
    btr.write_parquet(path)

    links = run_link(_context(tmp_path, btr_archive=archive, recoveries=False))["links"]

    assert links.filter(task="btr_crs").select(
        "left_id", "right_id", "link_method"
    ).rows() == [("BTR-DEU-2022-00001", "5c1a3ae4d0c8775b", "btr_id")]


def test_recoveries_read_their_inputs_only_when_on(tmp_path: Path) -> None:
    # Normalise wrote neither `external` nor `orgs`.
    write_normalised(tmp_path)
    run_link(_context(tmp_path, key_repair=False, recoveries=False))
    with pytest.raises(StageInputMissingError):
        run_link(_context(tmp_path, key_repair=False, recoveries=True))


def test_the_jev_step_runs_only_when_on(tmp_path: Path) -> None:
    # The fixture has no truth group to set Jev's scale, so the step refuses.
    write_normalised(tmp_path)
    run_link(_context(tmp_path, key_repair=False, recoveries=False))
    with pytest.raises(JevFitError):
        run_link(_context(tmp_path, key_repair=False, recoveries=False, jev_links=True))


def _table(table: schema.TableSchema, rows: list[dict[str, object]]) -> pl.DataFrame:
    """`rows` in `table`'s columns, null where a row gives none."""
    return pl.DataFrame(
        [{name: row.get(name) for name in table.columns} for row in rows],
        schema=table.dtypes,
    )


def _link(context: StageContext) -> dict[str, pl.DataFrame]:
    """`run_link`'s tables, each collected."""
    return {
        name: frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        for name, frame in run_link(context).items()
    }


def test_a_rejected_key_link_frees_its_disbursement_and_stays_unlinked(
    tmp_path: Path,
) -> None:
    # d22 continues c19 by project number under a new crs id: an exact tier links
    # it, and key repair's `repair_new_crs_id` would link it again. c18 shares its
    # recipient, purpose and title.
    shared: dict[str, object] = {"agency_code": 10, "recipient_code": 289}
    shared |= {"purpose_code": 23210, "title": "Solar grid phase one"}
    lines = [
        ("c19", 2019, "A", "P-1", 100.0, None),
        ("c18", 2018, "B", "P-2", 100.0, None),
        ("d22", 2022, "A2", "P-1", None, 30.0),
    ]
    crs = _table(
        schema.CRS,
        [
            shared
            | {"record_id": name, "year": year, "crs_id": crs_id}
            | {"project_number": pn, "donor_code": 1, "bi_multi": 1, "flow_code": 11}
            | {"commitment_usd": commit, "disbursement_usd": disburse}
            for name, year, crs_id, pn, commit, disburse in lines
        ],
    )
    crdf = _table(
        schema.CRDF,
        [
            shared
            | {"row_id": name, "perspective": "recipient", "provider_code": 1}
            | {"year": year, "crs_id": crs_id, "project_number": pn}
            | {"face_usd": commit, "climate_usd": commit / 4}
            for name, year, crs_id, pn, commit, _ in lines
            if commit is not None
        ],
    )
    reflows = pl.DataFrame(schema=schema.REFLOWS.dtypes)
    write_inputs(tmp_path, "normalise", {"crs": crs, "reflows": reflows, "crdf": crdf})
    write_fetched_years(tmp_path)
    linked = _link(_context(tmp_path, recoveries=False))
    unit = {
        record_ids[0]: unit_id
        for unit_id, record_ids in linked["units"]
        .select("unit_id", "record_ids")
        .iter_rows()
    }
    traced = linked["links"].filter(task="trace")
    assert traced.select("left_id", "right_id", "link_method").rows() == [
        (unit["d22"], unit["c19"], "pn_recipient_purpose")
    ]
    assert linked["candidates"].is_empty()

    decisions = tmp_path / "review_decisions.parquet"
    _table(
        schema.REVIEW_DECISIONS,
        [
            {"task": "trace", "left_id": unit["d22"], "right_id": unit["c19"]}
            | {"decision": "reject", "label_source": "human", "reviewer": "analyst"}
            | {"review_reason": "band", "decided_at": datetime(2026, 1, 1, tzinfo=UTC)}
        ],
    ).write_parquet(decisions)
    rejected = _link(_context(tmp_path, decisions, recoveries=False))

    assert rejected["links"].filter(task="trace").is_empty()
    candidates = rejected["candidates"].filter("eligible")
    assert set(candidates.select("left_id", "right_id").rows()) == {
        (unit["d22"], unit["c18"]),
        (unit["d22"], unit["c19"]),
    }
