"""The key-repair rules on hand-built CRS records, after the exact key tiers."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.allocate import allocate
from climate_finance.build.config import load_config
from climate_finance.build.link import run_link
from climate_finance.build.link.decisions import PAIR, drop_pairs
from climate_finance.build.link.key_repair import (
    MAX_COMMITMENTS,
    project_core,
    repair_keys,
    title_tokens,
    title_words,
)
from climate_finance.build.link.keys import build_units, link_by_keys, rounded_digits
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.stages import REGISTRY, StageContext, table_path

OECD = Path(__file__).parent / "fixtures" / "raw" / "oecd"
FIRST_YEAR = 2010
_RECORD = {
    "donor_code": 913,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "long_description": None,
    "recipient_code": 238,
    "purpose_code": 23210,
    "year": 2020,
    "commitment_date": None,
    "commitment_usd": None,
    "disbursement_usd": None,
    "expected_start_date": None,
    "completion_date": None,
}
_CRS_SCHEMA = {
    "record_id": pl.String,
    "donor_code": pl.Int32,
    "agency_code": pl.Int32,
    "crs_id": pl.String,
    "project_number": pl.String,
    "title": pl.String,
    "long_description": pl.String,
    "recipient_code": pl.Int32,
    "purpose_code": pl.Int32,
    "year": pl.Int16,
    "commitment_date": pl.Date,
    "commitment_usd": pl.Float64,
    "disbursement_usd": pl.Float64,
    "expected_start_date": pl.Date,
    "completion_date": pl.Date,
}


def _crs(records: dict[str, dict[str, object]]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_RECORD, "record_id": name, **fields} for name, fields in records.items()],
        schema=_CRS_SCHEMA,
    )


def _links(
    records: dict[str, dict[str, object]],
    *,
    rejected: tuple[tuple[str, str], ...] = (),
) -> tuple[pl.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """Units, exact links plus key-repair links, and the CRS records, with the
    (disbursement, commitment) record pairs of `rejected` rejected."""
    crs = _crs(records)
    units = build_units(crs).collect()
    unit = dict(zip(units["record_ids"].list.first(), units["unit_id"], strict=True))
    pairs = pl.DataFrame(
        [("trace", unit[left], unit[right]) for left, right in rejected],
        schema=dict.fromkeys(PAIR, pl.String),
        orient="row",
    )
    exact = link_by_keys(units, first_year=FIRST_YEAR)
    repaired = repair_keys(
        units,
        crs=crs,
        links=exact,
        first_year=FIRST_YEAR,
        decide=lambda rule: drop_pairs(rule, pairs=pairs, accepted=pairs.clear()),
    )
    schema.validate(schema.LINKS, repaired)
    return units, pl.concat([exact, repaired]), crs


def _repaired(
    records: dict[str, dict[str, object]],
    *,
    rejected: tuple[tuple[str, str], ...] = (),
) -> dict[str, dict[str, list[str]]]:
    """Each disbursement record the rules link, with the commitment records each
    rule reaches from it. Every test record is its own unit."""
    units, links, _ = _links(records, rejected=rejected)
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    repaired: dict[str, dict[str, list[str]]] = {}
    for row in links.filter(
        pl.col("link_method").is_in(schema.KEY_REPAIR_METHODS)
    ).iter_rows(named=True):
        by_method = repaired.setdefault(names[row["left_id"]], {})
        by_method.setdefault(row["link_method"], []).append(names[row["right_id"]])
    return {
        name: {method: sorted(reached) for method, reached in by_method.items()}
        for name, by_method in repaired.items()
    }


def _commitment(**fields: object) -> dict[str, object]:
    return {"year": 2019, "commitment_usd": 10.0, **fields}


def _disbursement(**fields: object) -> dict[str, object]:
    return {"year": 2021, "disbursement_usd": 4.0, **fields}


@pytest.mark.parametrize(
    ("commitment", "disbursement", "method"),
    [
        # Another recipient code under the same project number, so the
        # project-number tiers miss it; the titles agree.
        (
            {"crs_id": "A", "project_number": "G-1", "title": "Water supply"},
            {
                "crs_id": "B",
                "project_number": "G-1",
                "title": "Water supply",
                "recipient_code": 289,
            },
            "repair_new_crs_id",
        ),
        (
            {"crs_id": "A", "project_number": "KFW-0012345", "title": "Roads"},
            {"crs_id": "B", "project_number": "KFW12345.CRS2", "title": "Health"},
            "repair_pn_variant",
        ),
        (
            {"crs_id": "A", "project_number": "2100150000123", "title": "Eggsp II"},
            {"crs_id": "B", "project_number": "2.10015E+12", "title": "EGGSP phase II"},
            "repair_afdb_scientific",
        ),
        (
            {"crs_id": "A", "title": "Rural water supply, phase two"},
            {"crs_id": "B", "title": "Phase two: rural water supply"},
            "repair_title_recipient",
        ),
    ],
)
def test_each_rule_links_what_the_exact_tiers_and_earlier_rules_miss(
    commitment: dict[str, object], disbursement: dict[str, object], method: str
) -> None:
    repaired = _repaired(
        {"c": _commitment(**commitment), "d": _disbursement(**disbursement)}
    )
    assert repaired == {"d": {method: ["c"]}}


def test_commitments_the_exact_tiers_reach_are_left_to_them() -> None:
    # The crs_id tier links d1 to c, so the rules leave c to it, though d2 shares
    # its project number. The crs_id tier links e to c2, and what e leaves of its
    # disbursement may go on to c3.
    elsewhere = {"title": "Water", "recipient_code": 289}
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="G-1", title="Water"),
            "d1": _disbursement(crs_id="A"),
            "d2": _disbursement(crs_id="B", project_number="G-1", **elsewhere),
            "c2": _commitment(crs_id="E"),
            "c3": _commitment(crs_id="F", project_number="G-2", title="Water"),
            "e": _disbursement(crs_id="E", project_number="G-2", **elsewhere),
        }
    )
    assert repaired == {"e": {"repair_new_crs_id": ["c3"]}}


def test_a_unit_stays_out_of_a_group_holding_its_own_crs_id() -> None:
    # d links to c by crs id and purpose; c2 shares d's crs id under another
    # purpose and c3 its project number, so the project-number group holds both.
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="G-1"),
            "c2": _commitment(crs_id="A", project_number="G-1", purpose_code=41010),
            "c3": _commitment(crs_id="C", project_number="G-1", year=2018),
            "d": _disbursement(crs_id="A", project_number="G-1"),
        }
    )
    assert repaired == {}


# One record that commits 10 and disburses 30, and a line of the same project
# under another crs id and purpose, each linked to itself by crs id and purpose.
_LINE = {
    "crs_id": "A",
    "project_number": "G-1",
    "year": 2020,
    "commitment_usd": 10.0,
    "disbursement_usd": 30.0,
}
_SIBLING = {**_LINE, "crs_id": "B", "purpose_code": 41010}
_SPARE = {"commitment_usd": 50.0, "disbursement_usd": 5.0}


@pytest.mark.parametrize(
    ("others", "repaired"),
    [
        # The sibling committed more than it disbursed, so it is a commitment
        # to the rules.
        (
            {"sibling": {**_SIBLING, **_SPARE}},
            {"line": {"repair_new_crs_id": ["sibling"]}},
        ),
        # The sibling disbursed beyond its commitment too, so each commitment is
        # left to its unit's link to itself and no group holds it.
        ({"sibling": _SIBLING}, {}),
        # A unit that committed more than it disbursed is no disbursement unit,
        # though an open commitment shares its project number.
        (
            {
                "sibling": {**_SIBLING, **_SPARE},
                "earlier": _commitment(crs_id="C", project_number="G-1"),
            },
            {"line": {"repair_new_crs_id": ["earlier", "sibling"]}},
        ),
        # A line whose own commitment covers its disbursement has nothing left
        # for the rules.
        (
            {
                "line": {**_LINE, "disbursement_usd": 10.0},
                "sibling": {**_SIBLING, **_SPARE},
            },
            {},
        ),
        # A line that committed more than it disbursed is no disbursement unit
        # when a later disbursement reaches it by crs id too.
        (
            {
                "line": {**_LINE, **_SPARE},
                "later": _disbursement(crs_id="A", project_number="G-1"),
                "earlier": _commitment(crs_id="C", project_number="G-1"),
            },
            {"later": {"repair_new_crs_id": ["earlier"]}},
        ),
    ],
)
def test_a_unit_takes_one_side_in_the_rules(
    others: dict[str, dict[str, object]], repaired: dict[str, dict[str, list[str]]]
) -> None:
    assert _repaired({"line": _LINE, **others}) == repaired


def test_allocation_gives_a_sibling_line_what_a_line_disbursed_beyond_its_own() -> None:
    units, links, crs = _links({"line": _LINE, "sibling": {**_SIBLING, **_SPARE}})
    assert not links.select("left_id", "right_id").is_duplicated().any()

    pairs = allocate(units.lazy(), crs=crs, links=links.lazy())
    filled = {
        (record, commitment): amount
        for record, commitment, amount in pairs.filter(
            pl.col("commitment_id").is_not_null()
        )
        .select("record_id", "commitment_id", "allocated_usd")
        .iter_rows()
    }
    assert filled == {
        ("line", "line"): 10.0,
        ("line", "sibling"): 20.0,
        ("sibling", "sibling"): 5.0,
    }
    assert pairs.filter(rule="excess").is_empty()


_PROGRAMME = {"donor_code": 302, "project_number": "35_80", "title": "Operations"}
_KENYA = {**_PROGRAMME, "recipient_code": 248}


@pytest.mark.parametrize(
    ("others", "repaired"),
    [
        # Commitments in two countries, as the US `35_80` has: the Kenyan
        # disbursement reaches the Kenyan commitments of its purpose, and the
        # Malawian one reaches none.
        (
            {
                "c_eth": _commitment(**_PROGRAMME, crs_id="A"),
                "d_mwi": _disbursement(**_PROGRAMME, crs_id="D", recipient_code=253),
            },
            {"d_ken": {"repair_new_crs_id": ["c_ken", "c_ken2"]}},
        ),
        # Commitments in one country and disbursements in three, as a US element
        # number has.
        (
            {
                "d_mwi": _disbursement(**_PROGRAMME, crs_id="D", recipient_code=253),
                "d_tza": _disbursement(**_PROGRAMME, crs_id="E", recipient_code=282),
            },
            {"d_ken": {"repair_new_crs_id": ["c_ken", "c_ken2"]}},
        ),
        # One loan under a stray recipient code, as at the IDB, links across it.
        (
            {"d_mwi": _disbursement(**_PROGRAMME, crs_id="D", recipient_code=253)},
            {
                name: {"repair_new_crs_id": ["c_ken", "c_ken2", "c_ken_other"]}
                for name in ("d_ken", "d_mwi")
            },
        ),
    ],
)
def test_a_programme_number_links_only_within_the_disbursements_recipient(
    others: dict[str, dict[str, object]], repaired: dict[str, dict[str, list[str]]]
) -> None:
    # Two Kenyan crs ids keep the exact tiers off.
    records = {
        "c_ken": _commitment(**_KENYA, crs_id="B"),
        "c_ken2": _commitment(**_KENYA, crs_id="B2", year=2020),
        "c_ken_other": _commitment(**_KENYA, crs_id="B3", purpose_code=91010),
        "d_ken": _disbursement(**_KENYA, crs_id="C"),
        **others,
    }
    assert _repaired(records) == repaired


@pytest.mark.parametrize(
    ("placed", "repaired"),
    [
        # Committed after the floor: the disbursement's own commitment lies
        # before the data.
        ({}, {}),
        # A later record carries the floor year's commitment date.
        (
            {"commitment_date": date(FIRST_YEAR - 2, 5, 1)},
            {"d": {"repair_new_crs_id": ["c"]}},
        ),
    ],
)
def test_a_floor_before_the_first_year_links_where_the_group_holds_its_commitment(
    placed: dict[str, object], repaired: dict[str, dict[str, list[str]]]
) -> None:
    # Another recipient keeps the exact tiers off.
    records = {
        "c": _commitment(
            crs_id="A",
            project_number="G-1",
            title="Water supply",
            year=FIRST_YEAR,
            **placed,
        ),
        "d": _disbursement(
            crs_id="B",
            project_number="G-1",
            title="Water supply",
            recipient_code=289,
            commitment_date=date(FIRST_YEAR - 2, 5, 1),
        ),
    }
    assert _repaired(records) == repaired


def test_a_unit_links_only_where_title_or_recipient_agrees_with_its_whole_group() -> (
    None
):
    shared = {"crs_id": "B", "project_number": "G-1", "recipient_code": 289}
    agree = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="G-1", title="Solar plant"),
            "d": _disbursement(**shared, title="Solar plant, phase one"),
        }
    )
    assert agree == {"d": {"repair_new_crs_id": ["c"]}}

    # A second commitment of the group under another title and recipient blocks
    # the whole unit, since allocation would pool its disbursement across both.
    assert (
        _repaired(
            {
                "c": _commitment(crs_id="A", project_number="G-1", title="Solar plant"),
                "c2": _commitment(
                    crs_id="C",
                    project_number="G-1",
                    title="Teacher training",
                    recipient_code=645,
                ),
                "d": _disbursement(**shared, title="Solar plant, phase one"),
            }
        )
        == {}
    )


@pytest.mark.parametrize(
    ("n", "linked"), [(MAX_COMMITMENTS, True), (MAX_COMMITMENTS + 1, False)]
)
def test_a_unit_reaches_at_most_max_commitments(n: int, linked: bool) -> None:
    commitments = {
        f"c{i}": _commitment(crs_id=f"A{i}", project_number="G-1") for i in range(n)
    }
    # Commitments placed after the record year or before its floor count for
    # nothing.
    ineligible = {
        "later": _commitment(crs_id="L1", project_number="G-1", year=2022),
        "earlier": _commitment(crs_id="L2", project_number="G-1", year=2016),
    }
    repaired = _repaired(
        {
            **commitments,
            **ineligible,
            "d": _disbursement(
                crs_id="B", project_number="G-1", commitment_date=date(2018, 3, 1)
            ),
        }
    )
    assert repaired == (
        {"d": {"repair_new_crs_id": sorted(commitments)}} if linked else {}
    )


def test_the_record_year_and_floor_bound_the_commitments() -> None:
    repaired = _repaired(
        {
            "c2018": _commitment(crs_id="A", project_number="G-1", year=2018),
            "c2020": _commitment(crs_id="A2", project_number="G-1", year=2020),
            # Committed after the record year.
            "c2022": _commitment(crs_id="A3", project_number="G-1", year=2022),
            # Dated 2019: fills commitments from 2019 on.
            "d": _disbursement(
                crs_id="B", project_number="G-1", commitment_date=date(2019, 3, 1)
            ),
        }
    )
    assert repaired == {"d": {"repair_new_crs_id": ["c2020"]}}


@pytest.mark.parametrize(
    ("rejected", "linked"),
    [
        ((), {"d": {"repair_new_crs_id": ["c"]}}),
        # With d's link rejected, c is free for the title rule, which links d2.
        ((("d", "c"),), {"d2": {"repair_title_recipient": ["c"]}}),
    ],
)
def test_each_unit_takes_the_first_rule_that_links_it(
    rejected: tuple[tuple[str, str], ...], linked: dict[str, dict[str, list[str]]]
) -> None:
    # d matches c on project number and on title, and d2 only on title, which
    # finds c taken. A later commitment under another crs id keeps the project
    # number from the exact tiers.
    repaired = _repaired(
        {
            "c": _commitment(
                crs_id="A", project_number="G-1", title="Coastal defences"
            ),
            "later": _commitment(crs_id="X", project_number="G-1", year=2023),
            "d": _disbursement(
                crs_id="B", project_number="G-1", title="Coastal defences"
            ),
            "d2": _disbursement(crs_id="C", title="Coastal defences"),
        },
        rejected=rejected,
    )
    assert repaired == linked


def test_a_unit_carries_a_group_of_each_rule_that_links_it() -> None:
    title = "Coastal defences"
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="KFW-1234567", title=title),
            # The same project core, for the variant rule.
            "c2": _commitment(
                crs_id="C", project_number="KFW1234567.CRS2", title=title
            ),
            "d": _disbursement(
                crs_id="B",
                project_number="KFW-1234567",
                title=title,
                recipient_code=289,
            ),
        }
    )
    # Allocation runs the variant group on what the project-number group leaves.
    assert repaired == {"d": {"repair_new_crs_id": ["c"], "repair_pn_variant": ["c2"]}}


def test_a_rounded_number_links_only_to_one_full_number() -> None:
    title = "Eggsp phase II"
    longer = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="21001500001234", title=title),
            "d": _disbursement(crs_id="B", project_number="2.10015E+12", title=title),
        }
    )
    assert longer == {}

    two = _repaired(
        {
            "c1": _commitment(crs_id="A1", project_number="2100150000123", title=title),
            "c2": _commitment(crs_id="A2", project_number="2100150000999", title=title),
            "d": _disbursement(crs_id="B", project_number="2.10015E+12", title=title),
        }
    )
    assert two == {}

    # Titles choose the full number.
    chosen = _repaired(
        {
            "c1": _commitment(crs_id="A1", project_number="2100150000123", title=title),
            "c2": _commitment(
                crs_id="A2", project_number="2100150000999", title="Railways"
            ),
            "d": _disbursement(crs_id="B", project_number="2.10015E+12", title=title),
        }
    )
    assert chosen == {"d": {"repair_afdb_scientific": ["c1"]}}


@pytest.mark.parametrize(
    ("second_title", "linked"),
    [
        # One project whose yearly disbursements carry new crs ids, as the AfDB
        # files Egypt National Railways.
        ("Egypt national railways modernization", True),
        # Another project whose title also agrees with the commitment's.
        ("Egypt national railways modernization phase II", False),
    ],
)
def test_a_rounded_number_links_when_one_project_reaches_the_commitment(
    second_title: str, linked: bool
) -> None:
    title = "Egypt national railways modernization"
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", project_number="2000200004952", title=title),
            "d1": _disbursement(
                crs_id="2023000368", project_number="2000200000000", title=title
            ),
            "d2": _disbursement(
                crs_id="2024000125", project_number="2.0002E+12", title=second_title
            ),
        }
    )
    rounded = {
        name: by_method["repair_afdb_scientific"]
        for name, by_method in repaired.items()
        if "repair_afdb_scientific" in by_method
    }
    assert rounded == ({"d1": ["c"], "d2": ["c"]} if linked else {})


@pytest.mark.parametrize(
    ("other", "linked"),
    [
        # Another commitment of the place shares the title under another crs id.
        ({"c2": _commitment(crs_id="A2", title="Rural water supply")}, False),
        # Two projects match the title.
        (
            {
                "d2": _disbursement(
                    crs_id="B2", project_number="P-2", title="Rural water supply"
                )
            },
            False,
        ),
        # A second crs id of one project, with no project number.
        ({"d2": _disbursement(crs_id="B2", title="Rural water supply")}, True),
        ({}, True),
    ],
)
def test_the_title_rule_needs_a_title_that_names_one_project(
    other: dict[str, dict[str, object]], linked: bool
) -> None:
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", title="Rural water supply"),
            "d": _disbursement(
                crs_id="B", project_number="P-1", title="rural water SUPPLY"
            ),
            **other,
        }
    )
    assert ("d" in repaired) is linked


@pytest.mark.parametrize(
    ("rounded", "linked"),
    [
        # A rounded number the full number can stand for is one project.
        ("2.10016E+12", True),
        # One that it cannot is another project.
        ("2.0002E+12", False),
    ],
)
def test_the_title_rule_counts_a_rounded_number_apart_from_a_full_one(
    rounded: str, linked: bool
) -> None:
    title = "Rural water supply"
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", title=title),
            "d": _disbursement(crs_id="B", project_number="2100160000123", title=title),
            "d2": _disbursement(crs_id="C", project_number=rounded, title=title),
        }
    )
    assert ("d" in repaired) is linked


def test_the_title_rule_counts_two_spellings_of_one_rounded_number_once() -> None:
    title = "Rural water supply"
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", title=title),
            "d": _disbursement(crs_id="B", project_number="2.0002E+12", title=title),
            "d2": _disbursement(
                crs_id="C", project_number="2000200000000", title=title
            ),
        }
    )
    assert repaired == {
        "d": {"repair_title_recipient": ["c"]},
        "d2": {"repair_title_recipient": ["c"]},
    }


def test_the_title_rule_ignores_short_titles() -> None:
    repaired = _repaired(
        {
            "c": _commitment(crs_id="A", title="Water supply"),
            "d": _disbursement(crs_id="B", title="Water supply"),
        }
    )
    assert repaired == {}


# A line committed in one country and disbursed in another under its crs id,
# which `crs_key` keys by recipient. The two share the crs id, so no other rule
# takes the pair.
_STRAY = {"donor_code": 301, "crs_id": "S1", "project_number": "D-7307"}
_TEXT = {
    "title": "Maternal and child health",
    "long_description": "Trains midwives in rural clinics",
}
_UNRELATED = {
    "title": "Customs modernisation",
    "project_number": "Q-55",
    "long_description": "Upgrades border posts",
}


@pytest.mark.parametrize(
    ("disbursement", "others", "linked"),
    [
        ({}, {}, True),
        # Records that agree only on the project core, or only on the long
        # description.
        ({**_UNRELATED, "project_number": "D7307.CRS2"}, {}, True),
        ({**_UNRELATED, "long_description": _TEXT["long_description"]}, {}, True),
        ({**_UNRELATED}, {}, False),
        # The disbursement's own country holds a commitment under its project
        # number, filed under another agency code as Canada does, so that line
        # is its own.
        (
            {"project_number": "P-7307"},
            {
                "own": _commitment(
                    **{
                        **_STRAY,
                        "crs_id": "S2",
                        "agency_code": 3,
                        "project_number": "P-7307",
                    }
                )
            },
            False,
        ),
        # Canada disburses under a P number and commits under a D number, so the
        # own line sits under the commitment's number.
        (
            {"project_number": "P-7307"},
            {
                "own": _commitment(
                    **{**_UNRELATED, **_STRAY, "crs_id": "S2", "agency_code": 3}
                )
            },
            False,
        ),
        # The own line sits under the shared crs id, committed before the
        # disbursement's floor year, so the crs-id tier passes it by.
        (
            {"commitment_date": date(2019, 3, 1)},
            {"own": _commitment(**{**_STRAY, **_UNRELATED, "year": 2018})},
            False,
        ),
    ],
)
def test_a_crs_id_links_across_countries_where_the_records_agree(
    disbursement: dict[str, object], others: dict[str, dict[str, object]], linked: bool
) -> None:
    repaired = _repaired(
        {
            "c": _commitment(**_STRAY, **_TEXT, recipient_code=248),
            "d": _disbursement(**{**_STRAY, **_TEXT, **disbursement}),
            **others,
        }
    )
    assert repaired == ({"d": {"repair_stray_country": ["c"]}} if linked else {})


def test_allocation_fills_only_the_commitments_each_unit_links_to() -> None:
    # Two rounded numbers of one prefix stand for two projects, chosen by title.
    records = {
        "c1": _commitment(
            crs_id="A1", project_number="2000200005006", title="EGGSP phase II"
        ),
        "c2": _commitment(
            crs_id="A2", project_number="2000200004952", title="National railways"
        ),
        "d1": _disbursement(
            crs_id="B1",
            project_number="2000200000000",
            title="EGGSP phase II",
            disbursement_usd=10.0,
        ),
        "d2": _disbursement(
            crs_id="B2",
            project_number="2.0002E+12",
            title="National railways",
            disbursement_usd=3.0,
        ),
    }
    units, links, crs = _links(records)
    assert links["link_method"].unique().to_list() == ["repair_afdb_scientific"]

    pairs = allocate(units.lazy(), crs=crs, links=links.lazy())
    filled = dict(
        pairs.filter(pl.col("commitment_id").is_not_null())
        .select("record_id", "commitment_id")
        .iter_rows()
    )
    assert filled == {"d1": "c1", "d2": "c2"}


@pytest.mark.parametrize(
    ("published", "core"),
    [
        ("KFW-0012345", "KFW12345"),
        ("KFW12345.CRS2", "KFW12345"),
        ("0012A0003", "12A3"),
        ("A-1", None),
        (None, None),
    ],
)
def test_project_core(published: str | None, core: str | None) -> None:
    frame = pl.DataFrame({"pn": [published]}, schema={"pn": pl.String})
    assert frame.select(project_core(pl.col("pn"))).item() == core


@pytest.mark.parametrize(
    ("published", "rounded"),
    [
        ("2.10015E+12", {"prefix": "210015", "digits": 13}),
        ("2100150000000", {"prefix": "210015", "digits": 13}),
        (" 2.10015e+12 ", {"prefix": "210015", "digits": 13}),
        ("2100150000123", None),
        ("210000", None),
        ("G-1", None),
    ],
)
def test_rounded_digits(published: str, rounded: dict[str, object] | None) -> None:
    frame = pl.DataFrame({"pn": [published]})
    assert frame.select(rounded_digits(pl.col("pn"))).item() == rounded


def test_title_words() -> None:
    frame = pl.DataFrame({"title": ["Phase two: Rural water-supply, phase TWO", None]})
    tokens = frame.select(title_tokens(pl.col("title"))).to_series()
    assert tokens.to_list() == ["phase two rural water supply phase two", ""]
    words = pl.select(title_words(pl.lit(tokens[0]))).item()
    assert words == "phase rural supply two water"


def _context(release_dir: Path, **switches: bool) -> StageContext:
    # The fixture has no truth group to set Jev's scale.
    config = load_config(Path(__file__).parents[1] / "build.toml")
    return StageContext(
        release="test",
        release_dir=release_dir,
        config=config.model_copy(update={"jev_links": False} | switches),
        offline=True,
    )


def test_run_link_adds_key_repair_links_when_on(tmp_path: Path) -> None:
    crs = normalise_crs(
        pl.scan_parquet(OECD / "CRS.parquet"), years=(2010, 2025)
    ).collect()
    crdf = normalise_crdf(
        pl.scan_parquet(OECD / "crdf_recipient.parquet"),
        perspective="recipient",
        crs=crs.lazy(),
    ).collect()
    for name, frame in (("crs", crs), ("crdf", crdf)):
        path = table_path(tmp_path, "normalise", name)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    link = next(stage for stage in REGISTRY if stage.name == "link")

    off = run_link(_context(tmp_path, key_repair=False, recoveries=False))["links"]
    on = run_link(_context(tmp_path, key_repair=True, recoveries=False))
    write_stage(link, on, tmp_path)

    added = (
        on["links"]
        .lazy()
        .collect()
        .join(off.lazy().collect(), on=["task", "left_id", "right_id"], how="anti")
    )
    assert set(added["link_method"]) <= set(schema.KEY_REPAIR_METHODS)
