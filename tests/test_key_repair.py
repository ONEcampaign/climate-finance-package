"""The key-repair rules on hand-built CRS records, after the exact key tiers."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from normalised import named_records
from stage_runs import registered, stage_context, write_normalised

from climate_finance import schema
from climate_finance.build import write_stage
from climate_finance.build.allocate import allocate
from climate_finance.build.link import run_link
from climate_finance.build.link.key_repair import (
    MAX_COMMITMENTS,
    project_core,
    repair_keys,
    rounded_digits,
    title_tokens,
    title_words,
    wb_loan,
)
from climate_finance.build.link.keys import build_units, link_by_keys

FIRST_YEAR = 2010


def _crs(records: dict[str, dict[str, object]]) -> pl.LazyFrame:
    """CRS records named by their record id."""
    return named_records(
        records, donor_code=913, recipient_code=238, purpose_code=23210, year=2020
    )


def _links(
    records: dict[str, dict[str, object]],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """Units, exact links plus key-repair links, and the CRS records."""
    crs = _crs(records)
    units = build_units(crs).collect()
    exact = link_by_keys(units, first_year=FIRST_YEAR)
    repaired = repair_keys(units, crs=crs, links=exact, first_year=FIRST_YEAR)
    schema.validate(schema.LINKS, repaired)
    return units, pl.concat([exact, repaired]), crs


def _repaired(
    records: dict[str, dict[str, object]],
) -> dict[str, dict[str, list[str]]]:
    """Each disbursement record the rules link, with the commitment records each
    rule reaches from it. Every test record is its own unit."""
    units, links, _ = _links(records)
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
        # Additional financing booked under the parent project's number.
        (
            {"crs_id": "A", "project_number": "P179981.IDA72500.CRS4"},
            {"crs_id": "B", "project_number": "P170000.IDA72500.CRS1"},
            "repair_wb_parent",
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
        # A USAID award renumbered under the same title.
        (
            {"crs_id": "A", "project_number": "76_57124", "title": "Feed the Future"},
            {"crs_id": "B", "project_number": "76_57132", "title": "Feed the Future"},
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


@pytest.mark.parametrize(
    "elsewhere",
    [
        # Committed in Ethiopia too.
        {"c_eth": {"crs_id": "A"}},
        # Committed in Kenya alone: the Malawian disbursement names the second
        # country.
        {},
    ],
)
def test_a_programme_number_links_only_within_the_disbursements_recipient(
    elsewhere: dict[str, dict[str, object]],
) -> None:
    # The number falls in two countries, so it names a programme, as the US
    # `35_80` does: the Kenyan disbursement reaches the Kenyan commitments of its
    # purpose, and the Malawian one reaches none. Two Kenyan crs ids keep the
    # exact tiers off.
    programme = {"donor_code": 302, "project_number": "35_80", "title": "Operations"}
    kenya = {**programme, "recipient_code": 248}
    repaired = _repaired(
        {
            **{name: _commitment(**programme, **f) for name, f in elsewhere.items()},
            "c_ken": _commitment(**kenya, crs_id="B"),
            "c_ken2": _commitment(**kenya, crs_id="B2", year=2020),
            "c_ken_other": _commitment(**kenya, crs_id="B3", purpose_code=91010),
            "d_ken": _disbursement(**kenya, crs_id="C"),
            "d_mwi": _disbursement(**programme, crs_id="D", recipient_code=253),
        }
    )
    assert repaired == {"d_ken": {"repair_new_crs_id": ["c_ken", "c_ken2"]}}


@pytest.mark.parametrize(
    ("components", "reached"),
    [
        # USAID award 76_48621 commits its components under their own crs ids and
        # purposes, so the HIV/AIDS component (13040) fills none of them.
        ({"c_gov": 15152, "c_trade": 33110}, {}),
        # A disbursement of a committed component reaches that component alone.
        (
            {"c_gov": 15152, "c_trade": 33110, "c_hiv": 13040},
            {"d": {"pn_recipient_purpose": ["c_hiv"]}},
        ),
        # An award that commits under one purpose links across purposes.
        ({"c_gov": 15152}, {"d": {"pn_recipient": ["c_gov"]}}),
    ],
)
def test_a_component_award_links_only_within_a_purpose(
    components: dict[str, int], reached: dict[str, dict[str, list[str]]]
) -> None:
    award = {"donor_code": 302, "project_number": "76_48621", "year": 2015}
    records = {
        name: _commitment(**award, crs_id=f"20150146{index}", purpose_code=purpose)
        for index, (name, purpose) in enumerate(components.items())
    }
    records["d"] = _disbursement(**award, crs_id="2015014629", purpose_code=13040)
    units, links, _ = _links(records)
    names = dict(zip(units["unit_id"], units["record_ids"].list.first(), strict=True))
    linked: dict[str, dict[str, list[str]]] = {}
    for row in links.sort("right_id").iter_rows(named=True):
        by_method = linked.setdefault(names[row["left_id"]], {})
        by_method.setdefault(row["link_method"], []).append(names[row["right_id"]])
    assert linked == reached


def test_a_disbursement_before_the_first_year_links_to_nothing() -> None:
    repaired = _repaired(
        {
            "c": _commitment(
                crs_id="A",
                project_number="G-1",
                year=FIRST_YEAR,
                commitment_date=date(FIRST_YEAR - 2, 5, 1),
            ),
            "d": _disbursement(
                crs_id="B",
                project_number="G-1",
                commitment_date=date(FIRST_YEAR - 2, 5, 1),
            ),
        }
    )
    assert repaired == {}


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


def test_each_unit_takes_the_first_rule_that_links_it() -> None:
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
        }
    )
    assert repaired == {"d": {"repair_new_crs_id": ["c"]}}


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

    pairs = allocate(
        units.lazy(),
        crs=crs,
        links=links.lazy(),
        crdf=pl.LazyFrame(schema=schema.CRDF.dtypes),
    )
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
    ("published", "loan"),
    [
        ("P179981.IDA72500.CRS4", "IDA72500"),
        ("P123.TF0B1234.CRS1", "TF0B1234"),
        ("P179981", None),
        ("P179981.IDA72500", None),
    ],
)
def test_wb_loan(published: str, loan: str | None) -> None:
    frame = pl.DataFrame({"pn": [published]})
    assert frame.select(wb_loan(pl.col("pn"))).item() == loan


@pytest.mark.parametrize(
    ("published", "rounded"),
    [
        ("2.10015E+12", {"prefix": "210015", "digits": 13}),
        ("2100150000000", {"prefix": "210015", "digits": 13}),
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


def test_run_link_adds_key_repair_links_when_on(tmp_path: Path) -> None:
    write_normalised(tmp_path)

    off = run_link(stage_context(tmp_path, key_repair=False, recoveries=False))
    on = run_link(stage_context(tmp_path, key_repair=True, recoveries=False))
    write_stage(registered("link"), on, tmp_path)

    added = (
        on["links"]
        .lazy()
        .collect()
        .join(
            off["links"].lazy().collect(),
            on=["task", "left_id", "right_id"],
            how="anti",
        )
    )
    assert set(added["link_method"]) <= set(schema.KEY_REPAIR_METHODS)
