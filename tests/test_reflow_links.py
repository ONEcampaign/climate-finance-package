"""Reflow units reach their loan's commitment units through the exact key tiers.

Ids and amounts are invented; donor codes are real DAC codes.
"""

from datetime import date

import polars as pl
import pytest
from normalised import named_records

from climate_finance import schema
from climate_finance.build.link.keys import build_units, link_by_keys
from climate_finance.build.link.reflows import build_reflow_units, link_reflows

FIRST_YEAR = 2010
_DEFAULTS: dict[str, object] = {
    "donor_code": 5,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 238,
    "purpose_code": 23210,
    "channel_code": None,
    "bi_multi": 1,
    "flow_code": 13,
    "finance_t": 421,
    "received_usd": 2.0,
    "interest_usd": 0.5,
    "commitment_date": None,
    "rio_adaptation": 0,
    "rio_mitigation": 2,
}


def _units(commitment: dict[str, object]) -> pl.DataFrame:
    """The commitment unit of one 2019 loan record."""
    records = {"c": {"year": 2019, "commitment_usd": 10.0, **commitment}}
    return build_units(
        named_records(records, donor_code=5, recipient_code=238, purpose_code=23210)
    ).collect()


def _reflow_units(*records: dict[str, object]) -> pl.DataFrame:
    rows = [
        _DEFAULTS | {"record_id": f"r{n}", "year": 2023} | record
        for n, record in enumerate(records)
    ]
    reflows = pl.DataFrame(rows, schema=schema.CRS_REFLOWS.dtypes)
    schema.validate(schema.CRS_REFLOWS, reflows)
    units = build_reflow_units(reflows.lazy()).collect()
    schema.validate(schema.REFLOW_UNITS, units)
    return units


def _methods(commitment: dict[str, object], reflow: dict[str, object]) -> list[str]:
    """The tiers by which the reflow reaches the commitment."""
    units = _units(commitment)
    links = link_reflows(units, _reflow_units(reflow), first_year=FIRST_YEAR)
    schema.validate(schema.REFLOW_LINKS, links)
    assert set(links["unit_id"]) <= set(units["unit_id"])
    assert set(links["target"]) <= {"commitment"}
    return links["link_method"].to_list()


@pytest.mark.parametrize(
    ("commitment", "reflow", "methods"),
    [
        # A bilateral loan repaid under its own crs id.
        ({"crs_id": "2019000101"}, {"crs_id": "2019000101"}, ["crs_id_purpose"]),
        # The IBRD files each year's repayment under a fresh crs id; the loan
        # number in the project number reaches the loan.
        (
            {"donor_code": 901, "crs_id": "2019P1", "project_number": "P1.IBRD1.CRS1"},
            {"donor_code": 901, "crs_id": "2023P9", "project_number": "P1.IBRD1.CRS7"},
            ["wb_loan"],
        ),
        # The AsDB books one loan number under a new crs id each year.
        (
            {"donor_code": 915, "crs_id": "2019000001", "project_number": "LN3375"},
            {"donor_code": 915, "crs_id": "2023000077", "project_number": "LN3375"},
            ["pn_recipient_purpose"],
        ),
        # The EBRD files repayments under a fresh crs id and no project number.
        (
            {"donor_code": 990, "crs_id": "2019000001"},
            {"donor_code": 990, "crs_id": "2023000077"},
            [],
        ),
        # A loan committed before the first CRS year the build loads repays under
        # a crs id a later commitment reused, which is not its loan.
        (
            {"crs_id": "2019000101"},
            {"crs_id": "2019000101", "commitment_date": date(2005, 3, 1)},
            [],
        ),
    ],
)
def test_a_reflow_reaches_its_loan_through_the_key_tiers(
    commitment: dict[str, object], reflow: dict[str, object], methods: list[str]
) -> None:
    assert _methods(commitment, reflow) == methods


def test_a_reflow_dated_by_its_loan_reaches_the_loan() -> None:
    # The control for the pre-window case: a commitment date the build loads.
    dated = {"crs_id": "2019000101", "commitment_date": date(2019, 3, 1)}
    assert _methods({"crs_id": "2019000101"}, dated) == ["crs_id_purpose"]


@pytest.mark.parametrize(
    ("reflow", "targets"),
    [
        # A loan with no commitment the build sees repays under the crs id its
        # disbursements carry.
        ({"crs_id": "2019000101"}, [("disbursement", "crs_id_purpose")]),
        # A loan whose disbursements predate the first CRS year the build loads
        # does not tie to a later disbursement under its crs id.
        ({"crs_id": "2019000101", "commitment_date": date(2005, 3, 1)}, []),
    ],
)
def test_a_reflow_with_no_commitment_ties_to_its_loans_disbursements(
    reflow: dict[str, object], targets: list[tuple[str, str]]
) -> None:
    records = {"d": {"crs_id": "2019000101", "year": 2019, "disbursement_usd": 4.0}}
    units = build_units(
        named_records(records, donor_code=5, recipient_code=238, purpose_code=23210)
    ).collect()

    links = link_reflows(units, _reflow_units(reflow), first_year=FIRST_YEAR)

    schema.validate(schema.REFLOW_LINKS, links)
    assert links.select("target", "link_method").rows() == targets
    assert set(links["unit_id"]) <= set(units["unit_id"])


def test_reflow_unit_ids_differ_from_the_trace_units_of_the_same_key() -> None:
    record = {"crs_id": "A", "year": 2023}
    trace = build_units(
        named_records(
            {"r0": {**record, "disbursement_usd": 1.0}},
            donor_code=5,
            recipient_code=238,
            purpose_code=23210,
        )
    ).collect()
    reflow = _reflow_units(record)
    assert reflow["received_usd"].to_list() == [2.0]
    assert trace["unit_id"].item() != reflow["unit_id"].item()


def test_a_reflow_in_another_recipient_leaves_the_trace_keys_alone() -> None:
    # The disbursement links under the crs-id tier, whose key and group hold the
    # recipient only where the crs id names lines in several countries.
    records = {
        "c": {"crs_id": "A", "year": 2019, "commitment_usd": 10.0},
        "d": {"crs_id": "A", "purpose_code": 41010, "disbursement_usd": 4.0},
    }
    units = build_units(
        named_records(
            records, donor_code=5, recipient_code=238, purpose_code=23210, year=2021
        )
    ).collect()
    reflow = _reflow_units({"crs_id": "A", "recipient_code": 645})
    with_reflow = pl.concat(
        [units, reflow.drop("received_usd", "interest_usd")], how="diagonal"
    )

    trace = link_by_keys(units, first_year=FIRST_YEAR)
    assert trace["link_method"].to_list() == ["crs_id"]
    assert link_by_keys(with_reflow, first_year=FIRST_YEAR).equals(trace)
    # The reflow reaches the loan as such a disbursement would, and the records'
    # recipients disagree, so the link is not certain.
    reflow_links = link_reflows(units, reflow, first_year=FIRST_YEAR)
    assert reflow_links.select("link_method", "records_disagree").rows() == [
        ("crs_id", True)
    ]
