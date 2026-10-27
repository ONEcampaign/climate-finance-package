"""A loan's repayments split over what its disbursements reached.

Ids and amounts are invented.
"""

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.reflows import (
    attribute_reflows,
    loan_records,
    loan_shares,
    release_reflows,
)

# CRDF rows by id: (rio_adaptation, rio_mitigation, climate_share).
ROWS = {
    "R1": (0, 2, None),
    "R2": (1, 1, None),
    "BANK": (None, None, 0.4),
    "ZERO": (0, 0, None),
}


def _repayment(record_id: str, **values: object) -> dict[str, object]:
    """A repayment of loan `g` through a certain crs id link."""
    return {
        "record_id": record_id,
        "year": 2023,
        "received_usd": 10.0,
        "interest_usd": 1.0,
        "rio_adaptation": 0,
        "rio_mitigation": 0,
        "loan": "g",
        "target": "commitment",
        "method": "crs_id",
        "disagree": False,
    } | values


def _release(
    repayments: list[dict[str, object]],
    allocations: list[tuple[str | None, str, float, float, str]],
    *,
    untraced: tuple[tuple[str, int, int, str], ...] = (),
    unallocated: dict[str, float] | None = None,
    loans: dict[str, list[str]] | None = None,
) -> pl.DataFrame:
    """The `reflows` rows of `repayments`, given each disbursement record's
    allocations as (row_id, record_id, allocated, allocated_low, rule), the
    disbursements of the records allocation holds no row of (`unallocated`), and
    the records of `untraced` as (record_id, rio_adaptation, rio_mitigation,
    reason).

    Each loan of `loans` has one disbursement unit holding its records, which a
    repayment reaches directly where its target is `disbursement`, and otherwise
    through the unit's trace link to the loan's one commitment unit; by default
    loan `g` holds every record.
    """
    unallocated = unallocated or {}
    if loans is None:
        allocated = {record for _, record, *_ in allocations}
        loans = {"g": sorted(allocated | set(unallocated))}
    reflows = pl.DataFrame(
        [
            {
                "record_id": repayment["record_id"],
                "year": repayment["year"],
                "donor_code": 50,
                "recipient_code": 238,
                "bi_multi": 1,
                "flow_code": 13,
                "finance_t": 421,
                "received_usd": repayment["received_usd"],
                "interest_usd": repayment["interest_usd"],
                "rio_adaptation": repayment["rio_adaptation"],
                "rio_mitigation": repayment["rio_mitigation"],
            }
            for repayment in repayments
        ],
        schema=schema.CRS_REFLOWS.dtypes,
    )
    reflow_units = pl.DataFrame(
        {
            "unit_id": [f"x-{r['record_id']}" for r in repayments],
            "record_ids": [[r["record_id"]] for r in repayments],
        }
    )
    reflow_links = pl.DataFrame(
        [
            {
                "reflow_unit_id": f"x-{r['record_id']}",
                "unit_id": f"{r['target'][0]}-{r['loan']}",
                "target": r["target"],
                "group_id": r["loan"],
                "link_method": r["method"],
                "records_disagree": r["disagree"],
            }
            for r in repayments
            if r["loan"] is not None
        ],
        schema=schema.REFLOW_LINKS.dtypes,
    )
    links = pl.DataFrame(
        {
            "task": ["trace"] * len(loans),
            "left_id": [f"d-{loan}" for loan in loans],
            "right_id": [f"c-{loan}" for loan in loans],
        }
    )
    units = pl.DataFrame(
        {
            "unit_id": [f"d-{loan}" for loan in loans],
            "record_ids": list(loans.values()),
        },
        schema={"unit_id": pl.String(), "record_ids": pl.List(pl.String())},
    )
    allocated = pl.DataFrame(
        allocations,
        schema={
            "row_id": pl.String(),
            "record_id": pl.String(),
            "allocated_usd": pl.Float64(),
            "allocated_low_usd": pl.Float64(),
            "rule": pl.String(),
        },
        orient="row",
    ).with_columns(disbursement_source=pl.lit("crs"))
    traced = pl.DataFrame(
        untraced,
        schema={
            "record_id": pl.String(),
            "rio_adaptation": pl.Int8(),
            "rio_mitigation": pl.Int8(),
            "reason": pl.String(),
        },
        orient="row",
    )
    crs = pl.DataFrame(
        {
            "record_id": list(unallocated),
            "disbursement_usd": list(unallocated.values()),
        },
        schema={"record_id": pl.String(), "disbursement_usd": pl.Float64()},
    )
    crdf = pl.DataFrame(
        [
            {"row_id": row_id, "rio_adaptation": a, "rio_mitigation": m}
            | {"climate_share": share}
            for row_id, (a, m, share) in ROWS.items()
        ],
        schema={
            "row_id": pl.String(),
            "rio_adaptation": pl.Int8(),
            "rio_mitigation": pl.Int8(),
            "climate_share": pl.Float64(),
        },
    )

    shares = loan_shares(
        loan_records(reflow_links.lazy(), links=links.lazy(), units=units.lazy()),
        allocations=allocated.lazy(),
        untraced=traced.lazy(),
        crs=crs.lazy(),
    )
    attributed = attribute_reflows(
        reflows.lazy(),
        reflow_units=reflow_units.lazy(),
        reflow_links=reflow_links.lazy(),
        shares=shares,
    )
    released = release_reflows(
        attributed, reflows=reflows.lazy(), crdf=crdf.lazy()
    ).collect()
    schema.validate(schema.REFLOWS, released)
    return released.sort(schema.REFLOWS.sort_key, nulls_last=True)


def _received(frame: pl.DataFrame) -> dict[tuple[str, str | None], float]:
    return {
        (basis, row_id): pytest.approx(received)
        for basis, row_id, received in frame.select(
            "basis", "row_id", "received_usd"
        ).iter_rows()
    }


def test_a_loan_counted_at_40_percent_repays_at_40_percent() -> None:
    released = _release([_repayment("P")], [("BANK", "D1", 100.0, 100.0, "fifo")]).row(
        0, named=True
    )

    climate = released["received_usd"] * released["climate_share"]

    assert released["basis"] == "commitment"
    assert climate == pytest.approx(4.0)


def test_a_loan_split_over_two_rows_repays_on_each_at_its_markers() -> None:
    released = _release(
        [_repayment("P")],
        [("R1", "D1", 60.0, 60.0, "fifo"), ("R2", "D2", 40.0, 40.0, "fifo")],
    )

    assert _received(released) == {("commitment", "R1"): 6.0, ("commitment", "R2"): 4.0}
    assert released.select(
        "row_id", "inherited_rio_adaptation", "inherited_rio_mitigation"
    ).rows() == [("R1", 0, 2), ("R2", 1, 1)]
    assert released["interest_usd"].to_list() == pytest.approx([0.6, 0.4])


@pytest.mark.parametrize(
    ("method", "disagree", "low"),
    [
        # Certain allocations carried half of R1's disbursement.
        ("crs_id", False, 5.0),
        ("pn_recipient", False, 0.0),
        # A certain tier whose records disagree on recipient or project number.
        ("crs_id", True, 0.0),
    ],
)
def test_the_low_bound_holds_what_certain_links_carried(
    method: str, disagree: bool, low: float
) -> None:
    released = _release(
        [_repayment("P", method=method, disagree=disagree)],
        [("R1", "D1", 100.0, 50.0, "fifo")],
    ).row(0, named=True)

    assert released["received_low_usd"] == pytest.approx(low)
    assert released["interest_low_usd"] == pytest.approx(low / 10)


def test_parts_off_a_crdf_row_take_their_own_basis() -> None:
    # D1's excess is untraced at its own markers; D2's is unmarked excess.
    released = _release(
        [_repayment("P")],
        [
            ("R1", "D1", 50.0, 50.0, "fifo"),
            (None, "D1", 20.0, 20.0, "fifo"),
            (None, "D1", 20.0, 0.0, "excess"),
            (None, "D2", 10.0, 0.0, "excess"),
        ],
        untraced=(("D1", 1, 0, "excess"),),
    )

    assert _received(released) == {
        ("commitment", "R1"): 5.0,
        ("excess", None): 1.0,
        ("outside_release", None): 2.0,
        ("untraced", None): 2.0,
    }
    untraced = released.filter(pl.col("basis") == "untraced").row(0, named=True)
    assert (
        untraced["inherited_rio_adaptation"],
        untraced["inherited_rio_mitigation"],
    ) == (1, 0)
    assert released["loan_disbursement_usd"].unique().to_list() == [100.0]


def test_a_loan_that_disbursed_nothing_or_no_loan_leaves_the_repayment_whole() -> None:
    released = _release(
        [_repayment("P", loan="h"), _repayment("Q", loan=None, method=None)],
        [("R1", "D1", 100.0, 100.0, "fifo")],
        loans={"g": ["D1"], "h": ["H1"]},
    )

    assert released.select("record_id", "basis", "row_id", "received_usd").rows() == [
        ("P", "no_disbursement", None, 10.0),
        ("Q", "unlinked", None, 10.0),
    ]
    assert released["interest_usd"].to_list() == [1.0, 1.0]


def test_principal_past_the_loans_disbursement_is_beyond_disbursed() -> None:
    # The loan disbursed 100. The 2020 repayment takes 70, the first of 2021 the
    # 30 left, and the rest of both 2021 repayments is beyond it.
    released = _release(
        [
            _repayment("B", year=2021, received_usd=50.0),
            _repayment("C", year=2021, received_usd=5.0),
            _repayment("A", year=2020, received_usd=70.0),
        ],
        [("R1", "D1", 100.0, 100.0, "fifo")],
    )

    assert released.select("record_id", "basis", "received_usd").rows() == [
        ("A", "commitment", 70.0),
        ("B", "beyond_disbursed", 20.0),
        ("B", "commitment", 30.0),
        ("C", "beyond_disbursed", 5.0),
        ("C", "commitment", 0.0),
    ]
    # Interest is not principal, so it takes the shares in full.
    commitment = released.filter(pl.col("basis") == "commitment")
    assert commitment["interest_usd"].to_list() == [1.0, 1.0, 1.0]


def test_a_repayment_weighs_at_its_commitments_markers_not_its_own() -> None:
    released = _release(
        [_repayment("P", rio_adaptation=2, rio_mitigation=2)],
        [("ZERO", "D1", 100.0, 100.0, "fifo")],
    ).row(0, named=True)

    assert (
        released["inherited_rio_adaptation"],
        released["inherited_rio_mitigation"],
    ) == (0, 0)
    assert (released["record_rio_adaptation"], released["record_rio_mitigation"]) == (
        2,
        2,
    )


def _tied(record_id: str, **values: object) -> dict[str, object]:
    """A repayment of loan `g` that reaches its disbursement units, not a
    commitment."""
    return _repayment(record_id, target="disbursement", **values)


def test_a_loan_gross_counts_as_untraced_repays_at_its_records_markers() -> None:
    released = _release(
        [_tied("P")],
        [],
        unallocated={"D1": 100.0},
        untraced=(("D1", 0, 2, "unlinked"),),
    )

    assert released.select(
        "basis", "received_usd", "inherited_rio_adaptation", "inherited_rio_mitigation"
    ).rows() == [("untraced", 10.0, 0, 2)]
    assert released["received_low_usd"].to_list() == [10.0]


def test_a_tied_loans_unmarked_records_take_their_share_at_no_markers() -> None:
    # D1 is Rio-marked and untraced; D2 is unmarked, which no total counts.
    released = _release(
        [_tied("P")],
        [],
        unallocated={"D1": 60.0, "D2": 40.0},
        untraced=(("D1", 0, 2, "unlinked"),),
    )

    assert released.select(
        "basis", "received_usd", "inherited_rio_mitigation", "loan_id"
    ).rows() == [("unmeasured", 4.0, None, "g"), ("untraced", 6.0, 2, "g")]


def test_a_marked_repayment_of_an_unmarked_tied_loan_weighs_nothing() -> None:
    released = _release(
        [_tied("P", rio_adaptation=2, rio_mitigation=2)],
        [],
        unallocated={"D1": 100.0},
    ).row(0, named=True)

    assert (released["basis"], released["loan_id"]) == ("unmeasured", "g")
    assert (
        released["inherited_rio_adaptation"],
        released["inherited_rio_mitigation"],
    ) == (None, None)


def test_a_tied_loans_principal_stops_at_its_disbursement() -> None:
    released = _release(
        [
            _tied("A", year=2020, received_usd=70.0),
            _tied("B", year=2021, received_usd=50.0),
        ],
        [],
        unallocated={"D1": 100.0},
        untraced=(("D1", 0, 2, "unlinked"),),
    )

    assert released.select("record_id", "basis", "received_usd").rows() == [
        ("A", "untraced", 70.0),
        ("B", "beyond_disbursed", 20.0),
        ("B", "untraced", 30.0),
    ]
