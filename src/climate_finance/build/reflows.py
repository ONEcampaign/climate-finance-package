"""CRS repayments split over what their loan's disbursements reached.

A loan is the key group of a reflow link (`reflow_links.group_id`): one crs id, loan
number or project number under one tier. Its records (`loan_records`) are the CRS
records of every disbursement unit with a final `trace` link to a commitment unit
any of the loan's reflow units reach, or where the reflow units reach no commitment
unit, of the disbursement units they reach. Each record's allocations, over every
row they reach (CRDF rows, commitments with no CRDF row, and excess), or a record
linked to no commitment its whole disbursement, sum to the loan's disbursement D,
so each part's share of D is a share of the loan, and the shares sum to 1
(`loan_shares`).

A repayment takes each part's share (`attribute_reflows`). The part on a CRDF row
is basis `commitment` and weighs as the row's disbursements do, at the row's markers
and climate share. Allocations to commitments with no CRDF row are
`outside_release`. A record's excess, or the disbursement of a record linked to no
commitment, is `untraced` where the release counts it in `untraced`, at the
record's own markers. Otherwise excess is `excess`, and the disbursement of a
record linked to no commitment, which carries no Rio marker gross counts, is
`unmeasured`. So a loan that reaches no CRDF commitment repays at what gross
counted of it. Principal attributed per loan stops at D, filled by repayment year
then record id, and the rest is `beyond_disbursed`. Interest takes the shares in
full. A loan whose records disbursed nothing in the release's years is
`no_disbursement`, and a repayment that no key tier ties to a loan is `unlinked`.

A part's low amount is its amount where the reflow link is certain, as
`allocate._trace_links` decides for a trace link, and on a `commitment` row only
the share certain allocations carried (`allocated_low_usd`). It is 0 otherwise.
"""

import polars as pl

from climate_finance.build.sums import ordered_sum
from climate_finance.schema import CERTAIN_METHODS, REFLOWS, measured_adaptation

# A part of a loan: its basis, CRDF row, and for `untraced` the disbursement
# records' own markers.
_MARKERS = ("inherited_rio_adaptation", "inherited_rio_mitigation")
_PART = ("loan_id", "basis", "row_id", *_MARKERS)


def loan_records(
    reflow_links: pl.LazyFrame, *, links: pl.LazyFrame, units: pl.LazyFrame
) -> pl.LazyFrame:
    """Each loan (`loan_id`) and the CRS records (`record_id`) of every disbursement
    unit with a `trace` link in `links` to a commitment unit the loan reaches, and
    of every disbursement unit it reaches (`target` `disbursement`)."""
    reached = reflow_links.select(
        "target", loan_id="group_id", unit_id="unit_id"
    ).unique()
    trace = links.filter(pl.col("task") == "trace").select(
        unit_id="right_id", disbursement_unit_id="left_id"
    )
    disbursed = pl.concat(
        [
            reached.filter(pl.col("target") == "commitment").join(trace, on="unit_id"),
            reached.filter(pl.col("target") == "disbursement").with_columns(
                disbursement_unit_id="unit_id"
            ),
        ],
        how="diagonal",
    )
    records = units.select(
        disbursement_unit_id="unit_id", record_id="record_ids"
    ).explode("record_id")
    return (
        disbursed.join(records, on="disbursement_unit_id")
        .select("loan_id", "record_id")
        .unique()
    )


def loan_shares(
    loans: pl.LazyFrame,
    *,
    allocations: pl.LazyFrame,
    untraced: pl.LazyFrame,
    crs: pl.LazyFrame,
) -> pl.LazyFrame:
    """Each loan's parts as shares of its disbursement, from `loans`
    (`loan_records`), the CRS rows of `allocations`, the release's `untraced` and
    the `crs` records allocation holds no row of.

    A part is a loan, `basis`, `row_id` and pair of inherited markers, the markers
    set only on `untraced` parts. `share` and `low_share` are its allocated and
    certainly allocated amounts over the loan's disbursement
    (`loan_disbursement_usd`); off a CRDF row `low_share` is `share`. A record
    allocation holds no row of is linked to no commitment, and its positive
    disbursement is one part. A loan whose records allocate nothing has no part.
    """
    crs_allocations = allocations.filter(pl.col("disbursement_source") == "crs")
    unallocated = (
        crs.filter(pl.col("disbursement_usd") > 0)
        .join(loans, on="record_id")
        .join(crs_allocations, on="record_id", how="anti")
        .select(
            "loan_id",
            "record_id",
            allocated_usd="disbursement_usd",
            rule=pl.lit("unlinked"),
        )
    )
    # A linked record's excess is untraced as `excess`, and an unlinked record's
    # disbursement as `unlinked` (`allocate.untraced_records`).
    counted = untraced.select(
        "record_id",
        rule="reason",
        untraced=pl.lit(value=True),
        inherited_rio_adaptation="rio_adaptation",
        inherited_rio_mitigation="rio_mitigation",
    )
    off_row = pl.col("rule").is_in(("excess", "unlinked"))
    parts = (
        pl.concat(
            [crs_allocations.join(loans, on="record_id"), unallocated],
            how="diagonal_relaxed",
        )
        .join(counted, on=["record_id", "rule"], how="left", validate="m:1")
        .with_columns(
            basis=pl.when(off_row & pl.col("untraced").fill_null(value=False))
            .then(pl.lit("untraced"))
            .when(pl.col("rule") == "excess")
            .then(pl.lit("excess"))
            .when(off_row)
            .then(pl.lit("unmeasured"))
            .when(pl.col("row_id").is_null())
            .then(pl.lit("outside_release"))
            .otherwise(pl.lit("commitment")),
        )
        .with_columns(
            pl.when(pl.col("basis") == "untraced").then(pl.col(_MARKERS)),
            low=pl.when(pl.col("basis") == "commitment")
            .then(pl.col("allocated_low_usd"))
            .otherwise(pl.col("allocated_usd")),
        )
        .group_by(_PART)
        .agg(
            part_usd=ordered_sum(pl.col("allocated_usd")),
            part_low_usd=ordered_sum(pl.col("low")),
        )
    )
    disbursed = parts.group_by("loan_id").agg(
        loan_disbursement_usd=ordered_sum(pl.col("part_usd"))
    )
    return (
        parts.join(disbursed, on="loan_id", validate="m:1")
        .filter(pl.col("loan_disbursement_usd") > 0)
        .select(
            *_PART,
            "loan_disbursement_usd",
            share=pl.col("part_usd") / pl.col("loan_disbursement_usd"),
            low_share=pl.col("part_low_usd") / pl.col("loan_disbursement_usd"),
        )
    )


def attribute_reflows(
    reflows: pl.LazyFrame,
    *,
    reflow_units: pl.LazyFrame,
    reflow_links: pl.LazyFrame,
    shares: pl.LazyFrame,
) -> pl.LazyFrame:
    """Each record of `reflows` split into its parts, by `basis`, `row_id` and
    inherited markers, with `received_usd`, `interest_usd` and their low amounts,
    the record's `record_received_usd` and `record_interest_usd`, its loan
    (`loan_id`, `loan_disbursement_usd`) and its reflow unit's `link_method`.

    `shares` is `loan_shares`. A record's principal takes the shares up to what
    the loan has left of its disbursement, after the loan's earlier repayments by
    year then record id, and the rest is `beyond_disbursed`.
    """
    unit_of = reflow_units.select(
        reflow_unit_id="unit_id", record_id="record_ids"
    ).explode("record_id")
    # A reflow unit's links share its first matching tier's one key, so one group.
    loan_of = reflow_links.group_by("reflow_unit_id").agg(
        loan_id=pl.col("group_id").first(),
        link_method=pl.col("link_method").first(),
        certain=(
            pl.col("link_method").is_in(CERTAIN_METHODS) & ~pl.col("records_disagree")
        ).all(),
    )
    disbursed = shares.select("loan_id", "loan_disbursement_usd").unique()
    records = (
        reflows.select(
            "record_id",
            "year",
            record_received_usd=pl.col("received_usd").fill_null(0.0),
            record_interest_usd=pl.col("interest_usd").fill_null(0.0),
        )
        .join(unit_of, on="record_id", how="left", validate="1:1")
        .join(loan_of, on="reflow_unit_id", how="left", validate="m:1")
        .join(disbursed, on="loan_id", how="left", validate="m:1")
        .with_columns(pl.col("certain").fill_null(value=False))
    )
    # What the loan's repayments up to and including the record attribute, held as
    # a column so that the shift's window does not nest the sum's.
    received = pl.col("record_received_usd")
    order = ["year", "record_id"]
    capped = (
        records.with_columns(
            held=pl.min_horizontal(
                received.cum_sum().over("loan_id", order_by=order),
                pl.col("loan_disbursement_usd"),
            )
        )
        .with_columns(
            attributed=pl.col("held")
            - pl.col("held").shift(1, fill_value=0.0).over("loan_id", order_by=order)
        )
        .drop("held")
    )

    def low(amount: pl.Expr) -> pl.Expr:
        return pl.when(pl.col("certain")).then(amount).otherwise(0.0)

    shared = pl.col("loan_disbursement_usd").is_not_null()
    record = (
        "record_id",
        "record_received_usd",
        "record_interest_usd",
        "loan_id",
        "loan_disbursement_usd",
        "link_method",
    )
    parts = capped.filter(shared).join(
        shares.drop("loan_disbursement_usd"), on="loan_id"
    )
    attributed = parts.select(
        *record,
        "basis",
        "row_id",
        *_MARKERS,
        received_usd=pl.col("attributed") * pl.col("share"),
        received_low_usd=low(pl.col("attributed") * pl.col("low_share")),
        interest_usd=pl.col("record_interest_usd") * pl.col("share"),
        interest_low_usd=low(pl.col("record_interest_usd") * pl.col("low_share")),
    )
    beyond = capped.filter(shared, received != pl.col("attributed")).select(
        *record,
        basis=pl.lit("beyond_disbursed"),
        received_usd=received - pl.col("attributed"),
        received_low_usd=low(received - pl.col("attributed")),
        interest_usd=pl.lit(0.0),
        interest_low_usd=pl.lit(0.0),
    )
    unattributed = capped.filter(~shared).select(
        *record,
        basis=pl.when(pl.col("loan_id").is_null())
        .then(pl.lit("unlinked"))
        .otherwise(pl.lit("no_disbursement")),
        received_usd=received,
        received_low_usd=low(received),
        interest_usd=pl.col("record_interest_usd"),
        interest_low_usd=low(pl.col("record_interest_usd")),
    )
    return pl.concat([attributed, beyond, unattributed], how="diagonal_relaxed")


def release_reflows(
    attributed: pl.LazyFrame, *, reflows: pl.LazyFrame, crdf: pl.LazyFrame
) -> pl.LazyFrame:
    """`attribute_reflows` rows in the `reflows` schema, with the record's own
    fields and markers, and on `commitment` rows the CRDF row's markers and climate
    share, as `label.inherit_markers` gives them to a disbursement."""
    record = reflows.select(
        "record_id",
        "year",
        "agency_code",
        "recipient_code",
        "channel_code",
        "bi_multi",
        "purpose_code",
        "flow_code",
        "finance_t",
        "psi_flag",
        "project_number",
        "title",
        provider_code="donor_code",
        record_rio_adaptation=measured_adaptation(pl.col("rio_adaptation")),
        record_rio_mitigation="rio_mitigation",
    )
    commitment = crdf.select(
        "row_id",
        "climate_share",
        row_rio_adaptation="rio_adaptation",
        row_rio_mitigation="rio_mitigation",
    )
    return (
        attributed.join(record, on="record_id", how="left", validate="m:1")
        .join(commitment, on="row_id", how="left", validate="m:1")
        .with_columns(
            inherited_rio_adaptation=pl.coalesce(
                "row_rio_adaptation", "inherited_rio_adaptation"
            ),
            inherited_rio_mitigation=pl.coalesce(
                "row_rio_mitigation", "inherited_rio_mitigation"
            ),
        )
        .select(list(REFLOWS.columns))
    )
