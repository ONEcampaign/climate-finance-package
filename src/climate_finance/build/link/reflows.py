"""Reflow records into units, linked to their loan's commitment units by the exact key
tiers.

A reflow unit sums the `reflows` records of one line (`keys.with_line_key`), as a
trace unit sums the CRS records. It takes the first tier in `TIERS` whose key
reaches an eligible commitment unit, under the floors, `floor_seen` and `_unique_pn`
the trace's disbursement units pass. So the World Bank's repayments reach their loan
through `wb_loan` whatever crs id they carry, the AsDB's through a project-number
tier, and a bilateral provider's through its crs id, while a reflow under neither a
known crs id nor a project number, as the EBRD files them, links nothing. Review
decisions and key repair do not apply.

A reflow record with no Bi_Multi code takes no unit. Every view lists the Bi_Multi
codes it counts, so none could count the repayment, and linking it would credit the
loan with a repayment that no net flow subtracts.

A reflow unit that reaches no commitment unit ties to its loan's disbursement units
through the same tiers and floors, with disbursement units in place of commitment
units (`target` `disbursement`). Gross counts a loan whose disbursements reach no
CRDF commitment at those disbursements' own markers (`untraced`), so its repayments
need the disbursements to subtract the same way. A loan with no disbursement in the
build's years ties to nothing.
"""

import polars as pl

from climate_finance import schema
from climate_finance.build.link.disagreement import flag_disagreement
from climate_finance.build.link.keys import (
    ID_KEY,
    LINE_KEY,
    id_purpose,
    key_columns,
    link_by_tiers,
    purpose_columns,
    with_line_key,
)
from climate_finance.build.normalise.ids import content_id
from climate_finance.build.normalise.project_numbers import normalise_pn

_SALT = "reflow"


def build_reflow_units(reflows: pl.LazyFrame) -> pl.LazyFrame:
    """Sum `reflows` records into units, in the `reflow_units` schema.

    A unit's commitment date is its largest record's, by principal plus interest,
    ties to the smaller record id. Placed and floor years follow `build_units`. A
    record with no `bi_multi` is left out.
    """
    dated = pl.col("commitment_year") < pl.col("year")
    size = pl.col("received_usd").abs().fill_null(0) + pl.col(
        "interest_usd"
    ).abs().fill_null(0)
    return (
        reflows.filter(pl.col("bi_multi").is_not_null())
        .with_columns(
            project_number=normalise_pn(pl.col("project_number")),
            commitment_year=pl.col("commitment_date").dt.year().cast(pl.Int16),
        )
        .pipe(with_line_key)
        .group_by(LINE_KEY)
        # Summed as lists in record order, as `build_units` sums.
        .agg(
            pl.col("received_usd", "interest_usd").fill_null(0).sort_by("record_id"),
            pl.col("commitment_date", "purpose_code")
            .sort_by(size, "record_id", descending=[True, False])
            .first(),
            *purpose_columns(),
            record_ids=pl.col("record_id").sort(),
        )
        .with_columns(pl.col("received_usd", "interest_usd").list.sum())
        .with_columns(id_purpose(), _salt=pl.lit(_SALT))
        .with_columns(
            unit_id=content_id((*ID_KEY, "_salt"), order_by=ID_KEY),
            placed_year=pl.when(dated)
            .then(pl.col("commitment_year"))
            .otherwise(pl.col("year")),
            floor_year=pl.when(dated).then(pl.col("commitment_year")),
        )
        .select(list(schema.REFLOW_UNITS.columns))
    )


def link_reflows(
    units: pl.DataFrame, reflow_units: pl.DataFrame, *, first_year: int
) -> pl.DataFrame:
    """Links from each of `reflow_units` to every eligible commitment unit of `units`
    under its first matching tier, or where none is, to every eligible disbursement
    unit, in the `reflow_links` schema.

    `first_year` is the first CRS year the build loads. The reflow units take their
    keys beside the trace units (`key_columns`), which keep theirs.
    """
    keyed = key_columns(
        pl.concat(
            [units, reflow_units.drop("received_usd", "interest_usd")],
            how="diagonal",
        )
    )
    reflows = keyed.join(reflow_units.select("unit_id"), on="unit_id", how="semi")
    to_commitments = link_by_tiers(
        reflows,
        commitments=keyed.filter(pl.col("commitment_usd") > 0),
        first_year=first_year,
    )
    to_disbursements = link_by_tiers(
        reflows.join(
            to_commitments.select(unit_id="left_id"), on="unit_id", how="anti"
        ),
        commitments=keyed.filter(pl.col("disbursement_usd") > 0),
        first_year=first_year,
    )
    facts = ("unit_id", "recipient_code", "project_number")
    flagged = flag_disagreement(
        pl.concat([to_commitments, to_disbursements]),
        units=pl.concat([units.select(facts), reflow_units.select(facts)]),
    )
    return flagged.select(
        reflow_unit_id="left_id",
        unit_id="right_id",
        target=pl.when(pl.col("left_id").is_in(to_commitments["left_id"].implode()))
        .then(pl.lit("commitment"))
        .otherwise(pl.lit("disbursement")),
        group_id="group_id",
        link_method="link_method",
        records_disagree="records_disagree",
    ).sort("reflow_unit_id", "unit_id")
