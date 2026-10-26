"""Reflow records into units, linked to their loan's commitment units by the exact key
tiers.

A reflow unit sums the `reflows` records that share every key field (`UNIT_KEY`), as
a trace unit sums the CRS records. It takes the first tier in `TIERS` whose key
reaches an eligible commitment unit, under the floors, `floor_seen` and `_unique_pn`
the trace's disbursement units pass. So the World Bank's repayments reach their loan
through `wb_loan` whatever crs id they carry, the AsDB's through a project-number
tier, and a bilateral provider's through its crs id, while a reflow under neither a
known crs id nor a project number, as the EBRD files them, links nothing. Review
decisions and key repair do not apply.
"""

import polars as pl

from climate_finance import schema
from climate_finance.build.link.disagreement import flag_disagreement
from climate_finance.build.link.keys import UNIT_KEY, key_columns, link_by_tiers
from climate_finance.build.normalise.ids import content_id
from climate_finance.build.normalise.project_numbers import normalise_pn

_SALT = "reflow"


def build_reflow_units(reflows: pl.LazyFrame) -> pl.LazyFrame:
    """Sum `reflows` records into units, in the `reflow_units` schema.

    A unit's commitment date is its largest record's, by principal plus interest,
    ties to the smaller record id. Placed and floor years follow `build_units`.
    """
    dated = pl.col("commitment_year") < pl.col("year")
    size = pl.col("received_usd").abs().fill_null(0) + pl.col(
        "interest_usd"
    ).abs().fill_null(0)
    return (
        reflows.with_columns(
            project_number=normalise_pn(pl.col("project_number")),
            commitment_year=pl.col("commitment_date").dt.year().cast(pl.Int16),
        )
        .group_by(UNIT_KEY)
        # Summed as lists in record order, as `build_units` sums.
        .agg(
            pl.col("received_usd", "interest_usd").fill_null(0).sort_by("record_id"),
            pl.col("commitment_date")
            .sort_by(size, "record_id", descending=[True, False])
            .first(),
            record_ids=pl.col("record_id").sort(),
        )
        .with_columns(pl.col("received_usd", "interest_usd").list.sum())
        .with_columns(_salt=pl.lit(_SALT))
        .with_columns(
            unit_id=content_id((*UNIT_KEY, "_salt"), order_by=UNIT_KEY),
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
    under its first matching tier, in the `reflow_links` schema.

    `first_year` is the first CRS year the build loads. The reflow units take their
    keys beside the trace units (`key_columns`), which keep theirs.
    """
    keyed = key_columns(
        pl.concat(
            [units, reflow_units.drop("received_usd", "interest_usd")],
            how="diagonal",
        )
    )
    reflow_ids = reflow_units.select("unit_id")
    links = link_by_tiers(
        keyed.join(reflow_ids, on="unit_id", how="semi"),
        commitments=keyed.filter(pl.col("commitment_usd") > 0),
        first_year=first_year,
    )
    facts = ("unit_id", "recipient_code", "project_number")
    flagged = flag_disagreement(
        links, units=pl.concat([units.select(facts), reflow_units.select(facts)])
    )
    return flagged.select(
        reflow_unit_id="left_id",
        unit_id="right_id",
        group_id="group_id",
        link_method="link_method",
        records_disagree="records_disagree",
    ).sort("reflow_unit_id", "unit_id")
