"""Whether the records of a trace link disagree on recipient or project number.

A disbursement unit's links under one method disagree when the commitments they
reach
- all sit in other recipients than the unit, one of them and the unit each in a
  country (`regional_codes` name none), or
- under a method whose key leaves the project number free (`NUMBER_FREE`), all
  carry another `project_core` than the unit, which carries one.

Of 329 links read against both sides' records, the 73 that disagree held all 4
that reached another project and all 25 that reached another recipient's line of
the right project. The 2 other errors were another tranche of the right project,
which no field marks. A unit's link to itself, as when one record commits and
disburses, counts for neither check.
"""

import polars as pl

from climate_finance.build.link.key_repair import project_core
from climate_finance.build.link.keys import regional_codes

# Methods whose key leaves the project number free though both sides should carry
# one. The other key tiers and rules match on it, or link records whose numbers
# differ by design: an EU contract and its decision, a World Bank loan and its
# project, and the title and crosswalk links.
NUMBER_FREE = ("crs_id_purpose", "crs_id", "classifier")


def flag_disagreement(links: pl.DataFrame, *, units: pl.DataFrame) -> pl.DataFrame:
    """`links` with `records_disagree` set on its `trace` links, true for every link
    of a disbursement unit and method whose records disagree. Other tasks keep
    theirs.

    Args:
        links: `links` rows.
        units: the link stage's units.
    """
    group = ("left_id", "link_method")
    facts = units.select(
        "unit_id",
        "recipient_code",
        core=project_core(pl.col("project_number")),
        country=~pl.col("recipient_code").is_in(regional_codes()),
    )
    recipient = (pl.col("country") & pl.col("country_c")).any() & ~(
        pl.col("recipient_code") == pl.col("recipient_code_c")
    ).any()
    number = (
        pl.col("link_method").first().is_in(NUMBER_FREE)
        & (pl.col("core").is_not_null() & pl.col("core_c").is_not_null()).any()
        & ~(pl.col("core") == pl.col("core_c")).any()
    )
    disagree = (
        links.filter(pl.col("task") == "trace", pl.col("left_id") != pl.col("right_id"))
        .select(*group, "right_id")
        .join(facts.rename({"unit_id": "left_id"}), on="left_id")
        .join(
            facts.rename(lambda name: f"{name}_c"),
            left_on="right_id",
            right_on="unit_id_c",
        )
        .group_by(group)
        .agg(disagree=recipient | number)
    )
    trace = pl.col("task") == "trace"
    return (
        links.join(disagree, on=group, how="left", maintain_order="left")
        .with_columns(
            records_disagree=pl.when(trace)
            .then(pl.col("disagree").fill_null(value=False))
            .otherwise(pl.col("records_disagree"))
        )
        .select(links.columns)
    )
