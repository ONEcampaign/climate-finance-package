"""CRDF rows to CRS records, within a year, with the recovery routes."""

import polars as pl

from climate_finance.build.link.keys import as_exact_links

# CRDF rows and CRS commitments that agree on these, the CRDF provider standing for
# the CRS donor, are one commitment: 99.5% of such keys pair one to one.
WITHIN_YEAR_KEY = ("donor_code", "crs_id", "year", "purpose_code")


def link_within_year(crdf: pl.LazyFrame, *, units: pl.LazyFrame) -> pl.LazyFrame:
    """Link each CRDF row to every commitment unit of the same year sharing its
    (donor, crs id, purpose), as `links` rows.

    `group_id` names the key, so a commitment's face value is the sum of its group's
    commitment units.
    """
    commitments = units.filter(pl.col("commitment_usd") > 0).select(
        "unit_id", *WITHIN_YEAR_KEY
    )
    return as_exact_links(
        crdf.select("row_id", *WITHIN_YEAR_KEY[1:], donor_code="provider_code")
        .drop_nulls(WITHIN_YEAR_KEY)
        .join(commitments, on=WITHIN_YEAR_KEY)
        .select(
            left_id="row_id",
            right_id="unit_id",
            group_id=pl.concat_str(
                [pl.lit("within_year"), *WITHIN_YEAR_KEY], separator="|"
            ),
            link_method=pl.lit("within_year"),
        ),
        task="crdf_crs",
    )


def recover(
    crdf: pl.LazyFrame, *, crs: pl.LazyFrame, external: pl.LazyFrame
) -> pl.LazyFrame:
    """EIB project-list, IATI crosswalk, title-recipient-amount, GCF and GEF joins."""
    raise NotImplementedError("slice S07")
