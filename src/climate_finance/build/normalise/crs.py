"""Bulk CRS records into the `crs` schema, in USD units, and the lookups of CRS
keys that the CRDF rules join on."""

import polars as pl

from climate_finance.build.normalise.ids import text_digest, with_content_id
from climate_finance.schema import CRS_UNSPECIFIED_RECIPIENT, UNSPECIFIED_RECIPIENT

# The bulk file publishes amounts in USD million at current prices.
_USD_PER_UNIT = 1e6
# Bi_Multi 2 is a donor's core contribution to a multilateral agency. The agency's
# own outflows (code 4) carry that money to recipients, so the trace leaves code 2
# out. Provider totals count it only as core contributions.
CORE_CONTRIBUTION = 2

_AMOUNTS = {
    "commitment_usd": "usd_commitment",
    "disbursement_usd": "usd_disbursement",
    "grant_equiv_usd": "usd_grant_equiv",
}
# Every published field that tells two records apart other than their amounts, so a
# record keeps its id when the OECD revises its amounts.
_IDENTITY = (
    "year",
    "donor_code",
    "agency_code",
    "crs_id",
    "project_number",
    "recipient_code",
    "purpose_code",
    "channel_code",
    "bi_multi",
    "flow_code",
    "finance_t",
    "commitment_date",
    "project_title",
    "short_description",
)
_TEXT = ("project_title", "short_description", "long_description")


def normalise_crs(raw: pl.LazyFrame, *, years: tuple[int, int]) -> pl.LazyFrame:
    """Records of `years` (inclusive) with an amount, in the `crs` schema.

    A record has an amount when its commitment, disbursement or grant equivalent is
    non-zero; records with only a grant equivalent carry negative grant equivalents
    and stay in. Bi_Multi is kept as published, core contributions included (see
    `trace_population`), and missing Rio markers stay null.

    `record_id` digests the record's identifying fields, so it is the same in every
    build of the same file and survives a revision of the record's amounts.
    """
    has_amount = pl.any_horizontal(
        pl.col(source).fill_null(0) != 0 for source in _AMOUNTS.values()
    )
    in_years = raw.filter(pl.col("year").is_between(*years), has_amount)
    return (
        with_content_id(
            in_years, "record_id", _IDENTITY, order_by=list(_AMOUNTS.values())
        )
        .with_columns(text_hash=text_digest(_TEXT))
        .select(
            "record_id",
            pl.col("year").cast(pl.Int16),
            pl.col("donor_code", "agency_code").cast(pl.Int32),
            pl.col("crs_id", "project_number").cast(pl.String),
            title=pl.col("project_title").cast(pl.String),
            # The bulk CRS publishes "Developing countries, unspecified" as 9998,
            # and the CRDF and the DAC recipient list use 998. Writing 998 here gives
            # every stage and release table one code.
            recipient_code=pl.col("recipient_code")
            .cast(pl.Int32)
            .replace(CRS_UNSPECIFIED_RECIPIENT, UNSPECIFIED_RECIPIENT),
            purpose_code=pl.col("purpose_code").cast(pl.Int32),
            channel_code=pl.col("channel_code").cast(pl.Int32),
            bi_multi=pl.col("bi_multi").cast(pl.Int8),
            flow_code=pl.col("flow_code").cast(pl.Int32),
            **{
                name: pl.col(source).cast(pl.Float64) * _USD_PER_UNIT
                for name, source in _AMOUNTS.items()
            },
            commitment_date=pl.col("commitment_date").cast(pl.Date),
            expected_start_date=pl.col("expected_start_date").cast(pl.Date),
            completion_date=pl.col("completion_date").cast(pl.Date),
            rio_adaptation=pl.col("climate_adaptation").cast(pl.Int8),
            rio_mitigation=pl.col("climate_mitigation").cast(pl.Int8),
            text_hash=pl.col("text_hash"),
        )
    )


def trace_population(crs: pl.LazyFrame) -> pl.LazyFrame:
    """The records the trace follows: every record except core contributions."""
    return crs.filter(pl.col("bi_multi") != CORE_CONTRIBUTION)


def commitment_keys(crs: pl.LazyFrame) -> pl.LazyFrame:
    """The (provider, crs id, year, purpose) keys the CRS commits money under.

    Records are summed by donor, agency, crs id, project number, recipient, purpose,
    year and commitment year, and a key has a commitment when one of its sums is
    positive.
    """
    units = [
        "donor_code",
        "agency_code",
        "crs_id",
        "project_number",
        "recipient_code",
        "purpose_code",
        "year",
        pl.col("commitment_date").dt.year().alias("commitment_year"),
    ]
    return (
        _crs_units(crs)
        .group_by(units)
        .agg(pl.col("commitment_usd").sum().alias("unit_commitment"))
        .filter(pl.col("unit_commitment") > 0, pl.col("crs_id").is_not_null())
        .select(
            provider_code="donor_code",
            crs_id="crs_id",
            year="year",
            purpose_code="purpose_code",
        )
        .unique()
    )


def crs_ids(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Every (provider, crs id) the CRS reports in any year."""
    return (
        _crs_units(crs)
        .filter(pl.col("crs_id").is_not_null())
        .select(provider_code="donor_code", crs_id="crs_id")
        .unique()
    )


def _crs_units(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Trace-population records that report a commitment or a disbursement."""
    return trace_population(crs).filter(
        pl.col("commitment_usd").is_not_null()
        | pl.col("disbursement_usd").is_not_null()
    )
