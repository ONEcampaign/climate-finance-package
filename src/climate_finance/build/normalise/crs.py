"""Bulk CRS records into the `crs` schema, in USD units, and the lookups of CRS
keys that the CRDF rules join on."""

import logging
from collections.abc import Mapping

import polars as pl

from climate_finance.build.normalise.ids import text_digest, with_content_id
from climate_finance.schema import (
    CORE_CONTRIBUTION,
    CRS_UNSPECIFIED_RECIPIENT,
    REFLOWS,
    UNSPECIFIED_RECIPIENT,
)

logger = logging.getLogger(__name__)

# The bulk file publishes amounts in USD million at current prices.
_USD_PER_UNIT = 1e6
_AMOUNTS = {
    "commitment_usd": "usd_commitment",
    "disbursement_usd": "usd_disbursement",
    "grant_equiv_usd": "usd_grant_equiv",
}
# Principal repaid and interest paid on a loan, which most records that carry one
# report on a row of their own.
_REFLOWS = {"received_usd": "usd_received", "interest_usd": "usd_interest"}
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
# Records equal on `_IDENTITY` are numbered by their long description, which a
# revision of amounts leaves alone, and then by their amounts. In the October 2026
# file 97% of such records also agree on every other published field, so nothing
# but their amounts tells them apart.
_OCCURRENCE = ("long_description", *_AMOUNTS.values())
# Reflow-only records, which no amount tells apart, are numbered by their long
# description and their repayments.
_REFLOW_OCCURRENCE = ("long_description", *_REFLOWS.values())
_REFLOW_SALT = "reflow"
_IDS = ("crs_id", "project_number")
_TEXT = ("project_title", "short_description", "long_description")


def normalise_crs(raw: pl.LazyFrame, *, years: tuple[int, int]) -> pl.LazyFrame:
    """Records of `years` (inclusive) with an amount, in the `crs` schema.

    A record has an amount when its commitment, disbursement or grant equivalent is
    non-zero. Records with only a grant equivalent carry negative grant equivalents
    and stay in. A negative grant equivalent on a positive disbursement is null, as
    for a record the CRS does not value, so no disbursement carries a negative grant
    equivalent; the count of such records is logged. Bi_Multi is kept as published,
    core contributions included (see `trace_population`), and missing Rio markers
    stay null. A kept record carries the principal repaid and interest paid it
    publishes, and the records that publish only those are `normalise_reflows`'s.

    `record_id` digests the record's identifying fields, so it is the same in every
    build of the same file and survives a revision of the record's amounts. Records
    that agree on those fields and on their long description are told apart by
    their amounts alone, so a revision that reorders their amounts swaps their ids.
    """
    in_years = _in_years(raw, years, _has_value(_AMOUNTS))
    correction = _is_correction("usd_grant_equiv", "usd_disbursement")
    if corrections := in_years.select(correction.sum()).collect().item():
        logger.warning(
            "%d CRS records with a negative grant equivalent on a positive "
            "disbursement; their grant equivalent is null",
            corrections,
        )
    return _in_schema(
        with_content_id(in_years, "record_id", _IDENTITY, order_by=_OCCURRENCE)
    ).with_columns(
        grant_equiv_usd=pl.when(
            ~_is_correction("grant_equiv_usd", "disbursement_usd")
        ).then(pl.col("grant_equiv_usd"))
    )


def normalise_reflows(raw: pl.LazyFrame, *, years: tuple[int, int]) -> pl.LazyFrame:
    """Records of `years` (inclusive) with principal repaid or interest paid, in the
    `reflows` schema and in USD.

    A record that also has an amount keeps its `crs` record id, so it is one record
    in both tables. A record with only a repayment takes the digest of its
    identifying fields salted with "reflow", numbered by its long description and
    its repayments where several agree, so no id collides with a `crs` id and a
    revision of amounts leaves the others alone.
    """
    has_amount, has_reflow = _has_value(_AMOUNTS), _has_value(_REFLOWS)
    rows = _in_years(raw, years, has_amount | has_reflow)
    with_amount = rows.filter(has_amount)
    # `normalise_crs` numbers records that agree on `_IDENTITY`, so the ids of the
    # records that also repay come from the groups they sit in, which are whole.
    shared = with_amount.filter(has_reflow).select(_IDENTITY).unique()
    kept = with_content_id(
        with_amount.join(shared, on=list(_IDENTITY), how="semi", nulls_equal=True),
        "record_id",
        _IDENTITY,
        order_by=_OCCURRENCE,
    ).filter(has_reflow)
    salted = with_content_id(
        rows.filter(has_reflow & ~has_amount).with_columns(_salt=pl.lit(_REFLOW_SALT)),
        "record_id",
        (*_IDENTITY, "_salt"),
        order_by=_REFLOW_OCCURRENCE,
    ).drop("_salt")
    return _in_schema(pl.concat([kept, salted])).select(*REFLOWS.columns)


def _has_value(sources: Mapping[str, str]) -> pl.Expr:
    """A record with a non-zero value in any of the raw columns `sources` names."""
    return pl.any_horizontal(
        pl.col(source).fill_null(0) != 0 for source in sources.values()
    )


def _in_years(raw: pl.LazyFrame, years: tuple[int, int], keep: pl.Expr) -> pl.LazyFrame:
    """The `keep` records of `years`, with their ids trimmed."""
    return (
        raw.filter(pl.col("year").is_between(*years), keep)
        # Some ids carry trailing spaces (Germany's 2020 records), which no key
        # would match to the same id written without them.
        .with_columns(pl.col(_IDS).cast(pl.String).str.strip_chars())
    )


def _in_schema(records: pl.LazyFrame) -> pl.LazyFrame:
    """`records`, raw columns and a `record_id`, in the `crs` schema and USD."""
    return records.with_columns(text_hash=text_digest(_TEXT)).select(
        "record_id",
        pl.col("year").cast(pl.Int16),
        pl.col("donor_code", "agency_code").cast(pl.Int32),
        pl.col(_IDS),
        title=pl.col("project_title").cast(pl.String),
        short_description=pl.col("short_description").cast(pl.String),
        long_description=pl.col("long_description").cast(pl.String),
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
        finance_t=pl.col("finance_t").cast(pl.Int32),
        **{
            name: pl.col(source).cast(pl.Float64) * _USD_PER_UNIT
            for name, source in (_AMOUNTS | _REFLOWS).items()
        },
        commitment_date=pl.col("commitment_date").cast(pl.Date),
        expected_start_date=pl.col("expected_start_date").cast(pl.Date),
        completion_date=pl.col("completion_date").cast(pl.Date),
        rio_adaptation=pl.col("climate_adaptation").cast(pl.Int8),
        rio_mitigation=pl.col("climate_mitigation").cast(pl.Int8),
        text_hash=pl.col("text_hash"),
    )


def _is_correction(grant_equiv: str, disbursement: str) -> pl.Expr:
    """A negative grant equivalent on a positive disbursement."""
    return ((pl.col(grant_equiv) < 0) & (pl.col(disbursement) > 0)).fill_null(False)


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
