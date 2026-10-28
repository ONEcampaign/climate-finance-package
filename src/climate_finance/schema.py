"""Table schemas for every build stage and release table, and `release.json`.

Amounts are float64 USD in units at current prices, DAC codes int32, ids strings,
years int16, `bi_multi` and Rio markers int8. Each table has a sort key. The stage
writer sorts by it before writing, so two builds from the same inputs hash the same.
"""

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl
import polars.selectors as cs
from pydantic import BaseModel, ConfigDict, JsonValue, StrictInt

from climate_finance.errors import SchemaError

STATUSES = (
    "disbursed",
    "partly_disbursed",
    "no_disbursement_record",
    "not_traceable",
    "pending_review",
)
KEY_TIERS = (
    "wb_loan",
    "crs_id_purpose",
    "crs_id",
    "crs_id_parent",
    "pn_recipient_date",
    "pn_recipient_purpose",
    "pn_recipient",
)
# The key tiers whose donor id names the commitment: a link of one of them whose
# records do not disagree (`LINKS.records_disagree`) is certain. So is the pair of a
# record with its own commitment, where one record commits and disburses, whatever
# link joined them. Every other pair is estimated.
CERTAIN_METHODS = ("wb_loan", "crs_id_purpose", "crs_id", "crs_id_parent")
# The `crdf_crs` routes that tie a CRDF row to its CRS commitment by construction:
# the same crs id in the same year, a contract matched within 0.01% in euros, and
# the CRS record the row was cut from. Every other route, `eib_list`,
# `crosswalk_amount`, `title_recipient`, `eib_text_amount` and `eib_amount` among
# them, is estimated, so a certain trace link reached through it counts nothing in
# the low bound.
CERTAIN_ROUTES = ("within_year", "crosswalk", "aggregate_partition")
KEY_REPAIR_METHODS = (
    "repair_new_crs_id",
    "repair_pn_variant",
    "repair_afdb_scientific",
    "repair_title_recipient",
    "repair_stray_country",
)
LINK_METHODS = (
    *KEY_TIERS,
    *KEY_REPAIR_METHODS,
    "within_year",
    "classifier",
    "classifier_jev",
    "weak_classifier",
    "splink_jev",
    "title_recipient",
    "eib_list",
    "aggregate_partition",
    "iati",
    "crosswalk",
    "crosswalk_amount",
    "programme_number",
    "btr_id",
    "btr_splink",
    "human",
    "eib_text_amount",
    "eib_amount",
)
LINK_TASKS = ("crdf_crs", "trace", "btr_crs")
REVIEW_REASONS = (
    "band",
    "jev_band",
    "rule_disagreement",
    "over_face",
    "ledger_contradicted",
    "calibration",
)
LEDGER_TRANSACTION_TYPES = (
    "commitment",
    "disbursement",
    "expenditure",
    "disbursed_to_date",
)
LEDGER_CHECKS = ("confirmed", "contradicted", "none", "confirmed_undisbursed")
DISBURSEMENT_SOURCES = ("crs", "bank_ledger", "iati", "none")
ALLOCATION_RULES = ("fifo", "dated", "window", "excess", "bank_ledger", "iati")
# Fill records the release leaves out, by the start of their `record_id`
# (`ledger:<publisher>:<key>:<year>`): the Green Climate Fund's projects API and
# readiness list. The Fund's website terms allow only private study, research and
# teaching, and no commercial derivative works without written consent, and its
# projects API states no terms of its own (`build.release` cites them). Its IATI
# files are CC0 on the IATI Registry, so `iati:gcf:` fills ship.
# A commitment's `withheld_usd` holds what the withheld records disbursed to it.
WITHHELD_RECORD_PREFIXES = ("ledger:gcf:", "ledger:gcf_readiness:")
OBJECTIVES = ("adaptation", "mitigation", "cross_cutting")
# An agency share's objective: one of OBJECTIVES, or `climate` for a total climate
# share with no split by objective, as the OECD gives before 2021.
SHARE_OBJECTIVES = (*OBJECTIVES, "climate")
LABEL_SOURCES = ("crdf", "marker", "jev")
# How a disbursement's own Rio markers relate to the ones it inherits.
MARKER_SOURCES = ("components", "commitment", "record_differs")
# Why a Rio-marked CRS record's disbursement reaches no commitment: it exceeds
# what its linked commitments take, or the record is linked to none.
UNTRACED_REASONS = ("excess", "unlinked")
# What a reflow link reaches: its loan's commitment units, or where no key tier
# reaches one, its loan's disbursement units.
REFLOW_TARGETS = ("commitment", "disbursement")
# Where a loan's repayment goes (`build.reflows`): to a CRDF commitment the loan's
# disbursements reached, to commitments with no CRDF row, to the loan's excess or
# records linked to no commitment, which gross counts as `untraced` where they are
# Rio-marked and leaves out otherwise (`excess`, `unmeasured`), past what the loan
# disbursed, or nowhere, as the loan disbursed nothing in the window or no key tier
# reached it (`unlinked`).
REFLOW_BASES = (
    "commitment",
    "outside_release",
    "excess",
    "untraced",
    "unmeasured",
    "beyond_disbursed",
    "no_disbursement",
    "unlinked",
)
PAIR_LABEL_SOURCES = ("key", "rule", "jev", "human", "agent", "ledger")
# Who decides a review pair: a person, or an agent reading a labelling packet.
DECIDERS = ("human", "agent")
# An agent's labelling rounds: the first label, a second label for a pair whose
# first label is unsure, and an adjudication with ids visible.
LABEL_ROUNDS = ("label", "second", "adjudicate")
ID_PRACTICE_CLASSES = (
    "id_persistent",
    "parent_id_persistent",
    "pn_persistent",
    "mixed",
    "neither",
    "unclassified",
)
SCORING_GROUPS = ("truth", "weak", "splink", "pooled_splink")
SHARE_SOURCES = ("oecd", "one")
# Where an agency share comes from: measured on the share years' commitments
# (`computed`), held from the last year measured on enough commitments (`held`), or
# the OECD's imputed share (`oecd`). `agency_spending` holds the first two, and every
# `agency_shares` row is the third.
SPENDING_ORIGINS = ("computed", "held")
SHARE_ORIGINS = (*SPENDING_ORIGINS, "oecd")
FETCH_METHODS = ("http", "registered", "manual")
PERSPECTIVES = ("provider", "recipient")
ORG_ROLES = ("provider", "agency", "channel")
FLOWS = ("commitments", "disbursements", "grant_equivalent")
# The flows a query counts: the stored ones, and disbursements less the repayments
# of their loans (`net_disbursements`) and less their interest too (`net_transfers`).
NET_FLOWS = ("net_disbursements", "net_transfers")
QUERY_FLOWS = (*FLOWS, *NET_FLOWS)
FUNDING_CLASSES = ("oda", "oof", "other")
# The currencies a query converts to, as `spec.Currency` spells them.
CURRENCIES = ("USD", "EUR", "GBP", "CAD")
DECISIONS = ("accept", "reject")
# The first year the CRDF and the CRS measure adaptation: the DAC introduced the
# adaptation marker for flows from 2010. Before it the recipient CRDF holds the rows
# its mitigation marker selects and prints "Not targeted/Not screened" as their
# adaptation marker. The few adaptation marks before 2010 predate the marker's start
# and count as not measured: 1 row in 2007, 2 in 2008 and 10 of 2,962 in 2009 marked
# principal or significant, and 49 climate-component rows in 2009, against 3,922 of
# 6,371 rows marked in 2010. Four of the 2009 rows (Kuwait, USD 142m) are marked for
# adaptation alone, so their climate value, the mitigation value, is 0.
ADAPTATION_FIRST_YEAR = 2010
# Code 6 holds private-sector and philanthropic outflows, kept as published. The query
# views list the codes they count and leave it out.
BI_MULTI_CODES = (1, 2, 3, 4, 6, 7, 8)
# Bilateral flows, multilateral outflows and core contributions. Code 2 is a donor's
# core contribution to a multilateral agency, whose own outflows (code 4) carry that
# money to recipients, so the trace leaves code 2 out. The recipient view counts
# bilateral flows and multilateral outflows.
BILATERAL = (1, 3, 7, 8)
MULTILATERAL_OUTFLOW = 4
CORE_CONTRIBUTION = 2
RECIPIENT_VIEW = (*BILATERAL, MULTILATERAL_OUTFLOW)
RIO_MARKERS = (0, 1, 2)

type DType = pl.DataType | type[pl.DataType]


@dataclass(frozen=True)
class Column:
    """One column: its dtype, whether it may hold nulls, its allowed values, and the
    smallest value it may hold.

    A dtype class (such as `pl.Struct`) accepts any parametrisation of it.
    """

    dtype: DType
    nullable: bool = False
    values: tuple[object, ...] | None = None
    minimum: float | None = None


@dataclass(frozen=True)
class TableSchema:
    """A table's columns in write order and the key it is sorted by."""

    name: str
    columns: Mapping[str, Column]
    sort_key: tuple[str, ...]

    def __post_init__(self) -> None:
        unknown = [key for key in self.sort_key if key not in self.columns]
        if unknown:
            raise ValueError(f"{self.name}: sort key columns {unknown} not declared")

    @property
    def dtypes(self) -> dict[str, DType]:
        """Each column's dtype, in write order, as a polars `schema=` argument."""
        return {name: column.dtype for name, column in self.columns.items()}


def _str(*, nullable: bool = False, values: tuple[str, ...] | None = None) -> Column:
    return Column(pl.String(), nullable=nullable, values=values)


def _f64(*, nullable: bool = False, minimum: float | None = None) -> Column:
    return Column(pl.Float64(), nullable=nullable, minimum=minimum)


_usd = _f64


def _code(*, nullable: bool = False) -> Column:
    return Column(pl.Int32(), nullable=nullable)


_count = _code


def _year(*, nullable: bool = False) -> Column:
    return Column(pl.Int16(), nullable=nullable)


def _bool(*, nullable: bool = False) -> Column:
    return Column(pl.Boolean(), nullable=nullable)


def _rio() -> Column:
    return Column(pl.Int8(), nullable=True, values=RIO_MARKERS)


def _bi_multi(*, nullable: bool = False) -> Column:
    return Column(pl.Int8(), nullable=nullable, values=BI_MULTI_CODES)


def _table(name: str, sort_key: tuple[str, ...], **columns: Column) -> TableSchema:
    return TableSchema(name=name, columns=columns, sort_key=sort_key)


# Stage tables --------------------------------------------------------------------

RAW_MANIFEST = _table(
    "raw_manifest",
    ("source", "path"),
    source=_str(),
    url=_str(nullable=True),
    path=_str(),
    sha256=_str(),
    etag=_str(nullable=True),
    version=_str(nullable=True),
    fetched_at=Column(pl.Datetime("us", "UTC")),
    method=_str(values=FETCH_METHODS),
    # For the CRS, the CRDF and MULTI, the years fetch asked for, and those of them
    # the file holds; the build reads the years asked that the file holds.
    first_year=_year(nullable=True),
    last_year=_year(nullable=True),
    held_first_year=_year(nullable=True),
    held_last_year=_year(nullable=True),
)

CRDF = _table(
    "crdf",
    ("row_id",),
    row_id=_str(),
    vintage=_year(),
    perspective=_str(values=PERSPECTIVES),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    # The CRDF row's long description.
    description=_str(nullable=True),
    recipient_code=_code(),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    purpose_code=_code(nullable=True),
    # The CRS flow category the CRDF publishes: 10 ODA, 21 other official flows,
    # 22, 30 private grants and 60 private market flows.
    flow_category=_code(nullable=True),
    # Provider-perspective imputed multilateral contribution rows carry a climate
    # total only, so their objective is `climate`.
    objective=_str(values=SHARE_OBJECTIVES),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    face_usd=_usd(),
    climate_usd=_usd(),
    # Null where the CRDF does not measure adaptation, before 2010.
    adaptation_usd=_usd(nullable=True),
    mitigation_usd=_usd(),
    overlap_usd=_usd(nullable=True),
    climate_share=_f64(nullable=True),
    id_scheme=_str(nullable=True),
    repeat_of=_str(nullable=True),
    parent_row_id=_str(nullable=True),
)
# Recipient rows of the year before the first `crdf` year, which give the first
# agency share year its two-year span.
CRDF_SPAN = _table("crdf_span", CRDF.sort_key, **CRDF.columns)

CRS = _table(
    "crs",
    ("record_id",),
    record_id=_str(),
    year=_year(),
    donor_code=_code(),
    agency_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    # The bulk CRS's project_title, short_description and long_description.
    title=_str(nullable=True),
    short_description=_str(nullable=True),
    long_description=_str(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    flow_code=_code(),
    # The type of finance, such as 110 for a standard grant.
    finance_t=_code(nullable=True),
    commitment_usd=_usd(nullable=True),
    disbursement_usd=_usd(nullable=True),
    # The CRS's grant equivalent, or for a grant (finance type 110 or 210) that
    # has none, its disbursement.
    grant_equiv_usd=_usd(nullable=True),
    # Principal repaid and interest paid on a loan in the year, as the bulk CRS
    # publishes them (`usd_received`, `usd_interest`). Most records that carry one
    # carry no commitment or disbursement, and `CRS_REFLOWS` holds them.
    received_usd=_usd(nullable=True),
    interest_usd=_usd(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
    # The project's expected start and completion, which the window rule reads.
    expected_start_date=Column(pl.Date(), nullable=True),
    completion_date=Column(pl.Date(), nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    text_hash=_str(nullable=True),
)

# The CRS records that report principal repaid or interest paid: one row per loan and
# year in the bulk file. A record that also has a commitment, disbursement or grant
# equivalent keeps its `crs` record id. A record with only a repayment takes an id
# salted with "reflow", which no `crs` id shares.
# The bulk file leaves Bi_Multi empty on 4,401 repayment-only rows of 2018 to 2024
# (USD 60bn of principal, mostly of 2018 to 2022, filed under France, Korea, the EU
# and others), so they stay in the table with a null `bi_multi`.
CRS_REFLOWS = _table(
    "reflows",
    ("record_id",),
    record_id=_str(),
    year=_year(),
    donor_code=_code(),
    agency_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(nullable=True),
    flow_code=_code(),
    finance_t=_code(nullable=True),
    received_usd=_usd(nullable=True),
    interest_usd=_usd(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
)

# One row per provider, channel, year and flow. The build writes every flow in FLOWS.
# MULTI publishes aggregate channels beside their members, such as 40000 for all
# multilaterals and 44000 for the World Bank Group. Normalise keeps them. The release
# keeps leaf channels, so each dollar counts once, plus a 40000 row for the part of
# a provider's 40000 total that no leaf itemises (`release.core_contributions`).
# The key within which a provider's leaf channels sum to its `TOP_CHANNEL` row.
CONTRIBUTION_KEY = ("provider_code", "year", "flow")
# The key that ties a CRS commitment to the CRDF row of the same commitment: the
# CRS commits money under it, and a CRDF row is backed where the CRS does.
COMMITMENT_KEY = ("provider_code", "crs_id", "year", "purpose_code")
# MULTI's total of a provider's core contributions, which sums every leaf channel
# it reports, 30010 included. The intermediate aggregates (41000, 47000 and the
# like) follow no code prefix, as MULTI files some 41xxx channels under 47000.
TOP_CHANNEL = 40000
CORE_CONTRIBUTIONS = _table(
    "core_contributions",
    ("provider_code", "channel_code", "year", "flow"),
    provider_code=_code(),
    channel_code=_code(),
    year=_year(),
    flow=_str(values=FLOWS),
    contribution_usd=_usd(),
)

# The OECD's imputed shares, under the same rule as `agency_shares`. A channel and
# year holds either split rows or one `climate` row. Split rows are inclusive:
# adaptation is the workbook's "adaptation only" plus "both", mitigation is
# "mitigation only" plus "both", and cross_cutting is "both".
IMPUTED_SHARES = _table(
    "imputed_shares",
    ("channel_code", "year", "objective"),
    channel_code=_code(),
    year=_year(),
    objective=_str(values=SHARE_OBJECTIVES),
    share=_f64(),
)

# Amounts are as the publisher reports them, in the original currency. `amount_usd`
# is filled by the reader for USD rows only. `cumulative_declared` marks activities
# whose files state that their disbursements are cumulative (GEF); the reader leaves
# those figures unchanged and the stage that reconciles them reads the flag.
EXTERNAL = _table(
    "external",
    (
        "source",
        "publisher",
        "activity_id",
        "year",
        "transaction_type",
        "currency",
        "recipient_iso",
    ),
    source=_str(values=("iati", "gcf")),
    publisher=_str(),
    activity_id=_str(),
    year=_year(),
    transaction_type=_str(values=("commitment", "disbursement")),
    currency=_str(),
    amount=_f64(),
    amount_usd=_usd(nullable=True),
    recipient_iso=_str(nullable=True),
    recipient_code=_code(nullable=True),
    cumulative_declared=_bool(),
    title=_str(nullable=True),
)

# Bank and bilateral ledgers, as the ledger readers return them: one row per
# transaction, or per World Bank loan and year. `key` is the id the CRS project number
# carries, so a ledger row and a CRS row join on it; `activity_id` is the id as
# published. Amounts are as published, in `currency`. The readers fill `amount_usd` for
# USD rows, and normalise converts the rest at DAC annual rates where the DAC publishes
# one for the currency. A World Bank row is the December-to-December flow of a loan,
# dated 31 December. A GEF disbursement is the rise of its running peak, since GEF
# declares its figures cumulative. `disbursed_to_date` is a GCF readiness grant's
# disbursed amount, which the list does not date, so its `date` and `year` are null and
# no calendar-year check can use it. `other_ids` lists the activity's other identifiers,
# which the bilateral id matching reads.
LEDGER = _table(
    "ledger",
    ("publisher", "key", "transaction_type", "date", "currency", "amount"),
    publisher=_str(),
    key=_str(),
    activity_id=_str(),
    transaction_type=_str(values=LEDGER_TRANSACTION_TYPES),
    date=Column(pl.Date(), nullable=True),
    year=_year(nullable=True),
    currency=_str(),
    amount=_f64(),
    amount_usd=_usd(nullable=True),
    title=_str(nullable=True),
    other_ids=Column(pl.List(pl.String())),
)

# `party` is the ISO 3 code; `provider_code` its DAC provider code, null for a party
# that reports no CRS. `amount_domestic` and `grant_equiv_domestic` are the face value
# and grant equivalent in `currency`, and `face_usd` and `grant_equiv_usd` the party's
# own conversion. `dac_rate` is the DAC annual rate of `currency` in the row's year,
# in `currency` per USD, the rate the CRS amounts use; null without one.
# `face_usd_at_dac` and `grant_equiv_usd_at_dac` restate the domestic amounts at it,
# and keep the party's USD where the domestic amount or the rate is missing.
BTR = _table(
    "btr",
    ("row_id",),
    row_id=_str(),
    party=_str(),
    provider_code=_code(nullable=True),
    year=_year(),
    recipient=_str(nullable=True),
    recipient_code=_code(nullable=True),
    title=_str(nullable=True),
    amount_domestic=_f64(nullable=True),
    grant_equiv_domestic=_f64(nullable=True),
    currency=_str(nullable=True),
    face_usd=_usd(nullable=True),
    grant_equiv_usd=_usd(nullable=True),
    dac_rate=_f64(nullable=True),
    face_usd_at_dac=_usd(nullable=True),
    grant_equiv_usd_at_dac=_usd(nullable=True),
    status=_str(nullable=True, values=("committed", "disbursed")),
    channel=_str(nullable=True),
    funding_source=_str(nullable=True),
    instrument=_str(nullable=True),
    support_type=_str(nullable=True),
    sector=_str(nullable=True),
    additional_information=_str(nullable=True),
    is_aggregate=_bool(),
    source_file=_str(),
)

# One row per BTR workbook the archive lists: the Table III.1 sheets read, the rows
# kept, the sheet rows dropped by reason, and the reporting tool's draft state in the
# file name.
BTR_WORKBOOKS = _table(
    "btr_workbooks",
    ("file",),
    file=_str(),
    party=_str(),
    sheets=Column(pl.List(pl.String())),
    rows_kept=_count(),
    dropped_blank=_count(),
    dropped_notation=_count(),
    dropped_total=_count(),
    draft_marker=_str(nullable=True, values=("started", "awaiting_submission")),
)

# One row per resolved organisation name and role. An agency name resolves within
# the donor it was printed under, so one name can hold a row per donor.
ORGS = _table(
    "orgs",
    ("role", "source_name", "donor_code"),
    role=_str(values=ORG_ROLES),
    source_name=_str(),
    donor_code=_code(nullable=True),
    agency_code=_code(nullable=True),
    channel_code=_code(nullable=True),
    method=_str(values=("exact_code", "exact_name", "pack", "propagated")),
)

# The comparison levels a scored pair carries, one Int8 level per field compared. The
# scoring stage fills them, and key links leave `levels` null.
COMPARISON_LEVELS = pl.Struct(
    {
        f"gamma_{name}": pl.Int8()
        for name in (
            "project_number",
            "text",
            "purpose",
            "recipient_code",
            "channel_code",
            "flow_code",
            "finance_type_code",
            "amount",
            "later_commitment",
            "year_gap",
        )
    }
)

LINKS = _table(
    "links",
    ("task", "left_id", "right_id"),
    task=_str(values=LINK_TASKS),
    left_id=_str(),
    right_id=_str(),
    group_id=_str(nullable=True),
    # On a part of a trace group that review decisions split (`decisions.drop_pairs`),
    # its rank at each split, outermost first; null on a group that is not split.
    split_path=Column(pl.List(pl.Int16()), nullable=True),
    # 1.0 on a certain rule link (`CERTAIN_METHODS`, `CERTAIN_ROUTES`, or a unit
    # linked to itself), the model's score on a scored link, null on any other.
    probability=_f64(nullable=True),
    link_method=_str(values=LINK_METHODS),
    p_model=_f64(nullable=True),
    p_jev=_f64(nullable=True),
    cosine=_f64(nullable=True),
    programme_level=Column(pl.Int8(), nullable=True),
    levels=Column(COMPARISON_LEVELS, nullable=True),
    model_version=_str(nullable=True),
    jev_model=_str(nullable=True),
    label_set=_str(nullable=True),
    ledger_check=_str(nullable=True, values=LEDGER_CHECKS),
    # On a trace link, whether its records disagree on recipient or project number
    # (`flag_disagreement`); null on other tasks.
    records_disagree=Column(pl.Boolean(), nullable=True),
    review=_bool(),
    review_reason=_str(nullable=True, values=REVIEW_REASONS),
)

# The CRS records the trace links, summed over records that share every key field:
# donor, agency, crs id, normalised project number, recipient, purpose, record year
# and commitment-date year. A unit with a positive commitment is a commitment; one
# with a positive disbursement is a disbursement record; one unit can be both. A
# commitment is placed at `placed_year`, its commitment-date year when that precedes
# the record year. A disbursement whose commitment date precedes its record year
# carries that year as `floor_year`, and fills only commitments placed from then on.
# `commitment_date` is the unit's largest record's.
UNITS = _table(
    "units",
    ("unit_id",),
    unit_id=_str(),
    donor_code=_code(),
    agency_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    year=_year(),
    commitment_year=_year(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
    placed_year=_year(),
    floor_year=_year(nullable=True),
    commitment_usd=_usd(),
    disbursement_usd=_usd(),
    record_ids=Column(pl.List(pl.String())),
)

# The `reflows` records summed as `UNITS` sums the trace's, under the same key, with
# `unit_id` salted with "reflow" so no unit of the trace shares one.
REFLOW_UNITS = _table(
    "reflow_units",
    ("unit_id",),
    unit_id=_str(),
    donor_code=_code(),
    agency_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    year=_year(),
    commitment_year=_year(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
    placed_year=_year(),
    floor_year=_year(nullable=True),
    received_usd=_usd(),
    interest_usd=_usd(),
    record_ids=Column(pl.List(pl.String())),
)

# Links from each reflow unit to every commitment unit its first matching key tier
# reaches, under the trace's rules (`keys.link_by_tiers`), or where no tier reaches
# one, to every disbursement unit its first matching tier reaches (`target`).
# `records_disagree` is as on a trace link (`flag_disagreement`), so a link is
# certain where its tier is in `CERTAIN_METHODS` and its records agree.
REFLOW_LINKS = _table(
    "reflow_links",
    ("reflow_unit_id", "unit_id"),
    reflow_unit_id=_str(),
    unit_id=_str(),
    target=_str(values=REFLOW_TARGETS),
    group_id=_str(),
    link_method=_str(values=KEY_TIERS),
    records_disagree=_bool(),
)

ID_PRACTICE = _table(
    "id_practice",
    ("donor_code", "agency_code", "period"),
    donor_code=_code(),
    agency_code=_code(),
    period=_str(values=("2000-2009", "2010-2014", "2015-2018", "2019-2022")),
    id_class=_str(values=ID_PRACTICE_CLASSES),
    open_commitments=_count(),
    id_share=_f64(nullable=True),
    parent_share=_f64(nullable=True),
    pn_share=_f64(nullable=True),
    keyed_disbursement_share=_f64(nullable=True),
)

# One rule per (donor, agency, project-number shape): strip `segments` trailing
# segments to reach the programme's number. `pairs` counts the pairs of distinct
# project numbers the rule merges, and `consistent_pairs` those whose titles agree.
PROGRAMME_STEMS = _table(
    "programme_stems",
    ("donor_code", "agency_code", "pattern"),
    donor_code=_code(),
    agency_code=_code(),
    pattern=_str(),
    segments=Column(pl.Int8(), values=(1, 2)),
    pairs=_count(),
    consistent_pairs=_count(),
)

# Unit pairs for the scoring stage: a disbursement unit (left) and a commitment unit a
# CRDF row reaches (right), of one donor, the disbursement of a later record year,
# under a blocking rule. `eligible` pairs are open to scoring: no key linked either
# unit and neither agency's ids persist. The other rows pair commitments of
# `id_persistent` agencies. `same_crs_id` says whether a pair's units share a raw
# crs id. The classifier trains on `score.id_truth`. `programme_level` is 2 for
# the same project number, 1 for siblings under one programme stem, else 0.
CANDIDATES = _table(
    "candidates",
    ("left_id", "right_id"),
    left_id=_str(),
    right_id=_str(),
    donor_code=_code(),
    year_gap=Column(pl.Int16()),
    programme_level=Column(pl.Int8(), values=(0, 1, 2)),
    same_crs_id=_bool(),
    id_persistent=_bool(),
    eligible=_bool(),
)

# Model scores for eligible candidate pairs. `model` names the model and its
# version, `match_weight` is its final weight and `p_raw` a Splink model's EM
# probability, which is evidence only. `probability` is calibrated, and null for a
# model that has no calibration labels yet. `label_set` counts the labels that
# calibrated it by source. `review_reason` is `calibration` on the pairs people
# should label to calibrate the model.
PAIR_SCORES = _table(
    "pair_scores",
    ("left_id", "right_id"),
    left_id=_str(),
    right_id=_str(),
    donor_code=_code(),
    model=_str(),
    match_weight=_f64(),
    p_raw=_f64(nullable=True),
    probability=_f64(nullable=True),
    levels=Column(COMPARISON_LEVELS),
    label_set=_str(nullable=True),
    review_reason=_str(nullable=True, values=REVIEW_REASONS),
)

# The id-less BTR rows drawn for people to label, so the BTR link's calibration
# covers the low match weights where few id rows sit, and, for a party without its
# own map, the weights from `link.btr.POOLED_FLOOR` up where its pooled links are
# checked (`link.btr.calibration_draw`):
# each row (`left_id`) with its best candidate CRS record (`right_id`), its party, the
# match weight its party's model gave the pair, and its place in its party's draw
# (`draw_rank`, from 1), which takes the strata in turn in the digest order of the
# pairs. `left_digest` and `right_digest` fingerprint what a labeller sees of the
# row and the record (`link.packet.BTR_SHOWN`, `CRS_SHOWN`). Empty without BTR rows.
BTR_CALIBRATION = _table(
    "btr_calibration",
    ("left_id", "right_id"),
    left_id=_str(),
    right_id=_str(),
    party=_str(),
    match_weight=_f64(),
    draw_rank=Column(pl.Int32()),
    left_digest=_str(),
    right_digest=_str(),
)

# One row per provider: the climate value of its counted CRDF commitments and, for a
# provider with a crosswalk (`link.crosswalks`), the part whose commitment a crosswalk
# link reaches, null for the others and for every provider when recoveries are off.
# The scoring columns (`link.score`) are null for a provider with no candidate pairs:
# `scoring_group` from the counts beside it, `true_pairs` counting true training
# pairs, `commitments` and `pairs` the commitments and pairs open to scoring, and for
# the truth group the isotonic map its pairs took (`calibration`), the classifier's
# link `threshold` (`link.score.LINK_THRESHOLD`), and the grid threshold of highest
# training F1 (`best_threshold`).
PROVIDER_PROFILE = _table(
    "provider_profile",
    ("provider_code",),
    provider_code=_code(),
    climate_usd=_usd(),
    crosswalk_usd=_usd(nullable=True),
    crosswalk_share=_f64(nullable=True),
    scoring_group=_str(nullable=True, values=SCORING_GROUPS),
    true_pairs=Column(pl.Int32(), nullable=True),
    commitments=Column(pl.Int32(), nullable=True),
    pairs=Column(pl.Int32(), nullable=True),
    calibration=_str(nullable=True, values=("own", "pooled")),
    threshold=_f64(nullable=True),
    best_threshold=_f64(nullable=True),
)

# A CRS record's disbursement at face value, split over CRDF commitments (`row_id`).
# A null `row_id` holds the part that reached CRS commitments outside the CRDF, or,
# under rule `excess`, the part that reached no commitment, so a linked record's
# rows sum to its disbursement. Bounds as in `disbursements`.
ALLOCATIONS = _table(
    "allocations",
    ("row_id", "record_id", "year", "rule"),
    row_id=_str(nullable=True),
    record_id=_str(),
    year=_year(),
    allocated_usd=_usd(),
    allocated_low_usd=_usd(),
    rule=_str(values=ALLOCATION_RULES),
    disbursement_source=_str(values=DISBURSEMENT_SOURCES),
    # The trace link that carried the allocation, from the record's unit to
    # `commitment_unit_id` under `link_method`. Where several carried parts of it,
    # the one of allocation's first pass that carried the most. Null under rule
    # `excess` and on fills.
    link_method=_str(nullable=True, values=LINK_METHODS),
    commitment_unit_id=_str(nullable=True),
)

# Status uses the linked CRS commitment's face. disbursed_usd is in the dollars
# disbursed and can sit below face for a disbursed commitment when exchange rates
# move.
COMMITMENT_STATUS = _table(
    "commitments",
    ("row_id",),
    row_id=_str(),
    status=_str(values=STATUSES),
    face_usd=_usd(),
    climate_usd=_usd(),
    disbursed_usd=_usd(),
    disbursed_low_usd=_usd(),
    project_disbursing=_bool(),
    # Status inputs that allocation fixes, so a later stage can recompute status:
    # whether the row reaches a CRS commitment through an agency whose ids persist,
    # whether allocation gives its id's disbursement to earlier commitments, and
    # the row's share of its linked CRS commitment's face, which status reads the
    # row's allocations against. The release table leaves crs_face_usd out.
    traceable=_bool(),
    id_disbursing=_bool(),
    crs_face_usd=_usd(),
)

CANCELLATIONS = _table(
    "cancellations",
    ("provider_code", "year"),
    provider_code=_code(),
    year=_year(),
    cancelled_usd=_usd(),
)

YEARS_SINCE_COMMITMENT = _table(
    "years_since_commitment",
    ("row_id", "years_since"),
    row_id=_str(),
    years_since=_year(),
    cumulative_disbursed_usd=_usd(),
)

BANK_YEAR_OVERLAP = _table(
    "bank_year_overlap",
    ("provider_code", "year"),
    provider_code=_code(),
    year=_year(),
    keys=_count(),
    ledger_usd=_usd(),
    crs_usd=_usd(),
    share_keys_within_10pct=_f64(),
    unkeyed_share=_f64(),
    passed=_bool(),
)

REVIEW_QUEUE = _table(
    "review_queue",
    ("task", "left_id", "right_id"),
    task=_str(values=LINK_TASKS),
    left_id=_str(),
    right_id=_str(nullable=True),
    probability=_f64(nullable=True),
    p_model=_f64(nullable=True),
    p_jev=_f64(nullable=True),
    review_reason=_str(values=REVIEW_REASONS),
    field_weights=_str(nullable=True),
    jev_answer=_str(nullable=True),
    decision=_str(nullable=True, values=DECISIONS),
    reviewer=_str(nullable=True),
    note=_str(nullable=True),
)

PAIR_LABELS = _table(
    "pair_labels",
    ("task", "left_id", "right_id", "pair_label_source"),
    task=_str(values=LINK_TASKS),
    left_id=_str(),
    right_id=_str(),
    is_match=_bool(),
    pair_label_source=_str(values=PAIR_LABEL_SOURCES),
)

# Decisions on queued pairs and blind-sample pairs, appended over time. A decision
# carries either the queue's `review_reason` or the blind sample's `sample_id`.
# Queue decisions become pair labels and link overrides, and blind-sample decisions
# only measure precision. `label_source` says whether a person or an agent decided.
# An agent's decision names its `model`, its `confidence` that the pair matches, and
# its labelling `round`. `reviewer` names the person or the agent run, and `note`
# holds the stated reason. A `btr_crs` decision carries the fingerprints of the BTR
# row and the CRS record its labeller saw (`left_digest`, `right_digest`), since a BTR
# row id names a position in a workbook; the build reuses it only while both still
# match. Other decisions, and rows written before the columns existed, hold none.
REVIEW_DECISIONS = _table(
    "review_decisions",
    ("task", "left_id", "right_id", "decided_at"),
    task=_str(values=LINK_TASKS),
    left_id=_str(),
    right_id=_str(nullable=True),
    decision=_str(values=DECISIONS),
    label_source=_str(values=DECIDERS),
    reviewer=_str(),
    model=_str(nullable=True),
    confidence=_f64(nullable=True),
    round=_str(nullable=True, values=LABEL_ROUNDS),
    note=_str(nullable=True),
    review_reason=_str(nullable=True, values=REVIEW_REASONS),
    sample_id=_str(nullable=True),
    decided_at=Column(pl.Datetime("us", "UTC")),
    left_digest=_str(nullable=True),
    right_digest=_str(nullable=True),
)

OBJECTIVE_LABELS = _table(
    "objectives",
    ("record_id", "objective", "label_source"),
    record_id=_str(),
    objective=_str(values=OBJECTIVES),
    level=Column(pl.Int8(), values=RIO_MARKERS),
    label_source=_str(values=LABEL_SOURCES),
    probability=_f64(nullable=True),
)

# Shares of the agency's spending by objective that hold under every method's
# coefficients: the OECD's imputed shares (`oecd`), and under `one` the OECD's
# imputed share for a channel and year that `agency_spending` does not measure, with
# `share_source` `oecd`. The adaptation and mitigation shares each include the
# cross_cutting share, as `adaptation_usd`, `mitigation_usd` and `overlap_usd` do,
# so the climate share is adaptation + mitigation - cross_cutting. A channel, year
# and source holds either those split rows or one `climate` row, the total climate
# share with no split by objective.
AGENCY_SHARES = _table(
    "agency_shares",
    ("channel_code", "year", "objective", "source"),
    channel_code=_code(),
    year=_year(),
    objective=_str(values=SHARE_OBJECTIVES),
    share=_f64(),
    source=_str(values=SHARE_SOURCES),
)

# Each agency's CRDF commitments over a share year's span, as fractions of its CRS
# commitments over the span, from which the query weighs the agency's climate share
# under any method's coefficients. A row with markers holds the face value of the
# commitments at those markers and at one `published` share, the climate share of
# face value the provider published, which caps each marker weight as IFAD's
# significant rows at 0.4 do; its component shares are null. The row with null
# markers and a null `published` holds the face value and climate components of the
# commitments that report components, adaptation and mitigation each including the
# overlap. A measured share year whose span holds no CRDF commitment holds one row at
# markers 0 and 0 with no face value and a null `published`, so its share is 0.
# `share_source` is `computed` on a share year measured on enough CRS commitments and
# `held` on a later year that rests on too few, which repeats the rows of the last
# computed year (`measure.agency_spending`). A row is not `adaptation_measured` where
# its span reaches a year the CRDF does not measure adaptation in, and the agency's
# adaptation and cross-cutting shares are then null.
AGENCY_SPENDING = _table(
    "agency_spending",
    ("channel_code", "year", "rio_adaptation", "rio_mitigation", "published"),
    channel_code=_code(),
    year=_year(),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    published=_f64(nullable=True),
    face_share=_f64(),
    adaptation_share=_f64(nullable=True),
    mitigation_share=_f64(nullable=True),
    overlap_share=_f64(nullable=True),
    adaptation_measured=_bool(),
    share_source=_str(values=SPENDING_ORIGINS),
)

MODELLED = _table(
    "modelled",
    ("provider_code", "year", "recipient_code"),
    provider_code=_code(),
    year=_year(),
    recipient_code=_code(nullable=True),
    low_usd=_usd(),
    high_usd=_usd(),
    basis=_str(),
)

# Release tables ------------------------------------------------------------------

# Recipient-perspective CRDF rows only: bilateral flows and multilateral outflows.
COMMITMENTS = _table(
    "commitments",
    ("row_id",),
    row_id=_str(),
    vintage=_year(),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    purpose_code=_code(nullable=True),
    # The CRS flow category the CRDF publishes: 10 ODA, 21 other official flows,
    # 22, 30 private grants and 60 private market flows. A disbursement carries its
    # CRS record's flow code (11 ODA grants, 13 ODA loans, 19 equity) instead, and a
    # fill, which has no CRS record, a null one.
    flow_category=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    climate_share=_f64(nullable=True),
    face_usd=_usd(),
    climate_usd=_usd(),
    # Adaptation and mitigation each include the overlap, so climate is a + m - overlap.
    # Adaptation and the overlap are null where the CRDF does not measure adaptation.
    adaptation_usd=_usd(nullable=True),
    mitigation_usd=_usd(),
    overlap_usd=_usd(nullable=True),
    status=_str(values=STATUSES),
    # Face value, as CRS records carry whole-project disbursements and FIFO caps at
    # face. The query applies the climate share. Sums of the commitment's
    # `disbursements` rows: allocated and allocated_low, with
    # 0 <= disbursed_low <= disbursed <= face. A negative face value (a published
    # downward revision) stays as published, with nothing disbursed.
    disbursed_usd=_usd(),
    disbursed_low_usd=_usd(),
    # The part of each disbursed amount from fill records the release leaves out
    # (`WITHHELD_RECORD_PREFIXES`), which no `disbursements` row carries. A fill
    # counts in full in both bounds, so it is the same part of both.
    withheld_usd=_usd(),
    project_disbursing=_bool(),
    # True where the bank's ledger (`ledger.UNDISBURSED_PUBLISHERS`) confirms no
    # disbursement, null where no ledger covers the row or it shows money moving.
    # It stands whatever `status` says, so a row the allocation calls disbursed and
    # the ledger does not points at a link to check.
    ledger_undisbursed=_bool(nullable=True),
    repeat_of=_str(nullable=True),
    # The published CRDF aggregate row this child was rebuilt from. The release
    # leaves such parents out, so the id names a CRDF row and no release row.
    parent_row_id=_str(nullable=True),
)

DISBURSEMENTS = _table(
    "disbursements",
    ("row_id", "record_id", "year"),
    row_id=_str(),
    record_id=_str(),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    purpose_code=_code(nullable=True),
    flow_code=_code(nullable=True),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    # Face value of the record's disbursement allocated to `row_id`, with the
    # allocation's low bound: 0 <= allocated_low <= allocated. The high bound is
    # allocated itself. allocated_low holds what certain trace links carried
    # (`CERTAIN_METHODS`, or one record that commits and disburses) to rows reached
    # through certain CRDF-to-CRS routes (`CERTAIN_ROUTES`). A bank-ledger or IATI
    # fill counts in full in both bounds, since it matches the commitment on its
    # publisher's own id: the bank's ledger key or the fund's project id.
    allocated_usd=_usd(),
    allocated_low_usd=_usd(),
    # The CRS record's own disbursement at face value and grant equivalent, the same
    # on every row of a record. The query takes an allocation's grant equivalent as
    # record_grant_equiv_usd * allocated_usd / record_disbursement_usd, so a record
    # split across commitments counts once. A record's allocated_usd sums to at most
    # record_disbursement_usd; the rest is excess that reaches no commitment. A
    # grant equivalent is never negative. A grant without a CRS grant equivalent
    # holds its disbursement (`normalise_crs`), so null stands for a loan or other
    # non-grant record the CRS gives none, and for a fill, which has no CRS record.
    record_disbursement_usd=_usd(),
    record_grant_equiv_usd=_usd(nullable=True, minimum=0.0),
    inherited_rio_adaptation=_rio(),
    inherited_rio_mitigation=_rio(),
    record_rio_adaptation=_rio(),
    record_rio_mitigation=_rio(),
    marker_source=_str(values=MARKER_SOURCES),
    climate_share=_f64(nullable=True),
    link_method=_str(nullable=True, values=LINK_METHODS),
    link_probability=_f64(nullable=True),
    rule=_str(values=ALLOCATION_RULES),
    disbursement_source=_str(values=DISBURSEMENT_SOURCES),
    ledger_check=_str(nullable=True, values=LEDGER_CHECKS),
)

DISBURSEMENT_PROFILE = _table(
    "disbursement_profile",
    ("group", "years_since"),
    group=_str(),
    years_since=_year(),
    cumulative_share=_f64(),
    vintages=Column(pl.Int16()),
)

# Every amount is climate value of counted commitments: all of them (`climate_usd`),
# those that trace to at least one CRS record (`linked_usd`), the linked ones whose
# CRDF rows reach their CRS commitment through `CERTAIN_ROUTES` alone
# (`certain_route_usd`), and those labelled by a model or by a report.
# `disbursed_climate_usd` and `certain_disbursed_climate_usd` are the climate value
# of their disbursements in the high and low bounds (`allocated_usd`,
# `allocated_low_usd`), at each commitment's climate share of face.
# `grant_equiv_coverage` is the part of the linked commitments' allocated value
# (`allocated_usd`) whose record has a grant equivalent, null without allocations.
# `repeat_climate_usd` is the climate value of rows that repeat an earlier vintage,
# which the counted amounts leave out.
QUALITY = _table(
    "quality",
    ("provider_code", "year"),
    provider_code=_code(),
    year=_year(),
    commitments=_count(),
    climate_usd=_usd(),
    linked_usd=_usd(),
    certain_route_usd=_usd(),
    disbursed_climate_usd=_usd(),
    certain_disbursed_climate_usd=_usd(),
    model_labelled_usd=_usd(),
    report_labelled_usd=_usd(),
    repeat_climate_usd=_usd(),
    grant_equiv_coverage=_f64(nullable=True),
)

# The cuts on Jev's answers at which a `jev_flags` record is flagged. Measured on
# hand-labelled texts of the climate-marked CRS, `no_aim` has precision 1.00 and
# recall 0.93 on 200 stratified texts, and `surprising` 0.83 and 0.60 on 240.
NO_AIM_CUT = 0.7
SURPRISING_CUT = 0.8
JEV_FLAG_PROBABILITIES = (
    "no_aim_p",
    "adaptation_aim_p",
    "mitigation_aim_p",
    "surprising_p",
    "counterproductive_p",
)

# Jev's answers about each CRS record of the flagged years with a Rio mitigation or
# adaptation marker of 1 or 2 and a positive commitment or disbursement, with the
# record's own facts. `no_aim_p` is 1 less the larger of the two aim answers, and
# `no_aim` and `surprising` hold where `no_aim_p` and `surprising_p` reach
# `NO_AIM_CUT` and `SURPRISING_CUT`. `counterproductive_p` ranks records and has no
# cut. A record whose text is too short to ask (`short_text`) has null answers. The
# table changes no total.
JEV_FLAGS = _table(
    "jev_flags",
    ("record_id",),
    record_id=_str(),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    flow_code=_code(),
    project_number=_str(nullable=True),
    crs_id=_str(nullable=True),
    title=_str(nullable=True),
    commitment_usd=_usd(nullable=True),
    disbursement_usd=_usd(nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    short_text=_bool(),
    **dict.fromkeys(JEV_FLAG_PROBABILITIES, _f64(nullable=True, minimum=0.0)),
    no_aim=_bool(nullable=True),
    surprising=_bool(nullable=True),
    model=_str(),
)

# Rio-marked CRS disbursement that reaches no commitment in the release, one row
# per CRS record, at face value. `untraced_usd` is a linked record's disbursement
# beyond what its commitments take (`reason` `excess`), or the whole disbursement of a
# record linked to no commitment (`unlinked`). The other columns are the record's
# own, as on `disbursements`, so the query weighs the row by the record's own Rio
# markers. The adaptation marker is null before `ADAPTATION_FIRST_YEAR`, as the
# CRDF's is.
UNTRACED = _table(
    "untraced",
    ("record_id",),
    record_id=_str(),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    purpose_code=_code(nullable=True),
    flow_code=_code(nullable=True),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    untraced_usd=_usd(),
    reason=_str(values=UNTRACED_REASONS),
    record_disbursement_usd=_usd(),
    record_grant_equiv_usd=_usd(nullable=True, minimum=0.0),
)

# Rio-marked CRS disbursement by provider and disbursement year, at face value: all
# of it (`marked_usd`), and its parts outside `disbursements`: a linked record's
# disbursement beyond what its commitments take (`excess_usd`), disbursements to
# commitments with no CRDF row in the release (`outside_release_usd`), records
# linked to no commitment (`unlinked_marked_usd`), and the unlinked records of the
# GCF and the GEF whose money the funds' IATI route counts (`fund_unlinked_usd`, 0
# with `gap_fill` off). Excess and unlinked count in the totals through `untraced`,
# and the release leaves the other two out. The four parts and the `disbursements`
# rows of Rio-marked CRS records sum to `marked_usd` (`release_invariants`).
UNALLOCATED = _table(
    "unallocated",
    ("provider_code", "year"),
    provider_code=_code(),
    year=_year(),
    marked_usd=_usd(),
    excess_usd=_usd(),
    outside_release_usd=_usd(),
    unlinked_marked_usd=_usd(),
    fund_unlinked_usd=_usd(),
)

# Each CRS repayment split over what its loan's disbursements reached, one row per
# record and `basis` (`REFLOW_BASES`), and per CRDF row (`row_id`) for basis
# `commitment` or pair of the disbursement records' own markers for `untraced`. A
# loan is the reflow link's key group (`loan_id`), and its disbursement
# (`loan_disbursement_usd`) the allocations of every CRS record of every
# disbursement unit traced to a commitment unit the loan's reflows reach. A part's
# share of the repayment is its share of that disbursement. Principal attributed
# per loan stops at the loan's disbursement, filled by year then record id, and the
# rest is `beyond_disbursed`. A record's rows sum to `record_received_usd` and
# `record_interest_usd`. The low amounts hold the parts a certain reflow link
# carried, and on `commitment` rows only what certain allocations carried, so
# 0 <= received_low <= received. `inherited_*` and `climate_share` weight the row:
# the commitment's, or for `untraced` the disbursement records' own markers. The
# record's own markers (`record_rio_*`) are for display. A record with no Bi_Multi code
# is in no view, so it links to no loan and its rows are all `unlinked`.
REFLOWS = _table(
    "reflows",
    (
        "record_id",
        "basis",
        "row_id",
        "inherited_rio_adaptation",
        "inherited_rio_mitigation",
    ),
    record_id=_str(),
    year=_year(),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(nullable=True),
    purpose_code=_code(nullable=True),
    flow_code=_code(),
    finance_t=_code(nullable=True),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    row_id=_str(nullable=True),
    basis=_str(values=REFLOW_BASES),
    received_usd=_usd(),
    received_low_usd=_usd(),
    interest_usd=_usd(),
    interest_low_usd=_usd(),
    record_received_usd=_usd(),
    record_interest_usd=_usd(),
    loan_id=_str(nullable=True),
    loan_disbursement_usd=_usd(nullable=True),
    inherited_rio_adaptation=_rio(),
    inherited_rio_mitigation=_rio(),
    climate_share=_f64(nullable=True),
    record_rio_adaptation=_rio(),
    record_rio_mitigation=_rio(),
    link_method=_str(nullable=True, values=KEY_TIERS),
)

# Every CRS flow, climate or not, summed. This is the denominator of `share_of_total`.
# `agency_code` lets a query take out the EIB's flows where the EIB counts as a
# channel. `rio_adaptation` and `rio_mitigation` are a record's Rio markers, 0 for
# none, so the BTR reconciliation can weight same-year climate disbursements as
# `weights` weights commitments.
TOTAL_FLOWS = _table(
    "total_flows",
    (
        "provider_code",
        "agency_code",
        "recipient_code",
        "year",
        "bi_multi",
        "flow",
        "rio_adaptation",
        "rio_mitigation",
    ),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    recipient_code=_code(),
    year=_year(),
    bi_multi=_bi_multi(),
    flow=_str(values=FLOWS),
    rio_adaptation=Column(pl.Int8(), values=RIO_MARKERS),
    rio_mitigation=Column(pl.Int8(), values=RIO_MARKERS),
    amount_usd=_usd(),
)

# Each BTR row's links to same-year CRS records of its party (`links` task
# `btr_crs`), with the CRS record's facts, since the release holds no CRS records.
# `rio_adaptation` and `rio_mitigation` are the record's Rio markers, 0 for none.
BTR_LINKS = _table(
    "btr_links",
    ("row_id", "record_id"),
    row_id=_str(),
    record_id=_str(),
    link_method=_str(values=("btr_id", "btr_splink")),
    probability=_f64(),
    provider_code=_code(),
    year=_year(),
    recipient_code=_code(),
    project_number=_str(nullable=True),
    title=_str(nullable=True),
    commitment_usd=_usd(nullable=True),
    disbursement_usd=_usd(nullable=True),
    rio_adaptation=Column(pl.Int8(), values=RIO_MARKERS),
    rio_mitigation=Column(pl.Int8(), values=RIO_MARKERS),
)

# Each party's BTR rows summed by year, status, funding class, currency and whether a
# CRS record links them (`btr_links`): the reconciliation's BTR side in a release
# that leaves out `btr` and `btr_links`. `funding` is `oda` or `oof` where the row's
# funding source names only that flow, and `other` otherwise. `rows` counts the rows;
# an amount is null where every row of the group lacks it. `dac_rate` is the rows'
# DAC rate, one per party and year.
BTR_TOTALS = _table(
    "btr_totals",
    ("party", "year", "status", "funding", "currency", "linked"),
    party=_str(),
    provider_code=_code(nullable=True),
    year=_year(),
    status=_str(nullable=True, values=("committed", "disbursed")),
    funding=_str(values=FUNDING_CLASSES),
    currency=_str(nullable=True),
    linked=_bool(),
    rows=_count(),
    face_usd=_usd(nullable=True),
    grant_equiv_usd=_usd(nullable=True),
    amount_domestic=_f64(nullable=True),
    face_usd_at_dac=_usd(nullable=True),
    grant_equiv_usd_at_dac=_usd(nullable=True),
    dac_rate=_f64(nullable=True),
)

# For each BTR party's DAC provider and each year its BTR rows cover, the bilateral
# CRS amount of each flow on climate-marked records (higher Rio marker 1 or 2) that
# no `btr_crs` link reaches: the reconciliation's OECD-only side, as the release
# holds no CRS records. Zero where every such record is linked, and null where the
# build's CRS holds no record of the provider and year, as for a year past its last.
BTR_CRS_UNLINKED = _table(
    "btr_crs_unlinked",
    ("provider_code", "year", "flow"),
    provider_code=_code(),
    year=_year(),
    flow=_str(values=("commitments", "disbursements")),
    amount_usd=_usd(nullable=True),
)

# The coefficients each provider applies to Rio-marked activities when it reports to
# the UNFCCC, as the document `source` states them at `locator` (a page, note or
# paragraph). A row covers `first_year` to `last_year`, null for no bound. It covers
# the agency `agency_code`, or with a null agency every agency of the provider without
# a row of its own. `both_significant` is the coefficient for an activity significant
# for adaptation and mitigation alike, null where it is `significant`. A provider that
# assesses each activity's climate share (`case_by_case`) has null coefficients, or
# the default its source gives for an activity whose share is unknown.
PROVIDER_COEFFICIENTS = _table(
    "provider_coefficients",
    ("provider_code", "agency_code", "first_year"),
    provider_code=_code(),
    agency_code=_code(nullable=True),
    first_year=_year(nullable=True),
    last_year=_year(nullable=True),
    significant=_f64(nullable=True),
    principal=_f64(nullable=True),
    both_significant=_f64(nullable=True),
    case_by_case=_bool(),
    source=_str(),
    locator=_str(),
)


# The multipliers from US dollars at current prices to each currency a query takes,
# by DAC provider and year, as pydeflate gives them from the OECD DAC's exchange
# rates and deflators when fetch runs. A row with a null `base_year` holds the
# exchange rate for current prices. A row with a base year holds the inverse of the
# DAC deflator for constant prices of that year. `entity_code` 20001 holds the DAC
# countries' total, which a provider without its own series takes. USD at current
# prices has no rows, as it needs no factor.
CONVERSION_FACTORS = _table(
    "conversion_factors",
    ("currency", "base_year", "entity_code", "year"),
    currency=_str(values=CURRENCIES),
    base_year=_year(nullable=True),
    entity_code=_code(),
    year=_year(),
    factor=_f64(),
)


def _by_name(*tables: TableSchema) -> Mapping[str, TableSchema]:
    return {table.name: table for table in tables}


RELEASE_TABLES = _by_name(
    COMMITMENTS,
    DISBURSEMENTS,
    OBJECTIVE_LABELS,
    AGENCY_SHARES,
    AGENCY_SPENDING,
    MODELLED,
    CORE_CONTRIBUTIONS,
    BTR,
    BTR_LINKS,
    BTR_TOTALS,
    BTR_CRS_UNLINKED,
    DISBURSEMENT_PROFILE,
    QUALITY,
    UNTRACED,
    UNALLOCATED,
    REFLOWS,
    TOTAL_FLOWS,
    PROVIDER_COEFFICIENTS,
    JEV_FLAGS,
    CONVERSION_FACTORS,
)

# The data format of the releases this package writes, and the newest it reads.
# It goes up by one when a release table or a `release.json` field is added,
# removed or changes meaning, independent of the package version.
DATA_FORMAT = 1
# The release tables each data format holds, by format number. Each format is a
# literal list, so adding a table to `RELEASE_TABLES` leaves older formats readable.
FORMAT_TABLES: Mapping[int, frozenset[str]] = {
    1: frozenset(
        {
            "commitments",
            "disbursements",
            "objectives",
            "agency_shares",
            "agency_spending",
            "modelled",
            "core_contributions",
            "btr",
            "btr_links",
            "btr_totals",
            "btr_crs_unlinked",
            "disbursement_profile",
            "quality",
            "untraced",
            "unallocated",
            "reflows",
            "total_flows",
            "provider_coefficients",
            "jev_flags",
            "conversion_factors",
        }
    )
}

STAGE_TABLES: Mapping[str, Mapping[str, TableSchema]] = {
    "fetch": _by_name(RAW_MANIFEST),
    "normalise": _by_name(
        CRDF,
        CRDF_SPAN,
        CRS,
        CRS_REFLOWS,
        CORE_CONTRIBUTIONS,
        IMPUTED_SHARES,
        EXTERNAL,
        BTR,
        BTR_WORKBOOKS,
        ORGS,
        LEDGER,
    ),
    "link": _by_name(
        LINKS,
        UNITS,
        REFLOW_UNITS,
        REFLOW_LINKS,
        ID_PRACTICE,
        PROGRAMME_STEMS,
        CANDIDATES,
        PAIR_SCORES,
        BTR_CALIBRATION,
        PROVIDER_PROFILE,
    ),
    "allocate": _by_name(
        ALLOCATIONS, COMMITMENT_STATUS, CANCELLATIONS, YEARS_SINCE_COMMITMENT
    ),
    "fill": _by_name(ALLOCATIONS, COMMITMENT_STATUS, BANK_YEAR_OVERLAP),
    "review": _by_name(LINKS, REVIEW_QUEUE, PAIR_LABELS, COMMITMENT_STATUS),
    "label": _by_name(OBJECTIVE_LABELS),
    "flag": _by_name(JEV_FLAGS),
    "measure": _by_name(AGENCY_SHARES, AGENCY_SPENDING, MODELLED),
    "assemble": RELEASE_TABLES,
}
# Tables a stage may leave out. A stage that reads one that was not written raises
# StageInputMissingError. `orgs` is written when recoveries are on, `btr` and
# `btr_workbooks` when the build config names a BTR archive, `ledger` when it
# names ledger sources.
# `bank_year_overlap` is written by the bank-ledger gap fill, `review_queue` by the
# review queue and `pair_labels` by the ledger check. With their switches off the two
# stages pass the tables they carry forward through unchanged. `jev_flags` is written
# when `jev_flags` is on.
OPTIONAL_STAGE_TABLES: Mapping[str, frozenset[str]] = {
    "normalise": frozenset({"btr", "btr_workbooks", "orgs", "ledger"}),
    "fill": frozenset({"bank_year_overlap"}),
    "review": frozenset({"review_queue", "pair_labels"}),
    "flag": frozenset({"jev_flags"}),
}


def _dtype_matches(actual: pl.DataType, expected: DType) -> bool:
    if isinstance(expected, type):
        return actual.base_type() is expected
    return actual == expected


def validate(table: TableSchema, frame: pl.DataFrame) -> None:
    """Check `frame` against `table`.

    Float columns hold finite values or null: a missing amount is null, so NaN and
    infinity are refused.

    Raises:
        SchemaError: naming every missing, undeclared, mistyped, null-holding,
            non-finite, below-minimum or out-of-vocabulary column.
    """
    problems: dict[str, str] = {
        name: "not in schema" for name in frame.columns if name not in table.columns
    }
    for name, column in table.columns.items():
        if name not in frame.columns:
            problems[name] = "missing"
            continue
        series = frame.get_column(name)
        if not _dtype_matches(series.dtype, column.dtype):
            problems[name] = f"dtype {series.dtype}, expected {column.dtype}"
            continue
        if not column.nullable and (nulls := series.null_count()):
            problems[name] = f"{nulls} nulls in a non-nullable column"
            continue
        if series.dtype.is_float() and (infinite := (~series.is_finite()).sum()):
            problems[name] = f"{infinite} NaN or infinite values"
            continue
        if column.minimum is not None and (low := (series < column.minimum).sum()):
            problems[name] = f"{low} values below {column.minimum}"
            continue
        if column.values is not None:
            allowed = pl.Series(column.values, dtype=series.dtype).implode()
            present = series.drop_nulls()
            unexpected = present.filter(~present.is_in(allowed))
            if len(unexpected):
                shown = sorted(unexpected.unique().to_list())[:5]
                problems[name] = f"values outside the vocabulary: {shown}"
    if problems:
        raise SchemaError(table.name, problems)


# Release invariants -------------------------------------------------------------

# Absolute tolerance, in USD, for the amount checks in `release_invariants`.
INVARIANT_TOLERANCE_USD = 1e-6
# The relative difference `btr_totals` may show from the `btr` rows it sums, as the
# two add the same amounts in different orders.
BTR_TOTALS_TOLERANCE = 1e-9
# Relative tolerance for a provider-year total of `unallocated.marked_usd`, where
# INVARIANT_TOLERANCE_USD is smaller. Its parts are sums of the allocations a
# record's disbursement splits into, so rounding parts them from the total by up to
# 1.1e-15 of it, 5e-6 USD on EU Institutions' 4.8bn of 2024 in an archive build.
INVARIANT_RELATIVE_TOLERANCE = 1e-12
# "Developing countries, unspecified": 998 in the CRDF and the DAC recipient list,
# 9998 in the bulk CRS. Release tables carry 998 only.
UNSPECIFIED_RECIPIENT = 998
CRS_UNSPECIFIED_RECIPIENT = 9998
# EU Institutions as a DAC provider, and the EIB, which reports as its agency 3.
EU_INSTITUTIONS = 918
EIB_AGENCY = 3
# DAC exchange rates are by country, and the euro's is held by EU Institutions.
EURO_ISSUER = "EUI"

# Pairs (smaller, larger) of amounts that must hold smaller <= larger. "0" is zero.
_ALLOCATION_ORDER = (
    ("0", "allocated_low_usd"),
    ("allocated_low_usd", "allocated_usd"),
    ("0", "record_grant_equiv_usd"),
)
_DISBURSED_ORDER = (
    ("0", "disbursed_low_usd"),
    ("disbursed_low_usd", "disbursed_usd"),
)
# Columns that hold one value across all rows of a CRS record.
_RECORD_COLUMNS = ("record_disbursement_usd", "record_grant_equiv_usd")
_DISBURSED_SUMS = {
    "disbursed_usd": "allocated_usd",
    "disbursed_low_usd": "allocated_low_usd",
}
# The amounts the checks compare, by table. A null among them fails no comparison,
# so each null is a problem of its own.
_COMPARED_AMOUNTS = {
    "commitments": ("face_usd", *_DISBURSED_SUMS, "withheld_usd"),
    "disbursements": (*_DISBURSED_SUMS.values(), "record_disbursement_usd"),
    "untraced": ("untraced_usd", "record_disbursement_usd"),
}


def withheld_record(record_id: pl.Expr) -> pl.Expr:
    """True where `record_id` names a fill record the release leaves out
    (`WITHHELD_RECORD_PREFIXES`)."""
    return pl.any_horizontal(
        record_id.str.starts_with(prefix) for prefix in WITHHELD_RECORD_PREFIXES
    ).fill_null(False)


def measured_adaptation(marker: pl.Expr) -> pl.Expr:
    """`marker`, an adaptation marker of a row with a `year`, null before
    `ADAPTATION_FIRST_YEAR`, as no CRDF or CRS row measures adaptation earlier."""
    return pl.when(pl.col("year") >= ADAPTATION_FIRST_YEAR).then(marker)


def aggregate_channel(code: pl.Expr) -> pl.Expr:
    """Whether a DAC channel code aggregates others: X0000 holds every code X????,
    and XY000 every code XY???, as 44000 holds the World Bank Group's 44001."""
    return code % 1_000 == 0


def _exceeds(smaller: str, larger: str) -> pl.Expr:
    """True where `smaller` is above `larger` by more than the tolerance."""

    def amount(name: str) -> pl.Expr:
        return pl.lit(0.0) if name == "0" else pl.col(name)

    return amount(smaller) - amount(larger) > INVARIANT_TOLERANCE_USD


# The commitment facts a fill copies, since it has no CRS record of its own.
_FILL_FACTS = (
    "provider_code",
    "agency_code",
    "recipient_code",
    "channel_code",
    "bi_multi",
    "purpose_code",
    "project_number",
)


def release_invariants(tables: Mapping[str, pl.DataFrame]) -> list[str]:
    """Problems with a release's `commitments`, `disbursements`, `untraced`,
    `unallocated`, `agency_shares`, `agency_spending`, `core_contributions`,
    `provider_coefficients` and BTR tables.

    Every amount is finite or null. The other checks skip a non-finite amount, and
    the sum, record and untraced checks skip the commitment and the record whose
    row holds one, and the `unallocated` total its provider and year, so it is
    reported once, as non-finite. Every disbursements row holds
    0 <= allocated_low <= allocated and 0 <= record_grant_equiv, and names a
    commitment. Per commitment, its rows' allocated and allocated_low, each plus
    `withheld_usd`, sum to disbursed and disbursed_low, and
    0 <= disbursed_low <= disbursed <= face,
    where a face of zero or below (a published downward revision) allows nothing
    disbursed. A record's rows share one `record_disbursement_usd` and one
    `record_grant_equiv_usd` (all null or all set), and its allocated amounts sum
    to at most `record_disbursement_usd`. Amounts compare within
    `INVARIANT_TOLERANCE_USD`, and none of them is null. Every disbursements row
    from the CRS names the link that carried it, and every fill (a bank-ledger or
    IATI row) carries its commitment's provider, agency, recipient, channel,
    Bi_Multi, purpose and project number. No disbursements row names a commitment
    that repeats an earlier vintage, as allocate moves a repeat's allocations to
    its first row. No commitment is another's `parent_row_id`. No table in
    `tables` holds a row of a fill record the release leaves out
    (`withheld_record`) or a row with the bulk CRS's `CRS_UNSPECIFIED_RECIPIENT`.
    A channel, year and source of `agency_shares` holds either split rows or one
    `climate` row. Each `agency_spending` row holds both markers and no component
    shares, or neither marker and all three component shares, a channel, year,
    pair of markers and published share holds one row, and a channel and year
    holds one `share_source`. A channel and year of `agency_spending` holds no
    `one` row of `agency_shares`. Each commitment's value under the OECD method
    equals its published `climate_usd`, and no channel of `core_contributions`
    aggregates another of its channels. Per party, year and status, `btr_totals`
    holds as many rows and linked rows as `btr` and `btr_links` do, and the same
    face and grant-equivalent sums within `BTR_TOTALS_TOLERANCE` of their size, and
    `btr_links` holds each pair once. The
    `untraced`, `unallocated`, `provider_coefficients`, `reflows` and `jev_flags`
    checks are those of `_untraced_problems`, `_marked_problems`,
    `_coefficient_problems`, `_reflow_problems` and `_jev_flag_problems`. An empty
    list means every check passed.
    """
    commitments, problems, _ = _finite(tables["commitments"], name="commitments")
    disbursements, non_finite, flagged = _finite(
        tables["disbursements"], name="disbursements"
    )
    problems.extend(non_finite)
    untraced, non_finite, _ = _finite(tables["untraced"], name="untraced")
    problems.extend(non_finite)
    # A non-finite amount is not null in `tables`, so it is reported once, above.
    problems.extend(
        f"{name} {_row_label(row)}: {column} is null"
        for name, columns in _COMPARED_AMOUNTS.items()
        for column in columns
        for row in tables[name].filter(pl.col(column).is_null()).iter_rows(named=True)
    )
    parents = commitments.join(
        commitments.select(row_id=pl.col("parent_row_id")).drop_nulls(),
        on="row_id",
        how="semi",
    )
    problems.extend(
        f"commitments {row_id}: an aggregate parent beside its children"
        for row_id in parents["row_id"].sort()
    )
    for smaller, larger in _ALLOCATION_ORDER:
        failing = disbursements.filter(_exceeds(smaller, larger))
        problems.extend(
            f"disbursements {_row_label(row)}: {smaller} > {larger}"
            for row in failing.iter_rows(named=True)
        )
    unlinked = disbursements.filter(
        pl.col("link_method").is_null(), pl.col("disbursement_source") == "crs"
    )
    problems.extend(
        f"disbursements {_row_label(row)}: no link carried it"
        for row in unlinked.iter_rows(named=True)
    )
    fills = disbursements.filter(pl.col("disbursement_source") != "crs").join(
        commitments.select("row_id", *_FILL_FACTS), on="row_id", suffix="_commitment"
    )
    for column in _FILL_FACTS:
        differs = fills.filter(
            ~pl.col(column).eq_missing(pl.col(f"{column}_commitment"))
        )
        problems.extend(
            f"disbursements {_row_label(row)}: a fill whose {column} is not its "
            "commitment's"
            for row in differs.iter_rows(named=True)
        )
    on_repeats = disbursements.join(
        commitments.filter(pl.col("repeat_of").is_not_null()), on="row_id", how="semi"
    )
    problems.extend(
        f"disbursements {row_id}: allocated to a row that repeats an earlier vintage"
        for row_id in on_repeats["row_id"].unique().sort()
    )
    orphans = disbursements.join(commitments, on="row_id", how="anti")
    problems.extend(
        f"disbursements {row_id}: no such commitment"
        for row_id in orphans["row_id"].unique().sort()
    )

    summed = ~pl.col("row_id").is_in(flagged["row_id"].implode())
    sums = (
        disbursements.filter(summed)
        .group_by("row_id")
        .agg(pl.col(part).sum().alias(f"_{part}") for part in _DISBURSED_SUMS.values())
    )
    totals = (
        commitments.filter(summed)
        .select("row_id", "withheld_usd", *_DISBURSED_SUMS)
        .join(sums, on="row_id", how="left")
        .with_columns(
            (pl.col(f"_{part}").fill_null(0.0) + pl.col("withheld_usd")).alias(
                f"_{part}"
            )
            for part in _DISBURSED_SUMS.values()
        )
    )
    for total, part in _DISBURSED_SUMS.items():
        failing = totals.filter(
            (pl.col(total) - pl.col(f"_{part}")).abs() > INVARIANT_TOLERANCE_USD
        )
        problems.extend(
            f"commitments {row['row_id']}: {total} {row[total]!r} differs from the "
            f"sum of {part} plus withheld_usd {row[f'_{part}']!r}"
            for row in failing.iter_rows(named=True)
        )
    for smaller, larger in _DISBURSED_ORDER:
        failing = commitments.filter(_exceeds(smaller, larger))
        problems.extend(
            f"commitments {row_id}: {smaller} > {larger}"
            for row_id in failing["row_id"]
        )
    cap = pl.col("face_usd").clip(lower_bound=0.0)
    failing = commitments.filter(
        pl.col("disbursed_usd") - cap > INVARIANT_TOLERANCE_USD
    )
    problems.extend(
        f"commitments {row_id}: disbursed_usd > max(face_usd, 0)"
        for row_id in failing["row_id"]
    )
    finite = disbursements.filter(
        ~pl.col("record_id").is_in(flagged["record_id"].implode())
    )
    problems.extend(_record_problems(finite))
    problems.extend(
        _btr_problems(
            tables["btr"], links=tables["btr_links"], totals=tables["btr_totals"]
        )
    )
    problems.extend(
        _untraced_problems(
            untraced, disbursements=finite, unallocated=tables["unallocated"]
        )
    )
    held = flagged.select("provider_code", "year").unique()
    problems.extend(
        _marked_problems(
            disbursements.join(held, on=["provider_code", "year"], how="anti"),
            unallocated=tables["unallocated"].join(
                held, on=["provider_code", "year"], how="anti"
            ),
        )
    )
    problems.extend(_share_problems(tables["agency_shares"]))
    problems.extend(_oecd_problems(commitments))
    core = tables["core_contributions"]
    channel = pl.col("channel_code")
    problems.extend(
        f"core_contributions {provider} {year} {flow} {code}: an aggregate channel "
        f"other than the {TOP_CHANNEL} remainder"
        for provider, year, flow, code in core.filter(
            aggregate_channel(channel), channel != TOP_CHANNEL
        )
        .select(*CONTRIBUTION_KEY, "channel_code")
        .sort(pl.all())
        .iter_rows()
    )
    problems.extend(
        f"core_contributions {provider} {year} {flow}: a {TOP_CHANNEL} remainder "
        "of zero or less"
        for provider, year, flow in core.filter(
            channel == TOP_CHANNEL,
            pl.col("contribution_usd") <= INVARIANT_TOLERANCE_USD,
        )
        .select(CONTRIBUTION_KEY)
        .sort(pl.all())
        .iter_rows()
    )
    problems.extend(
        _spending_problems(tables["agency_spending"], shares=tables["agency_shares"])
    )
    problems.extend(
        f"{name}: {count} rows of a fill record the release leaves out"
        for name, frame in tables.items()
        if "record_id" in frame.columns
        and (count := frame.filter(withheld_record(pl.col("record_id"))).height)
    )
    problems.extend(
        f"{name}: {count} rows with recipient_code {CRS_UNSPECIFIED_RECIPIENT}, "
        f"which releases write as {UNSPECIFIED_RECIPIENT}"
        for name, frame in tables.items()
        if "recipient_code" in frame.columns
        and (
            count := frame.filter(
                pl.col("recipient_code") == CRS_UNSPECIFIED_RECIPIENT
            ).height
        )
    )
    problems.extend(_coefficient_problems(tables["provider_coefficients"]))
    reflows, non_finite, _ = _finite(tables["reflows"], name="reflows")
    problems.extend(non_finite)
    problems.extend(_reflow_problems(reflows, commitments=commitments))
    problems.extend(_jev_flag_problems(tables["jev_flags"]))
    return problems


def _jev_flag_problems(flags: pl.DataFrame) -> list[str]:
    """Rows of `jev_flags` with an answer above 1, a flag that differs from its cut,
    or an answer or flag null other than exactly where `short_text` holds."""
    short = pl.col("short_text")
    checks = {
        "an answer above 1": pl.any_horizontal(
            pl.col(name) > 1 for name in JEV_FLAG_PROBABILITIES
        ),
        "no_aim differs from no_aim_p >= NO_AIM_CUT": pl.col("no_aim").ne_missing(
            pl.col("no_aim_p") >= NO_AIM_CUT
        ),
        "surprising differs from surprising_p >= SURPRISING_CUT": pl.col(
            "surprising"
        ).ne_missing(pl.col("surprising_p") >= SURPRISING_CUT),
        "an answer null other than where short_text holds": pl.any_horizontal(
            pl.col(name).is_null() != short
            for name in (*JEV_FLAG_PROBABILITIES, "no_aim", "surprising")
        ),
    }
    return [
        f"jev_flags {record_id}: {check}"
        for check, failing in checks.items()
        for record_id in flags.filter(failing.fill_null(False))["record_id"]
    ]


_COEFFICIENTS = ("significant", "principal", "both_significant")


def _coefficient_problems(coefficients: pl.DataFrame) -> list[str]:
    """Rows of `provider_coefficients` that the query could not apply as one rule.

    Coefficients lie between 0 and 1. A row that is not `case_by_case` holds a
    significant and a principal coefficient. A row's `first_year` is at most its
    `last_year`, and no two rows of a provider and agency cover the same year.
    """

    def name(row: dict[str, object]) -> str:
        return (
            f"provider_coefficients {row['provider_code']} {row['agency_code']} "
            f"{row['first_year']}"
        )

    problems = []
    for row in coefficients.iter_rows(named=True):
        problems.extend(
            f"{name(row)}: {column} {row[column]!r} outside 0 to 1"
            for column in _COEFFICIENTS
            if row[column] is not None and not 0.0 <= row[column] <= 1.0
        )
        if not row["case_by_case"] and None in (row["significant"], row["principal"]):
            problems.append(f"{name(row)}: no significant or principal coefficient")
        if None not in (row["first_year"], row["last_year"]) and (
            row["first_year"] > row["last_year"]
        ):
            problems.append(f"{name(row)}: first_year after last_year")
    return [*problems, *coefficient_overlaps(coefficients)]


def coefficient_overlaps(coefficients: pl.DataFrame) -> list[str]:
    """Each row of `provider_coefficients` that covers a year an earlier-starting
    row of its provider and agency covers, named by provider, agency and first year
    as `release_invariants` names rows, with the years the two share."""
    key = ["provider_code", "agency_code"]
    rows = (
        coefficients.select(*key, "first_year", "last_year")
        .sort(*key, "first_year", nulls_last=False)
        .with_row_index()
    )
    pairs = rows.join(rows, on=key, nulls_equal=True, suffix="_later").filter(
        pl.col("index") < pl.col("index_later")
    )
    # A null first or last year has no bound, so the two rows share every year past
    # the later start, or before the earlier end.
    shared = pairs.with_columns(
        start=pl.max_horizontal("first_year", "first_year_later"),
        end=pl.min_horizontal("last_year", "last_year_later"),
    ).filter(
        pl.col("start").is_null()
        | pl.col("end").is_null()
        | (pl.col("start") <= pl.col("end"))
    )
    return [
        f"provider_coefficients {row['provider_code']} {row['agency_code']} "
        f"{row['first_year_later']}: covers {row['start'] or 'every year'} to "
        f"{row['end'] or 'every later year'}, as does the row from "
        f"{row['first_year'] or 'no first year'}"
        for row in shared.iter_rows(named=True)
    ]


def _btr_problems(
    btr: pl.DataFrame, *, links: pl.DataFrame, totals: pl.DataFrame
) -> list[str]:
    """Where `totals` (`btr_totals`) differs from the `btr` rows it sums, and the
    pairs `links` (`btr_links`) holds more than once."""
    keys = ("party", "year", "status")
    amounts = (
        "face_usd",
        "grant_equiv_usd",
        "face_usd_at_dac",
        "grant_equiv_usd_at_dac",
    )
    sums = [pl.col(name).sum() for name in amounts]
    from_rows = (
        btr.with_columns(linked=pl.col("row_id").is_in(links["row_id"].implode()))
        .group_by(keys)
        .agg(*sums, rows=pl.len(), linked=pl.col("linked").sum())
    )
    from_totals = totals.group_by(keys).agg(
        *sums, rows=pl.col("rows").sum(), linked=pl.col("rows").filter("linked").sum()
    )
    compared = (
        from_rows.join(
            from_totals,
            on=keys,
            how="full",
            nulls_equal=True,
            coalesce=True,
            suffix="_totals",
        )
        .with_columns(cs.numeric().fill_null(0))
        .sort(keys, nulls_last=True)
    )
    problems = []
    for row in compared.iter_rows(named=True):
        label = " ".join(str(row[key]) for key in keys)
        problems.extend(
            f"btr_totals {label}: {row[f'{count}_totals']} {count}, btr has "
            f"{row[count]}"
            for count in ("rows", "linked")
            if row[count] != row[f"{count}_totals"]
        )
        problems.extend(
            f"btr_totals {label}: {name} {row[f'{name}_totals']!r}, btr sums to "
            f"{row[name]!r}"
            for name in amounts
            if abs(row[name] - row[f"{name}_totals"])
            > BTR_TOTALS_TOLERANCE * max(abs(row[name]), 1.0)
        )
    repeated = links.filter(pl.struct(BTR_LINKS.sort_key).is_duplicated())
    problems.extend(
        f"btr_links {row_id} {record_id}: the pair repeats"
        for row_id, record_id in repeated.select(BTR_LINKS.sort_key)
        .unique()
        .sort(BTR_LINKS.sort_key)
        .iter_rows()
    )
    return problems


def _row_label(row: Mapping[str, object]) -> str:
    """A commitments row's id, or a disbursements row's id, record and year."""
    if "record_id" in row:
        return f"{row['row_id']} {row['record_id']} {row['year']}"
    return str(row["row_id"])


def _oecd_problems(commitments: pl.DataFrame) -> list[str]:
    """Commitments whose value under the OECD method differs from `climate_usd`."""
    # `methods` reads its objectives from this module, so it is imported here.
    from climate_finance.methods import OECD, face_shares, weights

    oecd = weights(OECD, published=pl.col("share_climate"))["climate"]
    # Lazy, so only the columns the check reads are copied.
    failing = (
        commitments.lazy()
        .with_columns(**face_shares())
        .filter(
            (pl.col("face_usd") * oecd - pl.col("climate_usd")).abs()
            > INVARIANT_TOLERANCE_USD
        )
        .select("row_id")
        .collect()
    )
    return [
        f"commitments {row_id}: the OECD method's value differs from climate_usd"
        for row_id in failing["row_id"].sort()
    ]


def _share_problems(agency_shares: pl.DataFrame) -> list[str]:
    """Agency share keys that hold both a `climate` row and split rows."""
    climate = pl.col("objective") == "climate"
    mixed = (
        agency_shares.group_by("channel_code", "year", "source")
        .agg(climate.any().alias("_climate"), (~climate).any().alias("_split"))
        .filter(pl.col("_climate"), pl.col("_split"))
        .sort("channel_code", "year", "source")
    )
    return [
        f"agency_shares {row['channel_code']} {row['year']} {row['source']}: a "
        "climate row beside split rows"
        for row in mixed.iter_rows(named=True)
    ]


def _spending_problems(
    agency_spending: pl.DataFrame, *, shares: pl.DataFrame
) -> list[str]:
    """`agency_spending` rows that mix markers and component shares, keys held
    twice, channels and years of several share sources, and channels and years that
    `shares` also holds under `one`."""
    key = AGENCY_SPENDING.sort_key
    markers = pl.col("rio_adaptation", "rio_mitigation")
    components = pl.col("adaptation_share", "mitigation_share", "overlap_share")
    marker_row = pl.all_horizontal(markers.is_not_null()) & pl.all_horizontal(
        components.is_null()
    )
    component_row = pl.all_horizontal(markers.is_null()) & pl.all_horizontal(
        components.is_not_null()
    )
    mixed = agency_spending.filter(~(marker_row | component_row))
    repeated = agency_spending.filter(pl.struct(key).is_duplicated()).unique(key)
    sources = agency_spending.group_by("channel_code", "year").agg(
        pl.col("share_source").n_unique().alias("_sources")
    )
    both = (
        agency_spending.select("channel_code", "year")
        .unique()
        .join(shares.filter(pl.col("source") == "one"), on=["channel_code", "year"])
        .unique(["channel_code", "year"])
    )

    def label(row: Mapping[str, object]) -> str:
        return " ".join(str(row[part]) for part in key)

    return [
        *(
            f"agency_spending {label(row)}: markers and component shares mixed"
            for row in mixed.sort(key, nulls_last=True).iter_rows(named=True)
        ),
        *(
            f"agency_spending {label(row)}: held by more than one row"
            for row in repeated.sort(key, nulls_last=True).iter_rows(named=True)
        ),
        *(
            f"agency_spending {row['channel_code']} {row['year']}: rows of several "
            "share sources"
            for row in sources.filter(pl.col("_sources") > 1)
            .sort("channel_code", "year")
            .iter_rows(named=True)
        ),
        *(
            f"agency_spending {row['channel_code']} {row['year']}: also an "
            "agency_shares row of source one"
            for row in both.sort("channel_code", "year").iter_rows(named=True)
        ),
    ]


def _finite(
    frame: pl.DataFrame, *, name: str
) -> tuple[pl.DataFrame, list[str], pl.DataFrame]:
    """`frame` with NaN and infinite amounts set to null, a problem per such amount,
    naming its row by the table's sort key, and the rows that held one."""
    columns = [column for column, dtype in frame.schema.items() if dtype.is_float()]
    key = RELEASE_TABLES[name].sort_key
    problems = [
        f"{name} {' '.join(str(row[part]) for part in key)}: {column} is not finite"
        for column in columns
        for row in frame.filter(~pl.col(column).is_finite()).iter_rows(named=True)
    ]
    flagged = frame.filter(
        pl.any_horizontal(~pl.col(column).is_finite() for column in columns)
    )
    finite = frame.with_columns(
        pl.when(pl.col(column).is_finite()).then(pl.col(column)).alias(column)
        for column in columns
    )
    return finite, problems, flagged


def _record_problems(disbursements: pl.DataFrame) -> list[str]:
    """Record-level columns that differ between a record's rows, and records whose
    allocations exceed their disbursement."""

    def differs(column: str) -> pl.Expr:
        gap = pl.col(column).max() - pl.col(column).min()
        nulls = pl.col(column).null_count()
        return (gap.fill_null(0.0) > INVARIANT_TOLERANCE_USD) | (
            (nulls > 0) & (nulls < pl.len())
        )

    allocated = pl.col("allocated_usd").sum()
    disbursement = pl.col("record_disbursement_usd").max()
    flags = [f"_{column}_differs" for column in _RECORD_COLUMNS]
    failing = (
        disbursements.group_by("record_id")
        .agg(
            *(
                differs(column).alias(flag)
                for column, flag in zip(_RECORD_COLUMNS, flags, strict=True)
            ),
            _allocated=allocated,
            _disbursement=disbursement,
            _exceeds=allocated - disbursement > INVARIANT_TOLERANCE_USD,
        )
        .filter(pl.any_horizontal(*flags, "_exceeds"))
        .sort("record_id")
    )
    problems = []
    for row in failing.iter_rows(named=True):
        problems.extend(
            f"disbursements record {row['record_id']}: {column} differs between rows"
            for column, flag in zip(_RECORD_COLUMNS, flags, strict=True)
            if row[flag]
        )
        if row["_exceeds"]:
            problems.append(
                f"disbursements record {row['record_id']}: allocated_usd sums to "
                f"{row['_allocated']!r}, above record_disbursement_usd "
                f"{row['_disbursement']!r}"
            )
    return problems


def _untraced_problems(
    untraced: pl.DataFrame, *, disbursements: pl.DataFrame, unallocated: pl.DataFrame
) -> list[str]:
    """`untraced` rows that a query would count wrongly or twice.

    Each row holds a positive `untraced_usd` of at most `record_disbursement_usd`,
    a Rio marker above 0, no adaptation marker before `ADAPTATION_FIRST_YEAR`, and a
    Bi_Multi code of the recipient view. An `unlinked`
    row's record has no `disbursements` row and counts its whole disbursement. A
    record in both tables gives the same `record_disbursement_usd` and
    `record_grant_equiv_usd` in each, and its allocations and untraced amount sum to
    at most its disbursement. `unallocated`'s excess and unlinked amounts equal the
    `untraced` sums of each provider, year and reason.
    """
    amount, record = pl.col("untraced_usd"), pl.col("record_disbursement_usd")
    marked = (pl.col("rio_adaptation") > 0) | (pl.col("rio_mitigation") > 0)
    unlinked = pl.col("reason") == "unlinked"
    checks = {
        "an adaptation marker before ADAPTATION_FIRST_YEAR": pl.col(
            "rio_adaptation"
        ).is_not_null()
        & measured_adaptation(pl.lit(value=True)).is_null(),
        "untraced_usd is not positive": amount <= 0,
        "untraced_usd above record_disbursement_usd": amount - record
        > INVARIANT_TOLERANCE_USD,
        "no Rio marker above 0": ~marked.fill_null(value=False),
        "a Bi_Multi code the recipient view leaves out": ~pl.col("bi_multi").is_in(
            RECIPIENT_VIEW
        ),
        "unlinked but short of record_disbursement_usd": unlinked
        & ((amount - record).abs() > INVARIANT_TOLERANCE_USD),
    }
    problems = [
        f"untraced {record_id}: {problem}"
        for problem, failing in checks.items()
        for record_id in untraced.filter(failing)["record_id"].sort()
    ]
    allocated = disbursements.group_by("record_id").agg(
        pl.col("allocated_usd").sum().alias("_allocated"),
        *(pl.col(column).first().alias(f"_{column}") for column in _RECORD_COLUMNS),
    )
    both = untraced.join(allocated, on="record_id")
    problems.extend(
        f"untraced {record_id}: unlinked, but disbursements holds the record"
        for record_id in both.filter(unlinked)["record_id"].sort()
    )
    problems.extend(
        f"untraced {record_id}: allocated and untraced sum above "
        "record_disbursement_usd"
        for record_id in both.filter(
            pl.col("_allocated") + amount - record > INVARIANT_TOLERANCE_USD
        )["record_id"].sort()
    )
    for column in _RECORD_COLUMNS:
        differs = (pl.col(column) - pl.col(f"_{column}")).abs() > (
            INVARIANT_TOLERANCE_USD
        )
        failing = both.filter(
            differs.fill_null(value=False)
            | (pl.col(column).is_null() != pl.col(f"_{column}").is_null())
        )
        problems.extend(
            f"untraced {record_id}: {column} differs from disbursements"
            for record_id in failing["record_id"].sort()
        )
    key = ["provider_code", "year"]
    sums = untraced.group_by(key).agg(
        _excess=amount.filter(~unlinked).sum(), _unlinked=amount.filter(unlinked).sum()
    )
    ledger = (
        unallocated.select(*key, "excess_usd", "unlinked_marked_usd")
        .join(sums, on=key, how="full", coalesce=True)
        .with_columns(pl.exclude(key).fill_null(0.0))
        .sort(key)
    )
    for column, part in (
        ("excess_usd", "_excess"),
        ("unlinked_marked_usd", "_unlinked"),
    ):
        failing = ledger.filter(
            (pl.col(column) - pl.col(part)).abs() > INVARIANT_TOLERANCE_USD
        )
        problems.extend(
            f"unallocated {provider} {year}: {column} {total!r} differs from the "
            f"untraced sum {summed!r}"
            for provider, year, total, summed in failing.select(
                *key, column, part
            ).iter_rows()
        )
    return problems


# A reflow's amounts and the record totals its rows sum to.
_REFLOW_SUMS = {
    "received_usd": "record_received_usd",
    "interest_usd": "record_interest_usd",
}
# The bases whose principal a loan's disbursement caps.
_REFLOW_ATTRIBUTED = (
    "commitment",
    "outside_release",
    "excess",
    "untraced",
    "unmeasured",
)


def _reflow_problems(reflows: pl.DataFrame, *, commitments: pl.DataFrame) -> list[str]:
    """`reflows` rows that a query would weigh wrongly or count twice.

    Each low amount lies between 0 and its amount. A record's rows share one
    `record_received_usd` and one `record_interest_usd` and sum to them, within
    `INVARIANT_TOLERANCE_USD` or `INVARIANT_RELATIVE_TOLERANCE` of the total,
    whichever is larger. A row names a `row_id` exactly where its basis is
    `commitment`, and that row is a commitment of the release that repeats no
    earlier vintage, whose markers and climate share the row carries. Each loan's
    attributed principal, every basis it caps, sums to at most
    `loan_disbursement_usd`, which all its rows share.
    """

    def label(row: Mapping[str, object]) -> str:
        return f"reflows {row['record_id']} {row['basis']} {row['row_id']}"

    problems = []
    for amount in ("received", "interest"):
        value, low = pl.col(f"{amount}_usd"), pl.col(f"{amount}_low_usd")
        outside = (low * value.sign() < -INVARIANT_TOLERANCE_USD) | (
            low.abs() - value.abs() > INVARIANT_TOLERANCE_USD
        )
        problems.extend(
            f"{label(row)}: {amount}_low_usd outside 0 to {amount}_usd"
            for row in reflows.filter(outside).iter_rows(named=True)
        )
    named = pl.col("row_id").is_not_null()
    problems.extend(
        f"{label(row)}: basis {row['basis']} with a row_id"
        if row["row_id"] is not None
        else f"{label(row)}: basis commitment without a row_id"
        for row in reflows.filter(named != (pl.col("basis") == "commitment")).iter_rows(
            named=True
        )
    )
    rows = commitments.select(
        "row_id",
        "repeat_of",
        "climate_share",
        _rio_adaptation="rio_adaptation",
        _rio_mitigation="rio_mitigation",
        _known=pl.lit(value=True),
    )
    attributed = reflows.filter(named).join(rows, on="row_id", how="left")
    problems.extend(
        f"{label(row)}: no such commitment"
        for row in attributed.filter(pl.col("_known").is_null()).iter_rows(named=True)
    )
    problems.extend(
        f"{label(row)}: names a row that repeats an earlier vintage"
        for row in attributed.filter(pl.col("repeat_of").is_not_null()).iter_rows(
            named=True
        )
    )
    differs = ~(
        pl.col("inherited_rio_adaptation").eq_missing(pl.col("_rio_adaptation"))
        & pl.col("inherited_rio_mitigation").eq_missing(pl.col("_rio_mitigation"))
        & pl.col("climate_share").eq_missing(pl.col("climate_share_right"))
    )
    problems.extend(
        f"{label(row)}: markers or climate share differ from the commitment's"
        for row in attributed.filter(pl.col("_known"), differs).iter_rows(named=True)
    )

    def beyond(total: pl.Expr, part: pl.Expr) -> pl.Expr:
        return (total - part).abs() > pl.max_horizontal(
            pl.lit(INVARIANT_TOLERANCE_USD),
            INVARIANT_RELATIVE_TOLERANCE * total.abs(),
        )

    for amount, total in _REFLOW_SUMS.items():
        records = (
            reflows.group_by("record_id")
            .agg(
                _shared=pl.col(total).n_unique() == 1,
                _total=pl.col(total).first(),
                _summed=pl.col(amount).sum(),
            )
            .sort("record_id")
        )
        problems.extend(
            f"reflows record {record_id}: {total} differs between rows"
            for record_id in records.filter(~pl.col("_shared"))["record_id"]
        )
        problems.extend(
            f"reflows record {record_id}: {amount} sums to {summed!r}, not "
            f"{total} {expected!r}"
            for record_id, expected, summed in records.filter(
                pl.col("_shared"), beyond(pl.col("_total"), pl.col("_summed"))
            )
            .select("record_id", "_total", "_summed")
            .iter_rows()
        )
    loans = (
        reflows.filter(pl.col("loan_id").is_not_null())
        .group_by("loan_id")
        .agg(
            _shared=pl.col("loan_disbursement_usd").n_unique() == 1,
            _disbursed=pl.col("loan_disbursement_usd").first(),
            _attributed=pl.col("received_usd")
            .filter(pl.col("basis").is_in(_REFLOW_ATTRIBUTED))
            .sum(),
        )
        .sort("loan_id")
    )
    problems.extend(
        f"reflows loan {loan_id}: loan_disbursement_usd differs between rows"
        for loan_id in loans.filter(~pl.col("_shared"))["loan_id"]
    )
    problems.extend(
        f"reflows loan {loan_id}: attributed principal {attributed!r} above "
        f"loan_disbursement_usd {disbursed!r}"
        for loan_id, disbursed, attributed in loans.filter(
            pl.col("_shared"),
            (pl.col("_attributed") > pl.col("_disbursed"))
            & beyond(pl.col("_disbursed"), pl.col("_attributed")),
        )
        .select("loan_id", "_disbursed", "_attributed")
        .iter_rows()
    )
    return problems


def _marked_problems(
    disbursements: pl.DataFrame, *, unallocated: pl.DataFrame
) -> list[str]:
    """Providers and years whose `unallocated.marked_usd` differs from the sum of
    its parts: the allocated amounts of the `disbursements` rows of Rio-marked CRS
    records, and `unallocated`'s excess, outside-release, unlinked and fund-unlinked
    amounts. A Rio-marked record is in the recipient view's Bi_Multi codes and
    carries a measured adaptation marker (`measured_adaptation`) or a mitigation
    marker above 0, as `allocate.marked_records` selects them; a record that
    disburses nothing has no allocation to sum. A provider and year missing from
    one side counts 0 there. The two sides compare within `INVARIANT_TOLERANCE_USD`
    or `INVARIANT_RELATIVE_TOLERANCE` of `marked_usd`, whichever is larger."""
    key = ["provider_code", "year"]
    marked = (measured_adaptation(pl.col("record_rio_adaptation")) > 0) | (
        pl.col("record_rio_mitigation") > 0
    )
    released = (
        disbursements.filter(
            pl.col("disbursement_source") == "crs",
            pl.col("bi_multi").is_in(RECIPIENT_VIEW),
            marked.fill_null(value=False),
        )
        .group_by(key)
        .agg(_released=pl.col("allocated_usd").sum())
    )
    ledger = (
        unallocated.select(
            *key,
            "marked_usd",
            _parts=pl.sum_horizontal(
                "excess_usd",
                "outside_release_usd",
                "unlinked_marked_usd",
                "fund_unlinked_usd",
            ),
        )
        .join(released, on=key, how="full", coalesce=True)
        .with_columns(pl.exclude(key).fill_null(0.0))
        .with_columns(_summed=pl.col("_released") + pl.col("_parts"))
        .filter(
            (pl.col("marked_usd") - pl.col("_summed")).abs()
            > pl.max_horizontal(
                pl.lit(INVARIANT_TOLERANCE_USD),
                INVARIANT_RELATIVE_TOLERANCE * pl.col("marked_usd").abs(),
            )
        )
        .sort(key)
    )
    return [
        f"unallocated {provider} {year}: marked_usd {marked_usd!r} differs from "
        f"its parts' sum {summed!r}"
        for provider, year, marked_usd, summed in ledger.select(
            *key, "marked_usd", "_summed"
        ).iter_rows()
    ]


# release.json --------------------------------------------------------------------


class TableEntry(BaseModel):
    """One release table's file hash and row count."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sha256: str
    rows: int


class BuildIdentity(BaseModel):
    """The code and environment that built a release.

    `package_source_sha256` is the sha256 over the files of the installed package,
    `python` its interpreter's exact version, and `environment_sha256` the sha256
    over the sorted `name==version` lines of every installed distribution.
    `dependencies` holds the exact versions of the installed libraries the build's
    results depend on. `stage_identities` holds the sha256 of each build stage's
    `stage.json` before the release was sealed, by stage name.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    package_source_sha256: str
    python: str
    environment_sha256: str
    dependencies: dict[str, str]
    stage_identities: dict[str, str]


class ReleaseManifest(BaseModel):
    """The contents of `release.json`.

    `years` holds the first and last year the build read from each source it reads
    a range of years from, by source name. `withheld_tables` maps each release
    table a published pack leaves out to the note saying how to build it. `tables`
    lists every other release table.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    # The release's data format, a key of `FORMAT_TABLES`.
    data_format: StrictInt
    created_at: datetime
    package_version: str
    # The package files and environment that built the release.
    build: BuildIdentity
    tables: dict[str, TableEntry]
    source_versions: dict[str, str]
    years: dict[str, tuple[int, int]] = {}
    model_versions: dict[str, str]
    gates: dict[str, JsonValue]
    # Every build switch and year range the release was built with, by name.
    settings: dict[str, JsonValue]
    precision_gate_waived: bool = False
    # Why `cf-build release --waive-precision` let the precision, stratum-precision
    # and human-audit gates fail, null without a waiver.
    precision_waiver: str | None = None
    previous_release: str | None = None
    # The sha256 of the pack the drift gate's previous release came from, null when
    # the copy it read records none. `cf-build publish` checks it against the index.
    previous_pack_sha256: str | None = None
    withheld_tables: dict[str, str] = {}


def sha256_file(path: Path) -> str:
    """Hex sha256 of a file's bytes."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def scan_table(path: Path) -> pl.LazyFrame:
    """Scan the stage or release table at `path`.

    An empty table is read into memory instead. Polars 2.0.0 panics ("min > max")
    estimating the rows of a `unique` or `group_by` over a scan of a parquet file
    with no rows once the result meets a join, and not over the same empty frame
    held in memory.
    """
    scan = pl.scan_parquet(path)
    if scan.select(pl.len()).collect().item() == 0:
        return pl.read_parquet(path).lazy()
    return scan
