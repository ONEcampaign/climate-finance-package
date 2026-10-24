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
from pydantic import BaseModel, ConfigDict, JsonValue

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
# `crosswalk_amount` and `title_recipient` among them, is estimated, so a certain
# trace link reached through it counts nothing in the low bound.
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
OBJECTIVES = ("adaptation", "mitigation", "cross_cutting")
# An agency share's objective: one of OBJECTIVES, or `climate` for a total climate
# share with no split by objective, as the OECD gives before 2021.
SHARE_OBJECTIVES = (*OBJECTIVES, "climate")
LABEL_SOURCES = ("crdf", "marker", "jev")
# How a disbursement's own Rio markers relate to the ones it inherits.
MARKER_SOURCES = ("components", "commitment", "record_differs")
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
DECISIONS = ("accept", "reject")
# Code 6 holds private-sector and philanthropic outflows, kept as published. The query
# views list the codes they count and leave it out.
BI_MULTI_CODES = (1, 2, 3, 4, 6, 7, 8)
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


def _bi_multi() -> Column:
    return Column(pl.Int8(), values=BI_MULTI_CODES)


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
    grant_equiv_usd=_usd(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
    # The project's expected start and completion, which the window rule reads.
    expected_start_date=Column(pl.Date(), nullable=True),
    completion_date=Column(pl.Date(), nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    text_hash=_str(nullable=True),
)

# One row per provider, agency, year and flow. The build writes every flow in FLOWS.
# MULTI publishes aggregate channels beside their members, such as 40000 for all
# multilaterals and 44000 for the World Bank Group. Normalise keeps them. The release
# keeps leaf channels, so each dollar counts once, plus a 40000 row for the part of
# a provider's 40000 total that no leaf itemises (`release.core_contributions`).
# The key within which a provider's leaf channels sum to its `TOP_CHANNEL` row.
CONTRIBUTION_KEY = ("provider_code", "year", "flow")
# MULTI's total of a provider's core contributions, which sums every leaf channel
# it reports, 30010 included. The intermediate aggregates (41000, 47000 and the
# like) do not follow the code prefixes: MULTI files some 41xxx channels under 47000.
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
# published. Amounts are as published, in `currency`; the readers fill `amount_usd` for
# USD rows, and normalise converts the rest at DAC annual rates where the DAC publishes
# one for the currency. A World Bank row is the December-to-December flow of a loan,
# dated 31 December. A GEF disbursement is the rise of its running peak, since GEF
# declares its figures cumulative. `disbursed_to_date` is a GCF readiness grant's
# disbursed amount, which the list does not date: its `date` and `year` are null and no
# calendar-year check can use it. `other_ids` lists the activity's other identifiers,
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

BTR = _table(
    "btr",
    ("row_id",),
    row_id=_str(),
    party=_str(),
    year=_year(),
    recipient=_str(nullable=True),
    recipient_code=_code(nullable=True),
    title=_str(nullable=True),
    amount_domestic=_f64(nullable=True),
    currency=_str(nullable=True),
    face_usd=_usd(nullable=True),
    grant_equiv_usd=_usd(nullable=True),
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

ID_PRACTICE = _table(
    "id_practice",
    ("donor_code", "agency_code", "period"),
    donor_code=_code(),
    agency_code=_code(),
    period=_str(values=("2010-2014", "2015-2018", "2019-2022")),
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
# crs id; the classifier trains on `score.id_truth`. `programme_level` is 2 for
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

# One row per provider: the climate value of its counted CRDF commitments and, for a
# provider with a crosswalk (`link.crosswalks`), the part whose commitment a crosswalk
# link reaches, null for the others and for every provider when recoveries are off.
# The scoring columns (`link.score`) are null for a provider with no candidate pairs:
# `scoring_group` from the counts beside it, `true_pairs` counting true training
# pairs, `commitments` and `pairs` the commitments and pairs open to scoring, and for
# the truth group the isotonic map its pairs took (`calibration`) and the
# classifier's link `threshold`.
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
    allocated_high_usd=_usd(),
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
    disbursed_high_usd=_usd(),
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
# carries either the queue's `review_reason` or the blind sample's `sample_id`:
# queue decisions become pair labels and link overrides, and blind-sample decisions
# only measure precision. `label_source` says whether a person or an agent decided;
# an agent's decision names its `model`, its `confidence` that the pair matches, and
# its labelling `round`. `reviewer` names the person or the agent run, and `note`
# holds the stated reason.
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
    # `disbursements` rows: allocated, allocated_low and allocated_high, with
    # 0 <= disbursed_low <= disbursed <= disbursed_high <= face. A negative face
    # value (a published downward revision) stays as published, with nothing
    # disbursed.
    disbursed_usd=_usd(),
    disbursed_low_usd=_usd(),
    disbursed_high_usd=_usd(),
    project_disbursing=_bool(),
    # True or null: true where the bank's ledger
    # (`ledger.UNDISBURSED_PUBLISHERS`) confirms no disbursement; null where no
    # ledger covers the row or it shows money moving. It stands whatever `status`
    # says, so a row the allocation calls disbursed and the ledger does not points
    # at a link to check.
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
    # allocation's bounds: 0 <= allocated_low <= allocated <= allocated_high.
    # allocated_low holds what certain links carried (`CERTAIN_METHODS`). A
    # bank-ledger or IATI fill counts in full in both bounds, since it matches the
    # commitment on its publisher's own id: the bank's ledger key or the fund's
    # project id.
    allocated_usd=_usd(),
    allocated_low_usd=_usd(),
    allocated_high_usd=_usd(),
    # The CRS record's own disbursement at face value and grant equivalent, the same
    # on every row of a record. The query takes an allocation's grant equivalent as
    # record_grant_equiv_usd * allocated_usd / record_disbursement_usd, so a record
    # split across commitments counts once. A record's allocated_usd sums to at most
    # record_disbursement_usd; the rest is excess that reaches no commitment. A
    # grant equivalent is never negative: null stands for one the CRS lacks.
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
# of their disbursements in the high and low bounds (`allocated_high_usd`,
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

# Rio-marked CRS disbursements that reach no commitment in the release, by provider
# and disbursement year, at face value: a linked record's disbursement beyond what
# its commitments take (`excess_usd`), disbursements to commitments with no CRDF row
# in the release (`outside_release_usd`), and records linked to no commitment
# (`unlinked_marked_usd`). With the `disbursements` rows of CRS records they sum to
# every Rio-marked CRS disbursement.
UNALLOCATED = _table(
    "unallocated",
    ("provider_code", "year"),
    provider_code=_code(),
    year=_year(),
    excess_usd=_usd(),
    outside_release_usd=_usd(),
    unlinked_marked_usd=_usd(),
)

# Every CRS flow, climate or not, summed. This is the denominator of `share_of_total`.
TOTAL_FLOWS = _table(
    "total_flows",
    ("provider_code", "recipient_code", "year", "bi_multi", "flow"),
    provider_code=_code(),
    recipient_code=_code(),
    year=_year(),
    bi_multi=_bi_multi(),
    flow=_str(values=FLOWS),
    amount_usd=_usd(),
)

# Each provider's own significant-marker coefficient, from BTR-to-CRS amount ratios.
PROVIDER_COEFFICIENTS = _table(
    "provider_coefficients",
    ("provider_code",),
    provider_code=_code(),
    significant=_f64(),
    basis=_str(),
    confirmed=_bool(),
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
    DISBURSEMENT_PROFILE,
    QUALITY,
    UNALLOCATED,
    TOTAL_FLOWS,
    PROVIDER_COEFFICIENTS,
)

STAGE_TABLES: Mapping[str, Mapping[str, TableSchema]] = {
    "fetch": _by_name(RAW_MANIFEST),
    "normalise": _by_name(
        CRDF,
        CRDF_SPAN,
        CRS,
        CORE_CONTRIBUTIONS,
        IMPUTED_SHARES,
        EXTERNAL,
        BTR,
        ORGS,
        LEDGER,
    ),
    "link": _by_name(
        LINKS,
        UNITS,
        ID_PRACTICE,
        PROGRAMME_STEMS,
        CANDIDATES,
        PAIR_SCORES,
        PROVIDER_PROFILE,
    ),
    "allocate": _by_name(
        ALLOCATIONS, COMMITMENT_STATUS, CANCELLATIONS, YEARS_SINCE_COMMITMENT
    ),
    "fill": _by_name(ALLOCATIONS, COMMITMENT_STATUS, BANK_YEAR_OVERLAP),
    "review": _by_name(LINKS, REVIEW_QUEUE, PAIR_LABELS, COMMITMENT_STATUS),
    "label": _by_name(OBJECTIVE_LABELS),
    "measure": _by_name(AGENCY_SHARES, AGENCY_SPENDING, MODELLED),
    "assemble": RELEASE_TABLES,
}
# Tables a stage may leave out. A stage that reads one that was not written raises
# StageInputMissingError. `orgs` is written when recoveries are on, `btr` when the build
# config names an ingested BTR table, `ledger` when it names ledger sources.
# `bank_year_overlap` is written by the bank-ledger gap fill, `review_queue` by the
# review queue and `pair_labels` by the ledger check. With their switches off the two
# stages pass the tables they carry forward through unchanged.
OPTIONAL_STAGE_TABLES: Mapping[str, frozenset[str]] = {
    "normalise": frozenset({"btr", "orgs", "ledger"}),
    "fill": frozenset({"bank_year_overlap"}),
    "review": frozenset({"review_queue", "pair_labels"}),
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
    ("allocated_usd", "allocated_high_usd"),
    ("0", "record_grant_equiv_usd"),
)
_DISBURSED_ORDER = (
    ("0", "disbursed_low_usd"),
    ("disbursed_low_usd", "disbursed_usd"),
    ("disbursed_usd", "disbursed_high_usd"),
)
# Columns that hold one value across all rows of a CRS record.
_RECORD_COLUMNS = ("record_disbursement_usd", "record_grant_equiv_usd")
_DISBURSED_SUMS = {
    "disbursed_usd": "allocated_usd",
    "disbursed_low_usd": "allocated_low_usd",
    "disbursed_high_usd": "allocated_high_usd",
}
# The amounts the checks compare, by table. A null among them fails no comparison,
# so each null is a problem of its own.
_COMPARED_AMOUNTS = {
    "commitments": ("face_usd", *_DISBURSED_SUMS),
    "disbursements": (*_DISBURSED_SUMS.values(), "record_disbursement_usd"),
}


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
    """Problems with a release's `commitments`, `disbursements`, `agency_shares` and
    `core_contributions`.

    Every amount is finite or null; the other checks skip a non-finite amount, so it
    is reported once, as non-finite. Every disbursements row holds
    0 <= allocated_low <= allocated <= allocated_high and 0 <= record_grant_equiv,
    and names a commitment. Per commitment, its rows' allocated, allocated_low and
    allocated_high sum to disbursed, disbursed_low and disbursed_high, and
    0 <= disbursed_low <= disbursed <= disbursed_high <= face, where a face of zero
    or below (a published downward revision) allows nothing disbursed. A record's
    rows share one `record_disbursement_usd` and one `record_grant_equiv_usd` (all
    null or all set), and its allocated amounts sum to at most
    `record_disbursement_usd`. Amounts compare within `INVARIANT_TOLERANCE_USD`, and
    none of them is null. Every disbursements row from the CRS names the link that
    carried it, and every fill (a bank-ledger or IATI row) carries its commitment's
    provider, agency, recipient, channel, Bi_Multi, purpose and project number. No
    disbursements row names a commitment that repeats an earlier vintage, as
    allocate moves a repeat's allocations to its first row. No commitment is
    another's `parent_row_id`, and no table in `tables` holds the bulk CRS's
    `CRS_UNSPECIFIED_RECIPIENT`. A channel, year and source of `agency_shares`
    holds either split rows or one `climate` row. Each
    `agency_spending` row holds both markers and no component shares, or neither
    marker and all three component shares, a channel, year, pair of markers and
    published share holds one row, and a channel and year holds one `share_source`.
    A channel and year of `agency_spending` holds no `one` row of `agency_shares`.
    Each commitment's value under the OECD method equals its published
    `climate_usd`, and no channel of `core_contributions` aggregates another of its
    channels. An empty list means every check passed.
    """
    commitments, problems = _finite(tables["commitments"], name="commitments")
    disbursements, non_finite = _finite(tables["disbursements"], name="disbursements")
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
            f"disbursements {row['row_id']} {row['record_id']} {row['year']}: "
            f"{smaller} > {larger}"
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

    sums = disbursements.group_by("row_id").agg(
        pl.col(part).sum().alias(f"_{part}") for part in _DISBURSED_SUMS.values()
    )
    totals = (
        commitments.select("row_id", *_DISBURSED_SUMS)
        .join(sums, on="row_id", how="left")
        .with_columns(
            pl.col(f"_{part}").fill_null(0.0) for part in _DISBURSED_SUMS.values()
        )
    )
    for total, part in _DISBURSED_SUMS.items():
        failing = totals.filter(
            (pl.col(total) - pl.col(f"_{part}")).abs() > INVARIANT_TOLERANCE_USD
        )
        problems.extend(
            f"commitments {row['row_id']}: {total} {row[total]!r} differs from the "
            f"sum of {part} {row[f'_{part}']!r}"
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
        pl.col("disbursed_high_usd") - cap > INVARIANT_TOLERANCE_USD
    )
    problems.extend(
        f"commitments {row_id}: disbursed_high_usd > max(face_usd, 0)"
        for row_id in failing["row_id"]
    )
    problems.extend(_record_problems(disbursements))
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
    failing = commitments.with_columns(**face_shares()).filter(
        (pl.col("face_usd") * oecd - pl.col("climate_usd")).abs()
        > INVARIANT_TOLERANCE_USD
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


def _finite(frame: pl.DataFrame, *, name: str) -> tuple[pl.DataFrame, list[str]]:
    """`frame` with NaN and infinite amounts set to null, and a problem per such
    amount, naming its row by the table's sort key."""
    columns = [column for column, dtype in frame.schema.items() if dtype.is_float()]
    key = RELEASE_TABLES[name].sort_key
    problems = [
        f"{name} {' '.join(str(row[part]) for part in key)}: {column} is not finite"
        for column in columns
        for row in frame.filter(~pl.col(column).is_finite()).iter_rows(named=True)
    ]
    finite = frame.with_columns(
        pl.when(pl.col(column).is_finite()).then(pl.col(column)).alias(column)
        for column in columns
    )
    return finite, problems


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


# release.json --------------------------------------------------------------------


class TableEntry(BaseModel):
    """One release table's file hash and row count."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sha256: str
    rows: int


class ReleaseManifest(BaseModel):
    """The contents of `release.json`.

    `years` holds the first and last year the build read from each source it reads
    a range of years from, by source name.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    created_at: datetime
    package_version: str
    tables: dict[str, TableEntry]
    source_versions: dict[str, str]
    years: dict[str, tuple[int, int]] = {}
    model_versions: dict[str, str]
    gates: dict[str, JsonValue]
    # Every build switch and year range the release was built with, by name.
    settings: dict[str, JsonValue]
    precision_gate_waived: bool = False
    previous_release: str | None = None


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
