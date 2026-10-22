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
    "crs_id_purpose",
    "crs_id",
    "crs_id_parent",
    "pn_recipient_purpose",
    "pn_recipient",
)
KEY_REPAIR_METHODS = (
    "repair_new_crs_id",
    "repair_pn_variant",
    "repair_wb_parent",
    "repair_afdb_scientific",
    "repair_title_recipient",
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
)
LEDGER_TRANSACTION_TYPES = (
    "commitment",
    "disbursement",
    "expenditure",
    "disbursed_to_date",
)
LEDGER_CHECKS = ("confirmed", "contradicted", "none", "confirmed_undisbursed")
DISBURSEMENT_SOURCES = ("crs", "bank_ledger", "none")
ALLOCATION_RULES = ("fifo", "dated", "window", "excess", "bank_ledger")
OBJECTIVES = ("adaptation", "mitigation", "cross_cutting")
# An agency share's objective: one of OBJECTIVES, or `climate` for a total climate
# share with no split by objective, as the OECD gives before 2021.
SHARE_OBJECTIVES = (*OBJECTIVES, "climate")
LABEL_SOURCES = ("crdf", "marker", "jev")
# How a disbursement's own Rio markers relate to the ones it inherits.
MARKER_SOURCES = ("components", "commitment", "record_differs")
PAIR_LABEL_SOURCES = ("key", "rule", "jev", "human", "ledger")
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
    """One column: its dtype, whether it may hold nulls, and its allowed values.

    A dtype class (such as `pl.Struct`) accepts any parametrisation of it.
    """

    dtype: DType
    nullable: bool = False
    values: tuple[object, ...] | None = None


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


def _str(*, nullable: bool = False, values: tuple[str, ...] | None = None) -> Column:
    return Column(pl.String(), nullable=nullable, values=values)


def _f64(*, nullable: bool = False) -> Column:
    return Column(pl.Float64(), nullable=nullable)


_usd = _f64


def _code(*, nullable: bool = False) -> Column:
    return Column(pl.Int32(), nullable=nullable)


def _count() -> Column:
    return Column(pl.Int32())


def _year(*, nullable: bool = False) -> Column:
    return Column(pl.Int16(), nullable=nullable)


def _bool() -> Column:
    return Column(pl.Boolean())


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
    flow_code=_code(nullable=True),
    # Provider-perspective imputed multilateral contribution rows carry a climate
    # total only, so their objective is `climate`.
    objective=_str(values=SHARE_OBJECTIVES),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    face_usd=_usd(),
    climate_usd=_usd(),
    adaptation_usd=_usd(),
    mitigation_usd=_usd(),
    overlap_usd=_usd(),
    climate_share=_f64(nullable=True),
    id_scheme=_str(nullable=True),
    repeat_of=_str(nullable=True),
    parent_row_id=_str(nullable=True),
)

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
# published. Amounts are as published, in `currency`; `amount_usd` is filled for USD
# rows only. A World Bank row is the December-to-December flow of a loan, dated 31
# December. A GEF disbursement is the rise of its running peak, since GEF declares its
# figures cumulative. `disbursed_to_date` is a GCF readiness grant's disbursed amount,
# which the list does not date: its `date` and `year` are null and no calendar-year
# check can use it. `other_ids` lists the activity's other identifiers, which the
# bilateral id matching reads.
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
    probability=_f64(),
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
# `id_persistent` agencies, whose label is `same_crs_id`. `programme_level` is 2 for
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

PROVIDER_PROFILE = _table(
    "provider_profile",
    ("provider_code",),
    provider_code=_code(),
    scoring_group=_str(values=SCORING_GROUPS),
    true_pairs=_count(),
    commitments=_count(),
    pairs=_count(),
    calibration_labels=_count(),
    key_link_share=_f64(nullable=True),
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

REVIEW_DECISIONS = _table(
    "review_decisions",
    ("task", "left_id", "right_id"),
    task=_str(values=LINK_TASKS),
    left_id=_str(),
    right_id=_str(nullable=True),
    decision=_str(values=DECISIONS),
    reviewer=_str(),
    note=_str(nullable=True),
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
    # Jev labels only: 0.4 x p(significant) + 1.0 x p(principal), which the query maps
    # through `jev_level_map` to a calibrated level.
    expected_coefficient=_f64(nullable=True),
)

# The isotonic map from Jev's expected coefficient to a calibrated level, fitted on
# human-coded projects for one Jev model. A level between two points is interpolated
# linearly, and one outside them takes the nearest point's level.
JEV_LEVEL_MAP = _table(
    "jev_level_map",
    ("objective", "jev_model", "expected_coefficient"),
    objective=_str(values=OBJECTIVES),
    jev_model=_str(),
    expected_coefficient=_f64(),
    level=_f64(),
)

# Shares of the agency's spending by objective. The adaptation and mitigation shares
# each include the cross_cutting share, as `adaptation_usd`, `mitigation_usd` and
# `overlap_usd` do, so the climate share is adaptation + mitigation - cross_cutting.
# A channel, year and source holds either those split rows or one `climate` row, the
# total climate share with no split by objective.
AGENCY_SHARES = _table(
    "agency_shares",
    ("channel_code", "year", "objective", "source"),
    channel_code=_code(),
    year=_year(),
    objective=_str(values=SHARE_OBJECTIVES),
    share=_f64(),
    source=_str(values=SHARE_SOURCES),
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
    flow_code=_code(nullable=True),
    crs_id=_str(nullable=True),
    project_number=_str(nullable=True),
    rio_adaptation=_rio(),
    rio_mitigation=_rio(),
    climate_share=_f64(nullable=True),
    face_usd=_usd(),
    climate_usd=_usd(),
    # Adaptation and mitigation each include the overlap, so climate is a + m - overlap.
    adaptation_usd=_usd(),
    mitigation_usd=_usd(),
    overlap_usd=_usd(),
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
    allocated_usd=_usd(),
    allocated_low_usd=_usd(),
    allocated_high_usd=_usd(),
    # The CRS record's own disbursement at face value and grant equivalent, the same
    # on every row of a record. The query takes an allocation's grant equivalent as
    # record_grant_equiv_usd * allocated_usd / record_disbursement_usd, so a record
    # split across commitments counts once. A record's allocated_usd sums to at most
    # record_disbursement_usd. The rest is excess that reaches no commitment.
    record_disbursement_usd=_usd(),
    record_grant_equiv_usd=_usd(nullable=True),
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
# those that trace to at least one CRS record (`linked_usd`), those whose most
# probable link is at least 0.8, and those labelled by a model or by a report.
# `grant_equiv_coverage` is the share of linked records with a grant equivalent.
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
    linked_high_probability_usd=_usd(),
    model_labelled_usd=_usd(),
    report_labelled_usd=_usd(),
    repeat_climate_usd=_usd(),
    grant_equiv_coverage=_f64(nullable=True),
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

RELEASE_TABLES: Mapping[str, TableSchema] = {
    table.name: table
    for table in (
        COMMITMENTS,
        DISBURSEMENTS,
        OBJECTIVE_LABELS,
        AGENCY_SHARES,
        MODELLED,
        CORE_CONTRIBUTIONS,
        BTR,
        DISBURSEMENT_PROFILE,
        QUALITY,
        TOTAL_FLOWS,
        PROVIDER_COEFFICIENTS,
        JEV_LEVEL_MAP,
    )
}


def _by_name(*tables: TableSchema) -> Mapping[str, TableSchema]:
    return {table.name: table for table in tables}


STAGE_TABLES: Mapping[str, Mapping[str, TableSchema]] = {
    "fetch": _by_name(RAW_MANIFEST),
    "normalise": _by_name(
        CRDF, CRS, CORE_CONTRIBUTIONS, IMPUTED_SHARES, EXTERNAL, BTR, ORGS
    ),
    "link": _by_name(
        LINKS, UNITS, ID_PRACTICE, PROGRAMME_STEMS, CANDIDATES, PROVIDER_PROFILE
    ),
    "allocate": _by_name(
        ALLOCATIONS, COMMITMENT_STATUS, CANCELLATIONS, YEARS_SINCE_COMMITMENT
    ),
    "fill": _by_name(ALLOCATIONS, COMMITMENT_STATUS, BANK_YEAR_OVERLAP),
    "review": _by_name(LINKS, REVIEW_QUEUE, PAIR_LABELS, COMMITMENT_STATUS),
    "label": _by_name(OBJECTIVE_LABELS, JEV_LEVEL_MAP),
    "measure": _by_name(AGENCY_SHARES, MODELLED),
    "assemble": RELEASE_TABLES,
}
# Tables a stage may leave out. A stage that reads one that was not written raises
# StageInputMissingError. `orgs` is written when recoveries are on, `btr` when the
# build config names an ingested BTR table. `provider_profile` is written by the
# link stage's scoring step, and the other link steps write their tables without it.
# `bank_year_overlap` is written by the bank-ledger gap fill, and `review_queue` and
# `pair_labels` by the review queue. With their switches off the two stages pass the
# tables they carry forward through unchanged.
OPTIONAL_STAGE_TABLES: Mapping[str, frozenset[str]] = {
    "normalise": frozenset({"btr", "orgs"}),
    "link": frozenset({"provider_profile"}),
    "fill": frozenset({"bank_year_overlap"}),
    "review": frozenset({"review_queue", "pair_labels"}),
}


def _dtype_matches(actual: pl.DataType, expected: DType) -> bool:
    if isinstance(expected, type):
        return actual.base_type() is expected
    return actual == expected


def validate(table: TableSchema, frame: pl.DataFrame) -> None:
    """Check `frame` against `table`.

    Raises:
        SchemaError: naming every missing, undeclared, mistyped, null-holding or
            out-of-vocabulary column.
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

# Pairs (smaller, larger) of amounts that must hold smaller <= larger. "0" is zero.
_ALLOCATION_ORDER = (
    ("0", "allocated_low_usd"),
    ("allocated_low_usd", "allocated_usd"),
    ("allocated_usd", "allocated_high_usd"),
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


def _exceeds(smaller: str, larger: str) -> pl.Expr:
    """True where `smaller` is above `larger` by more than the tolerance."""

    def amount(name: str) -> pl.Expr:
        return pl.lit(0.0) if name == "0" else pl.col(name)

    return amount(smaller) - amount(larger) > INVARIANT_TOLERANCE_USD


def release_invariants(tables: Mapping[str, pl.DataFrame]) -> list[str]:
    """Problems with a release's `commitments`, `disbursements` and `agency_shares`.

    Every disbursements row holds 0 <= allocated_low <= allocated <= allocated_high
    and names a commitment. Per commitment, its rows' allocated, allocated_low and
    allocated_high sum to disbursed, disbursed_low and disbursed_high, and
    0 <= disbursed_low <= disbursed <= disbursed_high <= face, where a face of zero
    or below (a published downward revision) allows nothing disbursed. A record's
    rows share one `record_disbursement_usd` and one `record_grant_equiv_usd` (all
    null or all set), and its allocated amounts sum to at most
    `record_disbursement_usd`. Amounts compare within `INVARIANT_TOLERANCE_USD`, and
    none of them is null. Every disbursements row but a bank-ledger fill names the
    link that carried it. No disbursements row names a commitment that repeats an
    earlier vintage, as allocate moves a repeat's allocations to its first row. No
    commitment is another's `parent_row_id`, and no table in `tables` holds the
    bulk CRS's `CRS_UNSPECIFIED_RECIPIENT`. A channel, year and source of
    `agency_shares` holds either split rows or one `climate` row. An empty list
    means every check passed.
    """
    commitments = tables["commitments"]
    disbursements = tables["disbursements"]
    problems = [
        f"{name} {_row_label(row)}: {column} is null"
        for name, columns in _COMPARED_AMOUNTS.items()
        for column in columns
        for row in tables[name].filter(pl.col(column).is_null()).iter_rows(named=True)
    ]
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
        pl.col("link_method").is_null(), pl.col("rule") != "bank_ledger"
    )
    problems.extend(
        f"disbursements {_row_label(row)}: no link carried it"
        for row in unlinked.iter_rows(named=True)
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
    totals = commitments.join(sums, on="row_id", how="left").with_columns(
        pl.col(f"_{part}").fill_null(0.0) for part in _DISBURSED_SUMS.values()
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


def _record_problems(disbursements: pl.DataFrame) -> list[str]:
    """Record-level columns that differ between a record's rows, and records whose
    allocations exceed their disbursement."""
    records = disbursements.group_by("record_id").agg(
        pl.col("allocated_usd").sum().alias("_allocated"),
        *(
            expression
            for column in _RECORD_COLUMNS
            for expression in (
                (pl.col(column).max() - pl.col(column).min()).alias(f"_{column}_gap"),
                pl.col(column).null_count().alias(f"_{column}_nulls"),
            )
        ),
        pl.col("record_disbursement_usd").max().alias("_disbursement"),
        pl.len().alias("_rows"),
    )
    problems = []
    for row in records.sort("record_id").iter_rows(named=True):
        for column in _RECORD_COLUMNS:
            gap, nulls = row[f"_{column}_gap"], row[f"_{column}_nulls"]
            if (gap or 0.0) > INVARIANT_TOLERANCE_USD or 0 < nulls < row["_rows"]:
                problems.append(
                    f"disbursements record {row['record_id']}: {column} differs "
                    "between rows"
                )
        disbursement = row["_disbursement"]
        if (
            disbursement is not None
            and row["_allocated"] - disbursement > INVARIANT_TOLERANCE_USD
        ):
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
    """The contents of `release.json`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    created_at: datetime
    package_version: str
    tables: dict[str, TableEntry]
    source_versions: dict[str, str]
    model_versions: dict[str, str]
    gates: dict[str, JsonValue]
    precision_gate_waived: bool = False
    previous_release: str | None = None


def sha256_file(path: Path) -> str:
    """Hex sha256 of a file's bytes."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()
