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
LEDGER_CHECKS = ("confirmed", "contradicted", "none", "confirmed_undisbursed")
DISBURSEMENT_SOURCES = ("crs", "bank_ledger", "none")
ALLOCATION_RULES = ("fifo", "dated", "window", "excess", "bank_ledger")
OBJECTIVES = ("adaptation", "mitigation", "cross_cutting")
LABEL_SOURCES = ("crdf", "marker", "jev")
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
FLOWS = ("commitments", "disbursements", "grant_equivalent")
DECISIONS = ("accept", "reject")
BI_MULTI_CODES = (1, 2, 3, 4, 7, 8)
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
    objective=_str(values=OBJECTIVES),
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
    title=_str(nullable=True),
    recipient_code=_code(),
    purpose_code=_code(nullable=True),
    channel_code=_code(nullable=True),
    bi_multi=_bi_multi(),
    flow_code=_code(),
    commitment_usd=_usd(nullable=True),
    disbursement_usd=_usd(nullable=True),
    grant_equiv_usd=_usd(nullable=True),
    commitment_date=Column(pl.Date(), nullable=True),
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

IMPUTED_SHARES = _table(
    "imputed_shares",
    ("channel_code", "year", "objective"),
    channel_code=_code(),
    year=_year(),
    objective=_str(values=OBJECTIVES),
    share=_f64(),
)

EXTERNAL = _table(
    "external",
    ("source", "publisher", "activity_id", "year", "transaction_type"),
    source=_str(values=("iati", "gcf")),
    publisher=_str(),
    activity_id=_str(),
    year=_year(),
    transaction_type=_str(values=("commitment", "disbursement")),
    amount_usd=_usd(),
    recipient_code=_code(nullable=True),
    title=_str(nullable=True),
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

ORGS = _table(
    "orgs",
    ("source_name",),
    source_name=_str(),
    donor_code=_code(nullable=True),
    agency_code=_code(nullable=True),
    channel_code=_code(nullable=True),
    method=_str(values=("exact_code", "exact_name", "pack", "propagated")),
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
    levels=Column(pl.Struct, nullable=True),
    model_version=_str(nullable=True),
    jev_model=_str(nullable=True),
    label_set=_str(nullable=True),
    ledger_check=_str(nullable=True, values=LEDGER_CHECKS),
    review=_bool(),
    review_reason=_str(nullable=True, values=REVIEW_REASONS),
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

PROGRAMME_STEMS = _table(
    "programme_stems",
    ("donor_code", "agency_code", "pattern"),
    donor_code=_code(),
    agency_code=_code(),
    pattern=_str(),
    programme_level=Column(pl.Int8()),
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

ALLOCATIONS = _table(
    "allocations",
    ("row_id", "record_id", "year", "rule"),
    row_id=_str(nullable=True),
    record_id=_str(),
    year=_year(),
    allocated_usd=_usd(),
    rule=_str(values=ALLOCATION_RULES),
    disbursement_source=_str(values=DISBURSEMENT_SOURCES),
)

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
)

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
AGENCY_SHARES = _table(
    "agency_shares",
    ("channel_code", "year", "objective", "source"),
    channel_code=_code(),
    year=_year(),
    objective=_str(values=OBJECTIVES),
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
    # 0 <= disbursed_low <= disbursed <= disbursed_high <= face.
    disbursed_usd=_usd(),
    disbursed_low_usd=_usd(),
    disbursed_high_usd=_usd(),
    project_disbursing=_bool(),
    repeat_of=_str(nullable=True),
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
    # record_disbursement_usd; the rest is excess that reaches no commitment.
    record_disbursement_usd=_usd(),
    record_grant_equiv_usd=_usd(nullable=True),
    inherited_rio_adaptation=_rio(),
    inherited_rio_mitigation=_rio(),
    record_rio_adaptation=_rio(),
    record_rio_mitigation=_rio(),
    marker_source=_str(),
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
    )
}


def _by_name(*tables: TableSchema) -> Mapping[str, TableSchema]:
    return {table.name: table for table in tables}


STAGE_TABLES: Mapping[str, Mapping[str, TableSchema]] = {
    "fetch": _by_name(RAW_MANIFEST),
    "normalise": _by_name(
        CRDF, CRS, CORE_CONTRIBUTIONS, IMPUTED_SHARES, EXTERNAL, BTR, ORGS
    ),
    "link": _by_name(LINKS, ID_PRACTICE, PROGRAMME_STEMS, PROVIDER_PROFILE),
    "allocate": _by_name(
        ALLOCATIONS, COMMITMENT_STATUS, CANCELLATIONS, YEARS_SINCE_COMMITMENT
    ),
    "fill": _by_name(ALLOCATIONS, COMMITMENT_STATUS, BANK_YEAR_OVERLAP),
    "review": _by_name(LINKS, REVIEW_QUEUE, PAIR_LABELS, COMMITMENT_STATUS),
    "label": _by_name(OBJECTIVE_LABELS, JEV_LEVEL_MAP),
    "measure": _by_name(AGENCY_SHARES, MODELLED),
    "assemble": RELEASE_TABLES,
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
    ("disbursed_high_usd", "face_usd"),
)
# Columns that hold one value across all rows of a CRS record.
_RECORD_COLUMNS = ("record_disbursement_usd", "record_grant_equiv_usd")
_DISBURSED_SUMS = {
    "disbursed_usd": "allocated_usd",
    "disbursed_low_usd": "allocated_low_usd",
    "disbursed_high_usd": "allocated_high_usd",
}


def _exceeds(smaller: str, larger: str) -> pl.Expr:
    """True where `smaller` is above `larger` by more than the tolerance."""

    def amount(name: str) -> pl.Expr:
        return pl.lit(0.0) if name == "0" else pl.col(name)

    return amount(smaller) - amount(larger) > INVARIANT_TOLERANCE_USD


def release_invariants(tables: Mapping[str, pl.DataFrame]) -> list[str]:
    """Problems with the amounts in a release's `commitments` and `disbursements`.

    Every disbursements row holds 0 <= allocated_low <= allocated <= allocated_high
    and names a commitment. Per commitment, its rows' allocated, allocated_low and
    allocated_high sum to disbursed, disbursed_low and disbursed_high, and
    0 <= disbursed_low <= disbursed <= disbursed_high <= face. A record's rows share
    one `record_disbursement_usd` and one `record_grant_equiv_usd` (all null or all
    set), and its allocated amounts sum to at most `record_disbursement_usd`. Amounts
    compare within `INVARIANT_TOLERANCE_USD`. An empty list means every check passed.
    """
    commitments = tables["commitments"]
    disbursements = tables["disbursements"]
    problems = []
    for smaller, larger in _ALLOCATION_ORDER:
        failing = disbursements.filter(_exceeds(smaller, larger))
        problems.extend(
            f"disbursements {row['row_id']} {row['record_id']} {row['year']}: "
            f"{smaller} > {larger}"
            for row in failing.iter_rows(named=True)
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
    problems.extend(_record_problems(disbursements))
    return problems


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
        if row["_allocated"] - row["_disbursement"] > INVARIANT_TOLERANCE_USD:
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
