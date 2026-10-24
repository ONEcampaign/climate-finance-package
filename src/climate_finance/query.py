"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of three origins:

- `commitment`: a CRDF commitment at face value (`flow="commitments"`). Repeats of an
  earlier vintage, and aggregate rows rebuilt into child rows, are left out.
- `disbursement`: a CRS record's allocation to a commitment at face value
  (`allocated_usd`, or for `flow="grant_equivalent"` the record's grant equivalent
  times allocated_usd over the record's disbursement),
  weighted by the markers and climate share it inherits from that commitment. `low`
  and `high` weight the allocation's own bounds, `allocated_low_usd` and
  `allocated_high_usd`, converted to grant equivalents the same way.
- `attribution`: under `view="provider"`, a provider's core contribution of the
  spec's flow to an agency times the agency's climate share.

The release's commitments are recipient-perspective CRDF rows only, so views filter
on Bi_Multi alone and provider attribution comes only from core contributions.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from importlib.metadata import version
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import catalogue
from climate_finance.convert import convert, provenance
from climate_finance.methods import OECD, Method, label, weights
from climate_finance.result import (
    AMOUNTS,
    ROW_KEYS,
    Meta,
    Quality,
    Result,
    Table,
)
from climate_finance.spec import (
    BILATERAL,
    CORE_CONTRIBUTION,
    MULTILATERAL_OUTFLOW,
    ODA_FLOW_CODES,
    Flow,
    Spec,
    View,
)
from climate_finance.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.snapshot import Snapshot

# The objective label a record shows when several sources label it: the bank's own
# climate components first, then Jev, then the donor's markers.
LABEL_PRECEDENCE = {"crdf": 0, "jev": 1, "marker": 2}

KEYS = (*ROW_KEYS, "method")
ROW_COLUMNS: dict[str, pl.DataType] = {
    "origin": pl.String(),
    "row_id": pl.String(),
    "record_id": pl.String(),
    "year": pl.Int16(),
    "provider_code": pl.Int32(),
    "agency_code": pl.Int32(),
    "recipient_code": pl.Int32(),
    "channel_code": pl.Int32(),
    "bi_multi": pl.Int8(),
    "purpose_code": pl.Int32(),
    "flow_code": pl.Int32(),
    "project_number": pl.String(),
    "title": pl.String(),
    "status": pl.String(),
    "rio_adaptation": pl.Int8(),
    "rio_mitigation": pl.Int8(),
    "record_rio_adaptation": pl.Int8(),
    "record_rio_mitigation": pl.Int8(),
    "marker_source": pl.String(),
    "climate_share": pl.Float64(),
    "link_method": pl.String(),
    "link_probability": pl.Float64(),
    "allocation_weight": pl.Float64(),
    "objective_label": pl.String(),
    "label_source": pl.String(),
    "amount": pl.Float64(),
    "coefficient_basis": pl.String(),
    "coefficient_source": pl.String(),
}
# Inputs to the weighting that the rows do not show.
_WEIGHT_INPUTS: dict[str, pl.DataType] = {
    "share_adaptation": pl.Float64(),
    "share_mitigation": pl.Float64(),
    "share_overlap": pl.Float64(),
    "amount_low": pl.Float64(),
    "amount_high": pl.Float64(),
    "significant": pl.Float64(),
    "principal": pl.Float64(),
}


def spending(snapshot: Snapshot, *, spec: Spec) -> Result:
    """Climate finance for `spec` from `snapshot`'s release tables."""
    methods = spec.methods
    if any(method.relevance == "jev" for method in methods):
        raise NotImplementedError(
            "relevance='jev' is not supported: releases carry no Jev level map"
        )

    filters = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    conditions = filters
    if spec.oda_only:
        conditions = [
            *filters,
            pl.col("flow_code").is_in(ODA_FLOW_CODES)
            | (pl.col("origin") == "attribution"),
        ]
    base = _base_rows(snapshot, flow=spec.flow, view=spec.view).filter(*conditions)
    weighted = []
    for method in methods:
        frames = [base]
        if spec.view == "provider":
            attributed = _attributed_rows(snapshot, flow=spec.flow, method=method)
            frames.append(attributed.filter(*conditions))
        weighted.extend(
            _weigh(_coefficients(snapshot, frame, method=method), method=method)
            for frame in frames
        )

    rows = convert(
        pl.concat(weighted),
        columns=AMOUNTS,
        currency=spec.currency,
        prices=spec.prices,
    ).sort(
        "method", "origin", "provider_code", "year", "row_id", "record_id",
        "channel_code", nulls_last=True,
    )  # fmt: skip
    return Result(
        table=Table(_aggregate(rows, methods=[label(method) for method in methods])),
        modelled=Table(_modelled(snapshot, spec=spec, conditions=filters)),
        rows=rows,
        totals=_totals(snapshot, spec=spec, conditions=filters),
        spec=spec,
        meta=Meta(
            release=snapshot.manifest.release,
            package_version=version("climate-finance"),
            source_versions=snapshot.manifest.source_versions,
            model_versions=snapshot.manifest.model_versions,
            conversion=provenance(currency=spec.currency, prices=spec.prices),
        ),
    )


def quality(
    snapshot: Snapshot,
    *,
    providers: Sequence[str | int] | None = None,
    years: Iterable[int] | None = None,
) -> Quality:
    """Coverage and status statistics.

    The status split covers the commitments the trace follows: bilateral rows and
    multilateral outflows, without core contributions.
    """
    conditions = _conditions(providers=providers, recipients=None, years=years)
    table = snapshot.table("quality")
    status = (
        _counted_commitments(snapshot)
        .filter(_in_view("recipient"), *conditions)
        .select("provider_code", "year", "status", "climate_usd")
    )
    return Quality(table=Table(table.filter(*conditions)), status=Table(status))


def _codes(values: Sequence[str | int], *, listing: catalogue.Catalogue) -> list[int]:
    """DAC codes for `values`, with names resolved through `listing`."""
    return [
        listing.code(value) if isinstance(value, str) else value for value in values
    ]


def _conditions(
    *,
    providers: Sequence[str | int] | None,
    recipients: Sequence[str | int] | None,
    years: Iterable[int] | None,
) -> list[pl.Expr]:
    conditions = []
    if providers is not None:
        codes = _codes(providers, listing=catalogue.providers)
        conditions.append(pl.col("provider_code").is_in(codes))
    if recipients is not None:
        codes = _codes(recipients, listing=catalogue.recipients)
        conditions.append(pl.col("recipient_code").is_in(codes))
    if years is not None:
        conditions.append(pl.col("year").is_in(list(years)))
    return conditions


def _counted_commitments(snapshot: Snapshot) -> pl.LazyFrame:
    """Commitments that count: no repeats, and no aggregate rebuilt into children."""
    commitments = snapshot.table("commitments")
    parents = commitments.select(row_id=pl.col("parent_row_id")).drop_nulls().unique()
    return commitments.filter(pl.col("repeat_of").is_null()).join(
        parents, on="row_id", how="anti"
    )


def _in_view(view: View) -> pl.Expr:
    """Rows of the Bi_Multi codes that `view` counts."""
    codes = BILATERAL if view == "provider" else (*BILATERAL, MULTILATERAL_OUTFLOW)
    return pl.col("bi_multi").is_in(codes)


def _share(part: str) -> pl.Expr:
    """`part` as a fraction of `face_usd`, 0 for a zero face value."""
    face = pl.col("face_usd")
    return pl.when(face != 0).then(pl.col(part) / face).otherwise(0.0)


def _base_rows(snapshot: Snapshot, *, flow: Flow, view: View) -> pl.LazyFrame:
    shares = {
        "share_adaptation": _share("adaptation_usd"),
        "share_mitigation": _share("mitigation_usd"),
        "share_overlap": _share("overlap_usd"),
    }
    if flow == "commitments":
        commitments = _counted_commitments(snapshot).filter(_in_view(view))
        return _conform(
            commitments.with_columns(
                origin=pl.lit("commitment"),
                amount=pl.col("face_usd"),
                amount_low=pl.col("face_usd"),
                amount_high=pl.col("face_usd"),
                **shares,
            )
        )

    commitments = snapshot.table("commitments").select(
        "row_id",
        "status",
        "face_usd",
        "adaptation_usd",
        "mitigation_usd",
        "overlap_usd",
    )
    allocated = pl.col("allocated_usd")
    record_allocated = ordered_sum(allocated).over("record_id")
    disbursements = (
        snapshot.table("disbursements")
        .with_columns(
            allocation_weight=pl.when(record_allocated != 0).then(
                allocated / record_allocated
            )
        )
        .join(commitments, on="row_id", how="inner", validate="m:1")
        .filter(_in_view(view))
        .join(_labels(snapshot), on="record_id", how="left")
    )
    # Grant equivalents per USD disbursed, null where the record has neither.
    factor = pl.lit(1.0)
    if flow == "grant_equivalent":
        record = pl.col("record_disbursement_usd")
        factor = pl.when(record != 0).then(pl.col("record_grant_equiv_usd") / record)
    return _conform(
        disbursements.with_columns(
            origin=pl.lit("disbursement"),
            amount=allocated * factor,
            amount_low=pl.col("allocated_low_usd") * factor,
            amount_high=pl.col("allocated_high_usd") * factor,
            rio_adaptation=pl.col("inherited_rio_adaptation"),
            rio_mitigation=pl.col("inherited_rio_mitigation"),
            **shares,
        )
    )


def _labels(snapshot: Snapshot) -> pl.LazyFrame:
    """Per record, the highest-marker objective from its first-ranked label source."""
    rank = pl.col("label_source").replace_strict(LABEL_PRECEDENCE, return_dtype=pl.Int8)

    def level(objective: str) -> pl.Expr:
        return pl.col("level").filter(pl.col("objective") == objective).max()

    a, m, c = pl.col("a"), pl.col("m"), pl.col("c")
    return (
        snapshot.table("objectives")
        .filter(rank == rank.min().over("record_id"))
        .group_by("record_id")
        .agg(
            pl.col("label_source").first(),
            a=level("adaptation").fill_null(0),
            m=level("mitigation").fill_null(0),
            c=level("cross_cutting").fill_null(0),
        )
        .select(
            "record_id",
            "label_source",
            objective_label=pl.when((c > 0) | ((a == m) & (a > 0)))
            .then(pl.lit("cross_cutting"))
            .when(a > m)
            .then(pl.lit("adaptation"))
            .when(m > a)
            .then(pl.lit("mitigation")),
        )
    )


def _attributed_rows(snapshot: Snapshot, *, flow: Flow, method: Method) -> pl.LazyFrame:
    """Core contributions of `flow` times the agency's climate share by objective.

    Under OECD the share is the OECD's imputed share. Under any other method it is
    the agency's own climate spending in `agency_spending`, weighed with the method's
    coefficients, so agency shares follow the method's coefficients as bilateral
    rows do. Under provider coefficients agencies take the method's coefficients,
    since `provider_coefficients` holds the providers' own. Where `agency_spending`
    measures no share, the `one` rows of `agency_shares` stand in. The adaptation
    and mitigation shares each include the cross-cutting share. An agency without a
    share attributes nothing.
    """
    shares = _fixed_shares(snapshot, source="oecd" if method == OECD else "one")
    if method != OECD:
        measured = _measured_shares(snapshot, method=method)
        shares = pl.concat(
            [measured, shares.join(measured, on=["channel_code", "year"], how="anti")]
        )
    return _conform(
        snapshot.table("core_contributions")
        .filter(pl.col("flow") == flow)
        .join(shares, on=["channel_code", "year"], how="inner")
        .with_columns(
            origin=pl.lit("attribution"),
            bi_multi=pl.lit(CORE_CONTRIBUTION, pl.Int8),
            amount=pl.col("contribution_usd"),
            amount_low=pl.col("contribution_usd"),
            amount_high=pl.col("contribution_usd"),
            climate_share=pl.col("share_adaptation")
            + pl.col("share_mitigation")
            - pl.col("share_overlap"),
        )
    )


def _fixed_shares(snapshot: Snapshot, *, source: str) -> pl.LazyFrame:
    """The `agency_shares` of `source` by channel and year."""

    def share(objective: str) -> pl.Expr:
        # Picks the one share a channel and year hold per objective.
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    return (
        snapshot.table("agency_shares")
        .filter(pl.col("source") == source)
        .group_by("channel_code", "year")
        .agg(
            share_adaptation=share("adaptation"),
            share_mitigation=share("mitigation"),
            share_overlap=share("cross_cutting"),
        )
    )


def _measured_shares(snapshot: Snapshot, *, method: Method) -> pl.LazyFrame:
    """Each agency's climate shares by channel and year from `agency_spending`.

    Each row weighs as a commitment of its markers or components would under
    `method`, with its `face_share` as the amount.
    """
    face = pl.col("face_share")

    def part(name: str) -> pl.Expr:
        return pl.when(face != 0).then(pl.col(f"{name}_share") / face).otherwise(0.0)

    fractions = weights(method)
    overlap = fractions["cross_cutting"]
    # The highest-marker rule takes the overlap out of adaptation and mitigation,
    # and the shares include it.
    added = overlap if method.highest_marker else pl.lit(0.0)
    return (
        snapshot.table("agency_spending")
        .with_columns(
            climate_share=pl.col("adaptation_share")
            + pl.col("mitigation_share")
            - pl.col("overlap_share"),
            share_adaptation=part("adaptation"),
            share_mitigation=part("mitigation"),
            share_overlap=part("overlap"),
        )
        .group_by("channel_code", "year")
        .agg(
            share_adaptation=ordered_sum(face * (fractions["adaptation"] + added)),
            share_mitigation=ordered_sum(face * (fractions["mitigation"] + added)),
            share_overlap=ordered_sum(face * overlap),
        )
    )


def _conform(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Select the row columns and weighting inputs, as typed nulls where absent."""
    present = frame.collect_schema().names()
    return frame.select(
        pl.col(name).cast(dtype) if name in present else pl.lit(None, dtype).alias(name)
        for name, dtype in (ROW_COLUMNS | _WEIGHT_INPUTS).items()
    )


def _coefficients(
    snapshot: Snapshot, frame: pl.LazyFrame, *, method: Method
) -> pl.LazyFrame:
    """Set `significant`, `principal`, `coefficient_basis` and `coefficient_source`
    for `method`.

    Under provider coefficients each row takes the `provider_coefficients` row of its
    provider, year and agency, or of its provider and year for an agency without a
    row of its own. An activity significant for both objectives takes the row's
    `both_significant`. Where the row gives no coefficient, because the provider
    assesses each activity case by case or has no row, the row takes the method's.
    `coefficient_basis` is `provider` for the provider's flat coefficients, `default`
    for the default share of a provider that assesses case by case, `case_by_case`
    where such a provider gives no default, and null where the release holds no row.
    A row with a `climate_share` weighs by its shares, which take no provider
    coefficient, so its `coefficient_basis` and `coefficient_source` are null.
    """
    if method.coefficients == "fixed":
        return frame.with_columns(
            significant=pl.lit(method.significant),
            principal=pl.lit(method.principal),
        )
    held = _coefficients_by_year(snapshot, years=frame).select(
        "provider_code",
        "agency_code",
        "year",
        _significant="significant",
        _principal="principal",
        _both="both_significant",
        _basis=pl.when(~pl.col("case_by_case"))
        .then(pl.lit("provider"))
        .when(pl.col("significant").is_not_null() | pl.col("principal").is_not_null())
        .then(pl.lit("default"))
        .otherwise(pl.lit("case_by_case")),
        _source=pl.format("{}, {}", "source", "locator"),
    )
    columns = ("_significant", "_principal", "_both", "_basis", "_source")
    by_agency = held.filter(pl.col("agency_code").is_not_null())
    by_provider = held.filter(pl.col("agency_code").is_null()).drop("agency_code")
    own_agency = pl.col("_source").is_not_null()

    def pick(name: str) -> pl.Expr:
        return pl.when(own_agency).then(pl.col(name)).otherwise(pl.col(f"{name}_all"))

    rio_a, rio_m = pl.col("rio_adaptation"), pl.col("rio_mitigation")
    significant = pl.col("_significant").fill_null(method.significant)
    marker = pl.col("climate_share").is_null()
    return (
        frame.join(
            by_agency,
            on=["provider_code", "agency_code", "year"],
            how="left",
            validate="m:1",
        )
        .join(
            by_provider,
            on=["provider_code", "year"],
            how="left",
            validate="m:1",
            suffix="_all",
        )
        .with_columns(pick(name).alias(name) for name in columns)
        .with_columns(
            significant=pl.when((rio_a == 1) & (rio_m == 1))
            .then(pl.col("_both").fill_null(significant))
            .otherwise(significant),
            principal=pl.col("_principal").fill_null(method.principal),
            coefficient_basis=pl.when(marker).then(pl.col("_basis")),
            coefficient_source=pl.when(marker).then(pl.col("_source")),
        )
        .drop(*columns, *(f"{name}_all" for name in columns))
    )


def _coefficients_by_year(snapshot: Snapshot, *, years: pl.LazyFrame) -> pl.LazyFrame:
    """Each `provider_coefficients` row once for every year of `years` its span
    covers.

    The span is expanded into its years and matched on `year`, an equi-join. A
    cross join filtered on the span would be rewritten by polars into an
    inequality join, which polars 1.33 to 1.35 collect wrongly where the plan is
    reused.
    """
    first, last = pl.col("first_year"), pl.col("last_year")
    held = years.select("year").unique()
    bounds = held.select(_low=pl.col("year").min(), _high=pl.col("year").max())
    return (
        snapshot.table("provider_coefficients")
        .join(bounds, how="cross")
        .with_columns(
            year=pl.int_ranges(
                first.fill_null(pl.col("_low")),
                last.fill_null(pl.col("_high")) + 1,
                dtype=pl.Int16,
            )
        )
        .explode("year")
        .drop("_low", "_high")
        .join(held, on="year", how="semi")
    )


def _weigh(frame: pl.LazyFrame, *, method: Method) -> pl.LazyFrame:
    amount = pl.col("amount")
    fractions = weights(
        method, significant=pl.col("significant"), principal=pl.col("principal")
    )
    return frame.with_columns(
        method=pl.lit(label(method)),
        adaptation=amount * fractions["adaptation"],
        mitigation=amount * fractions["mitigation"],
        cross_cutting=amount * fractions["cross_cutting"],
        value=amount * fractions["climate"],
        low=pl.col("amount_low") * fractions["climate"],
        high=pl.col("amount_high") * fractions["climate"],
    ).drop(*_WEIGHT_INPUTS)


def _sum(name: str) -> pl.Expr:
    """The sum of `name` in a fixed order, null when every value is missing."""
    column = pl.col(name)
    return pl.when(column.is_not_null().any()).then(ordered_sum(column))


def _aggregate(rows: pl.LazyFrame, *, methods: Sequence[str]) -> pl.LazyFrame:
    """Sum rows by `KEYS`. `low` and `high` then span every method for the same key.

    Every method in `methods` holds a row for every key any method holds, with zero
    amounts where it counts nothing, so a method that has no row for a key still
    bounds it.
    """
    summed = rows.group_by(KEYS).agg(
        *(_sum(name).alias(name) for name in AMOUNTS), _held=pl.len()
    )
    grid = (
        summed.select(ROW_KEYS)
        .unique()
        .join(pl.LazyFrame({"method": methods}), how="cross")
    )
    held = pl.col("_held").is_not_null()
    return (
        grid.join(summed, on=KEYS, how="left", nulls_equal=True)
        .with_columns(
            pl.when(held).then(pl.col(name)).otherwise(0.0).alias(name)
            for name in AMOUNTS
        )
        .drop("_held")
        .with_columns(
            low=pl.col("low").min().over(ROW_KEYS),
            high=pl.col("high").max().over(ROW_KEYS),
        )
        .sort(KEYS, nulls_last=True)
    )


def _totals(
    snapshot: Snapshot, *, spec: Spec, conditions: Sequence[pl.Expr]
) -> pl.LazyFrame:
    """All CRS flows of `spec.flow` by `ROW_KEYS`, plus a row per provider, year and
    Bi_Multi code with a null recipient that sums every recipient, for rows such as
    attribution that have none."""
    flows = snapshot.table("total_flows").filter(
        pl.col("flow") == spec.flow, *conditions
    )
    by_recipient = flows.select(*ROW_KEYS, total="amount_usd")
    all_recipients = (
        flows.group_by("year", "provider_code", "bi_multi")
        .agg(total=ordered_sum(pl.col("amount_usd")))
        .with_columns(recipient_code=pl.lit(None, pl.Int32))
        .select(*ROW_KEYS, "total")
    )
    return convert(
        pl.concat([by_recipient, all_recipients]),
        columns=("total",),
        currency=spec.currency,
        prices=spec.prices,
    )


def _modelled(
    snapshot: Snapshot, *, spec: Spec, conditions: Sequence[pl.Expr]
) -> pl.LazyFrame:
    """The "modelled, not reported" line, shown for recipient-view disbursements."""
    modelled = snapshot.table("modelled").select(
        "year",
        "provider_code",
        "recipient_code",
        "basis",
        low=pl.col("low_usd"),
        high=pl.col("high_usd"),
    )
    if spec.flow != "disbursements" or spec.view != "recipient" or spec.oda_only:
        return modelled.clear()
    return convert(
        modelled.filter(*conditions),
        columns=("low", "high"),
        currency=spec.currency,
        prices=spec.prices,
    ).sort("provider_code", "year", "recipient_code", nulls_last=True)
