"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of three origins:

- `commitment`: a CRDF commitment at face value (`flow="commitments"`). Repeats of an
  earlier vintage, and aggregate rows rebuilt into child rows, are left out.
- `disbursement`: a CRS record's allocation to a commitment at face value
  (`allocated_usd`, or its pro-rata `grant_equiv_usd` for `flow="grant_equivalent"`),
  weighted by the markers and climate share it inherits from that commitment. `low`
  and `high` scale by the allocation's own bounds.
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
from climate_finance.convert import convert
from climate_finance.methods import OECD, Method, label, weights
from climate_finance.result import AMOUNTS, Meta, Quality, Result, Table
from climate_finance.spec import Spec, View

if TYPE_CHECKING:
    from climate_finance.snapshot import Snapshot

BILATERAL = (1, 3, 7, 8)
MULTILATERAL_OUTFLOW = 4
CORE_CONTRIBUTION = 2
ODA_FLOW_CODES = (11, 13, 19)
# The objective label a record shows when several sources label it: the bank's own
# climate components first, then Jev, then the donor's markers.
LABEL_PRECEDENCE = {"crdf": 0, "jev": 1, "marker": 2}

KEYS = ("year", "provider_code", "recipient_code", "bi_multi", "method")
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
}
# Inputs to the weighting that the rows do not show.
_WEIGHT_INPUTS: dict[str, pl.DataType] = {
    "share_adaptation": pl.Float64(),
    "share_mitigation": pl.Float64(),
    "share_overlap": pl.Float64(),
    "low_ratio": pl.Float64(),
    "high_ratio": pl.Float64(),
    "significant": pl.Float64(),
}


def spending(snapshot: Snapshot, *, spec: Spec) -> Result:
    """Climate finance for `spec` from `snapshot`'s release tables."""
    methods = spec.methods
    if any(method.relevance == "jev" for method in methods):
        raise NotImplementedError("relevance='jev' needs jev_level_map: slice S18b")

    conditions = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    if spec.oda_only:
        conditions.append(
            pl.col("flow_code").is_in(ODA_FLOW_CODES)
            | (pl.col("origin") == "attribution")
        )
    base = _base_rows(snapshot, spec=spec)
    weighted = []
    for method in methods:
        frames = [base]
        if spec.view == "provider":
            frames.append(_attributed_rows(snapshot, spec=spec, method=method))
        for frame in frames:
            filtered = frame.filter(*conditions) if conditions else frame
            weighted.append(
                _weigh(_coefficients(snapshot, filtered, method=method), method=method)
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
        table=Table(_aggregate(rows)),
        modelled=Table(_modelled(snapshot, spec=spec)),
        rows=rows,
        totals=_totals(snapshot, spec=spec),
        spec=spec,
        meta=Meta(
            release=snapshot.manifest.release,
            package_version=version("climate-finance"),
            source_versions=snapshot.manifest.source_versions,
            model_versions=snapshot.manifest.model_versions,
        ),
    )


def quality(
    snapshot: Snapshot,
    *,
    providers: Sequence[str | int] | None = None,
    years: Iterable[int] | None = None,
) -> Quality:
    """Coverage and status statistics; the precision interval comes from the gate.

    The status split covers the commitments the trace follows: bilateral rows and
    multilateral outflows, without core contributions.
    """
    conditions = _conditions(providers=providers, recipients=None, years=years)
    table = snapshot.table("quality")
    status = (
        _counted_commitments(snapshot)
        .filter(_in_view("recipient"))
        .select("provider_code", "year", "status", "climate_usd")
    )
    if conditions:
        table, status = table.filter(*conditions), status.filter(*conditions)
    return Quality(table=Table(table), status=Table(status))


def _codes(values: Sequence[str | int], *, listing: catalogue.Catalogue) -> list[int]:
    """DAC codes for `values`: codes pass through, names resolve through `listing`."""
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


def _base_rows(snapshot: Snapshot, *, spec: Spec) -> pl.LazyFrame:
    shares = {
        "share_adaptation": _share("adaptation_usd"),
        "share_mitigation": _share("mitigation_usd"),
        "share_overlap": _share("overlap_usd"),
    }
    if spec.flow == "commitments":
        commitments = _counted_commitments(snapshot).filter(_in_view(spec.view))
        return _conform(
            commitments.with_columns(
                origin=pl.lit("commitment"),
                amount=pl.col("face_usd"),
                low_ratio=pl.lit(1.0),
                high_ratio=pl.lit(1.0),
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
    disbursements = (
        snapshot.table("disbursements")
        .with_columns(
            allocation_weight=allocated / allocated.sum().over("record_id"),
            low_ratio=pl.when(allocated > 0)
            .then(pl.col("allocated_low_usd") / allocated)
            .otherwise(1.0),
            high_ratio=pl.when(allocated > 0)
            .then(pl.col("allocated_high_usd") / allocated)
            .otherwise(1.0),
        )
        .join(commitments, on="row_id", how="inner", validate="m:1")
        .filter(_in_view(spec.view))
        .join(_labels(snapshot), on="record_id", how="left")
    )
    amount = "grant_equiv_usd" if spec.flow == "grant_equivalent" else "allocated_usd"
    return _conform(
        disbursements.with_columns(
            origin=pl.lit("disbursement"),
            amount=pl.col(amount),
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


def _attributed_rows(snapshot: Snapshot, *, spec: Spec, method: Method) -> pl.LazyFrame:
    """Core contributions of `spec.flow` times the agency's climate share by objective.

    Under OECD the share is the OECD's imputed share; under any other method it is
    the agency's own climate spending. The adaptation and mitigation shares each
    include the cross-cutting share. An agency without a share attributes nothing.
    """
    source = "oecd" if method == OECD else "one"

    def share(objective: str) -> pl.Expr:
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    shares = (
        snapshot.table("agency_shares")
        .filter(pl.col("source") == source)
        .group_by("channel_code", "year")
        .agg(
            share_adaptation=share("adaptation"),
            share_mitigation=share("mitigation"),
            share_overlap=share("cross_cutting"),
        )
    )
    return _conform(
        snapshot.table("core_contributions")
        .filter(pl.col("flow") == spec.flow)
        .join(shares, on=["channel_code", "year"], how="inner")
        .with_columns(
            origin=pl.lit("attribution"),
            bi_multi=pl.lit(CORE_CONTRIBUTION, pl.Int8),
            amount=pl.col("contribution_usd"),
            climate_share=pl.col("share_adaptation")
            + pl.col("share_mitigation")
            - pl.col("share_overlap"),
            low_ratio=pl.lit(1.0),
            high_ratio=pl.lit(1.0),
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
    """Set `significant` and `coefficient_basis` for `method`.

    Under provider coefficients a provider with a confirmed row in
    `provider_coefficients` takes its own significant coefficient and that row's
    basis; any other provider takes the method's, with a null basis.
    """
    if method.coefficients == "fixed":
        return frame.with_columns(significant=pl.lit(method.significant))
    own = (
        snapshot.table("provider_coefficients")
        .filter(pl.col("confirmed"))
        .select("provider_code", _own="significant", _basis="basis")
    )
    return (
        frame.join(own, on="provider_code", how="left", validate="m:1")
        .with_columns(
            significant=pl.col("_own").fill_null(method.significant),
            coefficient_basis=pl.col("_basis"),
        )
        .drop("_own", "_basis")
    )


def _weigh(frame: pl.LazyFrame, *, method: Method) -> pl.LazyFrame:
    amount = pl.col("amount")
    fractions = weights(method, significant=pl.col("significant"))
    return (
        frame.with_columns(
            method=pl.lit(label(method)),
            adaptation=amount * fractions["adaptation"],
            mitigation=amount * fractions["mitigation"],
            cross_cutting=amount * fractions["cross_cutting"],
            value=amount * fractions["climate"],
        )
        .with_columns(
            low=pl.col("value") * pl.col("low_ratio"),
            high=pl.col("value") * pl.col("high_ratio"),
        )
        .drop(*_WEIGHT_INPUTS)
    )


def _sum(name: str) -> pl.Expr:
    """The sum of `name`, null when every value is missing."""
    return pl.when(pl.col(name).is_not_null().any()).then(pl.col(name).sum())


def _aggregate(rows: pl.LazyFrame) -> pl.LazyFrame:
    """Sum rows by `KEYS`; `low` and `high` then span every method for the same key."""
    across_methods = [key for key in KEYS if key != "method"]
    return (
        rows.group_by(KEYS)
        .agg(_sum(name).alias(name) for name in AMOUNTS)
        .with_columns(
            low=pl.col("low").min().over(across_methods),
            high=pl.col("high").max().over(across_methods),
        )
        .sort(KEYS, nulls_last=True)
    )


def _totals(snapshot: Snapshot, *, spec: Spec) -> pl.LazyFrame:
    """All CRS flows of `spec.flow` by the table's keys, plus a row per provider, year
    and Bi_Multi code with a null recipient that sums every recipient, for rows such
    as attribution that have none."""
    keys = ["year", "provider_code", "recipient_code", "bi_multi"]
    conditions = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    flows = snapshot.table("total_flows").filter(
        pl.col("flow") == spec.flow, *conditions
    )
    by_recipient = flows.select(*keys, total="amount_usd")
    all_recipients = (
        flows.group_by("year", "provider_code", "bi_multi")
        .agg(total=pl.col("amount_usd").sum())
        .with_columns(recipient_code=pl.lit(None, pl.Int32))
        .select(*keys, "total")
    )
    return convert(
        pl.concat([by_recipient, all_recipients]),
        columns=("total",),
        currency=spec.currency,
        prices=spec.prices,
    )


def _modelled(snapshot: Snapshot, *, spec: Spec) -> pl.LazyFrame:
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
    conditions = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    if conditions:
        modelled = modelled.filter(*conditions)
    return convert(
        modelled, columns=("low", "high"), currency=spec.currency, prices=spec.prices
    ).sort("provider_code", "year", "recipient_code", nulls_last=True)
