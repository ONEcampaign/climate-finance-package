"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of three origins:

- `commitment`: a CRDF commitment at face value (`flow="commitments"`). Rows that
  repeat an earlier vintage are left out, and `quality()` reports them.
- `disbursement`: a CRS record's allocation to a counted commitment at face value
  (`allocated_usd`, or for `flow="grant_equivalent"` the record's grant equivalent
  times allocated_usd over the record's disbursement),
  weighted by the markers and climate share it inherits from that commitment. `low`
  and `high` scale by the allocation's own bounds: `low` counts what certain links
  carried (`schema.CERTAIN_METHODS`) to rows reached by certain routes
  (`schema.CERTAIN_ROUTES`), and `high` everything allocated.
- `attribution`: under `view="provider"`, a provider's core contribution of the
  spec's flow to an agency times the agency's climate share. An agency share with no
  split by objective counts in `unsplit`, with the objectives 0. `share_source` says
  where the share comes from: `computed`, `held` or `oecd`.

Under `view="provider"` the method's `eu` setting counts EU Institutions once. As a
channel, the own rows of each EU Institutions agency leave a result in the years
where that agency's channel attributes another provider's contribution, and
`quality()` reports them. As a provider, the EU channels attribute nothing.

Every row holds `value` = adaptation + mitigation - cross_cutting + unsplit, or
+ cross_cutting under the highest-marker rule, whose objectives are disjoint.

A disbursement whose record Jev labelled shows `jev_level`, Jev's adaptation level
(0, 1 or 2). Under `relevance="jev"` it replaces the adaptation marker the row
inherits.

The release's commitments are recipient-perspective CRDF rows only, so views filter
on Bi_Multi alone and provider attribution comes only from core contributions.
"""

from __future__ import annotations

import tomllib
from collections.abc import Iterable, Sequence
from importlib.metadata import version
from importlib.resources import files
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import catalogue
from climate_finance.convert import convert
from climate_finance.methods import OECD, ONE, Method, face_shares, label, weights
from climate_finance.result import AMOUNTS, Meta, Quality, Result, Table
from climate_finance.schema import FLOWS, SHARE_ORIGINS
from climate_finance.spec import Spec, View

if TYPE_CHECKING:
    from climate_finance.snapshot import Snapshot

BILATERAL = (1, 3, 7, 8)
MULTILATERAL_OUTFLOW = 4
CORE_CONTRIBUTION = 2
# A disbursement's ODA test reads its CRS record's flow code, and a row with no CRS
# record (a commitment or a fill) its commitment's CRDF flow category: grants (11),
# loans (13) and equity (19) form category 10.
ODA_FLOW_CATEGORY = 10
ODA_FLOW_CODES = (11, 13, 19)
EU_INSTITUTIONS = 918


def _eu_agency_channels() -> dict[int, int]:
    """Each EU Institutions agency's channel, as build/providers.toml maps them."""
    toml = files("climate_finance").joinpath("build", "providers.toml").read_text()
    return {
        agency: row["channel_code"]
        for row in tomllib.loads(toml)["agencies"]
        if row["provider_code"] == EU_INSTITUTIONS
        for agency in row["agency_codes"]
    }


# The EU budget (42001), the EDF (42003) and the EIB (42004), by agency.
EU_AGENCY_CHANNELS = _eu_agency_channels()
EU_CHANNELS = tuple(sorted(set(EU_AGENCY_CHANNELS.values())))
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
    "flow_category": pl.Int32(),
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
    "share_source": pl.String(),
    "link_method": pl.String(),
    "link_probability": pl.Float64(),
    "allocation_weight": pl.Float64(),
    "objective_label": pl.String(),
    "label_source": pl.String(),
    "jev_level": pl.Int8(),
    "amount": pl.Float64(),
    "coefficient_basis": pl.String(),
}
# Inputs to the weighting that the rows do not show.
_WEIGHT_INPUTS: dict[str, pl.DataType] = {
    "share_climate": pl.Float64(),
    "share_adaptation": pl.Float64(),
    "share_mitigation": pl.Float64(),
    "share_overlap": pl.Float64(),
    "share_unsplit": pl.Float64(),
    "low_ratio": pl.Float64(),
    "high_ratio": pl.Float64(),
    "significant": pl.Float64(),
}


def spending(snapshot: Snapshot, *, spec: Spec) -> Result:
    """Climate finance for `spec` from `snapshot`'s release tables."""
    methods = spec.methods

    conditions = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    if spec.oda_only:
        conditions.append(
            pl.any_horizontal(
                pl.when(pl.col("flow_code").is_not_null())
                .then(pl.col("flow_code").is_in(ODA_FLOW_CODES))
                .otherwise(pl.col("flow_category") == ODA_FLOW_CATEGORY),
                pl.col("origin") == "attribution",
            )
        )

    def filtered(frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.filter(*conditions) if conditions else frame

    base = filtered(_base_rows(snapshot, spec=spec))
    weighted = []
    for method in methods:
        frame = base
        if spec.view == "provider":
            attributed = filtered(_attributed_rows(snapshot, spec=spec, method=method))
            if method.eu == "channel":
                frame = _mark_eu_left_out(frame, attributed=attributed)
                frame = frame.filter(~pl.col("eu_left_out")).drop("eu_left_out")
            frame = pl.concat([frame, attributed])
        weighted.append(
            _weigh(_coefficients(snapshot, frame, method=method), method=method)
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
    method: Method = ONE,
) -> Quality:
    """Coverage and status statistics.

    The status split covers the commitments the trace follows: bilateral rows and
    multilateral outflows, without core contributions. `eu_left_out` holds the EU
    Institutions rows an all-provider provider view leaves out under `method`.
    """
    years = None if years is None else tuple(years)
    conditions = _conditions(providers=providers, recipients=None, years=years)
    table = snapshot.table("quality")
    status = (
        _counted_commitments(snapshot)
        .filter(_in_view("recipient"))
        .select("provider_code", "year", "status", "climate_usd")
    )
    if conditions:
        table, status = table.filter(*conditions), status.filter(*conditions)
    eu_left_out = _eu_left_out(snapshot, years=years, method=method)
    if providers is not None and EU_INSTITUTIONS not in _codes(
        providers, listing=catalogue.providers
    ):
        eu_left_out = eu_left_out.clear()
    return Quality(
        table=Table(table),
        status=Table(status),
        eu_left_out=Table(eu_left_out),
        attribution=Table(
            _attribution(snapshot, providers=providers, years=years, method=method)
        ),
    )


def _attribution(
    snapshot: Snapshot,
    *,
    providers: Sequence[str | int] | None,
    years: tuple[int, ...] | None,
    method: Method,
) -> pl.LazyFrame:
    """Per provider, year and flow, the value `method` attributes through core
    contributions, in one column per share source, in USD at current prices."""
    conditions = _conditions(providers=providers, recipients=None, years=years)
    parts = []
    for flow in FLOWS:
        spec = Spec(flow=flow, view="provider", method=method)
        attributed = _attributed_rows(snapshot, spec=spec, method=method)
        if conditions:
            attributed = attributed.filter(*conditions)
        parts.append(
            _weigh(_coefficients(snapshot, attributed, method=method), method=method)
            .group_by("provider_code", "year")
            .agg(
                pl.col("value")
                .filter(pl.col("share_source") == origin)
                .sum()
                .alias(f"{origin}_usd")
                for origin in SHARE_ORIGINS
            )
            .with_columns(flow=pl.lit(flow))
        )
    return (
        pl.concat(parts)
        .select("provider_code", "year", "flow", *(f"{o}_usd" for o in SHARE_ORIGINS))
        .sort("provider_code", "year", "flow")
    )


def _eu_left_out(
    snapshot: Snapshot, *, years: tuple[int, ...] | None, method: Method
) -> pl.LazyFrame:
    """Per EU Institutions agency, its channel, year and flow, the value of its own
    rows that a provider view of every provider leaves out under `method`, in USD at
    current prices. Empty when the method counts EU Institutions as a provider."""
    conditions = _conditions(providers=None, recipients=None, years=years)
    parts = []
    for flow in FLOWS:
        spec = Spec(flow=flow, view="provider", method=method, years=years)
        own = _base_rows(snapshot, spec=spec).filter(
            pl.col("provider_code") == EU_INSTITUTIONS, *conditions
        )
        attributed = _attributed_rows(snapshot, spec=spec, method=method)
        left_out = _mark_eu_left_out(own, attributed=attributed).filter("eu_left_out")
        parts.append(
            _weigh(
                _coefficients(snapshot, left_out.drop("eu_left_out"), method=method),
                method=method,
            )
            .group_by("agency_code", "year")
            .agg(value=pl.col("value").sum())
            .with_columns(flow=pl.lit(flow))
        )
    left_out = (
        pl.concat(parts)
        .with_columns(
            channel_code=pl.col("agency_code").replace_strict(
                EU_AGENCY_CHANNELS, return_dtype=pl.Int32
            )
        )
        .select("agency_code", "channel_code", "year", "flow", "value")
        .sort("agency_code", "year", "flow")
    )
    return left_out if method.eu == "channel" else left_out.clear()


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
    """Commitments that count: every row except repeats of an earlier vintage.

    The release leaves out aggregate rows rebuilt into children, so their children's
    `parent_row_id` names a published CRDF row outside the release and nothing joins
    on it.
    """
    return snapshot.table("commitments").filter(pl.col("repeat_of").is_null())


def _in_view(view: View) -> pl.Expr:
    """Rows of the Bi_Multi codes that `view` counts."""
    codes = BILATERAL if view == "provider" else (*BILATERAL, MULTILATERAL_OUTFLOW)
    return pl.col("bi_multi").is_in(codes)


def _base_rows(snapshot: Snapshot, *, spec: Spec) -> pl.LazyFrame:
    shares = {**face_shares(), "share_unsplit": pl.lit(0.0)}
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

    commitments = _counted_commitments(snapshot).select(
        "row_id",
        "flow_category",
        "status",
        "face_usd",
        "climate_usd",
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
    amount = allocated
    if spec.flow == "grant_equivalent":
        record = pl.col("record_disbursement_usd")
        amount = (
            pl.when(record != 0)
            .then(pl.col("record_grant_equiv_usd") * allocated / record)
            .otherwise(None)
        )
    return _conform(
        disbursements.with_columns(
            origin=pl.lit("disbursement"),
            amount=amount,
            rio_adaptation=pl.col("inherited_rio_adaptation"),
            rio_mitigation=pl.col("inherited_rio_mitigation"),
            **shares,
        )
    )


def _labels(snapshot: Snapshot) -> pl.LazyFrame:
    """Per record, the highest-marker objective from its first-ranked label source,
    and the level of its Jev adaptation label."""
    rank = pl.col("label_source").replace_strict(LABEL_PRECEDENCE, return_dtype=pl.Int8)

    def level(objective: str) -> pl.Expr:
        return pl.col("level").filter(pl.col("objective") == objective).max()

    a, m, c = pl.col("a"), pl.col("m"), pl.col("c")
    objectives = snapshot.table("objectives")
    return (
        objectives.filter(rank == rank.min().over("record_id"))
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
        .join(
            objectives.filter(
                pl.col("label_source") == "jev", pl.col("objective") == "adaptation"
            ).select("record_id", jev_level="level"),
            on="record_id",
            how="left",
            validate="1:1",
        )
    )


def _attributed_rows(snapshot: Snapshot, *, spec: Spec, method: Method) -> pl.LazyFrame:
    """Core contributions of `spec.flow` times the agency's climate share by objective.

    Under OECD the share is the OECD's imputed share. Under any other method it is
    the agency's own climate spending. The adaptation and mitigation shares each
    include the cross-cutting share. A share with no split by objective (a `climate`
    row) is the unsplit share, and the three objective shares are 0. An agency
    without a share attributes nothing, and neither do the EU channels when the
    method counts EU Institutions as a provider. Each row carries its share's
    `share_source`, which `release_invariants` holds to one per channel, year and
    source.
    """
    source = "oecd" if method == OECD else "one"
    channels = snapshot.table("agency_shares").filter(pl.col("source") == source)
    if method.eu == "provider":
        channels = channels.filter(~pl.col("channel_code").is_in(EU_CHANNELS))

    def share(objective: str) -> pl.Expr:
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    shares = channels.group_by("channel_code", "year").agg(
        share_adaptation=share("adaptation"),
        share_mitigation=share("mitigation"),
        share_overlap=share("cross_cutting"),
        share_unsplit=share("climate"),
        share_source=pl.col("share_source").first(),
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
            - pl.col("share_overlap")
            + pl.col("share_unsplit"),
            low_ratio=pl.lit(1.0),
            high_ratio=pl.lit(1.0),
        )
    )


def _mark_eu_left_out(frame: pl.LazyFrame, *, attributed: pl.LazyFrame) -> pl.LazyFrame:
    """`frame` with `eu_left_out`, true on an EU Institutions row whose agency's
    channel holds another provider's contribution in `attributed` in the row's year,
    which already counts that money. `attributed` carries one flow, the spec's.

    The rule runs per agency, so a year in which the EU budget has no climate share
    keeps the budget's own rows even when the EIB's channel attributes.
    """
    attributing = (
        attributed.filter(
            pl.col("channel_code").is_in(EU_CHANNELS),
            pl.col("provider_code") != EU_INSTITUTIONS,
        )
        .select("year", _eu_channel="channel_code", eu_left_out=pl.lit(True))
        .unique()
    )
    channel = pl.col("agency_code").replace_strict(
        EU_AGENCY_CHANNELS, default=None, return_dtype=pl.Int32
    )
    return (
        frame.with_columns(
            _eu_channel=pl.when(pl.col("provider_code") == EU_INSTITUTIONS).then(
                channel
            )
        )
        .join(attributing, on=["_eu_channel", "year"], how="left")
        .with_columns(pl.col("eu_left_out").fill_null(False))
        .drop("_eu_channel")
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
    basis. Any other provider takes the method's, with a null basis.
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
    """Weight each row's amount by objective, unsplit and in total under `method`."""
    amount = pl.col("amount")
    fractions = weights(
        method, significant=pl.col("significant"), published=pl.col("share_climate")
    )
    unsplit = pl.col("share_unsplit")
    return (
        frame.with_columns(
            method=pl.lit(label(method)),
            adaptation=amount * fractions["adaptation"],
            mitigation=amount * fractions["mitigation"],
            cross_cutting=amount * fractions["cross_cutting"],
            unsplit=amount * unsplit,
            value=amount * (fractions["climate"] + unsplit),
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
    """Sum rows by `KEYS`. `low` and `high` then span every method for the same key."""
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
    as attribution that have none.

    CRDF commitments carry their provider's class, Bi_Multi 1 for a bilateral
    provider, where the CRS splits bilateral flows over codes 1, 3, 7 and 8, so
    commitment totals count those four codes as 1.
    """
    keys = ["year", "provider_code", "recipient_code", "bi_multi"]
    conditions = _conditions(
        providers=spec.providers, recipients=spec.recipients, years=spec.years
    )
    flows = snapshot.table("total_flows").filter(
        pl.col("flow") == spec.flow, *conditions
    )
    if spec.flow == "commitments":
        flows = flows.with_columns(
            bi_multi=pl.when(pl.col("bi_multi").is_in(BILATERAL))
            .then(pl.lit(BILATERAL[0], pl.Int8))
            .otherwise(pl.col("bi_multi"))
        )
    by_recipient = flows.group_by(keys).agg(total=pl.col("amount_usd").sum())
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
