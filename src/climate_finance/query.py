"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of three origins:

- `commitment`: a CRDF commitment at face value (`flow="commitments"`). Rows that
  repeat an earlier vintage are left out, and `quality()` reports them.
- `disbursement`: a CRS record's allocation to a counted commitment at face value
  (`allocated_usd`, or for `flow="grant_equivalent"` the record's grant equivalent
  times allocated_usd over the record's disbursement),
  weighted by the markers and climate share it inherits from that commitment. `low`
  and `high` weight the allocation's own bounds, `allocated_low_usd` and
  `allocated_high_usd`, converted to grant equivalents the same way: `low` counts
  what certain links carried (`schema.CERTAIN_METHODS`) to rows reached by certain
  routes (`schema.CERTAIN_ROUTES`), and `high` everything allocated.
- `attribution`: under `view="provider"`, a provider's core contribution of the
  spec's flow to an agency times the agency's climate share. An agency share with no
  split by objective counts in `unsplit`, with the objectives 0. `share_source` says
  where the share comes from: `computed`, `held` or `oecd`.

Under `view="provider"` the method's `eu` setting counts EU Institutions once. As a
channel, the own rows of each EU Institutions agency scale by one minus the share of
its channel's attributed contributions in the row's year that come from the
providers in the query: 1 when the query names none of the contributors, 0 when it
names them all, as an all-provider query does. `quality()` reports the rows an
all-provider query leaves out. As a provider, the EU budget and the EDF attribute
nothing and count in EU Institutions' own rows, while the EIB is a channel: its own
rows leave and contributions to it attribute.

Every row holds `value` = adaptation + mitigation - cross_cutting + unsplit, or
+ cross_cutting under the highest-marker rule, whose objectives are disjoint.
Where the CRDF does not measure adaptation (commitments before 2010, and agency
shares whose span reaches one), adaptation and cross_cutting are null and `value`
is the mitigation value. A table row's adaptation and cross_cutting are null when
any of its rows with an amount has them null.

A disbursement whose record Jev labelled shows `jev_level`, Jev's adaptation level
(0, 1 or 2). Under `relevance="jev"` it replaces the adaptation marker the row
inherits.

The release's commitments are recipient-perspective CRDF rows only, so views filter
on Bi_Multi alone and provider attribution comes only from core contributions.
"""

from __future__ import annotations

import tomllib
from collections.abc import Callable, Iterable, Mapping, Sequence
from importlib.metadata import version
from importlib.resources import files
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import catalogue
from climate_finance.convert import convert, provenance
from climate_finance.errors import MissingLabelsError
from climate_finance.methods import (
    ONE,
    Method,
    face_shares,
    label,
    uses_imputed_shares,
    weights,
)
from climate_finance.result import (
    AMOUNTS,
    ROW_KEYS,
    Meta,
    Quality,
    Result,
    Table,
)
from climate_finance.schema import EIB_AGENCY, EU_INSTITUTIONS, FLOWS, SHARE_ORIGINS
from climate_finance.spec import (
    BILATERAL,
    CORE_CONTRIBUTION,
    MULTILATERAL_OUTFLOW,
    ODA_FLOW_CATEGORY,
    ODA_FLOW_CODES,
    Flow,
    Spec,
    View,
)
from climate_finance.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.snapshot import Snapshot


def _eu_agency_channels() -> dict[int, int]:
    """Each EU Institutions agency's channel, as build/providers.toml maps them."""
    toml = files("climate_finance").joinpath("build", "providers.toml").read_text()
    return {
        agency: row["channel_code"]
        for row in tomllib.loads(toml)["agencies"]
        if row["provider_code"] == EU_INSTITUTIONS
        for agency in row["agency_codes"]
    }


# The channels of EU Institutions by agency: the EU budget (42001), the EDF (42003)
# and the EIB (42004), which stays a channel when EU Institutions count as a provider.
EU_AGENCY_CHANNELS = _eu_agency_channels()
EU_CHANNELS = tuple(sorted(set(EU_AGENCY_CHANNELS.values())))
EIB_CHANNEL = EU_AGENCY_CHANNELS[EIB_AGENCY]
EU_BUDGET_AND_EDF = tuple(code for code in EU_CHANNELS if code != EIB_CHANNEL)
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
    "coefficient_source": pl.String(),
}
# Inputs to the weighting that the rows do not show.
_WEIGHT_INPUTS: dict[str, pl.DataType] = {
    "share_climate": pl.Float64(),
    "share_adaptation": pl.Float64(),
    "share_mitigation": pl.Float64(),
    "share_overlap": pl.Float64(),
    "share_unsplit": pl.Float64(),
    "amount_low": pl.Float64(),
    "amount_high": pl.Float64(),
    "significant": pl.Float64(),
    "principal": pl.Float64(),
    "both_significant": pl.Float64(),
    "adaptation_measured": pl.Boolean(),
}
# The coefficient columns `_coefficients` sets, as `weights` takes them.
_COEFFICIENT_INPUTS = {
    name: pl.col(name) for name in ("significant", "principal", "both_significant")
}


def spending(snapshot: Snapshot, *, spec: Spec) -> Result:
    """Climate finance for `spec` from `snapshot`'s release tables.

    Raises:
        MissingLabelsError: when a method reads Jev's labels and the release's
            `objectives` hold none, as a release built with `jev_labels` off.
    """
    methods = spec.methods
    if any(method.relevance == "jev" for method in methods):
        _require_jev_labels(snapshot)
    named = _conditions(providers=spec.providers, recipients=None, years=None)
    others = _conditions(providers=None, recipients=spec.recipients, years=spec.years)
    filters = [*named, *others]
    if spec.oda_only:
        others = [
            *others,
            pl.any_horizontal(
                pl.when(pl.col("flow_code").is_not_null())
                .then(pl.col("flow_code").is_in(ODA_FLOW_CODES))
                .otherwise(pl.col("flow_category") == ODA_FLOW_CATEGORY),
                pl.col("origin") == "attribution",
            ),
        ]
    conditions = [*named, *others]
    base = _base_rows(snapshot, flow=spec.flow, view=spec.view).filter(*conditions)
    weighted = []
    # Per method label, the part of each EU channel-year's own rows that counts.
    eu_kept: dict[str, pl.LazyFrame] = {}
    for method in methods:
        frame = base
        if spec.view == "provider":
            everyone = _attributed_rows(snapshot, flow=spec.flow, method=method).filter(
                *others
            )
            attributed = everyone.filter(*named)
            kept = eu_kept[label(method)] = _eu_kept(
                attributed=attributed, everyone=everyone
            )
            frame = pl.concat(
                [
                    _count_eu_once(
                        frame,
                        method=method,
                        kept=kept,
                        columns=("amount", "amount_low", "amount_high"),
                    ),
                    attributed,
                ]
            )
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
        table=Table(_aggregate(rows, methods=[label(method) for method in methods])),
        modelled=Table(_modelled(snapshot, spec=spec, conditions=filters)),
        rows=rows,
        totals=_totals(snapshot, spec=spec, conditions=filters, eu_kept=eu_kept),
        spec=spec,
        meta=Meta(
            release=snapshot.manifest.release,
            package_version=version("climate-finance"),
            source_versions=snapshot.manifest.source_versions,
            years=snapshot.manifest.years,
            model_versions=snapshot.manifest.model_versions,
            conversion=provenance(currency=spec.currency, prices=spec.prices),
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
        .filter(_in_view("recipient"), *conditions)
        .select("provider_code", "year", "status", "climate_usd")
    )
    eu_left_out = _eu_left_out(snapshot, years=years, method=method)
    if providers is not None and EU_INSTITUTIONS not in _codes(
        providers, listing=catalogue.providers
    ):
        eu_left_out = eu_left_out.clear()
    return Quality(
        table=Table(table.filter(*conditions)),
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

    def by_provider(flow: Flow) -> pl.LazyFrame:
        attributed = _attributed_rows(snapshot, flow=flow, method=method).filter(
            *conditions
        )
        return (
            _weigh(_coefficients(snapshot, attributed, method=method), method=method)
            .group_by("provider_code", "year")
            .agg(
                ordered_sum(
                    pl.col("value").filter(pl.col("share_source") == origin)
                ).alias(f"{origin}_usd")
                for origin in SHARE_ORIGINS
            )
        )

    return (
        _per_flow(by_provider)
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

    def by_agency(flow: Flow) -> pl.LazyFrame:
        own = _base_rows(snapshot, flow=flow, view="provider").filter(
            pl.col("provider_code") == EU_INSTITUTIONS, *conditions
        )
        attributed = _attributed_rows(snapshot, flow=flow, method=method)
        kept = _eu_kept(attributed=attributed, everyone=attributed)
        left_out = _with_eu_kept(own, kept=kept).filter(pl.col("eu_kept") == 0)
        return (
            _weigh(
                _coefficients(snapshot, left_out.drop("eu_kept"), method=method),
                method=method,
            )
            .group_by("agency_code", "year")
            .agg(value=ordered_sum(pl.col("value")))
        )

    left_out = (
        _per_flow(by_agency)
        .with_columns(
            channel_code=pl.col("agency_code").replace_strict(
                EU_AGENCY_CHANNELS, return_dtype=pl.Int32
            )
        )
        .select("agency_code", "channel_code", "year", "flow", "value")
        .sort("agency_code", "year", "flow")
    )
    return left_out if method.eu == "channel" else left_out.clear()


def _per_flow(build: Callable[[Flow], pl.LazyFrame]) -> pl.LazyFrame:
    """`build`'s frame for each of `FLOWS`, with its `flow`, stacked."""
    return pl.concat(build(flow).with_columns(flow=pl.lit(flow)) for flow in FLOWS)


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


def _base_rows(snapshot: Snapshot, *, flow: Flow, view: View) -> pl.LazyFrame:
    shares = {
        **face_shares(),
        "share_unsplit": pl.lit(0.0),
        "adaptation_measured": pl.col("adaptation_usd").is_not_null(),
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
    record_allocated = allocated.sum().over("record_id")
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


def _require_jev_labels(snapshot: Snapshot) -> None:
    jev = (
        snapshot.table("objectives")
        .filter(pl.col("label_source") == "jev")
        .head(1)
        .collect()
    )
    if jev.is_empty():
        raise MissingLabelsError(
            'relevance="jev" needs Jev labels, and this release holds none: it was '
            "built with jev_labels off"
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


def _attributed_rows(snapshot: Snapshot, *, flow: Flow, method: Method) -> pl.LazyFrame:
    """Core contributions of `flow` times the agency's climate share by objective.

    Under OECD, with either EU setting, the share is the OECD's imputed share. Under
    any other method it is the agency's own climate spending in `agency_spending`,
    weighed with the method's coefficients, so agency shares follow the method's
    coefficients as bilateral rows do. Under provider coefficients agencies take the
    method's coefficients, since `provider_coefficients` holds the providers' own.
    Where `agency_spending` measures no share, the `one` rows of `agency_shares`
    stand in. The adaptation and mitigation shares each include the cross-cutting
    share. A share with no split by objective (a `climate` row) is the unsplit share,
    and the three objective shares are 0. A null adaptation share is not measured,
    and its row counts mitigation alone. An agency without a share attributes
    nothing, and neither do the EU budget and the EDF when the method counts EU
    Institutions as a provider. Each row carries its share's `share_source`:
    `computed` or `held` from `agency_spending`, `oecd` from `agency_shares`.
    """
    source = "oecd" if uses_imputed_shares(method) else "one"
    shares = _fixed_shares(snapshot, source=source)
    if source == "one":
        measured = measured_shares(snapshot.table("agency_spending"), method=method)
        shares = pl.concat(
            [measured, shares.join(measured, on=["channel_code", "year"], how="anti")]
        )
    if method.eu == "provider":
        shares = shares.filter(~pl.col("channel_code").is_in(EU_BUDGET_AND_EDF))
    return _conform(
        snapshot.table("core_contributions")
        .filter(pl.col("flow") == flow)
        .join(shares, on=["channel_code", "year"], how="inner")
        .with_columns(
            pl.col("share_adaptation", "share_overlap").fill_null(0.0),
            adaptation_measured=pl.col("share_adaptation").is_not_null(),
        )
        .with_columns(
            origin=pl.lit("attribution"),
            bi_multi=pl.lit(CORE_CONTRIBUTION, pl.Int8),
            amount=pl.col("contribution_usd"),
            amount_low=pl.col("contribution_usd"),
            amount_high=pl.col("contribution_usd"),
            climate_share=pl.col("share_adaptation")
            + pl.col("share_mitigation")
            - pl.col("share_overlap")
            + pl.col("share_unsplit"),
        )
    )


def _eu_kept(*, attributed: pl.LazyFrame, everyone: pl.LazyFrame) -> pl.LazyFrame:
    """Per EU channel (`_eu_channel`) and year, `eu_kept`: the part of an EU agency's
    own rows that no member's attribution already counts.

    `everyone` holds the attribution rows of every provider and `attributed` those
    of the providers in the query, both of the spec's flow. `eu_kept` is one minus
    the share of the channel's contributions from other providers in `everyone`
    that `attributed` holds: 1 when `attributed` holds none of them, and 0 when it
    holds them all. A channel-year with no attribution has no row.
    """
    keys = ["_eu_channel", "year"]

    def contributed(rows: pl.LazyFrame, name: str) -> pl.LazyFrame:
        return (
            rows.filter(
                pl.col("channel_code").is_in(EU_CHANNELS),
                pl.col("provider_code") != EU_INSTITUTIONS,
            )
            .group_by(pl.col("channel_code").alias("_eu_channel"), "year")
            .agg(ordered_sum(pl.col("amount")).alias(name))
        )

    total, named = pl.col("_total"), pl.col("_named")
    return (
        contributed(everyone, "_total")
        .join(contributed(attributed, "_named"), on=keys, how="left")
        .select(
            *keys,
            eu_kept=pl.when(named == total)
            .then(0.0)
            .when(named.is_null() | (total == 0))
            .then(1.0)
            .otherwise(1 - named / total),
        )
    )


def _with_eu_kept(frame: pl.LazyFrame, *, kept: pl.LazyFrame) -> pl.LazyFrame:
    """`frame` with `eu_kept` from `kept` on each EU Institutions row by its agency's
    channel and year, and 1 on every other row.

    The rule runs per agency, so a year in which the EU budget has no climate share
    keeps the budget's own rows in full even when the EIB's channel attributes.
    """
    channel = pl.col("agency_code").replace_strict(
        EU_AGENCY_CHANNELS, default=None, return_dtype=pl.Int32
    )
    return (
        frame.with_columns(
            _eu_channel=pl.when(pl.col("provider_code") == EU_INSTITUTIONS).then(
                channel
            )
        )
        .join(kept, on=["_eu_channel", "year"], how="left")
        .with_columns(pl.col("eu_kept").fill_null(1.0))
        .drop("_eu_channel")
    )


def _count_eu_once(
    frame: pl.LazyFrame, *, method: Method, kept: pl.LazyFrame, columns: Sequence[str]
) -> pl.LazyFrame:
    """`frame`'s provider-view rows under `method`'s EU setting, with `columns`
    scaled. As a channel, each EU Institutions row's `columns` scale by its
    `eu_kept` from `kept`, and rows at 0 leave. As a provider, the EIB's rows leave.
    """
    if method.eu == "provider":
        return frame.filter(
            pl.col("provider_code").ne_missing(EU_INSTITUTIONS)
            | pl.col("agency_code").ne_missing(EIB_AGENCY)
        )
    share = pl.col("eu_kept")
    return (
        _with_eu_kept(frame, kept=kept)
        .filter(share != 0)
        .with_columns(pl.col(*columns) * share)
        .drop("eu_kept")
    )


def _fixed_shares(snapshot: Snapshot, *, source: str) -> pl.LazyFrame:
    """The `agency_shares` of `source` by channel and year, each the OECD's imputed
    share."""

    def share(objective: str) -> pl.Expr:
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    return (
        snapshot.table("agency_shares")
        .filter(pl.col("source") == source)
        .group_by("channel_code", "year")
        .agg(
            share_adaptation=share("adaptation"),
            share_mitigation=share("mitigation"),
            share_overlap=share("cross_cutting"),
            share_unsplit=share("climate"),
            share_source=pl.lit("oecd"),
        )
    )


def measured_shares(spending: pl.LazyFrame, *, method: Method) -> pl.LazyFrame:
    """Each agency's climate shares by channel and year from `spending`, an
    `agency_spending` table.

    Each row weighs as a commitment of its markers or components would under
    `method`, with its `face_share` as the amount and its marker weights capped at
    its `published` share. Every measured share is split by objective, so the
    unsplit share is 0. The adaptation and cross-cutting shares are null where a row
    is not `adaptation_measured`. Each share keeps its rows' `share_source`.
    """
    face = pl.col("face_share")

    def part(name: str) -> pl.Expr:
        return pl.when(face != 0).then(pl.col(f"{name}_share") / face).otherwise(0.0)

    fractions = weights(method, published=pl.col("published"))
    overlap = fractions["cross_cutting"]
    # The highest-marker rule takes the overlap out of adaptation and mitigation,
    # and the shares include it.
    added = overlap if method.highest_marker else pl.lit(0.0)
    measured = pl.when(pl.col("adaptation_measured").all())
    return (
        spending.with_columns(
            climate_share=pl.col("adaptation_share")
            + pl.col("mitigation_share")
            - pl.col("overlap_share"),
            share_adaptation=part("adaptation"),
            share_mitigation=part("mitigation"),
            share_overlap=part("overlap"),
        )
        .group_by("channel_code", "year")
        .agg(
            share_adaptation=measured.then(
                (face * (fractions["adaptation"] + added)).sum()
            ),
            share_mitigation=(face * (fractions["mitigation"] + added)).sum(),
            share_overlap=measured.then((face * overlap).sum()),
            share_unsplit=pl.lit(0.0),
            share_source=pl.col("share_source").first(),
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
    """Set `significant`, `principal`, `both_significant`, `coefficient_basis` and
    `coefficient_source` for `method`.

    Under provider coefficients each row takes the `provider_coefficients` row of its
    provider, year and agency, or of its provider and year for an agency without a
    row of its own, and `weights` gives an activity significant for both objectives
    the row's `both_significant`. Where the row gives no coefficient, because the
    provider assesses each activity case by case or has no row, the row takes the
    method's.
    `coefficient_basis` is `provider` for the provider's flat coefficients, `default`
    for the default share of a provider that assesses case by case, `case_by_case`
    where such a provider gives no default, and null where the release holds no row.
    """
    if method.coefficients == "fixed":
        return frame.with_columns(
            significant=pl.lit(method.significant),
            principal=pl.lit(method.principal),
            both_significant=pl.lit(None, pl.Float64),
        )
    first, last, year = pl.col("first_year"), pl.col("last_year"), pl.col("year")
    held = (
        snapshot.table("provider_coefficients")
        .join(frame.select("year").unique(), how="cross")
        .filter(first.is_null() | (first <= year), last.is_null() | (year <= last))
        .select(
            "provider_code",
            "agency_code",
            "year",
            _significant="significant",
            _principal="principal",
            _both="both_significant",
            _basis=pl.when(~pl.col("case_by_case"))
            .then(pl.lit("provider"))
            .when(
                pl.col("significant").is_not_null() | pl.col("principal").is_not_null()
            )
            .then(pl.lit("default"))
            .otherwise(pl.lit("case_by_case")),
            _source=pl.format("{}, {}", "source", "locator"),
        )
    )
    columns = ("_significant", "_principal", "_both", "_basis", "_source")
    by_agency = held.filter(pl.col("agency_code").is_not_null())
    by_provider = held.filter(pl.col("agency_code").is_null()).drop("agency_code")
    own_agency = pl.col("_source").is_not_null()

    def pick(name: str) -> pl.Expr:
        return pl.when(own_agency).then(pl.col(name)).otherwise(pl.col(f"{name}_all"))

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
            significant=pl.col("_significant").fill_null(method.significant),
            principal=pl.col("_principal").fill_null(method.principal),
            both_significant=pl.col("_both"),
            coefficient_basis=pl.col("_basis"),
            coefficient_source=pl.col("_source"),
        )
        .drop(*columns, *(f"{name}_all" for name in columns))
    )


def _weigh(frame: pl.LazyFrame, *, method: Method) -> pl.LazyFrame:
    """Weight each row's amount by objective, unsplit and in total under `method`.

    Adaptation and cross_cutting are null where adaptation is not measured."""
    amount = pl.col("amount")
    fractions = weights(
        method, **_COEFFICIENT_INPUTS, published=pl.col("share_climate")
    )
    unsplit = pl.col("share_unsplit")
    climate = fractions["climate"] + unsplit
    measured = pl.col("adaptation_measured").fill_null(True)
    return frame.with_columns(
        method=pl.lit(label(method)),
        adaptation=pl.when(measured).then(amount * fractions["adaptation"]),
        mitigation=amount * fractions["mitigation"],
        cross_cutting=pl.when(measured).then(amount * fractions["cross_cutting"]),
        unsplit=amount * unsplit,
        value=amount * climate,
        low=pl.col("amount_low") * climate,
        high=pl.col("amount_high") * climate,
    ).drop(*_WEIGHT_INPUTS)


def _sum(name: str) -> pl.Expr:
    """The sum of `name` in a fixed order, null when every value is missing."""
    column = pl.col(name)
    return pl.when(column.is_not_null().any()).then(ordered_sum(column))


def _measured_sum(name: str) -> pl.Expr:
    """`_sum(name)`, null when a row with an amount leaves `name` unmeasured."""
    unmeasured = pl.col(name).is_null() & pl.col("amount").is_not_null()
    return pl.when(~unmeasured.any()).then(_sum(name))


def _aggregate(rows: pl.LazyFrame, *, methods: Sequence[str]) -> pl.LazyFrame:
    """Sum rows by `KEYS`, each method's `low` and `high` its own. Adaptation and
    cross_cutting are null where a row leaves them unmeasured.

    Every method in `methods` holds a row for every key any method holds, with zero
    amounts where it counts nothing, so a method that has no row for a key still
    bounds it when `Result` spans the bounds across methods.
    """
    summed = rows.group_by(KEYS).agg(
        *(
            (
                _measured_sum(name)
                if name in ("adaptation", "cross_cutting")
                else _sum(name)
            ).alias(name)
            for name in AMOUNTS
        ),
        _held=pl.len(),
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
        .sort(KEYS, nulls_last=True)
    )


def _totals(
    snapshot: Snapshot,
    *,
    spec: Spec,
    conditions: Sequence[pl.Expr],
    eu_kept: Mapping[str, pl.LazyFrame],
) -> pl.LazyFrame:
    """All CRS flows of `spec.flow` by `ROW_KEYS` and method, plus a row per
    provider, year, Bi_Multi code and method with a null recipient that sums every
    recipient, for rows such as attribution that have none.

    CRDF commitments carry their provider's class, Bi_Multi 1 for a bilateral
    provider, where the CRS splits bilateral flows over codes 1, 3, 7 and 8, so
    commitment totals count those four codes as 1. In the provider view, EU
    Institutions' flows count under each method's EU setting as its own rows do,
    through `_count_eu_once` with the method's `eu_kept`, so a share divides by the
    same rows as its amount.
    """
    flows = snapshot.table("total_flows").filter(
        pl.col("flow") == spec.flow, *conditions
    )
    if spec.flow == "commitments":
        flows = flows.with_columns(
            bi_multi=pl.when(pl.col("bi_multi").is_in(BILATERAL))
            .then(pl.lit(BILATERAL[0], pl.Int8))
            .otherwise(pl.col("bi_multi"))
        )
    by_method = pl.concat(
        (
            _count_eu_once(
                flows,
                method=method,
                kept=eu_kept[label(method)],
                columns=("amount_usd",),
            )
            if spec.view == "provider"
            else flows
        ).with_columns(method=pl.lit(label(method)))
        for method in spec.methods
    )
    total = ordered_sum(pl.col("amount_usd")).alias("total")
    by_recipient = by_method.group_by(KEYS).agg(total)
    all_recipients = (
        by_method.group_by("year", "provider_code", "bi_multi", "method")
        .agg(total)
        .with_columns(recipient_code=pl.lit(None, pl.Int32))
        .select(*KEYS, "total")
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
