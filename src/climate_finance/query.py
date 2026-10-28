"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of six origins:

- `commitment`: a CRDF commitment at face value (`flow="commitments"`). Rows that
  repeat an earlier vintage are left out, and `quality()` reports them.
- `disbursement`: a CRS record's allocation to a counted commitment at face value
  (`allocated_usd`, or for `flow="grant_equivalent"` the record's grant equivalent
  times allocated_usd over the record's disbursement, which for a grant the CRS
  gives none is its disbursement, as `normalise_crs` builds it),
  weighted by the markers and climate share it inherits from that commitment. `low`
  weights the allocation's low bound, `allocated_low_usd`, converted to grant
  equivalents the same way, and counts what certain links carried
  (`schema.CERTAIN_METHODS`) to rows reached by certain routes
  (`schema.CERTAIN_ROUTES`). `high` is the value, everything allocated.
- `untraced`: a Rio-marked CRS record's disbursement that reaches no commitment
  (`untraced_usd`), the excess beyond a full commitment or the whole of a record
  linked to none, converted to grant equivalents as a disbursement is. It weighs by
  the record's own markers, with no published share to cap them. It counts in
  `high` and not in `low`, which holds what certain links carry to commitments.
  `spec.untraced=False` leaves these rows out.
- `repayment` and `interest`: under `flow="net_disbursements"` and
  `flow="net_transfers"`, the negative of a CRS repayment's principal, and under
  `net_transfers` of its interest too, on the parts of its loan that gross counts
  (`reflows` basis `commitment`, and with `spec.untraced` basis `untraced`). A
  `commitment` part weighs as a disbursement allocated to that commitment does; an
  `untraced` part as an untraced row, by the disbursement records' own markers.
  `amount` subtracts the whole part. Of the whole part and what certain links
  carried (`received_low_usd`, `interest_low_usd`), `amount_low` subtracts the
  larger and `amount_high` the smaller, so each row keeps low <= value <= high
  whatever the sign. For a positive repayment `high` sits above `value`. Every other
  basis is unattributed and counts nowhere.
- `attribution`: under `view="provider"`, a provider's core contribution of the
  spec's flow to an agency times the agency's climate share, of disbursements for a
  net flow, since core contributions are not repaid. An agency share with no
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
+ cross_cutting under the highest-marker rule, whose objectives are disjoint, and
`untraced`, its value on an untraced row or a repayment's `untraced` part and 0 on
any other. Under `flow="grant_equivalent"` a disbursement or untraced row whose
record has no grant equivalent, a loan or a fill, counts 0 in `amount`, `value`,
`low` and `high`, and `no_grant_equivalent` holds its value at face; it is 0 on
every other row and under every other flow.
Where the row's adaptation marker is missing (commitments before 2010, the
disbursements and repayments of those commitments, untraced records and their
repayments whose CRS record carries no adaptation marker, as none does before 2010,
and agency shares whose span reaches one), adaptation and cross_cutting count 0,
`value` is the mitigation value, and `no_adaptation_marker` holds that value. It is
0 on every other row. So the objectives add up to `value` on every row, and
`no_adaptation_marker` says how much of it has no adaptation split.

A disbursement or untraced row whose record Jev labelled shows `jev_level`, Jev's
adaptation level (0, 1 or 2), where the adaptation marker it weighs by is 1 or 2:
the one a disbursement inherits, or an untraced row's own. Under `relevance="jev"`
it replaces that marker. A label belongs to the record, and Jev reads only records
that marker places among the human-coded adaptation projects, so a record's rows
under another marker, or 0, keep their marker.

The release's commitments are recipient-perspective CRDF rows only, so views filter
on Bi_Multi alone and provider attribution comes only from core contributions.
"""

from __future__ import annotations

import tomllib
import warnings
from collections.abc import Callable, Iterable, Mapping, Sequence
from importlib.metadata import version
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from climate_finance.catalogue import Agency, agency_name, name_of, with_names
from climate_finance.convert import convert, provenance, target_factors
from climate_finance.errors import (
    CoverageWarning,
    DoubleCountWarning,
    MissingLabelsError,
    ReleaseIntegrityError,
    SchemaError,
)
from climate_finance.methods import (
    ONE,
    PROVIDER,
    Method,
    MethodRange,
    face_shares,
    label,
    uses_imputed_shares,
    weights,
)
from climate_finance.result import (
    AGENCY_ROW_KEYS,
    AMOUNTS,
    ROW_KEYS,
    Meta,
    Quality,
    Reconciliation,
    Result,
    _listing,
    named_keys,
    year_runs,
)
from climate_finance.schema import (
    BILATERAL,
    COMMITMENT_KEY,
    CORE_CONTRIBUTION,
    EIB_AGENCY,
    EU_INSTITUTIONS,
    FLOWS,
    FUNDING_CLASSES,
    NET_FLOWS,
    RECIPIENT_VIEW,
    REFLOW_BASES,
    SHARE_ORIGINS,
    coefficient_overlaps,
)
from climate_finance.spec import (
    ODA_FLOW_CATEGORY,
    ODA_FLOW_CODES,
    Flow,
    Spec,
    View,
    provider_rows,
)
from climate_finance.sums import ordered_sum

if TYPE_CHECKING:
    from climate_finance.catalogue import Target
    from climate_finance.release import Release


def _eu_agency_channels() -> dict[int, int]:
    """Each EU Institutions agency's channel, as build/providers.toml maps them."""
    toml = (
        files("climate_finance")
        .joinpath("build", "providers.toml")
        .read_text(encoding="utf-8")
    )
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
# Warnings point at the caller's line, outside the package.
_PACKAGE_DIR = str(Path(__file__).parent)
# The objective label a record shows when several sources label it: the bank's own
# climate components first, then Jev, then the donor's markers.
LABEL_PRECEDENCE = {"crdf": 0, "jev": 1, "marker": 2}

# The first columns of `Result.rows()`, which show the record and its link; the
# amounts come last.
ROW_FIRST = (
    "origin", "year", "record_id", "link_method", "provider", "agency", "recipient",
    "method",
)  # fmt: skip
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
    "reason": pl.String(),
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
    # True on a row whose value counts in `untraced`.
    "untraced_part": pl.Boolean(),
    # Under `flow="grant_equivalent"`, the face value of a row whose record has no
    # grant equivalent, which counts in `no_grant_equivalent`; null on other rows.
    "no_grant_equivalent_usd": pl.Float64(),
}
# The coefficient columns `_coefficients` sets, as `weights` takes them.
_COEFFICIENT_INPUTS = {
    name: pl.col(name) for name in ("significant", "principal", "both_significant")
}


def spending(release: Release, *, spec: Spec) -> Result:
    """Climate finance for `spec` from the tables of `release`.

    Raises:
        MissingLabelsError: when a method reads Jev's labels and the release's
            `objectives` hold none, as a release built with `jev_labels` off.

    Warns:
        CoverageWarning: for named providers with no rows the query counts
            (`_warn_absent_providers`).
        DoubleCountWarning: for a provider view whose EU Institutions rows leave
            out the EU money of the members the query names (`_warn_eu_netted`).
    """
    methods = spec.methods
    if any(method.relevance == "jev" for method in methods):
        _require_jev_labels(release)
    _warn_absent_providers(release, spec=spec)
    factors = target_factors(
        release.table("conversion_factors"),
        release=release.manifest.release,
        currency=spec.currency,
        prices=spec.prices,
    )
    named = _conditions(providers=spec.providers, recipients=None, years=None)
    others = _conditions(providers=None, recipients=spec.recipients, years=spec.years)
    filters = [*named, *others]
    # `modelled` holds no agency, so an agency carries no modelled line.
    codes = None
    if spec.providers is not None:
        codes = tuple(code for code in spec.providers if isinstance(code, int))
    modelled = [*_conditions(providers=codes, recipients=None, years=None), *others]
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
    base = _base_rows(
        release, flow=spec.flow, view=spec.view, untraced=spec.untraced
    ).filter(*conditions)
    weighted = []
    # Per method label, the part of each EU channel-year's own rows that counts.
    eu_kept: dict[str, pl.LazyFrame] = {}
    for method in methods:
        frame = base
        if spec.view == "provider":
            everyone = _attributed_rows(
                release, flow=_gross(spec.flow), method=method
            ).filter(*others)
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
                        columns=(
                            "amount",
                            "amount_low",
                            "amount_high",
                            "no_grant_equivalent_usd",
                        ),
                    ),
                    attributed,
                ]
            )
        weighted.append(
            _weigh(_coefficients(release, frame, method=method), method=method)
        )
    _warn_eu_netted(spec, eu_kept=eu_kept)

    rows = convert(
        pl.concat(weighted),
        columns=AMOUNTS,
        factors=factors,
        release=release.manifest.release,
    ).sort(
        "method", "origin", "provider_code", "year", "row_id", "record_id",
        "channel_code", nulls_last=True,
    )  # fmt: skip
    rows = with_names(rows).select(
        *ROW_FIRST, pl.exclude(*ROW_FIRST, *AMOUNTS), *AMOUNTS
    )
    agency = _agency_key(spec.providers)
    return Result(
        table=_aggregate(
            rows, methods=[label(method) for method in methods], agency=agency
        ),
        modelled=_modelled(release, spec=spec, conditions=modelled, factors=factors),
        rows=rows,
        totals=_totals(
            release,
            spec=spec,
            conditions=filters,
            eu_kept=eu_kept,
            agency=agency,
            factors=factors,
        ),
        spec=spec,
        meta=Meta(
            release=release.manifest.release,
            package_version=version("climate-finance"),
            build=release.manifest.build,
            source_versions=release.manifest.source_versions,
            years=release.manifest.years,
            model_versions=release.manifest.model_versions,
            conversion=provenance(
                factors,
                table_sha256=release.manifest.tables["conversion_factors"].sha256,
            ),
        ),
    )


def _warn_absent_providers(release: Release, *, spec: Spec) -> None:
    """Warn once, naming each provider or agency in `spec.providers` that has no
    rows of the spec's flow and years in its view, and why.

    A provider has rows in the flow's tables: commitments, or disbursements, with
    `spec.untraced` untraced rows, and for a net flow the `reflows` parts the flow
    subtracts (`_COUNTED_BASES`, `untraced` only with `spec.untraced`). It has none
    when the release holds none of its rows in those years, or holds them only under
    Bi_Multi codes the view leaves out, such as an agency's own outflows (code 4) in
    the provider view, or a private donor's (code 6) in either view. The EIB's own
    rows leave the provider view under methods that count EU Institutions as a
    provider. In the provider view a provider's core contributions count only where
    they survive the share join of `_attributed_rows` under one of the spec's
    methods, so contributions to channels with no climate share in those years give
    no rows and say so. Recipients and `oda_only` narrow a query without a warning,
    since a provider may give nothing to a recipient.
    """
    if spec.providers is None:
        return
    table = "commitments" if spec.flow == "commitments" else "disbursements"
    columns = ("provider_code", "agency_code", "bi_multi", "year")
    rows = [
        (
            _counted_commitments(release)
            if table == "commitments"
            else release.table(table)
        ).select(columns)
    ]
    if table == "disbursements" and spec.untraced:
        rows.append(release.table("untraced").select(columns))
    if spec.flow in NET_FLOWS:
        bases = _COUNTED_BASES if spec.untraced else ("commitment",)
        rows.append(
            release.table("reflows")
            .filter(pl.col("basis").is_in(bases))
            .select(columns)
        )
    contributors: set[int] = set()
    if spec.view == "provider":
        rows.extend(
            _attributed_rows(release, flow=_gross(spec.flow), method=method).select(
                columns
            )
            for method in spec.methods
        )
        # A contribution carries no agency, so only a provider target has any.
        provider_codes = tuple(t for t in spec.providers if isinstance(t, int))
        contributors = set(
            release.table("core_contributions")
            .filter(
                pl.col("flow") == _gross(spec.flow),
                *_conditions(
                    providers=provider_codes, recipients=None, years=spec.years
                ),
            )
            .select("provider_code")
            .unique()
            .collect()["provider_code"]
        )
    held = (
        pl.concat(rows, how="vertical_relaxed")
        .filter(
            *_conditions(providers=spec.providers, recipients=None, years=spec.years)
        )
        .select("provider_code", "agency_code", "bi_multi")
        .unique()
        .collect()
    )
    counted = _VIEW_CODES[spec.view]
    other = "recipient" if spec.view == "provider" else "provider"
    eib_left = spec.view == "provider" and all(
        method.eu == "provider" for method in spec.methods
    )
    absent = []
    for target in spec.providers:
        name = name_of(target, kind="providers")
        codes = set(held.filter(provider_rows((target,)))["bi_multi"])
        if not codes & counted and target in contributors:
            absent.append(
                f"{name}, whose core contributions go to channels with no climate "
                "share in those years"
            )
        elif not codes:
            years = f" in {year_runs(spec.years)}" if spec.years else ""
            held_text = (
                "disbursements or repayments" if spec.flow in NET_FLOWS else table
            )
            absent.append(f"{name}, which has no {held_text} in the release{years}")
        elif not codes & counted:
            held_codes = _listing(sorted(codes))
            where = (
                f"view={other!r}"
                if codes & _VIEW_CODES[other]
                else "neither view, which count official flows only"
            )
            absent.append(
                f"{name}, whose rows (Bi_Multi {held_codes}) count in {where}"
            )
        elif eib_left and target == Agency(provider=EU_INSTITUTIONS, agency=EIB_AGENCY):
            absent.append(
                f"{name}, whose own rows leave the provider view under methods that "
                "count EU Institutions as a provider, such as OECD, as the EIB is a "
                "channel there"
            )
    if absent:
        warnings.warn(
            f"no rows count for {'; '.join(absent)}",
            CoverageWarning,
            skip_file_prefixes=(_PACKAGE_DIR,),
        )


# The Bi_Multi codes each view counts, core contributions as attributed rows.
_VIEW_CODES = {
    "recipient": set(RECIPIENT_VIEW),
    "provider": {*BILATERAL, CORE_CONTRIBUTION},
}


def _warn_eu_netted(spec: Spec, *, eu_kept: Mapping[str, pl.LazyFrame]) -> None:
    """Warn when a provider view names EU Institutions or an EU agency, and some
    member outside the query attributes EU money that its own rows keep.

    Under a method that counts EU Institutions as a channel, their own rows count
    only the EU money that no member in the query attributes, so the figure
    depends on which members a query names, and results of separate queries count
    EU money twice when added.
    """
    if spec.view != "provider" or spec.providers is None:
        return
    named = any(
        (target.provider if isinstance(target, Agency) else target) == EU_INSTITUTIONS
        for target in spec.providers
    )
    channel = [eu_kept[label(m)] for m in spec.methods if m.eu == "channel"]
    if not named or not channel:
        return
    kept = pl.concat(channel).filter(pl.col("eu_kept") > 0).head(1)
    if kept.collect().is_empty():
        return
    warnings.warn(
        "EU Institutions' provider-view rows count only the EU money that no "
        "member in this query attributes through its core contributions, so their "
        "figure depends on which members a query names, and adding provider-view "
        "results of separate queries counts EU money twice. Name the providers in "
        "one query to count it once.",
        DoubleCountWarning,
        skip_file_prefixes=(_PACKAGE_DIR,),
    )


def quality(
    release: Release,
    *,
    providers: tuple[int, ...] | None = None,
    years: tuple[int, ...] | None = None,
    method: Method = ONE,
) -> Quality:
    """Coverage, status and precision statistics.

    The status split covers the commitments the trace follows: bilateral rows and
    multilateral outflows, without core contributions. `eu_left_out` holds the EU
    Institutions rows an all-provider provider view leaves out under `method`,
    `untraced` the Rio-marked CRS disbursement that reaches no commitment, and
    `repayments` the CRS repayments by where their principal goes.
    """
    conditions = _conditions(providers=providers, recipients=None, years=years)
    table = release.table("quality")
    status = (
        _counted_commitments(release)
        .filter(_in_view("recipient"), *conditions)
        .select("provider_code", "year", "status", "climate_usd")
    )
    eu_left_out = _eu_left_out(release, years=years, method=method)
    if providers is not None and EU_INSTITUTIONS not in providers:
        eu_left_out = eu_left_out.clear()
    return Quality(
        table=with_names(table.filter(*conditions)),
        status=with_names(status),
        eu_left_out=eu_left_out,
        attribution=with_names(
            _attribution(release, providers=providers, years=years, method=method)
        ),
        precision=with_names(_precision(release, providers=providers)),
        stratum_precision=with_names(_stratum_precision(release, providers=providers)),
        untraced=with_names(
            _untraced(release, conditions=conditions, method=method).sort(
                "provider_code", "year"
            )
        ),
        repayments=with_names(
            _repayments(release, conditions=conditions, method=method).sort(
                "provider_code", "year"
            )
        ),
    )


def jev_flags(
    release: Release,
    *,
    providers: tuple[int, ...] | None = None,
    years: tuple[int, ...] | None = None,
    flagged: str | None = None,
    experimental: bool = False,
) -> pl.DataFrame:
    """The `jev_flags` rows for `providers` and `years`, with names, `in_release` and,
    when `experimental`, `mitigation_aim_p`; `flagged` keeps the rows where that
    flag holds.

    A record is `in_release` when the release's totals draw on it: `disbursements`
    or `untraced` holds it, or it reports a commitment and a counted `commitments`
    row, one that repeats no earlier vintage, shares its `COMMITMENT_KEY` (provider,
    CRS id, year and purpose), the key that ties a CRS commitment to its CRDF row.

    Raises:
        MissingLabelsError: when the release was built with `jev_flags` off.
    """
    if not release.manifest.settings.get("jev_flags"):
        raise MissingLabelsError(
            "jev_flags() needs Jev's flags, and this release holds none: it was "
            "built with jev_flags off"
        )
    conditions = _conditions(providers=providers, recipients=None, years=years)
    if flagged is not None:
        conditions.append(pl.col(flagged))
    in_totals = (
        pl.concat(
            release.table(table).select("record_id")
            for table in ("disbursements", "untraced")
        )
        .unique()
        .with_columns(in_totals=pl.lit(value=True))
    )
    committed = (
        _counted_commitments(release)
        .select(*COMMITMENT_KEY, committed=pl.lit(value=True))
        .unique()
    )
    flags = (
        release.table("jev_flags")
        .filter(*conditions)
        .join(in_totals, on="record_id", how="left", validate="m:1")
        .join(committed, on=COMMITMENT_KEY, how="left", validate="m:1")
        .with_columns(
            in_release=pl.col("in_totals").is_not_null()
            | (
                (pl.col("commitment_usd") > 0).fill_null(value=False)
                & pl.col("committed").is_not_null()
            )
        )
        .drop("in_totals", "committed")
    )
    if not experimental:
        flags = flags.drop("mitigation_aim_p")
    return with_names(flags).sort("provider_code", "year", "record_id").collect()


def _untraced(
    release: Release, *, conditions: Sequence[pl.Expr], method: Method
) -> pl.LazyFrame:
    """Per provider and disbursement year, the release's `unallocated` amounts at
    face value, and the climate value of the untraced excess and unlinked
    disbursement under `method` in the recipient view, in USD at current prices."""
    reason = pl.col("reason")
    value = pl.col("value")
    climate = (
        _weigh(
            _coefficients(
                release,
                _untraced_rows(release, view="recipient", flow="disbursements"),
                method=method,
            ),
            method=method,
        )
        .filter(*conditions)
        .group_by("provider_code", "year")
        .agg(
            excess_climate_usd=ordered_sum(value.filter(reason == "excess")),
            unlinked_climate_usd=ordered_sum(value.filter(reason == "unlinked")),
        )
    )
    key = ["provider_code", "year"]
    return (
        release.table("unallocated")
        .filter(*conditions)
        .join(climate, on=key, how="left", validate="1:1")
        .with_columns(
            pl.col("excess_climate_usd", "unlinked_climate_usd").fill_null(0.0)
        )
    )


# The `reflows` bases a net flow subtracts. Every other basis is unattributed.
_COUNTED_BASES = ("commitment", "untraced")


def _repayments(
    release: Release, *, conditions: Sequence[pl.Expr], method: Method
) -> pl.LazyFrame:
    """Per provider and repayment year, the `reflows` principal at face value in
    total and by basis, the climate value of the principal a net flow subtracts
    under `method` in the recipient view, the interest, and the share of principal
    a net flow subtracts, in USD at current prices."""
    key = ["provider_code", "year"]
    rows = _reflow_rows(release, view="recipient", untraced=True, interest=False)
    climate = (
        _weigh(_coefficients(release, rows, method=method), method=method)
        .filter(*conditions)
        .group_by(key)
        .agg(repaid_climate_usd=ordered_sum(0.0 - pl.col("value")))
    )
    basis = pl.col("basis")
    received = pl.col("received_usd")
    return (
        release.table("reflows")
        .filter(*conditions)
        .group_by(key)
        .agg(
            ordered_sum(received),
            *(
                ordered_sum(received.filter(basis == name)).alias(f"{name}_usd")
                for name in REFLOW_BASES
            ),
            interest_usd=ordered_sum(pl.col("interest_usd")),
            measured_usd=ordered_sum(received.filter(basis.is_in(_COUNTED_BASES))),
        )
        .join(climate, on=key, how="left", validate="1:1")
        .select(
            *key,
            "received_usd",
            *(f"{name}_usd" for name in REFLOW_BASES),
            pl.col("repaid_climate_usd").fill_null(0.0),
            "interest_usd",
            measured_share=pl.when(pl.col("received_usd") != 0).then(
                pl.col("measured_usd") / pl.col("received_usd")
            ),
        )
    )


# A gate row in `release.json`, as `cf-build gate` writes it.
_GATE_ROW = pl.Schema(
    {
        "subject": pl.String(),
        "value": pl.Float64(),
        "lower": pl.Float64(),
        "upper": pl.Float64(),
        "threshold": pl.Float64(),
        "passed": pl.Boolean(),
        "waived": pl.Boolean(),
        "detail": pl.String(),
    }
)
# A precision row's strata in `release.json`.
_STRATA_ROW = pl.Schema(
    {
        "subject": pl.String(),
        "strata": pl.List(
            pl.Struct(
                {
                    "stratum": pl.String(),
                    "allocated_usd": pl.Float64(),
                    "labels": pl.Int32(),
                    "estimate": pl.Float64(),
                    "lower": pl.Float64(),
                    "upper": pl.Float64(),
                }
            )
        ),
    }
)


def _gate_rows(release: Release, gate: str, *, schema: pl.Schema) -> pl.LazyFrame:
    """The rows `release.json` records for `gate`, in `schema`; none when the
    release was not gated.

    Raises:
        SchemaError: when `release.json` holds rows of `gate` that are not a list.
    """
    rows = release.manifest.gates.get(gate, [])
    if not isinstance(rows, list):
        raise SchemaError("release.json", {f"gates.{gate}": "not a list of rows"})
    return pl.LazyFrame(rows, schema=schema)


def _precision(release: Release, *, providers: tuple[int, ...] | None) -> pl.LazyFrame:
    """The precision gate's rows in `release.json`, one per provider; none when the
    release was not gated."""
    frame = _gate_rows(release, "precision", schema=_GATE_ROW).select(
        provider_code=pl.col("subject").cast(pl.Int32),
        estimate="value",
        lower="lower",
        upper="upper",
        passed="passed",
        waived="waived",
        detail="detail",
    )
    conditions = _conditions(providers=providers, recipients=None, years=None)
    return frame.filter(*conditions) if conditions else frame


def _stratum_precision(
    release: Release, *, providers: tuple[int, ...] | None
) -> pl.LazyFrame:
    """One row per provider and stratum of its blind sample, from the precision
    rows' `strata` in `release.json`, largest allocated value first, with the
    verdict of the stratum's stratum_precision row; null verdicts for a stratum
    the release does not gate, and no rows when the release was not gated."""
    verdicts = _gate_rows(release, "stratum_precision", schema=_GATE_ROW).select(
        pl.col("subject")
        .str.split_exact("/", 1)
        .struct.rename_fields(["provider_code", "stratum"])
        .struct.unnest(),
        "passed",
        "waived",
        "detail",
    )
    frame = (
        _gate_rows(release, "precision", schema=_STRATA_ROW)
        .select(provider_code=pl.col("subject").cast(pl.Int32), s="strata")
        .explode("s")
        .drop_nulls("s")
        .unnest("s")
        .join(
            verdicts.with_columns(pl.col("provider_code").cast(pl.Int32)),
            on=["provider_code", "stratum"],
            how="left",
            maintain_order="left",
        )
    )
    conditions = _conditions(providers=providers, recipients=None, years=None)
    return frame.filter(*conditions) if conditions else frame


# The OECD figures a party's BTR face value is set against, in `reconcile`'s order.
COMPARATORS = (
    "crdf_at_party_weight",
    "crdf_at_100",
    "traced_climate_value",
    "crs_disbursed_at_party_weight",
)
# Per BTR status, the comparators of its flow, the one it reads against first, and
# the CRS flow `oecd_only_usd` sums.
_STATUS_FLOWS = {
    "committed": (COMPARATORS[:3], "commitments"),
    "disbursed": (COMPARATORS[3:], "disbursements"),
}
# The comparator a party's BTR grant equivalent is set against.
GRANT_EQUIVALENT_COMPARATOR = "crs_grant_equivalent_at_party_weight"
# A comparator below this many USD is too small for its ratio to say much.
THIN_COMPARATOR_USD = 10e6
# The marker rule the CRDF comparators weight by, with the party's coefficient or 1.0
# for Significant.
_PARTY_RULE = Method(significant=1.0, principal=1.0, highest_marker=True)
_PARTY_YEAR = ("party", "year")


def _party_coefficients(release: Release) -> pl.LazyFrame:
    """Each provider's `provider_coefficients` row for every agency without a row of
    its own, by year of the release's BTR totals, as `reconcile` shows it."""
    return (
        _coefficients_by_year(release, years=release.table("btr_totals"))
        .filter(pl.col("agency_code").is_null())
        .select(
            "provider_code",
            "year",
            party_weight="significant",
            party_principal="principal",
            weight_source="citation",
            coefficient_basis=_coefficient_basis(),
        )
    )


def reconcile(
    release: Release,
    *,
    providers: tuple[int, ...] | None,
    years: tuple[int, ...] | None,
    method: Method | MethodRange,
) -> Reconciliation:
    """Each party's BTR Table III.1 total against OECD figures for its bilateral
    finance, one row per party, year, status and method.

    `providers` filters the parties by their DAC provider codes, and `years` the
    report years; None takes every one the BTR tables hold.

    The BTR side comes from `btr_totals`, so a release without the row-level `btr`
    and `btr_links` reconciles alike. Each status has its own row, and rows without a
    status one more: Japan reports loans as committed and grants as disbursed, and
    Denmark each activity under both. `btr_face` and `btr_grant_equivalent` sum the
    status's rows in USD at the DAC rate of the year, the rate the CRS amounts use:
    each row's domestic amount over `dac_rate`, or the party's own USD where either
    is missing. `btr_face_reported` sums the party's own USD face value, and
    `party_rate` is the rate it implies in domestic currency per USD
    (`dac_rate` times `btr_face` over `btr_face_reported`), with rows the party gave
    in USD only counting at the DAC rate. `unfccc_only_usd` is the face value of the
    rows no CRS record links to, split by funding source into `unfccc_only_oda_usd`,
    `unfccc_only_oof_usd` and `unfccc_only_other_usd` (a source naming both flows
    or neither). An unlinked row may be an activity no CRS
    record carries or a link the build missed, so read it beside `oecd_only_usd`:
    the face amounts of the status's CRS flow (commitments or disbursements) on the
    party-year's bilateral climate-marked records that no BTR row links to, null for
    rows without a status. Each BTR sum is null when every row lacks the amount, and
    zero when the rows report zero or the known face value is all linked.

    The comparators sum the party's bilateral rows (`BILATERAL`) of the year, and of
    the CRDF its Rio-marked rows only. A party weight applies to markers, and a
    party's BTR leaves out the climate components its development bank publishes,
    as the EU's leaves out the EIB's. The party's coefficients are its provider's
    `provider_coefficients` row of the year for every agency without a row of its
    own: `party_weight` the significant one, `party_principal` the principal one,
    `weight_source` their source, and `coefficient_basis` the row's basis as
    `spending` gives it. The weighted comparators weigh each activity or record as
    `PROVIDER` does, by its agency's row, else its provider's. Where that row
    gives no coefficient for the activity's marker, because it assesses each
    activity case by case at the agency level as for France's AFD or the provider
    level as for the UK, or because the provider has no row, the activity takes
    `PROVIDER`'s coefficients, as `spending` gives it. `crdf_at_party_weight` is its
    CRDF commitments so weighted; `crdf_at_100` the same commitments with
    Significant at 100%; `traced_climate_value` the climate value `method` gives
    what those commitments disbursed; `crs_disbursed_at_party_weight` the year's
    CRS disbursements on Rio-marked records so weighted, at the both-significant
    coefficient for a record significant on both objectives where the provider
    states one; and `crs_grant_equivalent_at_party_weight` their grant equivalent,
    weighted alike. `crdf_method_share` is the share of `crdf_at_party_weight` that
    rests on `PROVIDER`'s coefficients in place of the party's own,
    `crs_method_share` the same share of `crs_disbursed_at_party_weight`, and
    `crs_grant_equivalent_method_share` that of
    `crs_grant_equivalent_at_party_weight`, which differs from `crs_method_share`
    where loans concentrate in agencies without coefficients of their own; each is
    null where its comparator is zero or null, and 1.0 for a party whose provider
    has no row. A party without a DAC provider code has null comparators.

    Each `btr_over_<comparator>` divides `btr_face` by it, null where the
    comparator is zero or null, and is null for a
    comparator of the other flow: committed rows read against the first three,
    disbursed rows against `crs_disbursed_at_party_weight`, and rows without a
    status against all four. `comparator` names the one a status reads against
    first, `crdf_at_party_weight` or `crs_disbursed_at_party_weight`, with its ratio
    as `btr_over_comparator`, and `thin_comparator` is true where it is below
    `THIN_COMPARATOR_USD`. `btr_grant_equivalent_over_crs_grant_equivalent` divides
    `btr_grant_equivalent` by its comparator on every row, null where that is zero.
    """
    conditions = _conditions(providers=providers, recipients=None, years=years)
    totals = release.table("btr_totals").filter(*conditions)
    face = pl.col("face_usd_at_dac")
    unlinked = ~pl.col("linked")

    def unfccc_only(condition: pl.Expr) -> pl.Expr:
        return pl.when(face.is_not_null().any()).then(
            ordered_sum(face.filter(condition))
        )

    flow = pl.col("status").replace_strict(
        {status: flow for status, (_, flow) in _STATUS_FLOWS.items()},
        default=None,
        return_dtype=pl.String,
    )
    reported = (
        totals.group_by(*_PARTY_YEAR, "provider_code", "status")
        .agg(
            btr_face_reported=_sum("face_usd"),
            btr_face=_sum("face_usd_at_dac"),
            btr_grant_equivalent=_sum("grant_equiv_usd_at_dac"),
            dac_rate=pl.col("dac_rate").max(),
            unfccc_only_usd=unfccc_only(unlinked),
            **{
                f"unfccc_only_{funding}_usd": unfccc_only(
                    unlinked & (pl.col("funding") == funding)
                )
                for funding in FUNDING_CLASSES
            },
        )
        .with_columns(
            party_rate=pl.when(pl.col("btr_face_reported") > 0).then(
                pl.col("dac_rate") * pl.col("btr_face") / pl.col("btr_face_reported")
            ),
            flow=flow,
        )
        .join(
            release.table("btr_crs_unlinked").select(
                "provider_code", "year", "flow", oecd_only_usd="amount_usd"
            ),
            on=["provider_code", "year", "flow"],
            how="left",
        )
        .join(
            _party_coefficients(release),
            on=["provider_code", "year"],
            how="left",
            validate="m:1",
        )
    )
    party_years = reported.select("provider_code", "year").unique()
    bilateral = pl.col("bi_multi").is_in(BILATERAL)
    commitments = (
        _counted_commitments(release)
        .filter(bilateral, pl.col("climate_share").is_null())
        # Jev labels CRS records only, so a Jev method reads markers here.
        .with_columns(**face_shares(), jev_level=pl.lit(None, pl.Int8))
        .join(party_years, on=["provider_code", "year"], how="semi")
    )

    def crdf_at(**coefficients: pl.Expr) -> pl.Expr:
        fractions = weights(
            _PARTY_RULE, **coefficients, published=pl.col("share_climate")
        )
        return pl.col("face_usd") * fractions["climate"]

    at_party_weight = crdf_at(**_COEFFICIENT_INPUTS)
    crdf = (
        _coefficients(release, commitments, method=PROVIDER)
        .group_by("provider_code", "year")
        .agg(
            crdf_at_party_weight=ordered_sum(at_party_weight),
            crdf_method_share=_method_share(at_party_weight),
            crdf_at_100=ordered_sum(crdf_at(significant=pl.lit(1.0))),
        )
    )
    crs_value = (
        pl.col("amount_usd") * weights(_PARTY_RULE, **_COEFFICIENT_INPUTS)["climate"]
    )

    def crs_at_party_weight(flow: str) -> pl.LazyFrame:
        """The year's CRS `flow` on Rio-marked records at `PROVIDER`'s coefficients
        of each record's agency, weighted as the CRDF comparators weight."""
        flows = (
            release.table("total_flows")
            .filter(pl.col("flow") == flow, bilateral)
            .join(party_years, on=["provider_code", "year"], how="semi")
        )
        # Every `total_flows` row weighs by its markers, so it has no shares.
        shares = (
            "climate_share",
            "share_adaptation",
            "share_mitigation",
            "share_overlap",
        )
        return (
            _coefficients(release, flows, method=PROVIDER)
            .with_columns(pl.lit(None, pl.Float64).alias(name) for name in shares)
            .group_by("provider_code", "year")
            .agg(
                amount_usd=ordered_sum(crs_value),
                crs_method_share=_method_share(crs_value),
            )
        )

    crs = crs_at_party_weight("disbursements").rename(
        {"amount_usd": "crs_disbursed_at_party_weight"}
    )
    grant_equivalent = crs_at_party_weight("grant_equivalent").rename(
        {
            "amount_usd": GRANT_EQUIVALENT_COMPARATOR,
            "crs_method_share": "crs_grant_equivalent_method_share",
        }
    )
    frames = []
    for each in method.methods if isinstance(method, MethodRange) else (method,):
        weighted = _coefficients(release, commitments, method=each)
        fractions = weights(
            each, **_COEFFICIENT_INPUTS, published=pl.col("share_climate")
        )
        traced = weighted.group_by("provider_code", "year").agg(
            traced_climate_value=ordered_sum(
                pl.col("disbursed_usd") * fractions["climate"]
            )
        )
        frames.append(
            reported.join(crdf, on=["provider_code", "year"], how="left")
            .join(traced, on=["provider_code", "year"], how="left")
            .join(crs, on=["provider_code", "year"], how="left")
            .join(grant_equivalent, on=["provider_code", "year"], how="left")
            .with_columns(method=pl.lit(label(each)))
        )
    status = pl.col("status")

    def reads(name: str) -> pl.Expr:
        """Whether a row reads against comparator `name`."""
        flows = [s for s, (names, _) in _STATUS_FLOWS.items() if name in names]
        return status.is_null() | status.is_in(flows)

    first = {s: names[0] for s, (names, _) in _STATUS_FLOWS.items()}
    comparator_usd = pl.coalesce(
        pl.when(status == s).then(pl.col(name)) for s, name in first.items()
    )
    ratios = [f"btr_over_{name}" for name in COMPARATORS]
    table = (
        pl.concat(frames)
        .with_columns(
            *(
                pl.when(reads(name))
                .then(_ratio(pl.col("btr_face"), pl.col(name)))
                .alias(f"btr_over_{name}")
                for name in COMPARATORS
            ),
            comparator=status.replace_strict(
                first, default=None, return_dtype=pl.String
            ),
            btr_over_comparator=_ratio(pl.col("btr_face"), comparator_usd),
            thin_comparator=comparator_usd < THIN_COMPARATOR_USD,
            btr_grant_equivalent_over_crs_grant_equivalent=_ratio(
                pl.col("btr_grant_equivalent"), pl.col(GRANT_EQUIVALENT_COMPARATOR)
            ),
        )
        .select(
            "party",
            "provider_code",
            "year",
            "status",
            "method",
            "party_weight",
            "party_principal",
            "weight_source",
            "coefficient_basis",
            "crdf_method_share",
            "crs_method_share",
            "crs_grant_equivalent_method_share",
            "btr_face",
            "btr_face_reported",
            "party_rate",
            "dac_rate",
            "btr_grant_equivalent",
            "unfccc_only_usd",
            *(f"unfccc_only_{funding}_usd" for funding in FUNDING_CLASSES),
            "oecd_only_usd",
            *COMPARATORS,
            GRANT_EQUIVALENT_COMPARATOR,
            "comparator",
            "btr_over_comparator",
            "thin_comparator",
            *ratios,
            "btr_grant_equivalent_over_crs_grant_equivalent",
        )
        .sort("party", "year", "status", "method", nulls_last=True)
    )
    return Reconciliation(
        table=with_names(table),
        rows=lambda: with_names(_reconciliation_rows(release, conditions=conditions)),
    )


def _reconciliation_rows(
    release: Release, *, conditions: Sequence[pl.Expr]
) -> pl.LazyFrame:
    """Each BTR row that meets `conditions` with each CRS record linked to it.

    Raises:
        WithheldTableError: when the release leaves out `btr` or `btr_links`.
    """
    return (
        release.table("btr")
        .filter(*conditions)
        .join(release.table("btr_links"), on="row_id", how="left", suffix="_crs")
        .select(
            "party",
            "provider_code",
            "year",
            "row_id",
            "status",
            "recipient",
            "title",
            "face_usd",
            "face_usd_at_dac",
            "grant_equiv_usd",
            "record_id",
            "link_method",
            "probability",
            crs_recipient_code="recipient_code_crs",
            crs_project_number="project_number",
            crs_title="title_crs",
            crs_commitment_usd="commitment_usd",
            crs_disbursement_usd="disbursement_usd",
            crs_rio_adaptation="rio_adaptation",
            crs_rio_mitigation="rio_mitigation",
            unfccc_only=pl.col("record_id").is_null(),
        )
        .sort("party", "year", "row_id", "record_id", nulls_last=True)
    )


def _attribution(
    release: Release,
    *,
    providers: tuple[int, ...] | None,
    years: tuple[int, ...] | None,
    method: Method,
) -> pl.LazyFrame:
    """Per provider, year and flow, the value `method` attributes through core
    contributions, in one column per share source, in USD at current prices."""
    conditions = _conditions(providers=providers, recipients=None, years=years)

    def by_provider(flow: Flow) -> pl.LazyFrame:
        attributed = _attributed_rows(release, flow=flow, method=method).filter(
            *conditions
        )
        return (
            _weigh(_coefficients(release, attributed, method=method), method=method)
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
    release: Release, *, years: tuple[int, ...] | None, method: Method
) -> pl.LazyFrame:
    """Per EU Institutions agency, its channel, year and flow, the value of its own
    rows that a provider view of every provider leaves out under `method`, in USD at
    current prices. Empty when the method counts EU Institutions as a provider."""
    conditions = _conditions(providers=None, recipients=None, years=years)

    def by_agency(flow: Flow) -> pl.LazyFrame:
        own = _base_rows(release, flow=flow, view="provider", untraced=True).filter(
            pl.col("provider_code") == EU_INSTITUTIONS, *conditions
        )
        attributed = _attributed_rows(release, flow=flow, method=method)
        kept = _eu_kept(attributed=attributed, everyone=attributed)
        left_out = _with_eu_kept(own, kept=kept).filter(pl.col("eu_kept") == 0)
        return (
            _weigh(
                _coefficients(release, left_out.drop("eu_kept"), method=method),
                method=method,
            )
            .group_by("agency_code", "year")
            .agg(value=ordered_sum(pl.col("value")))
        )

    names = {
        code: agency_name(Agency(provider=EU_INSTITUTIONS, agency=code))
        for code in EU_AGENCY_CHANNELS
    }
    left_out = (
        _per_flow(by_agency)
        .with_columns(
            agency=pl.col("agency_code").replace_strict(names, return_dtype=pl.String),
            channel_code=pl.col("agency_code").replace_strict(
                EU_AGENCY_CHANNELS, return_dtype=pl.Int32
            ),
        )
        .select("agency", "agency_code", "channel_code", "year", "flow", "value")
        .sort("agency_code", "year", "flow")
    )
    return left_out if method.eu == "channel" else left_out.clear()


def _per_flow(build: Callable[[Flow], pl.LazyFrame]) -> pl.LazyFrame:
    """`build`'s frame for each of `FLOWS`, with its `flow`, stacked."""
    return pl.concat(build(flow).with_columns(flow=pl.lit(flow)) for flow in FLOWS)


def _conditions(
    *,
    providers: tuple[Target, ...] | None,
    recipients: tuple[int, ...] | None,
    years: Iterable[int] | None,
) -> list[pl.Expr]:
    conditions = []
    if providers is not None:
        conditions.append(provider_rows(providers))
    if recipients is not None:
        conditions.append(pl.col("recipient_code").is_in(recipients))
    if years is not None:
        conditions.append(pl.col("year").is_in(list(years)))
    return conditions


def _counted_commitments(release: Release) -> pl.LazyFrame:
    """Commitments that count: every row except repeats of an earlier vintage.

    The release leaves out aggregate rows rebuilt into children, so their children's
    `parent_row_id` names a published CRDF row outside the release and nothing joins
    on it.
    """
    return release.table("commitments").filter(pl.col("repeat_of").is_null())


def _in_view(view: View) -> pl.Expr:
    """Rows of the Bi_Multi codes that `view` counts."""
    codes = BILATERAL if view == "provider" else RECIPIENT_VIEW
    return pl.col("bi_multi").is_in(codes)


def _gross(flow: Flow) -> Flow:
    """The stored flow `flow` reads: disbursements for a net flow, else itself."""
    return "disbursements" if flow in NET_FLOWS else flow


def _base_rows(
    release: Release, *, flow: Flow, view: View, untraced: bool
) -> pl.LazyFrame:
    """The rows of `flow` that `view` counts, before weighting: commitments, or
    disbursements and, with `untraced`, untraced rows, and for a net flow the
    repayments on them (`_reflow_rows`)."""
    shares = {
        **face_shares(),
        "share_unsplit": pl.lit(0.0),
        "adaptation_measured": pl.col("adaptation_usd").is_not_null(),
    }
    if flow == "commitments":
        commitments = _counted_commitments(release).filter(_in_view(view))
        return _conform(
            commitments.with_columns(
                origin=pl.lit("commitment"),
                amount=pl.col("face_usd"),
                amount_low=pl.col("face_usd"),
                amount_high=pl.col("face_usd"),
                **shares,
            )
        )

    commitments = _counted_commitments(release).select(
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
    record_allocated = ordered_sum(allocated).over("record_id")
    disbursements = (
        release.table("disbursements")
        .with_columns(
            allocation_weight=pl.when(record_allocated != 0).then(
                allocated / record_allocated
            )
        )
        .join(commitments, on="row_id", how="inner", validate="m:1")
        .filter(_in_view(view))
        .join(_labels(release), on="record_id", how="left")
    )
    factor, missing = _grant_equivalent(flow)
    traced = _conform(
        disbursements.with_columns(
            origin=pl.lit("disbursement"),
            amount=allocated * factor,
            amount_low=pl.col("allocated_low_usd") * factor,
            amount_high=allocated * factor,
            no_grant_equivalent_usd=pl.when(missing).then(allocated),
            rio_adaptation=pl.col("inherited_rio_adaptation"),
            rio_mitigation=pl.col("inherited_rio_mitigation"),
            jev_level=_scoped_jev(pl.col("inherited_rio_adaptation")),
            **shares,
        )
    )
    rows = [traced]
    if untraced:
        rows.append(_untraced_rows(release, view=view, flow=flow))
    if flow in NET_FLOWS:
        rows.append(
            _reflow_rows(
                release,
                view=view,
                untraced=untraced,
                interest=flow == "net_transfers",
            )
        )
    return pl.concat(rows)


def _grant_equivalent(flow: Flow) -> tuple[pl.Expr, pl.Expr]:
    """A disbursement's amount per USD disbursed under `flow`, and whether its
    record has no grant equivalent.

    Under `flow="grant_equivalent"` the factor is the record's grant equivalent over
    its disbursement: 0 for a record without one, whose face value counts in
    `no_grant_equivalent`, and null for a record that disburses nothing. Under any
    other flow it is 1.
    """
    if flow != "grant_equivalent":
        return pl.lit(1.0), pl.lit(value=False)
    record, grant_equiv = (
        pl.col("record_disbursement_usd"),
        pl.col("record_grant_equiv_usd"),
    )
    missing = grant_equiv.is_null()
    factor = pl.when(missing).then(0.0).when(record != 0).then(grant_equiv / record)
    return factor, missing


def _reflow_rows(
    release: Release, *, view: View, untraced: bool, interest: bool
) -> pl.LazyFrame:
    """The repayments `view` counts as negative rows of origin `repayment`, and with
    `interest` of origin `interest` too, before weighting.

    A `commitment` part joins its counted commitment and weighs by the markers and
    climate share it inherits, as a disbursement allocated to that commitment does.
    With `untraced`, an `untraced` part weighs by the disbursement records' own
    markers, as `_untraced_rows` weighs them. `amount` subtracts the part. Of the
    part and what certain links carried, `amount_low` subtracts the larger and
    `amount_high` the smaller.
    """
    reflows = release.table("reflows").filter(_in_view(view))
    commitments = _counted_commitments(release).select(
        "row_id",
        "flow_category",
        "status",
        "face_usd",
        "climate_usd",
        "adaptation_usd",
        "mitigation_usd",
        "overlap_usd",
    )
    inherited = {
        "rio_adaptation": pl.col("inherited_rio_adaptation"),
        "rio_mitigation": pl.col("inherited_rio_mitigation"),
        "share_unsplit": pl.lit(0.0),
    }
    parts = [
        reflows.filter(pl.col("basis") == "commitment")
        .join(commitments, on="row_id", how="inner", validate="m:1")
        .with_columns(
            **inherited,
            **face_shares(),
            adaptation_measured=pl.col("adaptation_usd").is_not_null(),
        )
    ]
    if untraced:
        parts.append(
            reflows.filter(pl.col("basis") == "untraced").with_columns(
                **inherited,
                marker_source=pl.lit("record"),
                adaptation_measured=pl.col("inherited_rio_adaptation").is_not_null(),
                untraced_part=pl.lit(value=True),
            )
        )
    amounts = {
        "repayment": "received",
        **({"interest": "interest"} if interest else {}),
    }

    def subtracted(name: str) -> dict[str, pl.Expr]:
        # 0 - x keeps a zero amount +0.0, as a sum of zeros needs. The CRS holds
        # negative interest, so the bounds take the lower and higher of the whole
        # part and its certain part, whatever their sign.
        whole, certain = 0.0 - pl.col(f"{name}_usd"), 0.0 - pl.col(f"{name}_low_usd")
        return {
            "amount": whole,
            "amount_low": pl.min_horizontal(whole, certain),
            "amount_high": pl.max_horizontal(whole, certain),
        }

    return pl.concat(
        _conform(part.with_columns(origin=pl.lit(origin), **subtracted(name)))
        for part in parts
        for origin, name in amounts.items()
    )


def _untraced_rows(release: Release, *, view: View, flow: Flow) -> pl.LazyFrame:
    """The `untraced` rows `view` counts, valued as `flow` values a disbursement
    (`_grant_equivalent`), weighed by the record's own markers.

    No commitment publishes a share for them, so their shares stay null and cap no
    marker weight. A record without an adaptation marker, as every record before
    `schema.ADAPTATION_FIRST_YEAR` is, counts mitigation alone, and its adaptation
    is not measured.
    """
    factor, missing = _grant_equivalent(flow)
    amount = pl.col("untraced_usd") * factor
    return _conform(
        release.table("untraced")
        .filter(_in_view(view))
        .join(_labels(release), on="record_id", how="left")
        .with_columns(
            origin=pl.lit("untraced"),
            amount=amount,
            # Null where the amount is, as for a record that disburses nothing.
            amount_low=pl.when(amount.is_not_null()).then(0.0),
            amount_high=amount,
            no_grant_equivalent_usd=pl.when(missing).then(pl.col("untraced_usd")),
            record_rio_adaptation=pl.col("rio_adaptation"),
            record_rio_mitigation=pl.col("rio_mitigation"),
            jev_level=_scoped_jev(pl.col("rio_adaptation")),
            marker_source=pl.lit("record"),
            share_unsplit=pl.lit(0.0),
            adaptation_measured=pl.col("rio_adaptation").is_not_null(),
            untraced_part=pl.lit(value=True),
        )
    )


def _scoped_jev(adaptation: pl.Expr) -> pl.Expr:
    """A row's `jev_level` where `adaptation`, the marker it weighs by, is 1 or 2,
    and null elsewhere."""
    return pl.when(adaptation.is_in([1, 2])).then(pl.col("jev_level"))


def _require_jev_labels(release: Release) -> None:
    jev = (
        release.table("objectives")
        .filter(pl.col("label_source") == "jev")
        .head(1)
        .collect()
    )
    if jev.is_empty():
        raise MissingLabelsError(
            'relevance="jev" needs Jev labels, and this release holds none: it was '
            "built with jev_labels off"
        )


def _labels(release: Release) -> pl.LazyFrame:
    """Per record, the highest-marker objective from its first-ranked label source,
    and the level of its Jev adaptation label."""
    rank = pl.col("label_source").replace_strict(LABEL_PRECEDENCE, return_dtype=pl.Int8)

    def level(objective: str) -> pl.Expr:
        return pl.col("level").filter(pl.col("objective") == objective).max()

    a, m, c = pl.col("a"), pl.col("m"), pl.col("c")
    objectives = release.table("objectives")
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


def _attributed_rows(release: Release, *, flow: Flow, method: Method) -> pl.LazyFrame:
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
    shares = _fixed_shares(release, source=source)
    if source == "one":
        measured = measured_shares(release.table("agency_spending"), method=method)
        shares = pl.concat(
            [measured, shares.join(measured, on=["channel_code", "year"], how="anti")]
        )
    if method.eu == "provider":
        shares = shares.filter(~pl.col("channel_code").is_in(EU_BUDGET_AND_EDF))
    return _conform(
        release.table("core_contributions")
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


def _fixed_shares(release: Release, *, source: str) -> pl.LazyFrame:
    """The `agency_shares` of `source` by channel and year, each the OECD's imputed
    share."""

    # `agency_shares` holds one row per channel, year, objective and source, so each
    # sum adds at most one share.
    def share(objective: str) -> pl.Expr:
        return pl.col("share").filter(pl.col("objective") == objective).sum()

    return (
        release.table("agency_shares")
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
                ordered_sum(face * (fractions["adaptation"] + added))
            ),
            share_mitigation=ordered_sum(face * (fractions["mitigation"] + added)),
            share_overlap=measured.then(ordered_sum(face * overlap)),
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
    release: Release, frame: pl.LazyFrame, *, method: Method
) -> pl.LazyFrame:
    """Set `significant`, `principal`, `both_significant`, `coefficient_basis`,
    `coefficient_source` and `method_coefficient` for `method`.

    Under provider coefficients each row takes the `provider_coefficients` row of its
    provider, year and agency, or of its provider and year for an agency without a
    row of its own, and `weights` gives an activity significant for both objectives
    the row's `both_significant`. Where the row gives no coefficient, because the
    provider assesses each activity case by case or has no row, the row takes the
    method's. Under capped coefficients the method's coefficients also cap the
    provider's, and a row significant for both objectives takes the provider's
    `both_significant`, else its significant coefficient uncapped, else 1.
    `coefficient_basis` is `provider` for the provider's flat coefficients, `default`
    for the default share of a provider that assesses case by case, `case_by_case`
    where such a provider gives no default, and null where the release holds no row.
    A row with a `climate_share` weighs by its shares, which take no provider
    coefficient, so its `coefficient_basis` and `coefficient_source` are null.
    `method_coefficient` is true for a marker row whose higher Rio marker weighs at
    the method's coefficient: its principal one for a Principal row, its significant
    one for a Significant row without a `both_significant` of its own. Under capped
    coefficients it is also true where the method's coefficient caps a higher one of
    the provider's, and for a row significant for both objectives that counts in
    full because the provider states neither coefficient.
    """
    # A frame without shares, such as `total_flows`, weighs every row by its markers.
    marker = (
        pl.col("climate_share").is_null()
        if "climate_share" in frame.collect_schema().names()
        else pl.lit(value=True)
    )
    if method.coefficients == "fixed":
        return frame.with_columns(
            significant=pl.lit(method.significant),
            principal=pl.lit(method.principal),
            both_significant=pl.lit(None, pl.Float64),
            method_coefficient=marker,
        )
    held = _coefficients_by_year(release, years=frame).select(
        "provider_code",
        "agency_code",
        "year",
        _significant="significant",
        _principal="principal",
        _both="both_significant",
        _basis=_coefficient_basis(),
        _source="citation",
    )
    columns = ("_significant", "_principal", "_both", "_basis", "_source")
    by_agency = held.filter(pl.col("agency_code").is_not_null())
    by_provider = held.filter(pl.col("agency_code").is_null()).drop("agency_code")
    own_agency = pl.col("_source").is_not_null()

    def pick(name: str) -> pl.Expr:
        return pl.when(own_agency).then(pl.col(name)).otherwise(pl.col(f"{name}_all"))

    capped = method.coefficients == "capped"

    def took(name: str, ceiling: float) -> pl.Expr:
        held = pl.col(name)
        return held.is_null() | (held > ceiling) if capped else held.is_null()

    def coefficient(name: str, ceiling: float) -> pl.Expr:
        if capped:
            return pl.min_horizontal(pl.col(name), pl.lit(ceiling))
        return pl.col(name).fill_null(ceiling)

    rio_a, rio_m = pl.col("rio_adaptation"), pl.col("rio_mitigation")
    both = (rio_a == 1) & (rio_m == 1)
    took_method = (
        pl.when(pl.max_horizontal(rio_a, rio_m) == 2)
        .then(took("_principal", method.principal))
        .when(both & pl.col("_both").is_not_null())
        .then(pl.lit(value=False))
        .when(both & capped)
        .then(pl.col("_significant").is_null())
        .otherwise(took("_significant", method.significant))
    )
    both_significant = (
        pl.coalesce("_both", "_significant", pl.lit(1.0)) if capped else pl.col("_both")
    )
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
            significant=coefficient("_significant", method.significant),
            principal=coefficient("_principal", method.principal),
            both_significant=both_significant,
            coefficient_basis=pl.when(marker).then(pl.col("_basis")),
            coefficient_source=pl.when(marker).then(pl.col("_source")),
            method_coefficient=marker & took_method,
        )
        .drop(*columns, *(f"{name}_all" for name in columns))
    )


def _coefficient_basis() -> pl.Expr:
    """A `provider_coefficients` row's basis: `provider` for flat coefficients,
    `default` for the default share of a provider that assesses case by case, and
    `case_by_case` where such a provider gives no default."""
    return (
        pl.when(~pl.col("case_by_case"))
        .then(pl.lit("provider"))
        .when(pl.col("significant").is_not_null() | pl.col("principal").is_not_null())
        .then(pl.lit("default"))
        .otherwise(pl.lit("case_by_case"))
    )


def _coefficients_by_year(release: Release, *, years: pl.LazyFrame) -> pl.LazyFrame:
    """Each `provider_coefficients` row once for every year of `years` its span
    covers, with `citation` its source and locator, matched on `year` after the
    span is expanded into its years.

    Raises:
        ReleaseIntegrityError: when two rows of one provider and agency cover a
            year in common, as a release edited after its build may hold.
    """
    coefficients = release.table("provider_coefficients").collect()
    if overlaps := coefficient_overlaps(coefficients):
        raise ReleaseIntegrityError(
            f"release {release.manifest.release!r} applies more than one "
            "coefficient to a year: " + "; ".join(overlaps)
        )
    first, last = pl.col("first_year"), pl.col("last_year")
    held = years.select("year").unique()
    bounds = held.select(_low=pl.col("year").min(), _high=pl.col("year").max())
    return (
        coefficients.lazy()
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
        .with_columns(citation=pl.format("{}, {}", "source", "locator"))
    )


def _weigh(frame: pl.LazyFrame, *, method: Method) -> pl.LazyFrame:
    """Weight each row's amount by objective, unsplit and in total under `method`.

    Where adaptation is not measured, adaptation and cross_cutting count 0 and
    `no_adaptation_marker` holds the row's value."""
    amount, face = pl.col("amount"), pl.col("no_grant_equivalent_usd")
    fractions = weights(
        method, **_COEFFICIENT_INPUTS, published=pl.col("share_climate")
    )
    unsplit = pl.col("share_unsplit")
    climate = fractions["climate"] + unsplit
    measured = pl.col("adaptation_measured").fill_null(value=True)

    def where_measured(fraction: pl.Expr) -> pl.Expr:
        # A literal 0.0, since amount * 0.0 is -0.0 for a repayment.
        return (
            pl.when(measured)
            .then(amount * fraction)
            .when(amount.is_not_null())
            .then(0.0)
        )

    return frame.with_columns(
        method=pl.lit(label(method)),
        adaptation=where_measured(fractions["adaptation"]),
        mitigation=amount * fractions["mitigation"],
        cross_cutting=where_measured(fractions["cross_cutting"]),
        unsplit=amount * unsplit,
        value=amount * climate,
        low=pl.col("amount_low") * climate,
        high=pl.col("amount_high") * climate,
        untraced=pl.when(pl.col("untraced_part").fill_null(value=False))
        .then(amount * climate)
        .otherwise(0.0),
        no_grant_equivalent=pl.when(face.is_not_null())
        .then(face * climate)
        .otherwise(0.0),
        no_adaptation_marker=pl.when(measured).then(0.0).otherwise(amount * climate),
    ).drop(*_WEIGHT_INPUTS, "method_coefficient")


def _method_share(value: pl.Expr) -> pl.Expr:
    """The share of `value`'s sum on rows that weigh at the method's coefficient,
    null where the sum is zero or null."""
    return _ratio(
        ordered_sum(value.filter(pl.col("method_coefficient"))), ordered_sum(value)
    )


def _ratio(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """`numerator` over `denominator`, null where the denominator is zero or null."""
    return pl.when(denominator != 0).then(numerator / denominator)


def _sum(name: str) -> pl.Expr:
    """The sum of `name` in a fixed order, null when every value is missing."""
    column = pl.col(name)
    return pl.when(column.is_not_null().any()).then(ordered_sum(column))


def _agency_key(providers: tuple[Target, ...] | None) -> pl.Expr | None:
    """The `agency_code` key of a result whose spec names agencies: a row's agency
    where the spec names its provider through agencies, else null. None for a spec
    that names no agency, whose result has no agency key."""
    owners = {
        target.provider for target in providers or () if isinstance(target, Agency)
    }
    if not owners:
        return None
    return pl.when(pl.col("provider_code").is_in(sorted(owners))).then(
        pl.col("agency_code")
    )


def _keyed(frame: pl.LazyFrame, *, agency: pl.Expr | None) -> pl.LazyFrame:
    """`frame` with `agency_code` as the key `_agency_key` gives, when it gives one."""
    return frame if agency is None else frame.with_columns(agency_code=agency)


def _aggregate(
    rows: pl.LazyFrame, *, methods: Sequence[str], agency: pl.Expr | None = None
) -> pl.LazyFrame:
    """Sum rows by their row keys and method, each method's `low` and `high` its
    own. With an `agency` key (`_agency_key`) the keys hold `agency_code`, and the table
    names the agency in `agency` beside its provider.

    Every method in `methods` holds a row for every key any method holds, with zero
    amounts where it counts nothing, so a method that has no row for a key still
    bounds it when `Result` spans the bounds across methods.

    The columns start with the year, the names and the method and end with the
    amounts (`AMOUNTS`).
    """
    row_keys = ROW_KEYS if agency is None else AGENCY_ROW_KEYS
    keys = (*row_keys, "method")
    summed = (
        _keyed(rows, agency=agency)
        .group_by(keys)
        .agg(*(_sum(name).alias(name) for name in AMOUNTS), _held=pl.len())
    )
    grid = (
        summed.select(row_keys)
        .unique()
        .join(pl.LazyFrame({"method": methods}), how="cross")
    )
    held = pl.col("_held").is_not_null()
    return with_names(
        grid.join(summed, on=keys, how="left", nulls_equal=True)
        .with_columns(
            pl.when(held).then(pl.col(name)).otherwise(0.0).alias(name)
            for name in AMOUNTS
        )
        .drop("_held")
        .sort(keys, nulls_last=True)
    ).select(*named_keys(row_keys), "method", *row_keys[1:], *AMOUNTS)


def _totals(
    release: Release,
    *,
    spec: Spec,
    conditions: Sequence[pl.Expr],
    eu_kept: Mapping[str, pl.LazyFrame],
    agency: pl.Expr | None,
    factors: pl.DataFrame | None,
) -> pl.LazyFrame:
    """All CRS flows of `spec.flow`, of disbursements for a net flow, of the
    Bi_Multi codes the view counts (`_VIEW_CODES`), by the result's row keys and
    method, plus a row per provider, year, Bi_Multi code and method with a null
    recipient that sums every recipient, for rows such as attribution that have
    none. With an `agency` key (`_agency_key`) the keys hold `agency_code`, as the
    result table's do.

    CRDF commitments carry their provider's class, Bi_Multi 1 for a bilateral
    provider, where the CRS splits bilateral flows over codes 1, 3, 7 and 8, so
    commitment totals count those four codes as 1. In the provider view, EU
    Institutions' flows count under each method's EU setting as its own rows do,
    through `_count_eu_once` with the method's `eu_kept`, so a share divides by the
    same rows as its amount.
    """
    flows = release.table("total_flows").filter(
        pl.col("flow") == _gross(spec.flow),
        pl.col("bi_multi").is_in(_VIEW_CODES[spec.view]),
        *conditions,
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
    keys = (*(ROW_KEYS if agency is None else AGENCY_ROW_KEYS), "method")
    by_method = _keyed(by_method, agency=agency)
    total = ordered_sum(pl.col("amount_usd")).alias("total")
    by_recipient = by_method.group_by(keys).agg(total)
    all_recipients = (
        by_method.group_by(*(key for key in keys if key != "recipient_code"))
        .agg(total)
        .with_columns(recipient_code=pl.lit(None, pl.Int32))
        .select(*keys, "total")
    )
    return convert(
        pl.concat([by_recipient, all_recipients]),
        columns=("total",),
        factors=factors,
        release=release.manifest.release,
    )


def _modelled(
    release: Release,
    *,
    spec: Spec,
    conditions: Sequence[pl.Expr],
    factors: pl.DataFrame | None,
) -> pl.LazyFrame:
    """The "modelled, not reported" line, shown for recipient-view disbursements
    and no net flow."""
    modelled = with_names(
        release.table("modelled").select(
            "year",
            "provider_code",
            "recipient_code",
            "basis",
            low=pl.col("low_usd"),
            high=pl.col("high_usd"),
        )
    )
    if spec.flow != "disbursements" or spec.view != "recipient" or spec.oda_only:
        return modelled.clear()
    return convert(
        modelled.filter(*conditions),
        columns=("low", "high"),
        factors=factors,
        release=release.manifest.release,
    ).sort("provider_code", "year", "recipient_code", nulls_last=True)
