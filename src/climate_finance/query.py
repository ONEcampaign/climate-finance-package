"""Queries as lazy polars plans over release tables, weighted at query time.

`spending()` builds one row per counted amount, weights it under each method,
converts it, and sums the rows into the result table. Rows have one of four origins:

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
- `untraced`: a Rio-marked CRS record's disbursement that reaches no commitment
  (`untraced_usd`), the excess beyond a full commitment or the whole of a record
  linked to none, converted to grant equivalents as a disbursement is. It weighs by
  the record's own markers, with no published share to cap them. It counts in
  `high` and not in `low`, which holds what certain links carry to commitments.
  `spec.untraced=False` leaves these rows out.
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
+ cross_cutting under the highest-marker rule, whose objectives are disjoint, and
`untraced`, its value on an untraced row and 0 on any other.
Where the CRDF does not measure adaptation (commitments before 2010, and agency
shares whose span reaches one), adaptation and cross_cutting are null and `value`
is the mitigation value. A table row's adaptation and cross_cutting are null when
any of its rows with an amount has them null.

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
from collections.abc import Callable, Iterable, Mapping, Sequence
from importlib.metadata import version
from importlib.resources import files
from typing import TYPE_CHECKING

import polars as pl

from climate_finance import catalogue
from climate_finance.convert import convert, provenance
from climate_finance.errors import (
    MissingLabelsError,
    ReleaseIntegrityError,
    SchemaError,
    UnknownProviderError,
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
    AMOUNTS,
    ROW_KEYS,
    Meta,
    Quality,
    Reconciliation,
    Result,
    Table,
)
from climate_finance.schema import (
    ADAPTATION_FIRST_YEAR,
    BILATERAL,
    CORE_CONTRIBUTION,
    EIB_AGENCY,
    EU_INSTITUTIONS,
    FLOWS,
    FUNDING_CLASSES,
    RECIPIENT_VIEW,
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
    """
    methods = spec.methods
    if any(method.relevance == "jev" for method in methods):
        _require_jev_labels(release)
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
            everyone = _attributed_rows(release, flow=spec.flow, method=method).filter(
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
            _weigh(_coefficients(release, frame, method=method), method=method)
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
        modelled=Table(_modelled(release, spec=spec, conditions=modelled)),
        rows=rows,
        totals=_totals(release, spec=spec, conditions=filters, eu_kept=eu_kept),
        spec=spec,
        meta=Meta(
            release=release.manifest.release,
            package_version=version("climate-finance"),
            source_versions=release.manifest.source_versions,
            years=release.manifest.years,
            model_versions=release.manifest.model_versions,
            conversion=provenance(currency=spec.currency, prices=spec.prices),
        ),
    )


def quality(
    release: Release,
    *,
    providers: Sequence[str | int] | None = None,
    years: Iterable[int] | None = None,
    method: Method = ONE,
) -> Quality:
    """Coverage, status and precision statistics.

    The status split covers the commitments the trace follows: bilateral rows and
    multilateral outflows, without core contributions. `eu_left_out` holds the EU
    Institutions rows an all-provider provider view leaves out under `method`, and
    `untraced` the Rio-marked CRS disbursement that reaches no commitment.
    """
    years = None if years is None else tuple(years)
    codes = _provider_codes(providers)
    conditions = _conditions(providers=codes, recipients=None, years=years)
    table = release.table("quality")
    status = (
        _counted_commitments(release)
        .filter(_in_view("recipient"), *conditions)
        .select("provider_code", "year", "status", "climate_usd")
    )
    eu_left_out = _eu_left_out(release, years=years, method=method)
    if codes is not None and EU_INSTITUTIONS not in codes:
        eu_left_out = eu_left_out.clear()
    return Quality(
        table=Table(table.filter(*conditions)),
        status=Table(status),
        eu_left_out=Table(eu_left_out),
        attribution=Table(
            _attribution(release, providers=codes, years=years, method=method)
        ),
        precision=Table(_precision(release, providers=codes)),
        untraced=Table(
            _untraced(release, conditions=conditions, method=method).sort(
                "provider_code", "year"
            )
        ),
    )


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
                _untraced_rows(release, view="recipient", factor=pl.lit(1.0)),
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


def _precision(release: Release, *, providers: tuple[int, ...] | None) -> pl.LazyFrame:
    """The precision gate's rows in `release.json`, one per provider; none when the
    release was not gated.

    Raises:
        SchemaError: when `release.json` holds precision rows that are not a list.
    """
    rows = release.manifest.gates.get("precision", [])
    if not isinstance(rows, list):
        raise SchemaError("release.json", {"gates.precision": "not a list of rows"})
    frame = pl.LazyFrame(rows, schema=_GATE_ROW).select(
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
    parties: Sequence[str] | None,
    years: Iterable[int],
    method: Method | MethodRange,
) -> Reconciliation:
    """Each party's BTR Table III.1 total against OECD figures for its bilateral
    finance, one row per party, year, status and method.

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
    years = tuple(years)
    totals = release.table("btr_totals").filter(pl.col("year").is_in(years))
    if parties is not None:
        totals = totals.filter(pl.col("party").is_in(list(parties)))
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
    bilateral = (pl.col("bi_multi").is_in(BILATERAL), pl.col("year").is_in(years))
    commitments = (
        _counted_commitments(release)
        .filter(*bilateral, pl.col("climate_share").is_null())
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
            .filter(pl.col("flow") == flow, *bilateral)
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
        table=Table(table),
        rows=lambda: _reconciliation_rows(release, parties=parties, years=years),
    )


def _reconciliation_rows(
    release: Release, *, parties: Sequence[str] | None, years: tuple[int, ...]
) -> pl.LazyFrame:
    """Each BTR row of `parties` and `years` with each CRS record linked to it.

    Raises:
        WithheldTableError: when the release leaves out `btr` or `btr_links`.
    """
    btr = release.table("btr").filter(pl.col("year").is_in(years))
    if parties is not None:
        btr = btr.filter(pl.col("party").is_in(list(parties)))
    return (
        btr.join(release.table("btr_links"), on="row_id", how="left", suffix="_crs")
        .select(
            "party",
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


def _provider_codes(values: Sequence[str | int] | None) -> tuple[int, ...] | None:
    """The DAC codes `values` name, for `quality`, whose tables count providers.

    Raises:
        UnknownProviderError: for a name or code `resolve` refuses, or a name of an
            agency.
    """
    targets = catalogue.resolve(values, kind="providers")
    if targets is None:
        return None
    for target in targets:
        if isinstance(target, catalogue.Agency):
            name = catalogue.name_of(target, kind="providers")
            raise UnknownProviderError(
                name, note=f"{name} is an agency; quality() counts providers only."
            )
    return tuple(target for target in targets if isinstance(target, int))


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


def _base_rows(
    release: Release, *, flow: Flow, view: View, untraced: bool
) -> pl.LazyFrame:
    """The rows of `flow` that `view` counts, before weighting: commitments, or
    disbursements and, with `untraced`, untraced rows."""
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
    # Grant equivalents per USD disbursed, null where the record has neither.
    factor = pl.lit(1.0)
    if flow == "grant_equivalent":
        record = pl.col("record_disbursement_usd")
        factor = pl.when(record != 0).then(pl.col("record_grant_equiv_usd") / record)
    traced = _conform(
        disbursements.with_columns(
            origin=pl.lit("disbursement"),
            amount=allocated * factor,
            amount_low=pl.col("allocated_low_usd") * factor,
            amount_high=pl.col("allocated_high_usd") * factor,
            rio_adaptation=pl.col("inherited_rio_adaptation"),
            rio_mitigation=pl.col("inherited_rio_mitigation"),
            jev_level=_scoped_jev(pl.col("inherited_rio_adaptation")),
            **shares,
        )
    )
    if not untraced:
        return traced
    return pl.concat([traced, _untraced_rows(release, view=view, factor=factor)])


def _untraced_rows(release: Release, *, view: View, factor: pl.Expr) -> pl.LazyFrame:
    """The `untraced` rows `view` counts, at `factor` per USD disbursed, weighed by
    the record's own markers.

    No commitment publishes a share for them, so their shares stay null and cap no
    marker weight. The CRS measures adaptation from `ADAPTATION_FIRST_YEAR`; before
    it the release holds a null adaptation marker, as the CRDF does, and the row
    counts mitigation alone.
    """
    amount = pl.col("untraced_usd") * factor
    return _conform(
        release.table("untraced")
        .filter(_in_view(view))
        .join(_labels(release), on="record_id", how="left")
        .with_columns(
            origin=pl.lit("untraced"),
            amount=amount,
            # Null where the amount is, as for a record without a grant equivalent.
            amount_low=pl.when(amount.is_not_null()).then(0.0),
            amount_high=amount,
            record_rio_adaptation=pl.col("rio_adaptation"),
            record_rio_mitigation=pl.col("rio_mitigation"),
            jev_level=_scoped_jev(pl.col("rio_adaptation")),
            marker_source=pl.lit("record"),
            share_unsplit=pl.lit(0.0),
            adaptation_measured=pl.col("year") >= ADAPTATION_FIRST_YEAR,
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
    method's.
    `coefficient_basis` is `provider` for the provider's flat coefficients, `default`
    for the default share of a provider that assesses case by case, `case_by_case`
    where such a provider gives no default, and null where the release holds no row.
    A row with a `climate_share` weighs by its shares, which take no provider
    coefficient, so its `coefficient_basis` and `coefficient_source` are null.
    `method_coefficient` is true for a marker row whose higher Rio marker weighs at
    the method's coefficient: its principal one for a Principal row, its significant
    one for a Significant row without a `both_significant` of its own.
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

    rio_a, rio_m = pl.col("rio_adaptation"), pl.col("rio_mitigation")
    took_method = (
        pl.when(pl.max_horizontal(rio_a, rio_m) == 2)
        .then(pl.col("_principal").is_null())
        .when((rio_a == 1) & (rio_m == 1) & pl.col("_both").is_not_null())
        .then(pl.lit(value=False))
        .otherwise(pl.col("_significant").is_null())
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
            significant=pl.col("_significant").fill_null(method.significant),
            principal=pl.col("_principal").fill_null(method.principal),
            both_significant=pl.col("_both"),
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
        untraced=pl.when(pl.col("origin") == "untraced")
        .then(amount * climate)
        .otherwise(0.0),
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
    release: Release,
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
    flows = release.table("total_flows").filter(
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
    release: Release, *, spec: Spec, conditions: Sequence[pl.Expr]
) -> pl.LazyFrame:
    """The "modelled, not reported" line, shown for recipient-view disbursements."""
    modelled = release.table("modelled").select(
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
