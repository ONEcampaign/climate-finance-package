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
from climate_finance.errors import SchemaError
from climate_finance.methods import (
    OECD,
    ONE,
    Method,
    MethodRange,
    face_shares,
    label,
    weights,
)
from climate_finance.result import (
    AMOUNTS,
    Meta,
    Quality,
    Reconciliation,
    Result,
    Table,
)
from climate_finance.schema import FLOWS, FUNDING_CLASSES, SHARE_ORIGINS
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
    "coefficient_source": pl.String(),
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
    "principal": pl.Float64(),
    "both_significant": pl.Float64(),
}
# The coefficient columns `_coefficients` sets, as `weights` takes them.
_COEFFICIENT_INPUTS = {
    name: pl.col(name) for name in ("significant", "principal", "both_significant")
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
    """Coverage, status and precision statistics.

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
        precision=Table(_precision(snapshot, providers=providers)),
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


def _precision(
    snapshot: Snapshot, *, providers: Sequence[str | int] | None
) -> pl.LazyFrame:
    """The precision gate's rows in `release.json`, one per provider; none when the
    release was not gated.

    Raises:
        SchemaError: when `release.json` holds precision rows that are not a list.
    """
    rows = snapshot.manifest.gates.get("precision", [])
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


def _party_coefficients(snapshot: Snapshot) -> pl.LazyFrame:
    """Each provider's `provider_coefficients` row for every agency, by year of the
    release's BTR totals, as `reconcile` reads it."""
    first, last, year = pl.col("first_year"), pl.col("last_year"), pl.col("year")
    return (
        snapshot.table("provider_coefficients")
        .filter(pl.col("agency_code").is_null())
        .join(snapshot.table("btr_totals").select("year").unique(), how="cross")
        .filter(first.is_null() | (first <= year), last.is_null() | (year <= last))
        .select(
            "provider_code",
            "year",
            party_weight="significant",
            party_principal="principal",
            party_both="both_significant",
            weight_source=pl.format("{}, {}", "source", "locator"),
        )
    )


def reconcile(
    snapshot: Snapshot,
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
    `provider_coefficients` row for every agency and the year: `party_weight` the
    significant one, `party_principal` the principal one, and `weight_source` their
    source. `crdf_at_party_weight` and `crdf_at_100` are its CRDF commitments at the
    party's coefficients, `both_significant` included, and with Significant at
    100%; `traced_climate_value` the climate value `method` gives what those
    commitments disbursed; `crs_disbursed_at_party_weight` the year's CRS
    disbursements on Rio-marked records at the party's significant and principal
    coefficients, by each record's higher marker; and
    `crs_grant_equivalent_at_party_weight` their grant equivalent, weighted alike.
    A party without a weight has null weighted comparators, and one without a DAC
    provider code null comparators.

    Each `btr_over_<comparator>` divides `btr_face` by it, and is null for a
    comparator of the other flow: committed rows read against the first three,
    disbursed rows against `crs_disbursed_at_party_weight`, and rows without a
    status against all four. `comparator` names the one a status reads against
    first, `crdf_at_party_weight` or `crs_disbursed_at_party_weight`, with its ratio
    as `btr_over_comparator`, and `thin_comparator` is true where it is below
    `THIN_COMPARATOR_USD`. `btr_grant_equivalent_over_crs_grant_equivalent` divides
    `btr_grant_equivalent` by its comparator on every row.
    """
    years = tuple(years)
    totals = snapshot.table("btr_totals").filter(pl.col("year").is_in(years))
    if parties is not None:
        totals = totals.filter(pl.col("party").is_in(list(parties)))
    face = pl.col("face_usd_at_dac")
    unlinked = ~pl.col("linked")

    def unfccc_only(condition: pl.Expr) -> pl.Expr:
        return pl.when(face.is_not_null().any()).then(face.filter(condition).sum())

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
            snapshot.table("btr_crs_unlinked").select(
                "provider_code", "year", "flow", oecd_only_usd="amount_usd"
            ),
            on=["provider_code", "year", "flow"],
            how="left",
        )
        .join(_party_coefficients(snapshot), on=["provider_code", "year"], how="left")
    )
    weight = pl.col("party_weight")
    # A party that states a significant coefficient but no principal one, as a
    # case-by-case default may, counts Principal in full.
    principal = pl.col("party_principal").fill_null(_PARTY_RULE.principal)
    party = ("provider_code", "party_weight", "party_principal", "party_both")
    bilateral = (pl.col("bi_multi").is_in(BILATERAL), pl.col("year").is_in(years))
    commitments = (
        _counted_commitments(snapshot)
        .filter(*bilateral, pl.col("climate_share").is_null())
        # Jev labels CRS records only, so a Jev method reads markers here.
        .with_columns(**face_shares(), jev_level=pl.lit(None, pl.Int8))
        .join(reported.select(*party, "year").unique(), on=["provider_code", "year"])
    )

    def crdf_at(**coefficients: pl.Expr) -> pl.Expr:
        fractions = weights(
            _PARTY_RULE, **coefficients, published=pl.col("share_climate")
        )
        return (pl.col("face_usd") * fractions["climate"]).sum()

    crdf = commitments.group_by("provider_code", "year").agg(
        crdf_at_party_weight=crdf_at(
            significant=weight,
            principal=principal,
            both_significant=pl.col("party_both"),
        ),
        crdf_at_100=crdf_at(significant=pl.lit(1.0)),
    )
    marker = pl.col("rio_marker")

    def crs_at_party_weight(flow: str) -> pl.LazyFrame:
        """The year's CRS `flow` on Rio-marked records at the party's significant
        and principal coefficients, by each record's higher marker."""
        return (
            snapshot.table("total_flows")
            .filter(pl.col("flow") == flow, *bilateral)
            .join(
                reported.select(*party, "year").unique(), on=["provider_code", "year"]
            )
            .group_by("provider_code", "year")
            .agg(
                (
                    pl.col("amount_usd")
                    * pl.when(marker == 2)
                    .then(principal)
                    .when(marker == 1)
                    .then(weight)
                    .otherwise(0.0)
                ).sum()
            )
        )

    crs = crs_at_party_weight("disbursements").rename(
        {"amount_usd": "crs_disbursed_at_party_weight"}
    )
    grant_equivalent = crs_at_party_weight("grant_equivalent").rename(
        {"amount_usd": GRANT_EQUIVALENT_COMPARATOR}
    )
    frames = []
    for each in method.methods if isinstance(method, MethodRange) else (method,):
        weighted = _coefficients(snapshot, commitments, method=each)
        fractions = weights(
            each, **_COEFFICIENT_INPUTS, published=pl.col("share_climate")
        )
        traced = weighted.group_by("provider_code", "year").agg(
            traced_climate_value=(pl.col("disbursed_usd") * fractions["climate"]).sum()
        )
        frames.append(
            reported.join(crdf, on=["provider_code", "year"], how="left")
            .join(traced, on=["provider_code", "year"], how="left")
            .join(crs, on=["provider_code", "year"], how="left")
            .join(grant_equivalent, on=["provider_code", "year"], how="left")
            .with_columns(method=pl.lit(label(each)))
        )
    weighted_comparators = (
        "crdf_at_party_weight",
        "crs_disbursed_at_party_weight",
        GRANT_EQUIVALENT_COMPARATOR,
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
            pl.when(weight.is_not_null()).then(pl.col(name)).alias(name)
            for name in weighted_comparators
        )
        .with_columns(
            *(
                pl.when(reads(name))
                .then(pl.col("btr_face") / pl.col(name))
                .alias(f"btr_over_{name}")
                for name in COMPARATORS
            ),
            comparator=status.replace_strict(
                first, default=None, return_dtype=pl.String
            ),
            btr_over_comparator=pl.col("btr_face") / comparator_usd,
            thin_comparator=comparator_usd < THIN_COMPARATOR_USD,
            btr_grant_equivalent_over_crs_grant_equivalent=(
                pl.col("btr_grant_equivalent") / pl.col(GRANT_EQUIVALENT_COMPARATOR)
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
        rows=lambda: _reconciliation_rows(snapshot, parties=parties, years=years),
    )


def _reconciliation_rows(
    snapshot: Snapshot, *, parties: Sequence[str] | None, years: tuple[int, ...]
) -> pl.LazyFrame:
    """Each BTR row of `parties` and `years` with each CRS record linked to it.

    Raises:
        WithheldTableError: when the release leaves out `btr` or `btr_links`.
    """
    btr = snapshot.table("btr").filter(pl.col("year").is_in(years))
    if parties is not None:
        btr = btr.filter(pl.col("party").is_in(list(parties)))
    return (
        btr.join(snapshot.table("btr_links"), on="row_id", how="left", suffix="_crs")
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
            crs_rio_marker="rio_marker",
            unfccc_only=pl.col("record_id").is_null(),
        )
        .sort("party", "year", "row_id", "record_id", nulls_last=True)
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
    """Weight each row's amount by objective, unsplit and in total under `method`."""
    amount = pl.col("amount")
    fractions = weights(
        method, **_COEFFICIENT_INPUTS, published=pl.col("share_climate")
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
