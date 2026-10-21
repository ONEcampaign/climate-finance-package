"""The measure stage: agency climate shares and the modelled line.

`agency_shares` holds, per multilateral channel and year, the share of the agency's
spending that is climate finance, from two sources. `oecd` copies the OECD's imputed
shares. `one` is the agency's own climate commitments under the ONE method (the
highest-marker rule, significant markers at 0.4, climate components as reported)
over its total CRS commitments, both summed over the year and the year before. This
is the OECD's imputed-share construction with ONE's coefficients: for the banks that
report climate components, which both methods take as reported, the two agree within
0.004 in 2021 and 2022 (IBRD 0.310 and 0.309, IDA 0.313 and 0.313, AfDB 0.399 and
0.398, CAF 0.224 and 0.226). Shares rest on commitments, where 1.x divided matched
disbursements by all disbursements: on the trace, untraced disbursement stays in the
denominator and pushes banks the trace cannot follow to zero. Adaptation and
mitigation shares each include the cross-cutting share.

`modelled` holds, for providers that publish no project disbursements, their CRDF
climate commitments times a range of disbursement ratios, as `providers.toml` sets.
"""

from __future__ import annotations

import tomllib
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from climate_finance.build.allocate import counted_rows
from climate_finance.build.errors import ConfigError
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.sums import ordered_sum
from climate_finance.methods import ONE, weights
from climate_finance.schema import AGENCY_SHARES, MODELLED, STATUSES

if TYPE_CHECKING:
    from collections.abc import Sequence

    from climate_finance.build.stages import StageContext, StageOutput

PROVIDERS_TOML = Path(__file__).with_name("providers.toml")
# Years summed for an agency's ONE share: the share year and the one before.
SHARE_YEARS = 2
# A commitment cannot disburse more than its face value, so ratios stop here.
RATIO_CAP = 1.0
_OBJECTIVES = ("adaptation", "mitigation", "cross_cutting")


class _Fact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class AgencyChannel(_Fact):
    """The CRS reporter behind a channel of delivery; with no `agency_codes`, every
    agency of the provider."""

    channel_code: int
    provider_code: int
    agency_codes: tuple[int, ...] | None = None


class ReportRatio(_Fact):
    """A disbursed-over-committed ratio a provider publishes, with where and for
    which year."""

    ratio: float
    year: int
    source: str


class ModelledProvider(_Fact):
    """How a provider's modelled disbursement range is measured.

    The range spans the provider's CRS ratios, unless `crs_ratios` is false, and its
    `report_ratios`, each capped at `RATIO_CAP`. The CRS ratios are the cumulative
    and window ratios, or for `vintage` providers the vintage ratio alone.
    """

    provider_code: int
    agency_code: int | None = None
    basis: str
    crs_ratios: bool = True
    by_recipient: bool = False
    vintage: bool = False
    report_ratios: tuple[ReportRatio, ...] = ()
    statuses: tuple[str, ...] | None = None
    cancelled_crs_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.crs_ratios and not self.report_ratios:
            raise ValueError("a provider without CRS ratios needs report_ratios")
        if self.vintage and self.by_recipient:
            raise ValueError("vintage ratios are portfolio-wide; drop by_recipient")
        if unknown := set(self.statuses or ()) - set(STATUSES):
            raise ValueError(f"unknown statuses {sorted(unknown)}")
        return self


class ProviderFacts(_Fact):
    """Everything in `providers.toml`."""

    agencies: tuple[AgencyChannel, ...]
    modelled: tuple[ModelledProvider, ...]

    @model_validator(mode="after")
    def _one_channel_each(self) -> Self:
        """Each agency reports for one channel, or its spending would count twice."""
        by_provider: dict[int, list[AgencyChannel]] = {}
        for entry in self.agencies:
            by_provider.setdefault(entry.provider_code, []).append(entry)
        for provider, entries in by_provider.items():
            codes = [code for entry in entries for code in entry.agency_codes or ()]
            whole = any(entry.agency_codes is None for entry in entries)
            if (whole and len(entries) > 1) or len(codes) != len(set(codes)):
                raise ValueError(f"provider {provider} reports for two channels")
        return self


@cache
def load_provider_facts(path: Path = PROVIDERS_TOML) -> ProviderFacts:
    """Read `path` as `ProviderFacts`.

    Raises:
        ConfigError: when the file is missing or a fact is missing or invalid.
    """
    try:
        return ProviderFacts.model_validate(tomllib.loads(path.read_text()))
    except FileNotFoundError as error:
        raise ConfigError(f"No provider facts at {path}") from error
    except ValidationError as error:
        raise ConfigError(f"{path}: {error}") from error


def run_measure(context: StageContext) -> StageOutput:
    """Write `agency_shares` and `modelled`."""
    facts = load_provider_facts()
    crdf = counted_commitments(context.read("normalise", "crdf"))
    crs = trace_population(context.read("normalise", "crs"))
    return {
        "agency_shares": agency_shares(
            crdf,
            crs=crs,
            imputed_shares=context.read("normalise", "imputed_shares"),
            agencies=facts.agencies,
            years=context.config.years.crdf,
        ),
        "modelled": modelled_ranges(
            crdf,
            crs=crs,
            status=context.read("review", "commitments"),
            providers=facts.modelled,
            last_year=context.config.years.crs_last_complete,
        ),
    }


def counted_commitments(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """CRDF commitments that count once: recipient-perspective rows, without repeats
    of an earlier vintage or aggregate parents that rebuilt children replace."""
    return counted_rows(crdf).filter(pl.col("repeat_of").is_null())


def agency_shares(
    crdf: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    imputed_shares: pl.LazyFrame,
    agencies: Sequence[AgencyChannel],
    years: tuple[int, int],
) -> pl.LazyFrame:
    """Each agency's climate share by channel, year and objective, ONE and OECD.

    Args:
        crdf: counted CRDF commitments.
        crs: the CRS records the trace follows.
        imputed_shares: the OECD's shares, copied with source `oecd`.
        agencies: the CRS reporter behind each channel.
        years: the CRDF's first and last years. ONE shares cover the years whose
            whole two-year span lies inside them, for channels with positive CRS
            commitments over the span.
    """
    climate = _rolling_sum(
        _one_climate(_by_channel(crdf, agencies=agencies)),
        columns=_OBJECTIVES,
        years=years,
    )
    total = _rolling_sum(
        _by_channel(
            crs.rename({"donor_code": "provider_code"}), agencies=agencies
        ).select("channel_code", "year", total=pl.col("commitment_usd").fill_null(0.0)),
        columns=("total",),
        years=years,
    )
    positive = pl.col("total") > 0
    one = (
        total.join(climate, on=["channel_code", "year"], how="left")
        .unpivot(
            index=["channel_code", "year", "total"],
            on=list(_OBJECTIVES),
            variable_name="objective",
            value_name="share",
        )
        .with_columns(
            share=pl.when(positive).then(
                pl.col("share").fill_null(0.0) / pl.col("total")
            ),
            source=pl.lit("one"),
        )
        .drop_nulls("share")
    )
    oecd = imputed_shares.with_columns(source=pl.lit("oecd"))
    columns = list(AGENCY_SHARES.columns)
    return pl.concat([one.select(columns), oecd.select(columns)])


def _by_channel(
    frame: pl.LazyFrame, *, agencies: Sequence[AgencyChannel]
) -> pl.LazyFrame:
    """`frame`'s rows with the channel their provider and agency report for. Rows of
    no listed channel are left out."""
    mapping = pl.LazyFrame(
        [
            (entry.channel_code, entry.provider_code, agency)
            for entry in agencies
            for agency in entry.agency_codes or (None,)
        ],
        schema={
            "channel_code": pl.Int32,
            "provider_code": pl.Int32,
            "_agency": pl.Int32,
        },
        orient="row",
    )
    # A record's own channel_code is where it was delivered, not who reports it.
    return (
        frame.drop("channel_code")
        .join(mapping, on="provider_code")
        .filter(
            pl.col("_agency").is_null() | (pl.col("_agency") == pl.col("agency_code"))
        )
    )


def _component(part: str) -> pl.Expr:
    face = pl.col("face_usd")
    return pl.when(face != 0).then(pl.col(part) / face).otherwise(0.0)


def _one_climate(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """Each commitment's inclusive adaptation, mitigation and cross-cutting value
    under ONE, whose objectives are disjoint before the cross-cutting part is added
    back to the other two."""
    weighted = crdf.with_columns(
        share_adaptation=_component("adaptation_usd"),
        share_mitigation=_component("mitigation_usd"),
        share_overlap=_component("overlap_usd"),
    ).with_columns(**weights(ONE))
    cross_cutting = pl.col("cross_cutting")
    return weighted.select(
        "channel_code",
        "year",
        adaptation=pl.col("face_usd") * (pl.col("adaptation") + cross_cutting),
        mitigation=pl.col("face_usd") * (pl.col("mitigation") + cross_cutting),
        cross_cutting=pl.col("face_usd") * cross_cutting,
    )


def _rolling_sum(
    frame: pl.LazyFrame, *, columns: Sequence[str], years: tuple[int, int]
) -> pl.LazyFrame:
    """Per channel and share year, `columns` summed over the share year and the
    `SHARE_YEARS - 1` years before it, for share years whose span lies in `years`."""
    first, last = years
    return (
        frame.with_columns(
            year=pl.int_ranges(
                pl.col("year"), pl.col("year") + SHARE_YEARS, dtype=pl.Int16
            )
        )
        .explode("year")
        .filter(pl.col("year").is_between(first + SHARE_YEARS - 1, last))
        .group_by("channel_code", "year")
        .agg(ordered_sum(pl.col(column)) for column in columns)
    )


def modelled_ranges(
    crdf: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    status: pl.LazyFrame,
    providers: Sequence[ModelledProvider],
    last_year: int,
) -> pl.LazyFrame:
    """Low and high disbursement for providers that publish none.

    Each counted CRDF commitment of a listed provider with a positive face value
    contributes its climate value times the lowest and the highest of its ratios.

    Args:
        crdf: counted CRDF commitments.
        crs: the CRS records the trace follows.
        status: commitment statuses, read for providers that limit their statuses.
        providers: how each provider's ratios are measured.
        last_year: the last complete CRS year, where CRS ratios' series end.

    Raises:
        ConfigError: when a provider measured from the CRS has no disbursement
            there, or one of its commitments has no ratio.
    """
    frames = [
        frame
        for provider in providers
        if (
            frame := _modelled(
                crdf,
                crs=crs.filter(pl.col("year") <= last_year),
                status=status,
                provider=provider,
            )
        )
        is not None
    ]
    columns = list(MODELLED.columns)
    if not frames:
        return pl.LazyFrame(
            schema={name: column.dtype for name, column in MODELLED.columns.items()}
        )
    return (
        pl.concat(frames)
        .group_by("provider_code", "year", "recipient_code", "basis")
        .agg(ordered_sum(pl.col("low_usd")), ordered_sum(pl.col("high_usd")))
        .select(columns)
    )


def _modelled(
    crdf: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    status: pl.LazyFrame,
    provider: ModelledProvider,
) -> pl.LazyFrame | None:
    """`provider`'s modelled rows, or None when it has no commitments to model."""
    rows = crdf.filter(
        pl.col("provider_code") == provider.provider_code, pl.col("face_usd") > 0
    )
    records = crs.filter(pl.col("donor_code") == provider.provider_code)
    if provider.agency_code is not None:
        rows = rows.filter(pl.col("agency_code") == provider.agency_code)
        records = records.filter(pl.col("agency_code") == provider.agency_code)
    if provider.statuses is not None:
        kept = status.filter(pl.col("status").is_in(provider.statuses))
        rows = rows.join(kept, on="row_id", how="semi")
    selected = rows.select(
        "row_id", "year", "recipient_code", "crs_id", "climate_usd"
    ).collect()
    if selected.is_empty():
        return None
    rows = selected.lazy()

    ratios = [pl.lit(report.ratio) for report in provider.report_ratios]
    if provider.crs_ratios:
        rows = _with_crs_ratios(rows, records=records, provider=provider)
        ratios.append(pl.col("_period"))
        if not provider.vintage:
            ratios.append(pl.col("_cumulative"))
    capped = [ratio.clip(upper_bound=RATIO_CAP) for ratio in ratios]
    cancelled = pl.col("crs_id").is_in(provider.cancelled_crs_ids)
    rows = rows.with_columns(
        _low=pl.when(cancelled).then(0.0).otherwise(pl.min_horizontal(capped)),
        _high=pl.when(cancelled).then(0.0).otherwise(pl.max_horizontal(capped)),
    )

    missing = rows.filter(pl.col("_low").is_null()).select("row_id").collect()
    if missing.height:
        raise ConfigError(
            f"providers.toml: no disbursement ratio for provider "
            f"{provider.provider_code}'s rows {missing['row_id'].head(5).to_list()}"
        )
    return rows.select(
        provider_code=pl.lit(provider.provider_code, pl.Int32),
        year="year",
        recipient_code="recipient_code",
        basis=pl.lit(provider.basis),
        low_usd=pl.col("climate_usd") * pl.col("_low"),
        high_usd=pl.col("climate_usd") * pl.col("_high"),
    )


def _with_crs_ratios(
    rows: pl.LazyFrame, *, records: pl.LazyFrame, provider: ModelledProvider
) -> pl.LazyFrame:
    """Add `_cumulative` and `_period` CRS ratios to `rows`.

    Ratios pool the provider's unbroken run of years with disbursement that ends at
    its latest. The cumulative ratio pools the whole run; the period ratio pools the
    run from the commitment year, or for `vintage` providers is the commitment
    year's vintage ratio. Recipient ratios replace portfolio ones where the run
    holds the recipient's commitments.
    """
    records = records.select(
        "year",
        "project_number",
        "recipient_code",
        _portfolio=pl.lit(0),
        com=pl.col("commitment_usd").fill_null(0.0),
        dis=pl.col("disbursement_usd").fill_null(0.0),
    )
    disbursing = (
        records.filter(pl.col("dis") > 0).select(pl.col("year").unique()).collect()
    )
    if not disbursing.height:
        raise ConfigError(
            f"providers.toml: provider {provider.provider_code} has no CRS "
            "disbursement; set crs_ratios = false and give report_ratios"
        )
    first, last = _reporting_run(set(disbursing["year"]))
    run = records.filter(pl.col("year").is_between(first, last))
    commitment_years = rows.select(commitment_year=pl.col("year").unique())

    def joined(frame: pl.LazyFrame, scope: str, suffix: str) -> pl.LazyFrame:
        cumulative = _ratio(run, by=[scope])
        if provider.vintage:
            period = _vintage_ratio(records, scope=scope)
        else:
            window = run.join(commitment_years, how="cross").filter(
                pl.col("year") >= pl.col("commitment_year")
            )
            period = _ratio(window, by=["commitment_year", scope])
        return frame.join(
            cumulative.rename({"ratio": f"_cumulative{suffix}"}), on=scope, how="left"
        ).join(
            period.rename({"commitment_year": "year", "ratio": f"_period{suffix}"}),
            on=["year", scope],
            how="left",
        )

    rows = joined(rows.with_columns(_portfolio=pl.lit(0)), "_portfolio", "")
    if provider.by_recipient:
        rows = joined(rows, "recipient_code", "_recipient").with_columns(
            _cumulative=pl.coalesce("_cumulative_recipient", "_cumulative"),
            _period=pl.coalesce("_period_recipient", "_period"),
        )
    return rows


def _reporting_run(years: set[int]) -> tuple[int, int]:
    """The unbroken run of `years` that ends at the latest."""
    first = last = max(years)
    while first - 1 in years:
        first -= 1
    return first, last


def _ratio(flows: pl.LazyFrame, *, by: Sequence[str]) -> pl.LazyFrame:
    """Disbursed over committed per `by`; null where commitments are not positive.

    The ratio is null, not filtered out: with a cached input, polars 2.0 pushes a
    filter on a sum below the aggregation when the sum keeps its input's name.
    """
    committed = pl.col("committed")
    return (
        flows.group_by(*by)
        .agg(
            committed=ordered_sum(pl.col("com")),
            disbursed=ordered_sum(pl.col("dis")),
        )
        .select(*by, ratio=pl.when(committed > 0).then(pl.col("disbursed") / committed))
    )


def _vintage_ratio(records: pl.LazyFrame, *, scope: str) -> pl.LazyFrame:
    """Per commitment year, disbursed over committed for the projects first
    committed that year, over all their records."""
    projects = records.filter(pl.col("project_number").is_not_null())
    first = (
        projects.filter(pl.col("com") > 0)
        .group_by("project_number")
        .agg(commitment_year=pl.col("year").min())
    )
    return _ratio(
        projects.join(first, on="project_number"), by=["commitment_year", scope]
    )
