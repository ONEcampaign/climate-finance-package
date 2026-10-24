"""The measure stage: agency climate spending and the modelled line.

`agency_spending` holds, per multilateral channel and share year, the agency's CRDF
commitments by Rio markers, and those reporting climate components, as fractions of
its total CRS commitments, both summed over the year and the year before. The query
weighs these rows with a method's coefficients into the agency's climate share,
which is the OECD's imputed-share construction with that method's coefficients.
Under ONE (the highest-marker rule, significant markers at 0.4, climate components
as reported) the banks that report climate components agree with the OECD within
0.004 in 2021 and 2022 (IBRD 0.310 and 0.309, IDA 0.313 and 0.313, AfDB 0.399 and
0.398, CAF 0.224 and 0.226). Shares rest on commitments. Dividing matched
disbursements by all disbursements would leave untraced disbursement in the
denominator and push banks the trace cannot follow to zero. Where the span of a
share year holds a year the CRDF does not measure adaptation in, its rows are not
`adaptation_measured`, and the query's adaptation and cross-cutting shares are null.
The stage refuses a table whose ONE shares differ from those summed row by row from
the CRDF.

A share's `share_source` says where it comes from. `computed` shares are measured.
A share year is measured once the agency's CRDF rows cover its span: from the year
after its first CRDF year, or from that year itself when the agency committed
nothing the year before, so that its first reporting year's one-year span stands in
for two. A span whose CRS commitments fall below `SHARE_FLOOR_USD`, as the European
Development Fund's do once it stops committing, repeats the last computed year's
rows as `held`. A channel and year with neither takes the OECD's imputed share as
an `agency_shares` row of source `one`: channels with no CRS reporter (the Asian
Development Fund, the Montreal Protocol fund, the UNFCCC and the IPCC) and years
before an agency's CRDF rows begin.

`agency_shares` holds the OECD's imputed shares, with source `oecd`, and those
fallback rows. Adaptation and mitigation shares each include the cross-cutting
share.

`modelled` holds, for providers that publish no project disbursements, their CRDF
climate commitments times a range of disbursement ratios, as `providers.toml` sets.
"""

from __future__ import annotations

import logging
import tomllib
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from climate_finance.build.allocate import counted_rows
from climate_finance.build.errors import ConfigError, ReleaseInvariantError
from climate_finance.build.fetch import fetched_years, last_complete_year
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.sums import ordered_sum
from climate_finance.methods import ONE, face_shares, weights
from climate_finance.query import measured_shares
from climate_finance.schema import AGENCY_SHARES, AGENCY_SPENDING, MODELLED, STATUSES

if TYPE_CHECKING:
    from collections.abc import Sequence

    from climate_finance.build.stages import StageContext, StageOutput

logger = logging.getLogger(__name__)

PROVIDERS_TOML = Path(__file__).with_name("providers.toml")
# Years summed for an agency's ONE share: the share year and the one before.
SHARE_YEARS = 2
# The least CRS commitment a ONE share's span rests on. In 2015 to 2024 the smallest
# span of any agency is USD 2.3m (the Special Climate Change Fund in 2018); the
# European Development Fund's 2024 span, after its last commitments in 2022, is USD
# 2,984.
SHARE_FLOOR_USD = 100_000.0
# A commitment cannot disburse more than its face value, so ratios stop here.
RATIO_CAP = 1.0
# Shares under SHARE_TOLERANCE apart agree; shares are fractions of one total.
SHARE_TOLERANCE = 1e-12
_MARKERS = ("rio_adaptation", "rio_mitigation")
_COMPONENTS = ("adaptation_usd", "mitigation_usd", "overlap_usd")
# The columns of `query.measured_shares`, without their `share_` prefix.
_SHARE_PARTS = ("adaptation", "mitigation", "overlap", "unsplit")


class _Fact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class AgencyChannel(_Fact):
    """The CRS reporter behind a channel of delivery. With no `agency_codes`, it is
    every agency of the provider. `without_crdf` gives, for each listed agency that
    has no CRDF rows, why its commitments belong in the channel's denominator."""

    channel_code: int
    provider_code: int
    agency_codes: tuple[int, ...] | None = None
    without_crdf: dict[int, str] = {}

    @model_validator(mode="after")
    def _listed(self) -> Self:
        if unlisted := set(self.without_crdf) - set(self.agency_codes or ()):
            raise ValueError(f"without_crdf names unlisted agencies {sorted(unlisted)}")
        return self


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
        """Each agency's spending measures one share, or it would count in two. An
        agency reports for one channel, or for several channels with the same
        agencies, which then take the same share."""
        by_provider: dict[int, set[tuple[int, ...] | None]] = {}
        for entry in self.agencies:
            by_provider.setdefault(entry.provider_code, set()).add(entry.agency_codes)
        for provider, groups in by_provider.items():
            codes = [code for group in groups for code in group or ()]
            if (None in groups and len(groups) > 1) or len(codes) != len(set(codes)):
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
    """Write `agency_shares`, `agency_spending` and `modelled`.

    Share years are the fetched recipient CRDF years. Shares also read `crdf_span`,
    the CRDF rows of the year before the first share year, so the first share year
    has its two-year span.

    Raises:
        ReleaseInvariantError: when the ONE shares weighed from `agency_spending`
            differ from those summed row by row from the CRDF.
    """
    facts = load_provider_facts()
    crdf = counted_commitments(context.read("normalise", "crdf"))
    crs = trace_population(context.read("normalise", "crs"))
    spanned = pl.concat(
        [counted_commitments(context.read("normalise", "crdf_span")), crdf]
    )
    years = fetched_years(context)["crdf_recipient"]
    spending = agency_spending(
        spanned, crs=crs, agencies=facts.agencies, years=years
    ).collect()
    if problems := one_share_problems(
        spending.lazy(),
        crdf=spanned,
        crs=crs,
        agencies=facts.agencies,
        years=years,
    ):
        raise ReleaseInvariantError(problems)
    shares = agency_shares(
        context.read("normalise", "imputed_shares"),
        spending=spending.lazy(),
        years=years,
    ).collect()
    gaps = unshared_contributions(
        spending.lazy(),
        shares=shares.lazy(),
        core_contributions=context.read("normalise", "core_contributions"),
    ).collect()
    for row in gaps.iter_rows(named=True):
        logger.warning(
            "No ONE agency share for channel %s in %s: USD %.1fm of core "
            "contributions disbursed attribute nothing",
            row["channel_code"],
            row["year"],
            row["contribution_usd"] / 1e6,
        )
    return {
        "agency_shares": shares,
        "agency_spending": spending,
        "modelled": modelled_ranges(
            crdf,
            crs=crs,
            status=context.read("review", "commitments"),
            providers=facts.modelled,
            last_year=last_complete_year(context),
        ),
    }


def counted_commitments(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """CRDF commitments that count once: recipient-perspective rows, without repeats
    of an earlier vintage or aggregate parents that rebuilt children replace."""
    return counted_rows(crdf).filter(pl.col("repeat_of").is_null())


def agency_shares(
    imputed_shares: pl.LazyFrame, *, spending: pl.LazyFrame, years: tuple[int, int]
) -> pl.LazyFrame:
    """The OECD's imputed shares as `agency_shares` rows of source `oecd`, and of
    source `one` for each channel and share year in `years` that `spending` does not
    measure."""
    first, last = years
    fallback = imputed_shares.filter(pl.col("year").is_between(first, last)).join(
        spending.select("channel_code", "year").unique(),
        on=["channel_code", "year"],
        how="anti",
    )
    columns = list(AGENCY_SHARES.columns)
    return pl.concat(
        [
            frame.with_columns(source=pl.lit(source)).select(columns)
            for frame, source in ((fallback, "one"), (imputed_shares, "oecd"))
        ]
    )


def agency_spending(
    crdf: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    agencies: Sequence[AgencyChannel],
    years: tuple[int, int],
) -> pl.LazyFrame:
    """Each agency's CRDF commitments by Rio markers over its CRS commitments.

    Per channel and measured share year (`_spans`), marker rows sum their face value
    by `rio_adaptation`, `rio_mitigation` and `published`, a null marker counting as
    0, and component rows sum their face value and climate components into the row
    with null markers. Each sum is divided by the CRS commitments over the same
    span. A measured share year with no CRDF commitment in its span holds one row at
    markers 0 and 0 with no face value, so its share is 0. These rows are
    `computed`. A later share year whose span rests on fewer than `SHARE_FLOOR_USD`
    of CRS commitments repeats the rows of the last computed year as `held`.

    Args:
        crdf: counted CRDF commitments. Rows of the year before `years` begin, when
            present, give the first share year its two-year span.
        crs: the CRS records the trace follows.
        agencies: the CRS reporter behind each channel.
        years: the first and last share years. Rows are not `adaptation_measured`
            where a year of the span has commitments with no adaptation amount,
            which the CRDF does not measure there.

    Raises:
        ConfigError: when an entry of `agencies` has no CRS commitments in the share
            spans, or lists an agency with no CRDF rows and no reason for it.
    """
    rows, spans = _share_spans(crdf, crs=crs, agencies=agencies, years=years)
    component = pl.col("climate_share").is_not_null()
    markers = _rolling_sum(
        rows.filter(~component).with_columns(
            pl.col(*_MARKERS).fill_null(0),
            published=face_shares()["share_climate"],
        ),
        by=(*_MARKERS, "published"),
        columns=("face_usd",),
        years=years,
    )
    components = _rolling_sum(
        rows.filter(component),
        columns=("face_usd", *_COMPONENTS),
        years=years,
    ).with_columns(pl.lit(None, pl.Int8).alias(name) for name in _MARKERS)
    keys = ["channel_code", "year"]
    total = spans.filter("enough").select(*keys, "total")
    measured = pl.concat([markers, components], how="diagonal")
    empty = total.join(measured, on=keys, how="anti").select(
        *keys,
        *(pl.lit(0, pl.Int8).alias(name) for name in _MARKERS),
        face_usd=pl.lit(0.0),
    )
    computed = (
        pl.concat([measured, empty], how="diagonal")
        .join(total, on=keys)
        .join(_adaptation_unmeasured(crdf), on="year", how="left")
        .select(
            *keys,
            *_MARKERS,
            "published",
            face_share=pl.col("face_usd") / pl.col("total"),
            **{
                f"{part.removesuffix('_usd')}_share": pl.col(part) / pl.col("total")
                for part in _COMPONENTS
            },
            adaptation_measured=pl.col("_unmeasured").is_null(),
            share_source=pl.lit("computed"),
        )
    )
    held = (
        spans.filter(~pl.col("enough"), pl.col("held_year").is_not_null())
        .select(*keys, "held_year")
        .join(
            computed.rename({"year": "held_year"}),
            on=["channel_code", "held_year"],
        )
        .with_columns(share_source=pl.lit("held"))
    )
    columns = list(AGENCY_SPENDING.columns)
    return pl.concat([computed.select(columns), held.select(columns)])


def one_share_problems(
    spending: pl.LazyFrame,
    *,
    crdf: pl.LazyFrame,
    crs: pl.LazyFrame,
    agencies: Sequence[AgencyChannel],
    years: tuple[int, int],
) -> list[str]:
    """Channels and years whose `computed` ONE shares weighed from `spending` by the
    query differ by more than `SHARE_TOLERANCE` from the shares summed row by row
    from `crdf` over the same measured share years, or that only one of the two
    holds. Shares both leave unmeasured agree. An empty list means they agree."""
    keys = ["channel_code", "year"]
    weighed = measured_shares(
        spending.filter(pl.col("share_source") == "computed"), method=ONE
    ).rename({f"share_{part}": part for part in _SHARE_PARTS})
    rows, spans = _share_spans(crdf, crs=crs, agencies=agencies, years=years)
    summed = (
        spans.filter("enough")
        .select(*keys, "total")
        .join(
            _rolling_sum(_one_climate(rows), columns=_SHARE_PARTS, years=years),
            on=keys,
            how="left",
        )
        .join(_adaptation_unmeasured(crdf), on="year", how="left")
        .select(
            *keys,
            *(pl.col(part).fill_null(0.0) / pl.col("total") for part in _SHARE_PARTS),
            "_unmeasured",
        )
        .with_columns(
            pl.when(pl.col("_unmeasured").is_null()).then(part)
            for part in ("adaptation", "overlap")
        )
        .drop("_unmeasured")
    )
    compared = (
        weighed.join(summed, on=keys, how="full", coalesce=True, suffix="_summed")
        .sort(keys)
        .collect()
    )
    problems = []
    for row in compared.iter_rows(named=True):
        label = f"agency_spending {row['channel_code']} {row['year']}"
        if row["unsplit"] is None or row["unsplit_summed"] is None:
            problems.append(f"{label}: a ONE share only one route measures")
            continue
        problems.extend(
            f"{label}: ONE {part} share {row[part]!r} differs from "
            f"{row[f'{part}_summed']!r}"
            for part in _SHARE_PARTS
            if _differ(row[part], row[f"{part}_summed"])
        )
    return problems


def _differ(share: float | None, summed: float | None) -> bool:
    """Whether two shares differ by more than `SHARE_TOLERANCE`, or only one is
    measured."""
    if share is None or summed is None:
        return (share is None) != (summed is None)
    return abs(share - summed) > SHARE_TOLERANCE


def _share_spans(
    crdf: pl.LazyFrame,
    *,
    crs: pl.LazyFrame,
    agencies: Sequence[AgencyChannel],
    years: tuple[int, int],
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """The CRDF rows of `agencies` by channel up to the last share year, and per
    channel and share year its span (`_spans`).

    Raises:
        ConfigError: as `_check_agencies` does.
    """
    first, last = years
    rows = _by_channel(crdf.filter(pl.col("year") <= last), agencies=agencies)
    # Collected once, as both the agency check and the spans read it and the CRS
    # is the largest input.
    records = (
        _by_channel(
            crs.rename({"donor_code": "provider_code"}).filter(
                pl.col("year").is_between(first - SHARE_YEARS + 1, last)
            ),
            agencies=agencies,
        )
        .select(
            "channel_code", "provider_code", "agency_code", "year", "commitment_usd"
        )
        .collect()
        .lazy()
    )
    _check_agencies(rows, records=records, agencies=agencies)
    spans = _spans(
        rows,
        committed=records.select(
            "channel_code", "year", total=pl.col("commitment_usd").fill_null(0.0)
        ),
        channels={entry.channel_code for entry in agencies},
        years=years,
    )
    return rows, spans


def _spans(
    rows: pl.LazyFrame,
    *,
    committed: pl.LazyFrame,
    channels: set[int],
    years: tuple[int, int],
) -> pl.LazyFrame:
    """Per channel and share year: the span's CRS commitments `total`, whether a
    share is measured on enough of them (`enough`), and else the last year that was
    (`held_year`).

    A share year is measured from the year after the channel's first CRDF year, or
    from that year when the channel has no CRS commitments in the year before.
    """
    first, last = years
    grid = pl.LazyFrame(
        [(channel, year) for channel in channels for year in range(first, last + 1)],
        schema={"channel_code": pl.Int32, "year": pl.Int16},
        orient="row",
    )
    start = rows.group_by("channel_code").agg(start=pl.col("year").min())
    year_before = (
        committed.group_by("channel_code", "year")
        .agg(before=ordered_sum(pl.col("total")))
        .with_columns(year=pl.col("year") + 1)
    )
    measured = (pl.col("year") > pl.col("start")) | (
        (pl.col("year") == pl.col("start")) & (pl.col("before").fill_null(0.0) <= 0)
    )
    return (
        grid.join(start, on="channel_code", how="left")
        .join(year_before, on=["channel_code", "year"], how="left")
        .join(
            _rolling_sum(committed, columns=("total",), years=years),
            on=["channel_code", "year"],
            how="left",
        )
        .with_columns(
            enough=(measured & (pl.col("total") >= SHARE_FLOOR_USD)).fill_null(False)
        )
        .sort("channel_code", "year")
        .with_columns(
            held_year=pl.when("enough")
            .then(pl.col("year"))
            .forward_fill()
            .over("channel_code")
        )
    )


def _check_agencies(
    rows: pl.LazyFrame, *, records: pl.LazyFrame, agencies: Sequence[AgencyChannel]
) -> None:
    """Refuse an entry with no CRS commitments in `records`, and a listed agency with
    no CRDF rows in `rows` and no reason in `without_crdf`."""
    committing = set(
        records.filter(pl.col("commitment_usd") > 0)
        .select("channel_code", "provider_code", "agency_code")
        .unique()
        .collect()
        .iter_rows()
    )
    reporting = set(
        rows.select("provider_code", "agency_code").unique().collect().iter_rows()
    )
    for entry in agencies:
        if not any(
            channel == entry.channel_code and provider == entry.provider_code
            for channel, provider, _ in committing
        ):
            raise ConfigError(
                f"providers.toml: provider {entry.provider_code} has no CRS "
                f"commitments for channel {entry.channel_code} in the share years"
            )
        missing = [
            agency
            for agency in entry.agency_codes or ()
            if (entry.provider_code, agency) not in reporting
            and agency not in entry.without_crdf
        ]
        if missing:
            raise ConfigError(
                f"providers.toml: provider {entry.provider_code}'s agencies {missing} "
                f"have no CRDF rows; drop them from channel {entry.channel_code} or "
                "give the reason in without_crdf"
            )


def unshared_contributions(
    spending: pl.LazyFrame,
    *,
    shares: pl.LazyFrame,
    core_contributions: pl.LazyFrame,
) -> pl.LazyFrame:
    """Disbursed core contributions to every channel in the years neither `spending`
    nor a `one` row of `shares` gives it a ONE share, by channel and year."""
    keys = ["channel_code", "year"]
    one = pl.concat(
        [
            spending.select(keys),
            shares.filter(pl.col("source") == "one").select(keys),
        ]
    )
    return (
        core_contributions.filter(
            pl.col("flow") == "disbursements", pl.col("contribution_usd") > 0
        )
        .join(one.unique(), on=keys, how="anti")
        .group_by(keys)
        .agg(ordered_sum(pl.col("contribution_usd")))
        .sort(keys)
    )


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
    # A record's own channel_code is its channel of delivery, so the reporting channel
    # comes from the mapping.
    return (
        frame.drop("channel_code")
        .join(mapping, on="provider_code")
        .filter(
            pl.col("_agency").is_null() | (pl.col("_agency") == pl.col("agency_code"))
        )
    )


def _adaptation_unmeasured(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """The share years whose span holds a commitment with no adaptation amount, with
    a true `_unmeasured`."""
    return (
        crdf.filter(pl.col("adaptation_usd").is_null())
        .select(
            year=pl.int_ranges(
                pl.col("year"), pl.col("year") + SHARE_YEARS, dtype=pl.Int16
            )
        )
        .explode("year")
        .unique()
        .with_columns(_unmeasured=pl.lit(True))
    )


def _one_climate(crdf: pl.LazyFrame) -> pl.LazyFrame:
    """Each commitment's inclusive adaptation and mitigation value under ONE, and
    its overlap, whose objectives are disjoint before the overlap is added back to
    the other two. A marker weight stops at the climate share the provider
    published, as IFAD's significant rows at 40% of face value."""
    # An unmeasured adaptation part weighs as 0 here, and `one_share_problems` nulls
    # the shares it would reach.
    weighted = crdf.with_columns(**face_shares()).with_columns(
        **weights(ONE, published=pl.col("share_climate"))
    )
    cross_cutting = pl.col("cross_cutting")
    return weighted.select(
        "channel_code",
        "year",
        adaptation=pl.col("face_usd") * (pl.col("adaptation") + cross_cutting),
        mitigation=pl.col("face_usd") * (pl.col("mitigation") + cross_cutting),
        overlap=pl.col("face_usd") * cross_cutting,
        unsplit=pl.lit(0.0),
    )


def _rolling_sum(
    frame: pl.LazyFrame,
    *,
    by: Sequence[str] = (),
    columns: Sequence[str],
    years: tuple[int, int],
) -> pl.LazyFrame:
    """Per channel, share year in `years` and `by`, `columns` summed over the share
    year and the `SHARE_YEARS - 1` years before it."""
    first, last = years
    return (
        frame.with_columns(
            year=pl.int_ranges(
                pl.col("year"), pl.col("year") + SHARE_YEARS, dtype=pl.Int16
            )
        )
        .explode("year")
        .filter(pl.col("year").is_between(first, last))
        .group_by("channel_code", "year", *by)
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
        return pl.LazyFrame(schema=MODELLED.dtypes)
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
    ).collect()

    missing = rows.filter(pl.col("_low").is_null())
    if missing.height:
        raise ConfigError(
            f"providers.toml: no disbursement ratio for provider "
            f"{provider.provider_code}'s rows {missing['row_id'].head(5).to_list()}"
        )
    return rows.lazy().select(
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
    its latest. The cumulative ratio pools the whole run. The period ratio pools the
    run from the commitment year, or for `vintage` providers is the commitment
    year's vintage ratio. Recipient ratios replace portfolio ones where the run
    holds the recipient's commitments.
    """
    # The provider's records are read once, then held for the several ratios.
    records = (
        records.select(
            "year",
            "project_number",
            "recipient_code",
            _portfolio=pl.lit(0),
            com=pl.col("commitment_usd").fill_null(0.0),
            dis=pl.col("disbursement_usd").fill_null(0.0),
        )
        .collect()
        .lazy()
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
    """Disbursed over committed per `by`.

    The ratio is null where commitments are not positive, and the group stays in the
    result. With a cached input, polars 2.0 pushes a filter on a sum below the
    aggregation when the sum keeps its input's name.
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
