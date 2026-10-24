"""Query results: `Result`, `Quality`, `Reconciliation`, `Comparison` and `compare`."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from html import escape
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.convert import Conversion
from climate_finance.methods import (
    Method,
    MethodRange,
    describe,
    label,
    uses_imputed_shares,
)
from climate_finance.schema import (
    BILATERAL,
    CORE_CONTRIBUTION,
    MULTILATERAL_OUTFLOW,
    STATUSES,
)
from climate_finance.spec import (
    ODA_FLOW_CATEGORY,
    ODA_FLOW_CODES,
    Spec,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

AMOUNTS = (
    "amount", "adaptation", "mitigation", "cross_cutting", "unsplit", "value", "low",
    "high", "untraced",
)  # fmt: skip
# The keys of a result table's rows, before the method.
ROW_KEYS = ("year", "provider_code", "recipient_code", "bi_multi")


class Table:
    """A result table that converts to polars, pandas or arrow."""

    def __init__(self, frame: pl.LazyFrame) -> None:
        self._frame = frame

    def to_polars(self) -> pl.DataFrame:
        """The table as a polars DataFrame."""
        return self._frame.collect()

    def to_pandas(self) -> pd.DataFrame:
        """The table as a pandas DataFrame."""
        return self.to_polars().to_pandas()

    def to_arrow(self) -> pa.Table:
        """The table as a pyarrow Table."""
        return self.to_polars().to_arrow()

    def __repr__(self) -> str:
        return repr(self.to_polars())

    def _repr_html_(self) -> str:
        return self.to_polars()._repr_html_()


class Meta(BaseModel):
    """Where a result came from: release, package and source versions.

    `conversion` identifies the pydeflate version and exchange-rate or deflator factors
    behind a converted result; it is None for USD at current prices. `years` copies
    the release's `years`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    package_version: str
    source_versions: dict[str, str]
    model_versions: dict[str, str]
    years: dict[str, tuple[int, int]] = {}
    conversion: Conversion | None = None


class Result:
    """The answer to one `Spec`: a table, the rows behind it, and its provenance.

    `table` takes each method's amounts by key, with that method's own `low` and
    `high`. Under `spec.measure="share_of_total"` the result divides each method's
    amounts by its `totals`, so a result always holds the measure its spec names.
    `low` and `high` then span every method of the key, the lowest `low` and the
    highest `high`, in amounts or in shares.
    """

    def __init__(
        self,
        *,
        table: Table,
        modelled: Table,
        rows: pl.LazyFrame,
        totals: pl.LazyFrame,
        spec: Spec,
        meta: Meta,
    ) -> None:
        measured = _shares(table, totals) if spec.measure == "share_of_total" else table
        self.table = Table(_bounds(measured._frame))
        self._sums = table
        self.modelled = modelled
        self.spec = spec
        self.meta = meta
        self._rows = rows
        self._totals = totals

    def __repr__(self) -> str:
        return f"{_title(self.spec, release=self.meta.release)}\n{self.table!r}"

    def _repr_html_(self) -> str:
        title = escape(_title(self.spec, release=self.meta.release))
        return f"<p>{title}</p>{self.table._repr_html_()}"

    def rows(self, **filters: object) -> pl.DataFrame:
        """The project rows behind the table, filtered by column equality.

        A filter value that is a list, tuple, set or range matches any of its items.

        Raises:
            KeyError: when a filter names a column the rows do not have.
        """
        columns = self._rows.collect_schema().names()
        if unknown := sorted(set(filters) - set(columns)):
            raise KeyError(f"rows have no column {unknown}; columns are {columns}")
        conditions = [
            pl.col(name).is_in(list(value))
            if isinstance(value, Collection) and not isinstance(value, str)
            else pl.col(name) == value
            for name, value in filters.items()
        ]
        return self._rows.filter(*conditions).collect()

    def explain(self) -> str:
        """A plain-language account of the method, flow and filters applied."""
        provider = [
            label(method)
            for method in self.spec.methods
            if method.coefficients == "provider"
        ]
        coefficients = None
        if provider:
            coefficients = (
                self._rows.filter(
                    pl.col("method").is_in(provider),
                    pl.col("climate_share").is_null(),
                )
                .group_by(
                    "method", "coefficient_basis", "coefficient_source", "provider_code"
                )
                .agg(
                    pl.col("agency_code").drop_nulls().unique().sort(),
                    pl.col("year").unique().sort(),
                )
                .collect()
            )
        by_method = (
            self._rows.group_by("method")
            .agg(
                unsplit=pl.col("channel_code")
                .filter(pl.col("origin") == "attribution", pl.col("unsplit") != 0)
                .unique()
                .sort(),
                unmeasured=pl.col("year")
                .filter(pl.col("adaptation").is_null(), pl.col("amount").is_not_null())
                .unique(),
            )
            .collect()
        )
        return explain(
            self.spec,
            release=self.meta.release,
            coefficients=coefficients,
            unsplit=dict(
                zip(by_method["method"], by_method["unsplit"].to_list(), strict=True)
            ),
            unmeasured=sorted(set(by_method["unmeasured"].explode().drop_nulls())),
            years=self.meta.years,
            conversion=self.meta.conversion,
        )

    def share_of_total(self) -> Result:
        """The same result with every amount divided by all CRS flows of its key.

        The denominator is the release's `total_flows` for the same flow, year,
        provider, recipient and Bi_Multi code. A row without a recipient, such as
        attributed finance, divides by the sum over recipients. Totals cover every
        flow code, so under `oda_only` the share is ODA climate finance over all
        flows. In the provider view, EU Institutions' total holds the same rows as
        their amounts under each method, at the same scaling. A key missing from
        `total_flows` gives null shares. The rows and the modelled line keep their
        amounts. The result's spec has `measure="share_of_total"`, so rerunning it
        returns the same shares.
        """
        if self.spec.measure == "share_of_total":
            return self
        return Result(
            table=self._sums,
            modelled=self.modelled,
            rows=self._rows,
            totals=self._totals,
            spec=self.spec.replace(measure="share_of_total"),
            meta=self.meta,
        )


def _bounds(frame: pl.LazyFrame) -> pl.LazyFrame:
    """`frame` with `low` and `high` spanning every method of the same key."""
    return frame.with_columns(
        low=pl.col("low").min().over(ROW_KEYS),
        high=pl.col("high").max().over(ROW_KEYS),
    )


def _shares(table: Table, totals: pl.LazyFrame) -> Table:
    """`table` with every amount divided by the total of its key and method in
    `totals`."""
    return Table(
        table._frame.join(
            totals,
            on=[*ROW_KEYS, "method"],
            how="left",
            nulls_equal=True,
            validate="m:1",
            maintain_order="left",
        )
        .with_columns(pl.col(name) / pl.col("total") for name in AMOUNTS)
        .drop("total")
    )


@dataclass(frozen=True, kw_only=True)
class Quality:
    """Link, coverage and status statistics for part of a release.

    `table` holds the release's `quality` rows by provider and year. `status` holds
    climate value by provider and commitment status. `eu_left_out` holds, by EU
    Institutions agency, channel, year and flow, the value of the agency's own rows
    that a provider view of every provider leaves out when EU Institutions count as
    a channel, because members' contributions to that channel stand in for them.
    Add it back to compare with totals that count EU Institutions as a provider.
    `attribution` holds, by provider, year and flow, the value attributed through
    core contributions, split by the agency share's source into `computed_usd`,
    `held_usd` and `oecd_usd`. `untraced` holds, by provider and disbursement year,
    the Rio-marked CRS disbursement outside `disbursements` at face value: the
    excess and the unlinked records that disbursement totals count as untraced, with
    their climate value under the method (`excess_climate_usd`,
    `unlinked_climate_usd`), and the two amounts no total counts, allocations to
    commitments with no CRDF row (`outside_release_usd`) and the route funds'
    unlinked records (`fund_unlinked_usd`).
    """

    table: Table
    status: Table
    eu_left_out: Table
    attribution: Table
    untraced: Table

    def summary(self) -> pl.DataFrame:
        """Per provider: link rate, certain-route share, label sources, status split.

        Shares are of the climate value of counted commitments. `link_rate` is the
        share that traces to CRS records, and `certain_route_share` the share that
        traces with every CRDF row reaching its CRS commitment through
        `schema.CERTAIN_ROUTES`. `grant_equiv_coverage` is the part of the linked
        commitments' allocated value whose record has a grant equivalent, each year's
        weighted by linked climate value over the years that have allocations.
        `repeat_climate_usd` is the climate value of rows that repeat an earlier
        vintage, which every total leaves out. Their disbursements count on the row
        they repeat.
        """
        climate = pl.col("climate_usd").sum()
        linked = pl.col("linked_usd").sum()
        coverage = pl.col("grant_equiv_coverage")
        known = coverage.is_not_null()
        rates = self.table._frame.group_by("provider_code").agg(
            pl.col("commitments").sum(),
            climate_usd=climate,
            repeat_climate_usd=pl.col("repeat_climate_usd").sum(),
            link_rate=linked / climate,
            certain_route_share=pl.col("certain_route_usd").sum() / climate,
            model_labelled_share=pl.col("model_labelled_usd").sum() / climate,
            report_labelled_share=pl.col("report_labelled_usd").sum() / climate,
            grant_equiv_coverage=pl.when(known.any()).then(
                (coverage * pl.col("linked_usd")).sum()
                / pl.col("linked_usd").filter(known).sum()
            ),
        )
        split = self.status._frame.group_by("provider_code").agg(
            (
                pl.col("climate_usd").filter(pl.col("status") == status).sum() / climate
            ).alias(f"{status}_share")
            for status in STATUSES
        )
        return (
            rates.join(split, on="provider_code", how="full", coalesce=True)
            .sort("provider_code")
            .collect()
        )


def explain(
    spec: Spec,
    *,
    release: str,
    coefficients: pl.DataFrame | None = None,
    unsplit: Mapping[str, Sequence[int]] | None = None,
    unmeasured: Sequence[int] = (),
    years: Mapping[str, tuple[int, int]] | None = None,
    conversion: Conversion | None = None,
) -> str:
    """The method, view, flow, filters and units behind `spec`, in plain language.

    `coefficients` holds, per provider-coefficient method label (`method`), each
    `coefficient_basis` and `coefficient_source` its marker rows took, with the
    `provider_code` and the lists of `agency_code` and `year` values that took them.
    `unsplit` maps a method's label to the agency channel codes whose climate share
    has no split by objective. `unmeasured` lists the years with rows whose
    adaptation is not measured. `years` is the release's `years`; under the provider
    view, the queried years (`spec.years`, else the recipient CRDF's) outside those
    of `core_contributions` or of the share table a method reads (`agency_shares`
    under the OECD's imputed shares, else `agency_spending`) are named as holding no
    attributed finance under that method. `conversion` names the conversion factors
    of a converted result.
    """
    unsplit = unsplit or {}
    methods = spec.methods
    lines = [f"Release {release}."]
    for method in methods:
        lines.append(describe(method))
        if coefficients is not None and method.coefficients == "provider":
            lines.extend(
                _coefficient_lines(
                    method, coefficients.filter(pl.col("method") == label(method))
                )
            )
        if channels := unsplit.get(label(method)):
            lines.append(
                f"Channels {', '.join(map(str, channels))}: the climate share has no "
                "split by objective, so their attributed value sits in unsplit."
            )
    if len(methods) > 1:
        lines.append("low and high are the lowest and highest bound across methods.")
    if unmeasured:
        lines.append(
            f"Adaptation is not measured for rows in {_runs(unmeasured)}, as the "
            "CRDF has no adaptation marker or climate components before 2010 and an "
            "agency share of 2010 spans 2009. Their adaptation and cross_cutting are "
            "null, and their climate value is mitigation only."
        )
    lines.append(_VIEWS[spec.view])
    if spec.view == "provider":
        lines.extend(_coverage(methods, queried=spec.years, years=years or {}))
    lines.append(_FLOWS[spec.flow])
    if spec.flow != "commitments":
        lines.append(_UNTRACED[spec.untraced])
    lines.append(
        "Commitments that repeat an earlier vintage are left out, and their "
        "disbursements count on the row they repeat; snap.quality() reports their "
        "value."
    )
    filters = [
        _field_text(field, getattr(spec, field))
        for field in ("providers", "recipients", "years")
        if getattr(spec, field) is not None
    ]
    if filters:
        lines.append(f"Filtered to {'; '.join(filters)}.")
    if spec.view == "provider":
        lines.append(
            "Attributed rows carry share_source: computed shares are measured on the "
            "agency's own commitments, held shares repeat its last share measured on "
            "enough commitments, and oecd shares are the OECD's imputed share, which "
            "OECD always uses and ONE uses where it has no share of its own."
        )
    if spec.recipients is not None and spec.view == "provider":
        lines.append("Attributed finance has no recipient, so it drops out.")
    if spec.oda_only:
        lines.append(
            f"ODA only: commitments of flow category {ODA_FLOW_CATEGORY} and "
            f"disbursements of flow codes {_listing(ODA_FLOW_CODES)}; rows without "
            "either drop out, and core contributions count as ODA."
        )
    prices = "current prices" if spec.prices == "current" else f"{spec.prices} prices"
    source = (
        ""
        if spec.currency == "USD" and spec.prices == "current"
        else ", converted with OECD DAC exchange rates and deflators by provider "
        "(the DAC total for providers without their own series)"
    )
    units = f"{spec.currency} at {prices}{source}"
    if spec.measure == "share_of_total":
        lines.append(
            "Values are shares of all CRS flows of the same flow, year, provider, "
            "recipient and Bi_Multi code, every flow code included. The rows and the "
            f"modelled table hold amounts in {units}."
        )
        if spec.view == "provider":
            lines.append(
                "EU Institutions' totals hold the same rows as their amounts, at the "
                "same scaling: the EIB's flows leave them where EU Institutions count "
                "as a provider, and each agency's flows scale with its own rows where "
                "they count as a channel."
            )
    else:
        lines.append(f"Amounts in {units}.")
    if conversion is not None:
        lines.append(
            f"Conversion factors: pydeflate {conversion.pydeflate_version}, "
            f"SHA-256 {conversion.factors_sha256}."
        )
    return "\n".join(lines)


# The share table each kind of method reads, and its name in running text.
_SHARE_TABLES = {
    True: ("agency_shares", "the OECD's imputed shares"),
    False: ("agency_spending", "agency spending"),
}


def _coverage(
    methods: Sequence[Method],
    *,
    queried: Sequence[int] | None,
    years: Mapping[str, tuple[int, int]],
) -> list[str]:
    """A line per share table `methods` read, in their order, naming the queried
    years outside the years of `core_contributions` or of that table."""
    tables: dict[bool, list[str]] = {}
    for method in methods:
        tables.setdefault(uses_imputed_shares(method), []).append(label(method))
    lines = []
    for imputed, labels in tables.items():
        table, text = _SHARE_TABLES[imputed]
        if outside := _outside_attribution(queried, table=table, years=years):
            lines.append(
                f"Under {_listing(labels)}, attributed finance covers core "
                f"contributions in {_span(years, 'core_contributions')} and {text} "
                f"in {_span(years, table)}; the provider view attributes none in "
                f"{_runs(outside)}."
            )
    return lines


def _outside_attribution(
    queried: Sequence[int] | None,
    *,
    table: str,
    years: Mapping[str, tuple[int, int]],
) -> list[int]:
    """The queried years outside the years of `core_contributions` or `table`.
    Without queried years, the recipient CRDF's are queried; a release that
    records no years gives none."""
    if not years:
        return []
    if queried is None:
        if "crdf_recipient" not in years:
            return []
        first, last = years["crdf_recipient"]
        queried = range(first, last + 1)
    spans = [years.get(name) for name in ("core_contributions", table)]
    return sorted(
        year
        for year in set(queried)
        if any(span is None or not span[0] <= year <= span[1] for span in spans)
    )


def _span(years: Mapping[str, tuple[int, int]] | None, name: str) -> str:
    """`name`'s years in `years` as text, or "no year"."""
    if years is None or name not in years:
        return "no year"
    first, last = years[name]
    return f"{first} to {last}"


def _runs(years: Sequence[int]) -> str:
    """Sorted `years` as running text, consecutive years as a span, such as
    "2000 to 2014 and 2025"."""
    runs: list[list[int]] = []
    for year in years:
        if runs and year == runs[-1][-1] + 1:
            runs[-1].append(year)
        else:
            runs.append([year])
    *rest, last = (
        str(run[0]) if len(run) == 1 else f"{run[0]} to {run[-1]}" for run in runs
    )
    return f"{', '.join(rest)} and {last}" if rest else last


def _listing(codes: Sequence[int | str]) -> str:
    """`codes` as running text, such as "1, 3, 7 and 8"."""
    *rest, last = map(str, codes)
    return f"{', '.join(rest)} and {last}" if rest else last


# The order in which `explain` lists the bases of provider coefficients.
_BASES = ("provider", "default", "case_by_case", None)


def _coefficient_lines(method: Method, uses: pl.DataFrame) -> list[str]:
    """One sentence per coefficient basis and source that `method`'s rows took.

    `uses` holds a row per basis, source and provider, with the provider's
    `agency_code` and `year` lists. A provider listed under several bases or sources
    is named with the agencies, or the years, that tell its entries apart.
    """
    entries: dict[tuple[str | None, str | None], list[dict]] = {}
    by_provider: dict[int, list[dict]] = {}
    for use in uses.sort("provider_code").iter_rows(named=True):
        key = (use["coefficient_basis"], use["coefficient_source"])
        entries.setdefault(key, []).append(use)
        by_provider.setdefault(use["provider_code"], []).append(use)

    def name(use: dict) -> str:
        others = by_provider[use["provider_code"]]
        parts = [
            f"{noun if len(use[column]) == 1 else plural} "
            f"{', '.join(map(str, use[column]))}"
            for noun, plural, column in (
                ("agency", "agencies", "agency_code"),
                ("year", "years", "year"),
            )
            if use[column] and len({tuple(other[column]) for other in others}) > 1
        ]
        code = str(use["provider_code"])
        return f"{code} ({'; '.join(parts)})" if parts else code

    fallback = f"significant {method.significant:g} and principal {method.principal:g}"
    lines = []
    for basis in _BASES:
        for (held, source), members in sorted(
            (item for item in entries.items() if item[0][0] == basis),
            key=lambda item: item[0][1] or "",
        ):
            names = ", ".join(map(name, members))
            match held:
                case "provider":
                    lines.append(f"Coefficients of providers {names}: {source}.")
                case "default":
                    lines.append(
                        f"Providers {names} assess each activity case by case; where "
                        "an activity's share is unknown they take the default share "
                        f"in {source}."
                    )
                case "case_by_case":
                    lines.append(
                        f"Providers {names} assess each activity case by case "
                        f"({source}) and state no flat coefficient, so they take "
                        f"{fallback}."
                    )
                case _:
                    lines.append(
                        f"Providers {names} have no coefficient in the release and "
                        f"take {fallback}."
                    )
    return lines


_VIEWS = {
    "recipient": f"Recipient view: bilateral flows (Bi_Multi {_listing(BILATERAL)}) "
    f"and multilateral outflows (code {MULTILATERAL_OUTFLOW}); core contributions "
    f"(code {CORE_CONTRIBUTION}) are left out.",
    "provider": f"Provider view: bilateral flows (Bi_Multi {_listing(BILATERAL)}) "
    "plus each provider's core contributions times the agency's climate share; "
    f"code {CORE_CONTRIBUTION} and code {MULTILATERAL_OUTFLOW} rows are left out.",
}
_FLOWS = {
    "commitments": "Flow: commitments at face value; low and high equal value.",
    "disbursements": "Flow: disbursements allocated to climate commitments, weighted "
    "by the commitment's markers; low counts the disbursements linked by the "
    "donor's CRS id or World Bank loan number where the linked records agree, and "
    "bank-ledger and IATI fills in full, since they match on the publisher's own "
    "id; high counts every linked disbursement and the untraced disbursement. "
    "Attributed finance counts core contributions disbursed. The modelled table "
    "holds a disbursement range for commitments whose disbursements no source "
    "reports.",
    "grant_equivalent": "Flow: grant equivalent of disbursements, shared across the "
    "commitments a record funds in proportion to the amount each receives; records "
    "without one stay missing, and snap.quality() reports coverage.",
}


_UNTRACED = {
    True: "Untraced: CRS disbursement with a Rio marker that reaches no commitment, "
    "the excess beyond a full commitment and records linked to none, weighted by "
    "the record's own markers. It counts in value and high, not in low, and the "
    "untraced column holds it; snap.quality() reports it with the marked "
    "disbursement no total counts.",
    False: "Untraced disbursement, Rio-marked CRS disbursement that reaches no "
    "commitment, is left out.",
}


class Reconciliation:
    """A party-by-year comparison of BTR reports with OECD data."""

    def __init__(self, *, table: Table, rows: pl.LazyFrame) -> None:
        self.table = table
        self._rows = rows

    def rows(self, party: str, year: int) -> pl.DataFrame:
        """The project-level link behind one party and year."""
        raise NotImplementedError("BTR reconciliation is not implemented")


@dataclass(frozen=True, kw_only=True)
class Comparison:
    """Several results side by side, as a long table with one column per result."""

    table: Table
    results: tuple[Result, ...]

    def __repr__(self) -> str:
        return f"{self._title()}\n{self.table!r}"

    def _repr_html_(self) -> str:
        return f"<p>{escape(self._title())}</p>{self.table._repr_html_()}"

    def _title(self) -> str:
        columns = self.table._frame.collect_schema().names()[len(ROW_KEYS) :]
        count = len(self.results)
        results = "result" if count == 1 else "results"
        return f"Comparison of {count} {results}: {'; '.join(columns)}"


def compare(*results: Result) -> Comparison:
    """Set results side by side: one row per year, provider, recipient and Bi_Multi
    code, and one `value` column per result and method.

    Columns are named by the spec fields that set the results apart, such as "ONE"
    and "OECD", or "commitments" and "disbursements". A result under several methods
    gives one column per method. A key a result has no row for is null in its column.

    Raises:
        ValueError: when no result is given.
    """
    if not results:
        raise ValueError("compare() needs at least one result")
    names = _result_names([result.spec for result in results])
    frames = []
    for name, result in zip(names, results, strict=True):
        methods = [label(method) for method in result.spec.methods]
        for method in methods:
            column = name if len(methods) == 1 else f"{name}: {method}"
            frames.append(
                result.table._frame.filter(pl.col("method") == method).select(
                    *ROW_KEYS, pl.col("value").alias(column)
                )
            )
    table = frames[0]
    for frame in frames[1:]:
        table = table.join(
            frame, on=ROW_KEYS, how="full", nulls_equal=True, coalesce=True
        )
    return Comparison(
        table=Table(table.sort(ROW_KEYS, nulls_last=True)), results=results
    )


def _result_names(specs: Sequence[Spec]) -> list[str]:
    """Per spec, the values of the fields that differ across `specs`, or its method
    when none differ. Specs that nothing sets apart are numbered as well.
    """
    differing = [
        field
        for field in Spec.model_fields
        if len({getattr(spec, field) for spec in specs}) > 1
    ] or ["method"]
    names = [
        ", ".join(_field_text(field, getattr(spec, field)) for field in differing)
        for spec in specs
    ]
    if len(set(names)) == len(names):
        return names
    return [f"result {number}: {name}" for number, name in enumerate(names, start=1)]


def _field_text(field: str, value: object) -> str:
    """One spec field as a short column-name fragment."""
    match field, value:
        case "method", Method() | MethodRange():
            return label(value)
        case "oda_only", bool():
            return "ODA only" if value else "all flows"
        case "untraced", bool():
            return "with untraced" if value else "traced only"
        case "measure", _:
            return "share of total" if value == "share_of_total" else "amounts"
        case "prices", _:
            return f"{value} prices"
        case "view", _:
            return f"{value} view"
        case (("providers" | "recipients" | "years"), None):
            return f"all {field}"
        case (("providers" | "recipients" | "years"), tuple()):
            return f"{field} {', '.join(map(str, value))}"
    return str(value)


def _title(spec: Spec, *, release: str) -> str:
    """One line naming the method, flow, view, filters, units and release."""
    parts = [
        label(spec.method),
        spec.flow,
        _field_text("view", spec.view),
        *(
            _field_text(field, getattr(spec, field))
            for field in ("providers", "recipients", "years")
            if getattr(spec, field) is not None
        ),
        f"{spec.currency} at {_field_text('prices', spec.prices)}",
    ]
    if spec.measure == "share_of_total":
        parts[-1] = f"share of total; rows in {parts[-1]}"
    if spec.oda_only:
        parts.append("ODA only")
    if not spec.untraced and spec.flow != "commitments":
        parts.append("traced only")
    return f"Result: {'; '.join(parts)}; release {release}"
