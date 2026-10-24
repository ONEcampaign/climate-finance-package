"""Query results: `Result`, `Quality`, `Reconciliation`, `Comparison` and `compare`."""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from html import escape
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.convert import Conversion
from climate_finance.methods import Method, MethodRange, describe, label
from climate_finance.schema import STATUSES
from climate_finance.spec import (
    BILATERAL,
    CORE_CONTRIBUTION,
    MULTILATERAL_OUTFLOW,
    ODA_FLOW_CODES,
    Spec,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

AMOUNTS = (
    "amount", "adaptation", "mitigation", "cross_cutting", "value", "low", "high",
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
    behind a converted result; it is None for USD at current prices.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    package_version: str
    source_versions: dict[str, str]
    model_versions: dict[str, str]
    conversion: Conversion | None = None


class Result:
    """The answer to one `Spec`: a table, the rows behind it, and its provenance.

    `table` takes amounts. Under `spec.measure="share_of_total"` the result divides
    them by `totals`, so a result always holds the measure its spec names.
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
        self.table = (
            _shares(table, totals) if spec.measure == "share_of_total" else table
        )
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
        return explain(
            self.spec,
            release=self.meta.release,
            coefficients=coefficients,
            conversion=self.meta.conversion,
        )

    def share_of_total(self) -> Result:
        """The same result with every amount divided by all CRS flows of its key.

        The denominator is the release's `total_flows` for the same flow, year,
        provider, recipient and Bi_Multi code; a row without a recipient, such as
        attributed finance, divides by the sum over recipients. Totals cover every
        flow code, so under `oda_only` the share is ODA climate finance over all
        flows. A key missing from `total_flows` gives null shares. The rows and the
        modelled line keep their amounts. The result's spec has
        `measure="share_of_total"`, so rerunning it returns the same shares.
        """
        if self.spec.measure == "share_of_total":
            return self
        return Result(
            table=self.table,
            modelled=self.modelled,
            rows=self._rows,
            totals=self._totals,
            spec=self.spec.replace(measure="share_of_total"),
            meta=self.meta,
        )


def _shares(table: Table, totals: pl.LazyFrame) -> Table:
    """`table` with every amount divided by the total of its key in `totals`."""
    return Table(
        table._frame.join(
            totals,
            on=ROW_KEYS,
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
    climate value by provider and commitment status.
    """

    table: Table
    status: Table

    def summary(self) -> pl.DataFrame:
        """Per provider: link rate, certain-route share, label sources, status split.

        Shares are of the climate value of commitments. `link_rate` is the share that
        traces to CRS records, and `certain_route_share` the share that traces with
        every CRDF row reaching its CRS commitment through `schema.CERTAIN_ROUTES`.
        `grant_equiv_coverage` is the part of the linked commitments' allocated value
        whose record has a grant equivalent, each year's weighted by linked climate
        value over the years that have allocations.
        """
        climate = pl.col("climate_usd").sum()
        linked = pl.col("linked_usd").sum()
        coverage = pl.col("grant_equiv_coverage")
        known = coverage.is_not_null()
        rates = self.table._frame.group_by("provider_code").agg(
            pl.col("commitments").sum(),
            climate_usd=climate,
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
    conversion: Conversion | None = None,
) -> str:
    """The method, view, flow, filters and units behind `spec`, in plain language.

    `coefficients` holds, per provider-coefficient method label (`method`), each
    `coefficient_basis` and `coefficient_source` its marker rows took, with the
    `provider_code` and the list of `agency_code` values that took them.
    `conversion` names the conversion factors of a converted result.
    """
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
    if len(methods) > 1:
        lines.append("low and high are the lowest and highest bound across methods.")
    lines.append(_VIEWS[spec.view])
    lines.append(_FLOWS[spec.flow])
    filters = [
        _field_text(field, getattr(spec, field))
        for field in ("providers", "recipients", "years")
        if getattr(spec, field) is not None
    ]
    if filters:
        lines.append(f"Filtered to {'; '.join(filters)}.")
    if spec.recipients is not None and spec.view == "provider":
        lines.append("Attributed finance has no recipient, so it drops out.")
    if spec.oda_only:
        lines.append(
            f"ODA only: flow codes {_listing(ODA_FLOW_CODES)}; rows without a flow "
            "code drop out, and core contributions count as ODA."
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
    else:
        lines.append(f"Amounts in {units}.")
    if conversion is not None:
        lines.append(
            f"Conversion factors: pydeflate {conversion.pydeflate_version}, "
            f"SHA-256 {conversion.factors_sha256}."
        )
    return "\n".join(lines)


def _listing(codes: Sequence[int]) -> str:
    """`codes` as running text, such as "1, 3, 7 and 8"."""
    *rest, last = map(str, codes)
    return f"{', '.join(rest)} and {last}"


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
    "by the commitment's markers; low and high scale by each allocation's bounds. "
    "Attributed finance counts core contributions disbursed. The modelled table "
    "holds providers that report no disbursements.",
    "grant_equivalent": "Flow: grant equivalent of disbursements, shared across the "
    "commitments a record funds in proportion to the amount each receives; records "
    "without one stay missing, and snap.quality() reports coverage.",
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
    return f"Result: {'; '.join(parts)}; release {release}"
