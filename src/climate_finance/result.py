"""Query results: `Result`, `Quality`, `Reconciliation`, `Comparison` and `compare`."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from html import escape
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.methods import Method, MethodRange, describe, label
from climate_finance.schema import STATUSES
from climate_finance.spec import Spec

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

AMOUNTS = (
    "amount", "adaptation", "mitigation", "cross_cutting", "unsplit", "value", "low",
    "high",
)  # fmt: skip


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
    """Where a result came from: release, package and source versions."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: str
    package_version: str
    source_versions: dict[str, str]
    model_versions: dict[str, str]


class Result:
    """The answer to one `Spec`: a table, the rows behind it, and its provenance."""

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
        self.table = table
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
        rows = self._rows.filter(*conditions) if conditions else self._rows
        return rows.collect()

    def explain(self) -> str:
        """A plain-language account of the method, flow and filters applied."""
        fallbacks = {
            label(method): self._rows.filter(
                pl.col("method") == label(method),
                pl.col("climate_share").is_null(),
                pl.col("coefficient_basis").is_null(),
            )
            .select(pl.col("provider_code").unique().sort())
            .collect()
            .to_series()
            .to_list()
            for method in self.spec.methods
            if method.coefficients == "provider"
        }
        unsplit = (
            self._rows.filter(pl.col("origin") == "attribution", pl.col("unsplit") != 0)
            .group_by("method")
            .agg(pl.col("channel_code").unique().sort())
            .collect()
        )
        return explain(
            self.spec,
            release=self.meta.release,
            fallbacks=fallbacks,
            unsplit=dict(unsplit.iter_rows()),
        )

    def share_of_total(self) -> Result:
        """The same result with every amount divided by all CRS flows of its key.

        The denominator is the release's `total_flows` for the same flow, year,
        provider, recipient and Bi_Multi code. A row without a recipient, such as
        attributed finance, divides by the sum over recipients. Totals cover every
        flow code, so under `oda_only` the share is ODA climate finance over all
        flows. A key missing from `total_flows` gives null shares. The rows and the
        modelled line keep their amounts.
        """
        keys = ["year", "provider_code", "recipient_code", "bi_multi"]
        table = (
            self.table._frame.join(
                self._totals,
                on=keys,
                how="left",
                nulls_equal=True,
                validate="m:1",
                maintain_order="left",
            )
            .with_columns(pl.col(name) / pl.col("total") for name in AMOUNTS)
            .drop("total")
        )
        return Result(
            table=Table(table),
            modelled=self.modelled,
            rows=self._rows,
            totals=self._totals,
            spec=self.spec,
            meta=self.meta,
        )


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
    `held_usd` and `oecd_usd`. `precision` holds, by provider, the share of
    allocated value whose link a blind human sample found right (`estimate`), its
    interval (`lower`, `upper`), whether it passed the release's precision gate,
    whether a failure was waived, and the gate's reading; it is empty for a release
    that was not gated.
    """

    def __init__(
        self,
        *,
        table: Table,
        status: Table,
        eu_left_out: Table,
        attribution: Table,
        precision: Table,
    ) -> None:
        self.table = table
        self.status = status
        self.eu_left_out = eu_left_out
        self.attribution = attribution
        self.precision = precision

    def summary(self) -> pl.DataFrame:
        """Per provider: link rate, linked share above 0.8, label sources, status split.

        Shares are of the climate value of counted commitments. `link_rate` is the
        share that traces to CRS records, and `linked_high_probability_share` the
        part of that whose most probable link is at least 0.8. `grant_equiv_coverage`
        is the share of linked records with a grant equivalent, weighted by linked
        climate value over the years that have linked records. `repeat_climate_usd`
        is the climate value of rows that repeat an earlier vintage, which every
        total leaves out. Their disbursements count on the row they repeat.
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
            linked_high_probability_share=pl.col("linked_high_probability_usd").sum()
            / linked,
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
    fallbacks: Mapping[str, Sequence[int]] | None = None,
    unsplit: Mapping[str, Sequence[int]] | None = None,
) -> str:
    """The method, view, flow, filters and units behind `spec`, in plain language.

    `fallbacks` maps each provider-coefficient method's label to the providers that
    have no confirmed coefficient of their own in the release. `unsplit` maps a
    method's label to the agency channel codes whose climate share has no split by
    objective.
    """
    fallbacks = fallbacks or {}
    unsplit = unsplit or {}
    methods = spec.methods
    lines = [f"Release {release}."]
    for method in methods:
        lines.append(describe(method))
        if missing := fallbacks.get(label(method)):
            lines.append(
                f"Provider coefficients: providers {', '.join(map(str, missing))} "
                f"have no confirmed coefficient and take {method.significant:g}."
            )
        if channels := unsplit.get(label(method)):
            lines.append(
                f"Channels {', '.join(map(str, channels))}: the climate share has no "
                "split by objective, so their attributed value sits in unsplit."
            )
    if len(methods) > 1:
        lines.append("low and high are the lowest and highest bound across methods.")
    lines.append(_VIEWS[spec.view])
    lines.append(_FLOWS[spec.flow])
    lines.append(
        "Commitments that repeat an earlier vintage are left out, and their "
        "disbursements count on the row they repeat; snap.quality() reports their "
        "value."
    )
    filters = [
        f"{name} {', '.join(map(str, values))}"
        for name, values in (
            ("providers", spec.providers),
            ("recipients", spec.recipients),
            ("years", spec.years),
        )
        if values is not None
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
            "ODA only: commitments of flow category 10 and disbursements of flow "
            "codes 11, 13 and 19; rows without either drop out, and core "
            "contributions count as ODA."
        )
    prices = "current prices" if spec.prices == "current" else f"{spec.prices} prices"
    source = (
        ""
        if spec.currency == "USD" and spec.prices == "current"
        else ", converted with OECD DAC exchange rates and deflators by provider "
        "(the DAC total for providers without their own series)"
    )
    lines.append(f"Amounts in {spec.currency} at {prices}{source}.")
    return "\n".join(lines)


_VIEWS = {
    "recipient": "Recipient view: bilateral flows (Bi_Multi 1, 3, 7 and 8) and "
    "multilateral outflows (code 4); core contributions (code 2) are left out.",
    "provider": "Provider view: bilateral flows (Bi_Multi 1, 3, 7 and 8) plus each "
    "provider's core contributions times the agency's climate share; code 2 and "
    "code 4 rows are left out.",
}
_FLOWS = {
    "commitments": "Flow: commitments at face value; low and high equal value.",
    "disbursements": "Flow: disbursements allocated to climate commitments, weighted "
    "by the commitment's markers; low counts the disbursements linked by the "
    "donor's CRS id or World Bank loan number where the linked records agree, or "
    "by one record that commits and disburses, to commitments whose CRDF row ties "
    "to its CRS record by construction (same crs id and year, a matched contract, "
    "or the record the row was cut from), and bank-ledger and IATI fills in full, "
    "since they match on the publisher's own id; high counts every linked "
    "disbursement. "
    "Attributed finance counts core contributions disbursed. The modelled table "
    "holds a disbursement range for commitments whose disbursements no source "
    "reports.",
    "grant_equivalent": "Flow: grant equivalent of disbursements, shared across the "
    "commitments a record funds in proportion to the amount each receives; records "
    "without one stay missing, and snap.quality() reports coverage.",
}


class Reconciliation:
    """A party-by-year comparison of BTR reports with OECD data.

    `table` holds one row per party, year and method (see `query.reconcile`).
    """

    def __init__(self, *, table: Table, rows: pl.LazyFrame) -> None:
        self.table = table
        self._rows = rows

    def rows(self, party: str, year: int) -> pl.DataFrame:
        """The project-level link behind one party and year: each BTR row of every
        status with each CRS record linked to it, the record's facts prefixed
        `crs_`, and `unfccc_only` true for a row no record links to."""
        return self._rows.filter(
            pl.col("party") == party, pl.col("year") == year
        ).collect()


class Comparison:
    """Several results side by side, as a long table with one column per result."""

    def __init__(self, *, table: Table, results: tuple[Result, ...]) -> None:
        self.table = table
        self.results = results

    def __repr__(self) -> str:
        return f"{self._title()}\n{self.table!r}"

    def _repr_html_(self) -> str:
        return f"<p>{escape(self._title())}</p>{self.table._repr_html_()}"

    def _title(self) -> str:
        columns = self.table._frame.collect_schema().names()[len(COMPARE_KEYS) :]
        count = len(self.results)
        results = "result" if count == 1 else "results"
        return f"Comparison of {count} {results}: {'; '.join(columns)}"


COMPARE_KEYS = ("year", "provider_code", "recipient_code", "bi_multi")


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
                    *COMPARE_KEYS, pl.col("value").alias(column)
                )
            )
    table = frames[0]
    for frame in frames[1:]:
        table = table.join(
            frame, on=COMPARE_KEYS, how="full", nulls_equal=True, coalesce=True
        )
    return Comparison(
        table=Table(table.sort(COMPARE_KEYS, nulls_last=True)), results=results
    )


def _result_names(specs: Sequence[Spec]) -> list[str]:
    """Per spec, the values of the fields that differ across `specs`, or its method
    when none differ. Specs that nothing sets apart are numbered as well.
    """
    differing = [
        field
        for field in Spec.model_fields
        if len({repr(getattr(spec, field)) for spec in specs}) > 1
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
    if spec.oda_only:
        parts.append("ODA only")
    return f"Result: {'; '.join(parts)}; release {release}"
