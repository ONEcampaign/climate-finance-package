"""Query results: `Result`, `Quality`, `Reconciliation`, `Comparison` and `compare`."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.methods import label
from climate_finance.schema import STATUSES
from climate_finance.spec import Spec

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

AMOUNTS = (
    "amount", "adaptation", "mitigation", "cross_cutting", "value", "low", "high",
)  # fmt: skip


class Table:
    """A result table that converts to polars, pandas or arrow."""

    def __init__(self, frame: pl.LazyFrame) -> None:
        self._frame = frame

    def to_polars(self) -> pl.DataFrame:
        """The table as a polars DataFrame."""
        return self._frame.collect()

    def to_pandas(self) -> pd.DataFrame:
        """The table as a pandas DataFrame; needs the [compat] extra."""
        raise NotImplementedError("slice S09b")

    def to_arrow(self) -> pa.Table:
        """The table as a pyarrow Table."""
        return self.to_polars().to_arrow()


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
        return explain(self.spec, release=self.meta.release, fallbacks=fallbacks)

    def share_of_total(self) -> Result:
        """The same result with every amount divided by all CRS flows of its key.

        The denominator is the release's `total_flows` for the same flow, year,
        provider, recipient and Bi_Multi code; a row without a recipient, such as
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
    """Link, coverage and status statistics for a slice of a release.

    `table` holds the release's `quality` rows by provider and year; `status` holds
    climate value by provider and commitment status.
    """

    def __init__(self, *, table: Table, status: Table) -> None:
        self.table = table
        self.status = status

    def summary(self) -> pl.DataFrame:
        """Per provider: link rate, linked share above 0.8, label sources, status split.

        Shares are of climate value, except `grant_equiv_coverage`, the share of
        linked records with a grant equivalent, weighted by linked value. The
        precision interval joins in slice S17.
        """
        climate = pl.col("climate_usd").sum()
        linked = pl.col("linked_usd").sum()
        coverage = pl.col("grant_equiv_coverage") * pl.col("linked_usd")
        rates = self.table._frame.group_by("provider_code").agg(
            pl.col("commitments").sum(),
            climate_usd=climate,
            link_rate=linked / climate,
            linked_high_probability_share=pl.col("linked_high_probability_usd").sum()
            / linked,
            model_labelled_share=pl.col("model_labelled_usd").sum() / climate,
            report_labelled_share=pl.col("report_labelled_usd").sum() / climate,
            grant_equiv_coverage=coverage.sum() / linked,
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
) -> str:
    """The method, view, flow, filters and units behind `spec`, in plain language.

    `fallbacks` maps each provider-coefficient method's label to the providers that
    have no coefficient of their own in the release.
    """
    fallbacks = fallbacks or {}
    methods = spec.methods
    lines = [f"Release {release}."]
    for method in methods:
        rule = (
            "each activity counts once, under its higher Rio marker, with equal "
            "markers as cross-cutting"
            if method.highest_marker
            else "adaptation and mitigation count separately; cross_cutting is their "
            "overlap, which value counts once"
        )
        lines.append(
            f"Method {label(method)}: significant markers count "
            f"{method.significant:.0%} of face value and principal markers "
            f"{method.principal:.0%}; {rule}. Climate-component rows from development "
            "banks count their reported climate share and ignore the coefficients."
        )
        if method.coefficients == "provider":
            lines.append(
                "Provider coefficients: each provider takes its own significant "
                "coefficient from the release"
                + (
                    f"; providers {', '.join(map(str, missing))} have none and take "
                    f"{method.significant:g}."
                    if (missing := fallbacks.get(label(method)))
                    else "."
                )
            )
    if len(methods) > 1:
        lines.append("low and high are the lowest and highest bound across methods.")
    lines.append(_VIEWS[spec.view])
    lines.append(_FLOWS[spec.flow])
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
    if spec.recipients is not None and spec.view == "provider":
        lines.append("Attributed finance has no recipient, so it drops out.")
    if spec.oda_only:
        lines.append(
            "ODA only: flow codes 11, 13 and 19; rows without a flow code drop out, "
            "and core contributions count as ODA."
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
    "by the commitment's markers; low and high scale each commitment's disbursed "
    "bounds. The modelled table holds providers that report no disbursements.",
    "grant_equivalent": "Flow: grant equivalent of disbursements; records without "
    "one stay missing, and snap.quality() reports coverage.",
}


class Reconciliation:
    """A party-by-year comparison of BTR reports with OECD data."""

    def __init__(self, *, table: Table, rows: pl.LazyFrame) -> None:
        self.table = table
        self._rows = rows

    def rows(self, party: str, year: int) -> pl.DataFrame:
        """The project-level link behind one party and year."""
        raise NotImplementedError("slice S19b")


class Comparison:
    """Several results side by side: a long table with one column per result."""

    def __init__(self, *, table: Table, results: tuple[Result, ...]) -> None:
        self.table = table
        self.results = results


def compare(*results: Result) -> Comparison:
    """Set results side by side, one value column per result."""
    raise NotImplementedError("slice S09b")
