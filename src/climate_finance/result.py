"""Query results: `Result`, `Quality`, `Reconciliation`, `Comparison` and `compare`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, ConfigDict

from climate_finance.spec import Spec

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class Table:
    """A result table that converts to polars, pandas or arrow."""

    def __init__(self, frame: pl.LazyFrame) -> None:
        self._frame = frame

    def to_polars(self) -> pl.DataFrame:
        """The table as a polars DataFrame."""
        raise NotImplementedError("slice S09a")

    def to_pandas(self) -> pd.DataFrame:
        """The table as a pandas DataFrame; needs the [compat] extra."""
        raise NotImplementedError("slice S09b")

    def to_arrow(self) -> pa.Table:
        """The table as a pyarrow Table."""
        raise NotImplementedError("slice S09a")


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
        spec: Spec,
        meta: Meta,
    ) -> None:
        self.table = table
        self.modelled = modelled
        self.spec = spec
        self.meta = meta
        self._rows = rows

    def rows(self, **filters: object) -> pl.DataFrame:
        """The project rows behind the table, filtered by column equality."""
        raise NotImplementedError("slice S09a")

    def explain(self) -> str:
        """A plain-language account of the method, flow and filters applied."""
        raise NotImplementedError("slice S09a")

    def share_of_total(self) -> Result:
        """The same result as a share of total flows."""
        raise NotImplementedError("slice S09a")


class Quality:
    """Link, coverage and status statistics for a slice of a release."""

    def __init__(self, *, table: Table) -> None:
        self.table = table

    def summary(self) -> pl.DataFrame:
        """Link rate, linked share above 0.8, label sources, status split, interval."""
        raise NotImplementedError("slice S09a (interval: slice S17)")


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
