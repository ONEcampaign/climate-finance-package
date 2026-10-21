"""`Snapshot` and `load`: open a release and answer queries against it."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

import polars as pl

from climate_finance.methods import ONE, Method, MethodRange
from climate_finance.result import Quality, Reconciliation, Result
from climate_finance.schema import ReleaseManifest
from climate_finance.spec import Flow, Spec


class Snapshot:
    """One release on disk, read through its release tables only."""

    def __init__(self, *, path: Path, manifest: ReleaseManifest) -> None:
        self.path = path
        self.manifest = manifest

    def table(self, name: str) -> pl.LazyFrame:
        """Scan one release table."""
        raise NotImplementedError("slice S09a")

    def spending(
        self,
        *,
        providers: str | int | Sequence[str | int] | None = None,
        recipients: str | int | Sequence[str | int] | None = None,
        years: int | Iterable[int] | None = None,
        method: Method | MethodRange = ONE,
        flow: Flow = "disbursements",
        currency: str = "USD",
        prices: int | Literal["current"] = "current",
        oda_only: bool = False,
        spec: Spec | None = None,
    ) -> Result:
        """Climate finance under `method`, or under `spec` when given."""
        raise NotImplementedError("slice S09a")

    def quality(
        self,
        *,
        providers: str | int | Sequence[str | int] | None = None,
        years: int | Iterable[int] | None = None,
    ) -> Quality:
        """Link, coverage and status statistics, with precision intervals."""
        raise NotImplementedError("slice S09a (interval: slice S17)")

    def reconcile(
        self,
        *,
        parties: str | Sequence[str] | None = None,
        years: Iterable[int] = range(2015, 2023),
        method: Method | MethodRange = ONE,
    ) -> Reconciliation:
        """Compare each party's BTR figures with OECD data."""
        raise NotImplementedError("slice S19b")

    def disbursement_profile(self) -> pl.DataFrame:
        """Cumulative disbursed share of traceable value by years since commitment."""
        raise NotImplementedError("slice S09c")


def load(
    release: str = "latest",
    *,
    path: Path | None = None,
    cache_dir: Path | None = None,
    refresh: bool = False,
) -> Snapshot:
    """Open a release from `path`, or download `release` into `cache_dir` once.

    Raises:
        ReleaseIntegrityError: when a table's sha256 differs from `release.json`.
    """
    raise NotImplementedError("slice S09a (path=), slice S10b (download)")
