"""`Snapshot` and `load`: open a release and answer queries against it."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

import polars as pl

from climate_finance import query
from climate_finance.errors import ReleaseIntegrityError
from climate_finance.methods import ONE, Method, MethodRange
from climate_finance.result import Quality, Reconciliation, Result
from climate_finance.schema import (
    RELEASE_TABLES,
    ReleaseManifest,
    scan_table,
    sha256_file,
)
from climate_finance.spec import Currency, Flow, Measure, Spec, View


class Snapshot:
    """One release on disk, read through its release tables only."""

    def __init__(self, *, path: Path, manifest: ReleaseManifest) -> None:
        self.path = path
        self.manifest = manifest

    def table(self, name: str) -> pl.LazyFrame:
        """Scan one release table.

        Raises:
            KeyError: when `name` is not a release table.
        """
        if name not in RELEASE_TABLES:
            raise KeyError(f"{name!r} is not a release table: {sorted(RELEASE_TABLES)}")
        return scan_table(self.path / f"{name}.parquet")

    def spending(
        self,
        *,
        providers: str | int | Sequence[str | int] | None = None,
        recipients: str | int | Sequence[str | int] | None = None,
        years: int | Iterable[int] | None = None,
        method: Method | MethodRange = ONE,
        flow: Flow = "disbursements",
        view: View = "recipient",
        currency: Currency = "USD",
        prices: int | Literal["current"] = "current",
        oda_only: bool = False,
        measure: Measure = "amount",
        untraced: bool = True,
        spec: Spec | None = None,
    ) -> Result:
        """Climate finance under `method`, or under `spec` when given.

        `spec` replaces every other argument, so a saved `Spec` reruns exactly.
        """
        if spec is None:
            spec = Spec(
                providers=_as_tuple(providers),
                recipients=_as_tuple(recipients),
                years=_years(years),
                method=method,
                flow=flow,
                view=view,
                currency=currency,
                prices=prices,
                oda_only=oda_only,
                measure=measure,
                untraced=untraced,
            )
        return query.spending(self, spec=spec)

    def quality(
        self,
        *,
        providers: str | int | Sequence[str | int] | None = None,
        years: int | Iterable[int] | None = None,
        method: Method = ONE,
    ) -> Quality:
        """Link, coverage and status statistics, and the EU Institutions rows that
        `method`'s provider view leaves out."""
        return query.quality(
            self, providers=_as_tuple(providers), years=_years(years), method=method
        )

    def reconcile(
        self,
        *,
        parties: str | Sequence[str] | None = None,
        years: Iterable[int] = range(2015, 2023),
        method: Method | MethodRange = ONE,
    ) -> Reconciliation:
        """Compare each party's BTR figures with OECD data."""
        raise NotImplementedError("BTR reconciliation is not implemented")

    def disbursement_profile(self) -> pl.DataFrame:
        """Cumulative disbursed share of traceable climate value by years since
        commitment.

        One row per group (`all`, `bilateral`, `multilateral`) and year since
        commitment, with `vintages`, the number of commitment years observed that
        long.
        """
        return self.table("disbursement_profile").sort("group", "years_since").collect()


def load(
    release: str = "latest",
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
) -> Snapshot:
    """Open a release from `path`, or download `release` into `cache_dir` once.

    A relative `path` resolves against the working directory at load, so the
    snapshot reads the files it verified after a later change of directory.

    Raises:
        ReleaseIntegrityError: when `release.json` or a release table is missing, or
            a table's sha256 differs from `release.json`.
    """
    if path is None:
        raise NotImplementedError("downloading a release is not implemented")
    path = Path(path).resolve()
    manifest_path = path / "release.json"
    if not manifest_path.is_file():
        raise ReleaseIntegrityError(f"{path} has no release.json")
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text())
    problems = [
        f"{name}: not in release.json"
        for name in RELEASE_TABLES
        if name not in manifest.tables
    ]
    for name, entry in manifest.tables.items():
        file = path / f"{name}.parquet"
        if not file.is_file():
            problems.append(f"{name}: {file.name} is missing")
        elif (actual := sha256_file(file)) != entry.sha256:
            problems.append(f"{name}: sha256 {actual}, release.json has {entry.sha256}")
    if problems:
        raise ReleaseIntegrityError(
            f"release {manifest.release!r} at {path} fails its check: "
            + "; ".join(problems)
        )
    return Snapshot(path=path, manifest=manifest)


def _as_tuple(
    values: str | int | Iterable[str | int] | None,
) -> tuple[str | int, ...] | None:
    """One name or code, or many, as a tuple; a string counts as one value."""
    if values is None:
        return None
    if isinstance(values, str | int):
        return (values,)
    return tuple(values)


def _years(years: int | Iterable[int] | None) -> tuple[int, ...] | None:
    if years is None:
        return None
    return (years,) if isinstance(years, int) else tuple(years)
