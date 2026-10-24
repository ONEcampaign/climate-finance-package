"""`Snapshot` and `load`: open a release and answer queries against it."""

from __future__ import annotations

import difflib
import hashlib
import os
import shutil
import tarfile
import tempfile
import urllib.request
from collections.abc import Iterable, Sequence
from importlib.resources import files
from pathlib import Path
from typing import Literal

import polars as pl
import pyarrow as pa
from pydantic import BaseModel, ConfigDict

from climate_finance import query
from climate_finance.errors import (
    ReleaseIntegrityError,
    SourceUnavailableError,
    UnknownNameError,
    WithheldTableError,
)
from climate_finance.methods import ONE, Method, MethodRange
from climate_finance.result import Quality, Reconciliation, Result
from climate_finance.schema import (
    RELEASE_TABLES,
    ReleaseManifest,
    scan_table,
    sha256_file,
)
from climate_finance.spec import Currency, Flow, Measure, Spec, View

# The release index shipped in the wheel.
RELEASE_INDEX = files("climate_finance") / "data" / "releases.json"
# The newest release index, attached to every published release.
INDEX_URL = (
    "https://github.com/ONEcampaign/climate-finance-package"
    "/releases/latest/download/releases.json"
)
# Seconds a download may wait on the server before it fails.
_TIMEOUT = 60
_CHUNK = 1 << 20


class IndexEntry(BaseModel):
    """Where one packed release is published and the sha256 of its pack."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    sha256: str


class ReleaseIndex(BaseModel):
    """The contents of `releases.json`: every published release and the newest."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    latest: str | None = None
    releases: dict[str, IndexEntry] = {}


def release_dirname(release: str) -> str:
    """The directory a packed `release` unpacks to."""
    return f"climate-finance-{release}"


class Snapshot:
    """One release on disk, read through its release tables only."""

    def __init__(self, *, path: Path, manifest: ReleaseManifest) -> None:
        self.path = path
        self.manifest = manifest

    def table(self, name: str) -> pl.LazyFrame:
        """Scan one release table.

        Raises:
            KeyError: when `name` is not a release table.
            WithheldTableError: when the release leaves `name` out.
        """
        if name not in RELEASE_TABLES:
            raise KeyError(f"{name!r} is not a release table: {sorted(RELEASE_TABLES)}")
        if note := self.manifest.withheld_tables.get(name):
            raise WithheldTableError(name, note=note)
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
        """Compare each party's BTR figures with OECD data (`query.reconcile`).

        `parties` are ISO 3 codes as the `btr` table holds them, such as "NOR" and
        "EUU". None compares every party.
        """
        return query.reconcile(
            self,
            parties=(parties,) if isinstance(parties, str) else parties,
            years=years,
            method=method,
        )

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

    Without `path`, `release` is looked up in the release index shipped in the wheel,
    or with `refresh=True` in the newest index online. A release already in
    `cache_dir` (else `CLIMATE_FINANCE_CACHE`, else `~/.cache/climate-finance`) opens
    without network. Otherwise its pack downloads, its sha256 is checked against the
    index, and it unpacks into `cache_dir`. A cached copy that fails its check
    downloads again.
    A relative `path` resolves against the working directory at load, so the
    snapshot reads the files it verified after a later change of directory.

    Raises:
        ReleaseIntegrityError: when `release.json` is missing or malformed, a
            release table it does not withhold is missing, a table's sha256
            differs from `release.json`, or a pack's sha256 differs from the
            index.
        UnknownNameError: when the index lists no release named `release`.
        SourceUnavailableError: when the index or a pack cannot be downloaded.
    """
    if path is not None:
        return _open(Path(path).resolve())
    index = _fetch_index() if refresh else _shipped_index()
    name = _resolve(index, release)
    cache = _cache_dir(cache_dir)
    target = cache / release_dirname(name)
    if target.is_dir():
        try:
            return _open(target, release=name)
        except ReleaseIntegrityError:
            shutil.rmtree(target)
    _download(index.releases[name], target=target)
    return _open(target, release=name)


def _open(path: Path, *, release: str | None = None) -> Snapshot:
    """Open the release in `path` once every table matches `release.json`."""
    manifest_path = path / "release.json"
    if not manifest_path.is_file():
        raise ReleaseIntegrityError(f"{path} has no release.json")
    try:
        manifest = ReleaseManifest.model_validate_json(manifest_path.read_text())
    except ValueError as error:  # bad JSON, bad fields or undecodable bytes
        raise ReleaseIntegrityError(f"{manifest_path} is malformed: {error}") from error
    problems = [
        f"{name}: not in release.json"
        for name in RELEASE_TABLES
        if name not in manifest.tables and name not in manifest.withheld_tables
    ]
    if release is not None and manifest.release != release:
        problems.append(f"release.json names release {manifest.release!r}")
    for name, entry in manifest.tables.items():
        file = path / f"{name}.parquet"
        if not file.is_file():
            problems.append(f"{name}: {file.name} is missing")
        elif (actual := sha256_file(file)) != entry.sha256:
            problems.append(f"{name}: sha256 {actual}, release.json has {entry.sha256}")
    if problems:
        raise ReleaseIntegrityError(
            f"release {release or manifest.release!r} at {path} fails its check: "
            + "; ".join(problems)
        )
    return Snapshot(path=path, manifest=manifest)


def _shipped_index() -> ReleaseIndex:
    return ReleaseIndex.model_validate_json(RELEASE_INDEX.read_text())


def _fetch_index() -> ReleaseIndex:
    try:
        with urllib.request.urlopen(INDEX_URL, timeout=_TIMEOUT) as response:
            body = response.read()
    except OSError as error:
        raise SourceUnavailableError(
            f"Could not fetch the release index from {INDEX_URL}: {error}"
        ) from error
    return ReleaseIndex.model_validate_json(body)


def _resolve(index: ReleaseIndex, release: str) -> str:
    """The release `release` names in `index`. "latest" names the newest.

    Raises:
        UnknownNameError: when `index` lists no such release.
    """
    name = index.latest if release == "latest" else release
    if name is None or name not in index.releases:
        listed = sorted(index.releases)
        raise UnknownNameError(
            release,
            kind="release",
            suggestions=difflib.get_close_matches(release, listed) if listed else (),
        )
    return name


def _cache_dir(cache_dir: str | Path | None) -> Path:
    if cache_dir is not None:
        return Path(cache_dir)
    if env := os.environ.get("CLIMATE_FINANCE_CACHE"):
        return Path(env)
    return Path.home() / ".cache" / "climate-finance"


def _download(entry: IndexEntry, *, target: Path) -> None:
    """Download the pack at `entry.url`, check its sha256 and unpack it to `target`.

    The pack downloads and unpacks in a temporary directory beside `target`, so a
    failed download leaves nothing in its place.

    Raises:
        ReleaseIntegrityError: when the pack's sha256 differs from `entry`, or the
            pack holds no `target.name` directory.
        SourceUnavailableError: when the pack cannot be downloaded.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".download-", dir=target.parent) as tmp:
        pack = Path(tmp) / f"{target.name}.tar.zst"
        if (actual := _fetch(entry.url, out_path=pack)) != entry.sha256:
            raise ReleaseIntegrityError(
                f"{entry.url} has sha256 {actual}, the release index has {entry.sha256}"
            )
        unpacked = Path(tmp) / "unpacked"
        with (
            pa.input_stream(str(pack), compression="zstd") as stream,
            tarfile.open(fileobj=stream, mode="r|") as tar,
        ):
            tar.extractall(unpacked, filter="data")
        if not (unpacked / target.name).is_dir():
            raise ReleaseIntegrityError(f"{entry.url} holds no {target.name}/")
        (unpacked / target.name).rename(target)


def _fetch(url: str, *, out_path: Path) -> str:
    """Download `url` to `out_path`; return its sha256."""
    digest = hashlib.sha256()
    try:
        with (
            urllib.request.urlopen(url, timeout=_TIMEOUT) as response,
            out_path.open("wb") as out,
        ):
            while chunk := response.read(_CHUNK):
                digest.update(chunk)
                out.write(chunk)
    except OSError as error:
        raise SourceUnavailableError(f"Could not download {url}: {error}") from error
    return digest.hexdigest()


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
