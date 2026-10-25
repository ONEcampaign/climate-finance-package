"""`Release` and `load`: open a release and answer queries against it."""

from __future__ import annotations

import functools
import hashlib
import inspect
import os
import shutil
import tarfile
import tempfile
import urllib.request
import warnings
from collections.abc import Callable, Iterable, Mapping
from importlib.resources import files
from pathlib import Path
from typing import Unpack

import polars as pl
import pyarrow as pa
from pydantic import BaseModel, ConfigDict

from climate_finance import query
from climate_finance.catalogue import unknown_name
from climate_finance.errors import (
    CoverageWarning,
    InvalidInputError,
    ReleaseIntegrityError,
    SourceUnavailableError,
    WithheldTableError,
)
from climate_finance.frozen import unknown_keyword
from climate_finance.methods import ONE, Method, MethodRange
from climate_finance.result import Quality, Reconciliation, Result, year_runs
from climate_finance.schema import (
    RELEASE_TABLES,
    ReleaseManifest,
    scan_table,
    sha256_file,
)
from climate_finance.spec import (
    Spec,
    SpecInputs,
    read_method,
    read_one_method,
    read_provider_codes,
    read_years,
)

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
# The release name `tests/fixtures/snapshot` carries, whose figures are invented.
# `cf-build release` refuses to publish a release of this name.
SYNTHETIC_RELEASE = "fixture"
# Where releases are published, for the error `load` gives before the first one.
RELEASES_PAGE = "https://github.com/ONEcampaign/climate-finance-package/releases"
# A warning names the first caller outside the package.
_PACKAGE_DIR = str(Path(__file__).parent)
# What each flow reads, as a coverage warning names it.
_FLOW_TEXT = {
    "commitments": "commitments",
    "disbursements": "disbursements",
    "grant_equivalent": "grant equivalents",
}


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


def _keywords[**P, R](method: Callable[P, R]) -> Callable[P, R]:
    """`method`, refusing an unknown keyword as `Spec` does, with a close name."""
    parameters = [
        name for name in inspect.signature(method).parameters if name != "self"
    ]
    owner = f"{getattr(method, '__name__', 'Release')}()"

    @functools.wraps(method)
    def checked(*args: P.args, **kwargs: P.kwargs) -> R:
        for name in kwargs:
            if name not in parameters:
                raise InvalidInputError(
                    unknown_keyword(name, owner=owner, choices=parameters)
                )
        return method(*args, **kwargs)

    return checked


class Release:
    """One release on disk, read through its release tables only."""

    def __init__(self, *, path: Path, manifest: ReleaseManifest) -> None:
        self.path = path
        self.manifest = manifest
        self._years: dict[str, tuple[int, ...]] = {}

    def __repr__(self) -> str:
        name = repr(self.manifest.release)
        if self.manifest.release == SYNTHETIC_RELEASE:
            name += ", synthetic figures"
        flows = ", ".join(
            f"{flow} {year_runs(years)}"
            if (years := self._years_of(flow))
            else f"no {flow}"
            for flow in ("commitments", "disbursements")
        )
        tables = f"{len(self.manifest.tables)} tables"
        if withheld := self.manifest.withheld_tables:
            tables += f", withholding {', '.join(sorted(withheld))}"
        return f"Release({name}): {flows}; {tables}"

    def table(self, name: str) -> pl.LazyFrame:
        """Scan one release table.

        Raises:
            UnknownNameError: when `name` is not a release table, with close names
                as suggestions.
            WithheldTableError: when the release leaves `name` out.
        """
        if name not in RELEASE_TABLES:
            raise unknown_name(
                name, kind="release table", choices=sorted(RELEASE_TABLES)
            )
        if note := self.manifest.withheld_tables.get(name):
            raise WithheldTableError(name, note=note)
        return scan_table(self.path / f"{name}.parquet")

    def spending(self, **inputs: Unpack[SpecInputs]) -> Result:
        """Climate finance for the query `inputs` describe, as keywords only.

        Each keyword is a `Spec` field and takes the forms `SpecInputs` lists, such as
        `providers="Japan"`, `years=range(2015, 2023)` or `method="OECD"`.

        Raises:
            UnknownProviderError: for a provider name, code or agency outside the
                listings.
            UnknownRecipientError: for a recipient name or code outside them.
            UnknownNameError: for a method name outside `cf.methods.list()`.
            InvalidInputError: for an unknown keyword, such as `spec=`, which
                `run` takes, years, a method or prices in a form `Spec` does not
                take, or a value outside a field's choices, such as
                `flow="commitment"`.

        Warns:
            CoverageWarning: for years the flow's tables hold no rows for.
        """
        return self.run(Spec(**inputs))

    def run(self, spec: Spec | Mapping[str, object]) -> Result:
        """Rerun a saved `Spec`, such as `result.spec` or one read back with
        `Spec.model_validate_json`. A mapping, such as a saved spec read back with
        `json.load`, reads as `Spec.model_validate` reads it.

        Raises:
            InvalidInputError: for a value that is neither a `Spec` nor a mapping,
                or a mapping `Spec` refuses.
            UnknownNameError: for a name in the mapping outside its listing, as
                `spending` raises.

        Warns:
            CoverageWarning: for years the flow's tables hold no rows for.
        """
        if isinstance(spec, Mapping):
            spec = Spec.model_validate(spec)
        elif not isinstance(spec, Spec):
            raise InvalidInputError(
                "run() takes a Spec, such as result.spec or "
                "cf.Spec.model_validate_json(text), or a mapping of its fields; "
                f"got {type(spec).__name__}"
            )
        # The flow's rows come from these tables only, so a year none holds has none.
        tables = ["commitments" if spec.flow == "commitments" else "disbursements"]
        if spec.flow != "commitments" and spec.untraced:
            tables.append("untraced")
        if spec.view == "provider":
            tables.append("core_contributions")
        self._warn_uncovered(spec.years, tables=tables, holds=_FLOW_TEXT[spec.flow])
        return query.spending(self, spec=spec)

    @_keywords
    def quality(
        self,
        *,
        providers: str | int | Iterable[str | int] | None = None,
        years: int | Iterable[int] | None = None,
        method: Method | str = ONE,
    ) -> Quality:
        """Link, coverage and status statistics, and the EU Institutions rows that
        `method`'s provider view leaves out.

        `providers` and `years` take the forms `spending` takes, and `method` one
        method or its name. None takes every provider or year.

        Raises:
            UnknownProviderError: for a provider name or code outside the listings.
            UnknownNameError: for a method name outside `cf.methods.list()`.
            InvalidInputError: for an agency, since the tables count providers,
                naming its provider; for a range of methods; for years or a
                method in a form `Spec` does not take; for an empty list of
                providers; or for an unknown keyword, naming a close one.

        Warns:
            CoverageWarning: for years the `quality` table holds no rows for.
        """
        read = read_years(years)
        self._warn_uncovered(read, tables=["quality"], holds="quality statistics")
        return query.quality(
            self,
            providers=read_provider_codes(providers, reader="quality()"),
            years=read,
            method=read_one_method(method, reader="quality()"),
        )

    @_keywords
    def reconcile(
        self,
        *,
        providers: str | int | Iterable[str | int] | None = None,
        years: int | Iterable[int] | None = None,
        method: Method | MethodRange | str | Iterable[Method | str] = ONE,
    ) -> Reconciliation:
        """Compare each BTR party's figures with OECD data (`query.reconcile`).

        `providers`, `years` and `method` take the forms `spending` takes, so
        `providers="Germany"`, `"DEU"` and `5` name one party. None compares every
        party, or every year the BTR tables hold.

        Raises:
            UnknownProviderError: for a provider name or code outside the listings.
            UnknownNameError: for a method name outside `cf.methods.list()`.
            InvalidInputError: for an agency, since the BTR reports providers,
                naming its provider; for years or a method in a form `Spec` does
                not take; for an empty list of providers; or for an unknown
                keyword, such as `parties=`, naming a close one.

        Warns:
            CoverageWarning: for years the `btr_totals` table holds no rows for.
        """
        read = read_years(years)
        self._warn_uncovered(read, tables=["btr_totals"], holds="BTR totals")
        return query.reconcile(
            self,
            providers=read_provider_codes(providers, reader="reconcile()"),
            years=read,
            method=read_method(method),
        )

    def disbursement_profile(self) -> pl.DataFrame:
        """Cumulative disbursed share of traceable climate value by years since
        commitment.

        One row per group (`all`, `bilateral`, `multilateral`) and year since
        commitment, with `vintages`, the number of commitment years observed that
        long.
        """
        return self.table("disbursement_profile").sort("group", "years_since").collect()

    def _years_of(self, table: str) -> tuple[int, ...]:
        """The years `table` holds rows for, sorted, read once per release."""
        if table not in self._years:
            held = self.table(table).select(pl.col("year").unique().sort()).collect()
            self._years[table] = tuple(held["year"].to_list())
        return self._years[table]

    def _warn_uncovered(
        self, years: Iterable[int] | None, *, tables: Iterable[str], holds: str
    ) -> None:
        """Warn once when `years` asks for years none of `tables` holds rows for;
        `holds` names what the tables hold."""
        if years is None:
            return
        held = sorted({year for table in tables for year in self._years_of(table)})
        if not (missing := sorted(set(years) - set(held))):
            return
        span = f"{holds} for {year_runs(held)}" if held else f"no {holds}"
        asked = (
            f"year {missing[0]} returns"
            if len(missing) == 1
            else f"years {year_runs(missing)} return"
        )
        warnings.warn(
            f"the release holds {span}; {asked} no rows",
            CoverageWarning,
            skip_file_prefixes=(_PACKAGE_DIR,),
        )


def load(
    release: str = "latest",
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
) -> Release:
    """Open a release from `path`, or download `release` into `cache_dir` once.

    Without `path`, `release` is looked up in the release index shipped in the wheel,
    or with `refresh=True` in the newest index online. A release already in
    `cache_dir` (else `CLIMATE_FINANCE_CACHE`, else `~/.cache/climate-finance`) opens
    without network. Otherwise its pack downloads, its sha256 is checked against the
    index, and it unpacks into `cache_dir`. A cached copy that fails its check
    downloads again.
    A relative `path` resolves against the working directory at load, so the
    release reads the files it verified after a later change of directory.

    Raises:
        ReleaseIntegrityError: when `release.json` is missing or malformed, a
            release table it does not withhold is missing, a table's sha256
            differs from `release.json`, or a pack's sha256 differs from the
            index.
        UnknownNameError: when the index lists no release named `release`, or no
            release at all, which names `path=` and where releases come from.
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


def _open(path: Path, *, release: str | None = None) -> Release:
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
    return Release(path=path, manifest=manifest)


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
    if not index.releases:
        raise unknown_name(
            release,
            kind="release",
            choices=(),
            note=(
                "No release is published yet. Open a release directory with "
                "cf.load(path=...). Releases are built with `cf-build release` and "
                f"published at {RELEASES_PAGE}."
            ),
        )
    if name is None or name not in index.releases:
        raise unknown_name(release, kind="release", choices=sorted(index.releases))
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
