"""`Release` and `load`: open a release and answer queries against it."""

from __future__ import annotations

import contextlib
import functools
import hashlib
import inspect
import json
import os
import re
import shutil
import tarfile
import tempfile
import urllib.request
import uuid
import warnings
from collections.abc import Callable, Iterable, Mapping
from importlib.resources import files
from pathlib import Path
from typing import Annotated, Literal, Self, Unpack

import polars as pl
import pyarrow as pa
from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    ValidationError,
    model_validator,
)

from climate_finance import query
from climate_finance.catalogue import unknown_name
from climate_finance.errors import (
    CoverageWarning,
    FormatTableError,
    InvalidInputError,
    ReleaseFormatError,
    ReleaseIntegrityError,
    SourceUnavailableError,
    WithheldTableError,
)
from climate_finance.frozen import unknown_keyword
from climate_finance.methods import ONE, Method, MethodRange
from climate_finance.result import Quality, Reconciliation, Result, year_runs
from climate_finance.schema import (
    DATA_FORMAT,
    FORMAT_TABLES,
    NET_FLOWS,
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
# The newest release index, an asset of the GitHub release tagged `data`, which also
# holds every pack. A fixed tag keeps the URL off GitHub's "latest" release, which
# follows code releases.
INDEX_URL = (
    "https://github.com/ONEcampaign/climate-finance-package"
    "/releases/download/data/releases.json"
)
# The variable that moves the cache, else `~/.cache/climate-finance`.
CACHE_DIR_VAR = "CLIMATE_FINANCE_CACHE_DIR"
# The file in a cached release directory holding the sha256 of the pack it came from.
PACK_SHA256 = "pack.sha256"
# Seconds a download may wait on the server before it fails.
_TIMEOUT = 60
_CHUNK = 1 << 20
# The release name `tests/fixtures/snapshot` carries, whose figures are invented.
# `cf-build release` refuses to publish a release of this name.
SYNTHETIC_RELEASE = "fixture"
# Where releases are published, for the error `load` gives before the first one.
RELEASES_PAGE = "https://github.com/ONEcampaign/climate-finance-package/releases"
# A release name is one path segment of letters, digits, dots, underscores and
# hyphens that does not start with a dot, so it cannot climb out of a directory.
_RELEASE_NAME = r"[A-Za-z0-9_][A-Za-z0-9._-]*"
# A warning names the first caller outside the package.
_PACKAGE_DIR = str(Path(__file__).parent)
# What each flow reads, as a coverage warning names it.
_FLOW_TEXT = {
    "commitments": "commitments",
    "disbursements": "disbursements",
    "grant_equivalent": "grant equivalents",
    "net_disbursements": "net disbursements",
    "net_transfers": "net transfers",
}


type ReleaseName = Annotated[str, StringConstraints(pattern=f"^{_RELEASE_NAME}$")]


class IndexEntry(BaseModel):
    """Where one packed release is published, the sha256 of its pack and the data
    format its `release.json` records.

    Fields a later package adds are ignored, so an older install still reads the
    index.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    url: str
    sha256: str
    data_format: int


class ReleaseIndex(BaseModel):
    """The contents of `releases.json`: every published release and the newest,
    which `releases` must list. Fields a later package adds are ignored."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    latest: ReleaseName | None = None
    releases: dict[ReleaseName, IndexEntry] = {}

    @model_validator(mode="after")
    def _latest_is_listed(self) -> Self:
        if self.latest is not None and self.latest not in self.releases:
            raise ValueError(f"latest is {self.latest!r}, which releases does not list")
        return self


def check_release_name(release: str) -> str:
    """`release`, once it is one safe path segment.

    Raises:
        InvalidInputError: when `release` holds anything but letters, digits, dots,
            underscores and hyphens, or starts with a dot.
    """
    if not re.fullmatch(_RELEASE_NAME, release):
        raise InvalidInputError(
            f"release name {release!r} must be letters, digits, dots, underscores "
            "and hyphens, and not start with a dot"
        )
    return release


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
            FormatTableError: when the release's data format predates `name`.
        """
        if name not in RELEASE_TABLES:
            raise unknown_name(
                name, kind="release table", choices=sorted(RELEASE_TABLES)
            )
        self._check_held(name)
        return scan_table(self.path / f"{name}.parquet")

    def _check_held(self, name: str) -> None:
        """Raise unless the release holds the release table `name`."""
        if name not in FORMAT_TABLES[self.manifest.data_format]:
            raise FormatTableError(name, data_format=self.manifest.data_format)
        if note := self.manifest.withheld_tables.get(name):
            raise WithheldTableError(name, note=note)

    def spending(self, **inputs: Unpack[SpecInputs]) -> Result:
        """Climate finance for the query `inputs` describe, as keywords only.

        Each keyword is a `Spec` field, and `cf.SpecInputs` types the forms it takes.

        Args:
            providers: names, DAC codes or `Agency` values, one or many, such as
                `"Japan"`, `701`, `"EIB"` or `["Germany", 701]`. None, the default,
                takes every provider. A name of one agency counts its rows only.
            recipients: names or DAC codes, one or many, such as `"Kenya"`,
                `"KEN"` or `248`. None, the default, takes every recipient.
            years: an int or ints, such as `2022` or `range(2015, 2023)`. None,
                the default, takes every year.
            method: a `Method`, a `MethodRange`, a name from `cf.methods.list()`
                ignoring case, such as `"OECD"`, or a list of methods and names,
                which becomes a range. Default `cf.methods.ONE`.
            flow: `"disbursements"` (default), `"commitments"`,
                `"grant_equivalent"`, `"net_disbursements"` or `"net_transfers"`.
            view: `"recipient"` (default), finance that reaches recipients, or
                `"provider"`, a provider's total with its core contributions to
                multilateral agencies.
            currency: `"USD"` (default), `"EUR"`, `"GBP"` or `"CAD"`.
            prices: `"current"` (default) or a base year, such as `2022`.
            oda_only: True or False (default).
            measure: `"amount"` (default) or `"share_of_total"`.
            untraced: True (default) or False.

        Raises:
            UnknownProviderError: for a provider name, code or agency outside the
                listings.
            UnknownRecipientError: for a recipient name or code outside them.
            UnknownNameError: for a method name outside `cf.methods.list()`.
            InvalidInputError: for an unknown keyword, such as `spec=`, which
                `run` takes, years, a method or prices in a form `Spec` does not
                take, a value outside a field's choices, such as
                `flow="commitment"`, or a base year outside those the release's
                `conversion_factors` hold for the currency.

        Warns:
            CoverageWarning: for years the flow's tables hold no rows for, and for
                named providers with no rows the query counts, naming why.
            DoubleCountWarning: for a provider view that names EU Institutions,
                whose rows then depend on which members the query names.
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
            FormatTableError: when the release's data format predates a table the
                query reads, such as `reflows` for a net flow.

        Warns:
            CoverageWarning: for years the flow's tables hold no rows for, and for
                named providers with no rows the query counts, naming why.
            DoubleCountWarning: for a provider view that names EU Institutions,
                whose rows then depend on which members the query names.
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
        if spec.flow in NET_FLOWS:
            tables.append("reflows")
        if spec.view == "provider":
            tables.append("core_contributions")
        for table in tables:
            self._check_held(table)
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

    @_keywords
    def jev_flags(
        self,
        *,
        provider: str | int | Iterable[str | int] | None = None,
        year: int | Iterable[int] | None = None,
        flagged: Literal["no_aim", "surprising"] | None = None,
        experimental: bool = False,
    ) -> pl.DataFrame:
        """Jev's answers about the release's climate-marked CRS records, one row per
        record, with the record's facts for checking it.

        `provider` and `year` take the forms `quality`'s `providers` and `years`
        take; None takes every record. `flagged="no_aim"` keeps records whose text
        shows no climate aim at all, and `"surprising"` those a well-informed reader
        would not expect to count as climate finance. The flags rank records for
        review and change no total. `counterproductive_p` ranks only. `in_release`
        is true for a record that `disbursements` or `untraced` holds, or that
        reports a commitment matching a counted `commitments` row on provider, CRS
        id, year and purpose, the key that ties a CRS commitment to its CRDF row.
        `mitigation_aim_p` has no external check and comes only with
        `experimental=True`.

        Raises:
            MissingLabelsError: when the release was built with `jev_flags` off.
            UnknownProviderError: for a provider name or code outside the listings.
            InvalidInputError: for an agency, since the table counts providers,
                naming its provider; for a year in a form `Spec` does not take; for
                an empty list of providers or years; for a `flagged` or
                `experimental` value outside those above; or for an unknown
                keyword, naming a close one.
        """
        if flagged not in (None, "no_aim", "surprising"):
            raise InvalidInputError(
                f'flagged takes "no_aim" or "surprising"; None takes every record; '
                f"got {flagged!r}"
            )
        if not isinstance(experimental, bool):
            raise InvalidInputError(
                f"experimental takes True or False; got {experimental!r}"
            )
        return query.jev_flags(
            self,
            providers=read_provider_codes(
                provider, reader="jev_flags()", keyword="provider"
            ),
            years=read_years(year),
            flagged=flagged,
            experimental=experimental,
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
    `cache_dir` (else `CLIMATE_FINANCE_CACHE_DIR`, else `~/.cache/climate-finance`)
    opens without network when it came from the pack the index names. Otherwise its
    pack downloads, its sha256 is checked against the index, and it unpacks into
    `cache_dir`. A cached copy from another pack, or one that fails its check,
    downloads again.
    A relative `path` resolves against the working directory at load, so the
    release reads the files it verified after a later change of directory.

    Raises:
        InvalidInputError: when the release's directory would lie outside the
            cache.
        ReleaseIntegrityError: when `release.json` is missing or malformed, a
            table of its data format that it does not withhold is missing, a
            table's sha256 differs from `release.json`, or a pack's sha256 differs
            from the index.
        ReleaseFormatError: when the index entry or `release.json` records a data
            format newer than `schema.DATA_FORMAT`, which a later version of the
            package reads. An index entry raises it before any download.
        UnknownNameError: when the index lists no release named `release`, or no
            release at all, which names `path=` and where releases come from.
            Without `refresh`, the message suggests `refresh=True`.
        SourceUnavailableError: when the index or a pack cannot be downloaded, or
            the index online is malformed.
    """
    if path is not None:
        return _open(Path(path).resolve())
    index = _fetch_index() if refresh else _shipped_index()
    name = _resolve(index, release, refreshed=refresh)
    entry = index.releases[name]
    if entry.data_format > DATA_FORMAT:
        raise ReleaseFormatError(
            name, data_format=entry.data_format, supported=DATA_FORMAT
        )
    cache = _cache_dir(cache_dir)
    target = cache / release_dirname(name)
    if not target.resolve().is_relative_to(cache.resolve()):
        raise InvalidInputError(
            f"release {name!r} would unpack to {target}, outside the cache {cache}"
        )
    # Another load may move or replace `target` at any step, so each pass reads the
    # stamp afresh and every discard leaves a copy from the entry's pack in place.
    while (stamp := _packed_sha256(target)) is not None or target.is_dir():
        if stamp != entry.sha256:
            _discard(target, keep=entry.sha256)
            continue
        try:
            opened = target.stat().st_ino
        except FileNotFoundError:
            continue
        try:
            return _open(target, release=name)
        except ReleaseIntegrityError:
            _discard(target, keep=entry.sha256, failed=opened)
    _download(entry, target=target)
    return _open(target, release=name)


def _packed_sha256(target: Path) -> str | None:
    """The sha256 of the pack the cached release in `target` came from, if
    recorded."""
    try:
        return (target / PACK_SHA256).read_text(encoding="utf-8").strip()
    except (FileNotFoundError, NotADirectoryError):  # no stamp, or no directory
        return None


def _discard(target: Path, *, keep: str, failed: int | None = None) -> None:
    """Delete the cached release in `target`, unless it came from the pack of sha256
    `keep` and is not the directory of inode `failed`, a copy that failed its check.

    The copy moves to a unique name beside `target` before it is deleted, so a
    concurrent load never deletes a copy it did not move. A copy from `keep`'s pack
    that another load placed after the caller's check moves back.
    """
    aside = target.with_name(f".stale-{uuid.uuid4().hex}")
    try:
        target.rename(aside)
    except FileNotFoundError:  # another load moved it first
        return
    if _packed_sha256(aside) == keep and aside.stat().st_ino != failed:
        with contextlib.suppress(OSError):
            aside.rename(target)
            return
    shutil.rmtree(aside)


def _open(path: Path, *, release: str | None = None) -> Release:
    """Open the release in `path` once every table of its data format matches
    `release.json`.

    The data format is read before the rest of `release.json`, so a release of a
    newer format, which may carry fields this package does not know, raises
    `ReleaseFormatError` before manifest validation can reject those fields.
    """
    manifest_path = path / "release.json"
    try:
        text = manifest_path.read_text(encoding="utf-8")
        raw = json.loads(text)
        data_format = raw.get("data_format") if isinstance(raw, dict) else None
        if type(data_format) is int and data_format > DATA_FORMAT:
            raise ReleaseFormatError(
                release or path.name,
                data_format=data_format,
                supported=DATA_FORMAT,
            )
        manifest = ReleaseManifest.model_validate_json(text)
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as error:
        raise ReleaseIntegrityError(f"{path} has no release.json") from error
    except ValueError as error:  # bad JSON, bad fields or undecodable bytes
        raise ReleaseIntegrityError(f"{manifest_path} is malformed: {error}") from error
    if manifest.data_format not in FORMAT_TABLES:
        raise ReleaseIntegrityError(
            f"{manifest_path} is malformed: data format {manifest.data_format} "
            f"is none of {sorted(FORMAT_TABLES)}"
        )
    problems = [
        f"{name}: not in release.json"
        for name in sorted(FORMAT_TABLES[manifest.data_format])
        if name not in manifest.tables and name not in manifest.withheld_tables
    ]
    if release is not None and manifest.release != release:
        problems.append(f"release.json names release {manifest.release!r}")
    for name, entry in manifest.tables.items():
        file = path / f"{name}.parquet"
        try:
            actual = sha256_file(file)
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            problems.append(f"{name}: {file.name} is missing")
            continue
        if actual != entry.sha256:
            problems.append(f"{name}: sha256 {actual}, release.json has {entry.sha256}")
    if problems:
        raise ReleaseIntegrityError(
            f"release {release or manifest.release!r} at {path} fails its check: "
            + "; ".join(problems)
        )
    return Release(path=path, manifest=manifest)


def _shipped_index() -> ReleaseIndex:
    return ReleaseIndex.model_validate_json(RELEASE_INDEX.read_text(encoding="utf-8"))


def _fetch_index() -> ReleaseIndex:
    try:
        with urllib.request.urlopen(INDEX_URL, timeout=_TIMEOUT) as response:
            body = response.read()
    except OSError as error:
        raise SourceUnavailableError(
            f"Could not fetch the release index from {INDEX_URL}: {error}"
        ) from error
    try:
        return ReleaseIndex.model_validate_json(body)
    except ValidationError as error:
        raise SourceUnavailableError(
            f"The release index at {INDEX_URL} is malformed: {error}"
        ) from error


def _resolve(index: ReleaseIndex, release: str, *, refreshed: bool) -> str:
    """The release `release` names in `index`. "latest" names the newest.
    `refreshed` says whether `index` is the newest index online.

    Raises:
        UnknownNameError: when `index` lists no such release.
    """
    name = index.latest if release == "latest" else release
    if not index.releases:
        if refreshed:
            note = (
                "No release is published yet. Open a release directory with "
                "cf.load(path=...). Releases are built with `cf-build release` and "
                f"published at {RELEASES_PAGE}."
            )
        else:
            # The index shipped in the wheel predates any release published since.
            named = "" if release == "latest" else f"{release!r}, "
            note = (
                f"This package's index lists no release. cf.load({named}"
                "refresh=True) reads the newest index online, and "
                "cf.load(path=...) opens a release directory."
            )
        raise unknown_name(release, kind="release", choices=(), note=note)
    if name is None or name not in index.releases:
        note = None
        if not refreshed and index.latest is not None:
            # A release published after this package shipped is only online.
            note = (
                f"Unknown release {release!r}. This package's index lists releases "
                f"up to {index.latest}; cf.load({release!r}, refresh=True) reads "
                "the newest index online."
            )
        # The refresh note replaces suggestions, which name only older releases.
        choices = () if note else sorted(index.releases)
        raise unknown_name(release, kind="release", choices=choices, note=note)
    return name


def _cache_dir(cache_dir: str | Path | None) -> Path:
    if cache_dir is not None:
        return Path(cache_dir)
    if env := os.environ.get(CACHE_DIR_VAR):
        return Path(env)
    return Path.home() / ".cache" / "climate-finance"


def _download(entry: IndexEntry, *, target: Path) -> None:
    """Download the pack at `entry.url`, check its sha256 and unpack it to `target`,
    recording the sha256 in `target / PACK_SHA256`.

    The pack downloads and unpacks in a temporary directory beside `target`, so a
    failed download leaves nothing in its place. When another load places a copy
    first, a copy from the same pack stays and `load` opens it, and a copy from
    another pack is discarded.

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
        release_dir = unpacked / target.name
        if not release_dir.is_dir():
            raise ReleaseIntegrityError(f"{entry.url} holds no {target.name}/")
        (release_dir / PACK_SHA256).write_text(actual, encoding="utf-8")
        while True:
            try:
                release_dir.rename(target)
            except OSError:
                # A concurrent load renamed its copy into place first.
                if not target.is_dir():
                    raise
            else:
                return
            if _packed_sha256(target) == actual:
                return
            _discard(target, keep=actual)


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
