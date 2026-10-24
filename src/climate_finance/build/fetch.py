"""The fetch stage: every source into the build's raw directory, and `raw_manifest`.

Fetch owns every network call of the build. It writes each source to its place in
the raw layout and records one `raw_manifest` row per file, with its sha256, and
for the CRS, the CRDF and MULTI the years the build reads (`resolve_years`). Every
later stage reads those years from `raw_manifest` through `fetched_years`. The raw
layout sits in the fetch stage's directory as `<release_dir>/fetch/raw/`, so the
raw files and the manifest that records them replace the previous ones in one
directory swap. Online, fetch downloads into `<release_dir>/raw.partial/`, which
`seal_fetch` moves into the new fetch directory, so a fetch that fails at any point
leaves the previous files and manifest in place. Offline, it makes no network call.
It checks that the previous fetch ran under the configured `SETTINGS`, other than
the years, and that `raw/` holds exactly the files the previous `raw_manifest`
records, each with its recorded sha256. It resolves the configured years against
the recorded ones, which must cover them, and returns the manifest with the
resolved years. An offline fetch of unchanged settings therefore writes the
`stage.json` the previous fetch wrote, and one of narrower years narrows every
later stage to them.
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict

import httpx
import polars as pl

from climate_finance.build.errors import (
    ConfigError,
    StageInputMissingError,
    StaleStageError,
)
from climate_finance.build.provenance import read_record, setting_value
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import RAW_MANIFEST, sha256_file

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pandas as pd

    from climate_finance.build.config import BuildConfig, LocalFile, Years
    from climate_finance.build.stages import StageContext, StageOutput

# The build settings fetch reads. An offline fetch needs the previous fetch's,
# other than the years, which its `raw_manifest` years need only cover.
SETTINGS = (
    "sources.crdf_etags",
    "sources.crs_file",
    "sources.crs_version",
    "sources.iati_archive",
    "sources.iati_publishers",
    "years.crdf",
    "years.crs",
)
# The raw layout's directory inside the fetch stage's directory.
RAW = "raw"
CRS_FILE = Path("oecd/CRS.parquet")
type Perspective = Literal["recipient", "provider"]

CRDF_FILES: dict[Perspective, Path] = {
    "recipient": Path("oecd/crdf_recipient.parquet"),
    "provider": Path("oecd/crdf_provider.parquet"),
}
MULTISYSTEM_FILE = Path("oecd/multisystem.parquet")
# Sources read for a range of years, which their `raw_manifest` rows record.
YEAR_SOURCES = (
    "crs",
    *(f"crdf_{perspective}" for perspective in CRDF_FILES),
    "multisystem",
)
type YearRange = tuple[int, int]
IMPUTED_SHARES_NAME = "Imputed_multilateral_shares.xlsx"
IMPUTED_SHARES_FILE = Path("oecd") / IMPUTED_SHARES_NAME
GCF_FILE = Path("external/gcf/projects.json")
IATI_DIR = Path("external/iati")
DAC_EXCHANGE_FILE = Path("dac_exchange.parquet")

CRDF_URL = "https://webfs.oecd.org/climate/"
GCF_URL = "https://api.gcfund.org/v1/projects"
_TIMEOUT = httpx.Timeout(30.0, read=300.0)


class RawFile(TypedDict):
    """One fetched file: a `raw_manifest` row."""

    source: str
    url: str | None
    path: str
    sha256: str
    etag: str | None
    version: str | None
    fetched_at: datetime
    method: str
    first_year: int | None
    last_year: int | None


def run_fetch(context: StageContext) -> StageOutput:
    """Fetch every source and return `raw_manifest`; `seal_fetch` places the files.

    Raises:
        SourceUnavailableError: when a source cannot be fetched, including a bulk
            CRS download that Cloudflare challenges (the message carries the
            `register_bulk_file` call for a browser download); or, under
            `context.offline`, when there is no earlier `raw_manifest` or `raw/`
            does not match it, or the earlier fetch ran under other `SETTINGS`,
            or the years it records do not cover the configured ones.
    """
    if context.offline:
        try:
            recorded = context.read("fetch", RAW_MANIFEST.name).collect()
        except StageInputMissingError as error:
            raise SourceUnavailableError(
                f"An offline fetch checks raw/ against the raw_manifest of an "
                f"earlier fetch, and there is none at {error.path}; run the fetch "
                "stage online once."
            ) from error
        verify_raw(context.raw_dir, recorded=recorded, config=context.config)
        verify_settings(context.release_dir, config=context.config)
        years = resolve_years(
            context.config.years, held=_recorded_years(recorded), source="raw_manifest"
        )
        return {RAW_MANIFEST.name: _with_years(recorded, years=years)}

    partial = _partial_dir(context.release_dir)
    shutil.rmtree(partial, ignore_errors=True)
    with _http_client() as client:
        files = fetch_sources(partial, config=context.config, client=client)
    return {RAW_MANIFEST.name: pl.DataFrame(files, schema=RAW_MANIFEST.dtypes)}


def seal_fetch(
    context: StageContext,
    tables: Mapping[str, pl.DataFrame],
    sha256: Mapping[str, str],
    out_dir: Path,
) -> None:
    """Put the raw files in `out_dir` beside the `raw_manifest` written there.

    Online, the downloaded files move in. Offline, the current raw files are linked
    in, each regular file as a hard link (a copy where a link is refused) and each
    symbolic link as itself, so the current fetch directory stays whole until the
    new one replaces it.
    """
    if context.offline:
        shutil.copytree(
            context.raw_dir,
            out_dir / RAW,
            symlinks=True,
            copy_function=lambda origin, target: _place(Path(origin), Path(target)),
        )
    else:
        _partial_dir(context.release_dir).rename(out_dir / RAW)


def _partial_dir(release_dir: Path) -> Path:
    """Where an online fetch downloads before `seal_fetch` moves the files."""
    return release_dir / f"{RAW}.partial"


def fetch_sources(
    raw: Path, *, config: BuildConfig, client: httpx.Client
) -> list[RawFile]:
    """Fetch every source into `raw` and describe each file written.

    The bulk CRS comes first, so a challenge stops the fetch before the long
    downloads. The years come from `resolve_years`, against the years the CRS
    file and the CRDF folder hold.

    Raises:
        SourceUnavailableError: when a source cannot be fetched, or does not hold
            the configured years.
    """
    import oda_reader

    sources = config.sources
    crs = (
        _link_crs(raw, local=sources.crs_file, version=sources.crs_version)
        if sources.crs_file is not None
        else _download_crs(raw)
    )
    years = resolve_years(
        config.years,
        held={
            "crs": held_years(raw / CRS_FILE),
            **_crdf_years(oda_reader.list_crdf_files()),
        },
        source="source",
    )
    crs["first_year"], crs["last_year"] = years["crs"]
    crdf = [
        _download_crdf(
            raw,
            perspective=perspective,
            years=years[f"crdf_{perspective}"],
            pins=sources.crdf_etags,
        )
        for perspective in CRDF_FILES
    ]
    return [
        crs,
        *crdf,
        _download_multisystem(raw, years=years["multisystem"]),
        _download_imputed_shares(raw),
        _download_gcf(raw, client=client),
        _download_dac_exchange(raw),
        *_copy_iati(
            raw, archive=sources.iati_archive, publishers=sources.iati_publishers
        ),
    ]


def expected_sources(config: BuildConfig) -> list[str]:
    """The `raw_manifest` source names a complete fetch under `config` records."""
    return [
        "crs",
        *YEAR_SOURCES,
        "imputed_shares",
        "gcf_projects",
        "dac_exchange",
        *(f"iati_{publisher}" for publisher in config.sources.iati_publishers),
    ]


def resolve_years(
    years: Years,
    *,
    held: Mapping[str, YearRange],
    source: Literal["source", "raw_manifest"],
) -> dict[str, YearRange]:
    """The years the build reads from each source in `YEAR_SOURCES`.

    The recipient-perspective CRDF and MULTI take `years.crdf`, or under `"all"`
    every year the recipient CRDF holds. The provider-perspective CRDF takes those
    of them it holds, and the CRS takes `years.crs`, or the first CRDF year to the
    last year the CRS holds.

    Args:
        years: the configured years.
        held: the years each source holds, by `YEAR_SOURCES` name. The CRDF
            perspectives and the CRS are required; MULTI is checked when present.
        source: what `held` describes, for the messages: `"source"` for the
            files themselves, `"raw_manifest"` for an earlier fetch's record.

    Raises:
        SourceUnavailableError: when a source in `held` lacks a year resolved for
            it, or the provider-perspective CRDF holds none of the CRDF years.
    """
    crdf = held["crdf_recipient"] if years.crdf == "all" else years.crdf
    provider = held["crdf_provider"]
    provider_years = (max(crdf[0], provider[0]), min(crdf[1], provider[1]))
    if provider_years[0] > provider_years[1]:
        raise SourceUnavailableError(
            f"The provider-perspective CRDF holds {_span(provider)}, none of the "
            f"CRDF years {_span(crdf)}."
        )
    resolved = {
        "crs": years.crs or (crdf[0], held["crs"][1]),
        "crdf_recipient": crdf,
        "crdf_provider": provider_years,
        "multisystem": crdf,
    }
    if short := [
        f"{name} ({_span(resolved[name])} asked, {_span(span)} held)"
        for name, span in sorted(held.items())
        if not span[0] <= resolved[name][0] <= resolved[name][1] <= span[1]
    ]:
        raise SourceUnavailableError(
            f"build.toml asks for years the {source} lacks: {', '.join(short)}; "
            + (
                "run the fetch stage online."
                if source == "raw_manifest"
                else "set narrower years under [years]."
            )
        )
    return resolved


def fetched_years(context: StageContext) -> dict[str, YearRange]:
    """The years the build reads from each source in `YEAR_SOURCES`, as the fetch
    stage's `raw_manifest` records them.

    Raises:
        StageInputMissingError: when the fetch stage wrote no `raw_manifest`.
        SourceUnavailableError: when it records no years for one of them.
    """
    return _recorded_years(context.read("fetch", RAW_MANIFEST.name).collect())


def last_complete_year(context: StageContext) -> int:
    """`years.crs_last_complete`, checked against the CRS years `raw_manifest`
    records.

    Raises:
        ConfigError: when it lies outside them.
    """
    complete = context.config.years.crs_last_complete
    crs = fetched_years(context)["crs"]
    if not crs[0] <= complete <= crs[1]:
        raise ConfigError(
            f"crs_last_complete {complete} lies outside the CRS years {_span(crs)}"
        )
    return complete


def _recorded_years(recorded: pl.DataFrame) -> dict[str, YearRange]:
    """The years `recorded` holds for each source in `YEAR_SOURCES`.

    Raises:
        SourceUnavailableError: when it records none, or more than one range, for
            one of them.
    """
    if not {"first_year", "last_year"} <= set(recorded.columns):
        recorded = recorded.with_columns(first_year=None, last_year=None)
    ranges = (
        recorded.filter(pl.col("source").is_in(YEAR_SOURCES))
        .select("source", "first_year", "last_year")
        .unique()
        .drop_nulls()
    )
    years = {
        source: (first, last)
        for source, first, last in ranges.iter_rows()
        if ranges["source"].to_list().count(source) == 1
    }
    if lacking := [source for source in YEAR_SOURCES if source not in years]:
        raise SourceUnavailableError(
            f"The raw_manifest records no single range of years for {lacking}; run "
            "the fetch stage online."
        )
    return years


def _with_years(
    recorded: pl.DataFrame, *, years: Mapping[str, YearRange]
) -> pl.DataFrame:
    """`recorded` with each source in `years` recording its years there."""
    source = pl.col("source")
    return recorded.with_columns(
        first_year=source.replace_strict(
            {name: first for name, (first, _) in years.items()},
            default=pl.col("first_year"),
            return_dtype=RAW_MANIFEST.dtypes["first_year"],
        ),
        last_year=source.replace_strict(
            {name: last for name, (_, last) in years.items()},
            default=pl.col("last_year"),
            return_dtype=RAW_MANIFEST.dtypes["last_year"],
        ),
    )


def _span(years: YearRange) -> str:
    return f"{years[0]} to {years[1]}"


def held_years(path: Path) -> YearRange:
    """The first and last value of the `year` column of the parquet at `path`."""
    first, last = (
        pl.scan_parquet(path)
        .select(first=pl.min("year"), last=pl.max("year"))
        .collect()
        .row(0)
    )
    return int(first), int(last)


def _crdf_years(listing: pd.DataFrame) -> dict[str, YearRange]:
    """The years each CRDF perspective's workbooks hold, by `YEAR_SOURCES` name,
    from `oda_reader.list_crdf_files()`."""
    dated = listing.dropna(subset=["start_year"])
    years = {
        f"crdf_{perspective}": (
            int(files["start_year"].min()),
            int(files["end_year"].max()),
        )
        for perspective, files in dated.groupby("perspective")
        if perspective in CRDF_FILES
    }
    if lacking := [
        f"crdf_{name}" for name in CRDF_FILES if f"crdf_{name}" not in years
    ]:
        raise SourceUnavailableError(
            f"The CRDF folder at {CRDF_URL} lists no dated workbook for {lacking}."
        )
    return years


def verify_settings(release_dir: Path, *, config: BuildConfig) -> None:
    """Check that the earlier fetch's `stage.json` records `config`'s value of each
    of `SETTINGS` but the years, and `"all"` CRDF years when `config` sets them.

    Raises:
        SourceUnavailableError: when the earlier fetch has no `stage.json` this
            package can read, or it records another value, or none, for one of
            them, or `config` sets `"all"` CRDF years and the earlier fetch other
            ones, which may hold fewer. The message names every such setting.
    """
    try:
        recorded = read_record(release_dir, "fetch").settings
    except StaleStageError as error:
        raise SourceUnavailableError(
            "An offline fetch checks the settings the earlier fetch ran under, and "
            f"that fetch {error.reason}; run the fetch stage online."
        ) from error
    missing = object()
    compared = [
        setting
        for setting in SETTINGS
        if not setting.startswith("years.")
        or (setting == "years.crdf" and config.years.crdf == "all")
    ]
    changed = [
        f"{setting} (fetched with {recorded.get(setting, 'none recorded')}, "
        f"build.toml sets {now})"
        for setting in compared
        if (now := setting_value(config, setting)) != recorded.get(setting, missing)
    ]
    if changed:
        raise SourceUnavailableError(
            f"build.toml changes settings the earlier fetch ran under: "
            f"{'; '.join(changed)}; run the fetch stage online."
        )


def verify_raw(raw: Path, *, recorded: pl.DataFrame, config: BuildConfig) -> None:
    """Check `raw` against the `raw_manifest` an earlier fetch wrote.

    Reads only local files.

    Raises:
        SourceUnavailableError: when the sources `recorded` holds differ from those
            `config` names, in either direction, or `recorded` names a CRS file
            other than the one `config` names, or when `raw` lacks a recorded file,
            holds an unrecorded one, or holds one whose sha256 differs from the
            recorded one. The message names every such source or file.
    """
    expected, held = set(expected_sources(config)), set(recorded["source"])
    if expected != held:
        raise SourceUnavailableError(
            f"The raw_manifest records no file for {sorted(expected - held)} and "
            f"records {sorted(held - expected)}, which build.toml does not name; "
            "run the fetch stage online."
        )
    crs = config.sources.crs_file
    if crs is not None and recorded.filter(source="crs")["sha256"].to_list() != [
        crs.sha256
    ]:
        raise SourceUnavailableError(
            f"build.toml names the CRS file {crs.path} (sha256 {crs.sha256}), and "
            "the raw_manifest records another; run the fetch stage online."
        )

    on_disk = {
        path.relative_to(raw).as_posix() for path in raw.rglob("*") if path.is_file()
    }
    problems = [
        f"{raw / path} is not in the raw_manifest"
        for path in sorted(on_disk - set(recorded["path"]))
    ]
    for path, sha256 in recorded.select("path", "sha256").iter_rows():
        if path not in on_disk:
            problems.append(f"{raw / path} is missing")
        elif sha256_file(raw / path) != sha256:
            problems.append(f"{raw / path} differs from its recorded sha256 {sha256}")
    if problems:
        raise SourceUnavailableError(
            "raw/ does not match the raw_manifest: " + "; ".join(problems)
        )


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=_TIMEOUT, follow_redirects=True)


def _record(
    raw: Path,
    relative: Path,
    *,
    source: str,
    method: str,
    url: str | None = None,
    etag: str | None = None,
    version: str | None = None,
    sha256: str | None = None,
    years: tuple[int, int] | None = None,
) -> RawFile:
    first_year, last_year = (None, None) if years is None else years
    return RawFile(
        source=source,
        url=url,
        path=relative.as_posix(),
        sha256=sha256 or sha256_file(raw / relative),
        etag=etag,
        version=version,
        fetched_at=datetime.now(UTC),
        method=method,
        first_year=first_year,
        last_year=last_year,
    )


def _target(raw: Path, relative: Path) -> Path:
    path = raw / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _place(origin: Path, target: Path) -> None:
    """Hard-link `origin` to `target`, or copy it where a link is refused."""
    try:
        target.hardlink_to(origin.resolve())
    except OSError:
        shutil.copyfile(origin, target)


def _link_crs(raw: Path, *, local: LocalFile, version: str) -> RawFile:
    """Put the browser-downloaded bulk CRS parquet in place, checking its sha256.

    Raises:
        SourceUnavailableError: when the file is missing or its sha256 differs.
    """
    if not local.path.is_file():
        raise SourceUnavailableError(
            f"build.toml names the CRS file {local.path}, which does not exist."
        )
    if (actual := sha256_file(local.path)) != local.sha256:
        raise SourceUnavailableError(
            f"{local.path} has sha256 {actual}, and build.toml expects {local.sha256}."
        )
    _place(local.path, _target(raw, CRS_FILE))
    return _record(
        raw,
        CRS_FILE,
        source="crs",
        method="manual",
        version=version,
        sha256=local.sha256,
    )


def _download_crs(raw: Path) -> RawFile:
    """Download the bulk CRS parquet with oda-reader.

    Raises:
        SourceUnavailableError: when Cloudflare challenges the download, carrying
            oda-reader's instructions for a browser download and its registration.
    """
    import oda_reader
    from oda_reader.crs import get_full_crs_parquet_url

    target = _target(raw, CRS_FILE)
    download = target.with_name("crs.download")
    download.mkdir()
    try:
        oda_reader.bulk_download_crs(save_to_path=download)
    except oda_reader.BulkDownloadChallengeError as error:
        raise SourceUnavailableError(
            f"{error}\n\nOr name the downloaded file and its sha256 as `crs_file` "
            "under [sources] in build.toml."
        ) from error
    [saved] = download.iterdir()
    saved.rename(target)
    download.rmdir()
    return _record(
        raw, CRS_FILE, source="crs", method="http", url=get_full_crs_parquet_url()
    )


def _download_crdf(
    raw: Path, *, perspective: Perspective, years: tuple[int, int], pins: dict[str, str]
) -> RawFile:
    """Write one perspective of the CRDF as oda-reader returns it.

    Raises:
        SourceUnavailableError: when `pins` is not empty and a workbook read is
            not pinned or its ETag differs from its pin.
    """
    import oda_reader

    first, last = years
    frame = oda_reader.download_crdf(
        perspective, start_year=first, end_year=last, pre_process=True
    )
    if pins:
        read = frame[["source_file", "file_version"]].drop_duplicates()
        if unpinned := sorted(
            f"{name} (ETag {etag})"
            for name, etag in read.itertuples(index=False)
            if pins.get(name) != etag
        ):
            raise SourceUnavailableError(
                f"CRDF workbooks differ from the ETags pinned in build.toml: {unpinned}"
            )
    relative = CRDF_FILES[perspective]
    frame.to_parquet(_target(raw, relative), index=False)
    return _record(
        raw,
        relative,
        source=f"crdf_{perspective}",
        method="http",
        url=CRDF_URL,
        years=years,
    )


def _download_multisystem(raw: Path, *, years: tuple[int, int]) -> RawFile:
    import oda_reader

    first, last = years
    frame = oda_reader.download_multisystem(
        start_year=first, end_year=last, pre_process=True, dotstat_codes=True
    )
    frame.to_parquet(_target(raw, MULTISYSTEM_FILE), index=False)
    return _record(
        raw, MULTISYSTEM_FILE, source="multisystem", method="http", years=years
    )


def _download_imputed_shares(raw: Path) -> RawFile:
    import oda_reader

    oda_reader.download_crdf_file(
        IMPUTED_SHARES_NAME, _target(raw, IMPUTED_SHARES_FILE)
    )
    return _record(
        raw,
        IMPUTED_SHARES_FILE,
        source="imputed_shares",
        method="http",
        url=f"{CRDF_URL}{IMPUTED_SHARES_NAME}",
    )


def _download_gcf(raw: Path, *, client: httpx.Client) -> RawFile:
    """Save the GCF projects API response as served.

    Raises:
        SourceUnavailableError: when the request fails or the response is not a
            JSON list of projects.
    """
    try:
        response = client.get(GCF_URL)
        response.raise_for_status()
        projects = response.json()
    except (httpx.HTTPError, ValueError) as error:
        raise SourceUnavailableError(
            f"{GCF_URL} could not be fetched: {error}"
        ) from error
    if not isinstance(projects, list):
        raise SourceUnavailableError(f"{GCF_URL} did not return a list of projects.")
    _target(raw, GCF_FILE).write_bytes(response.content)
    return _record(
        raw,
        GCF_FILE,
        source="gcf_projects",
        method="http",
        url=GCF_URL,
        etag=response.headers.get("etag"),
    )


def _download_dac_exchange(raw: Path) -> RawFile:
    """Write pydeflate's DAC annual exchange rates in local currency per US dollar.

    Columns: `iso_code` (the country whose currency it is), `year`, `exchange_rate`.
    """
    import pydeflate

    rates = pydeflate.get_oecd_dac_exchange_rates(
        source_currency="USA", target_currency="LCU"
    )
    rates.to_parquet(_target(raw, DAC_EXCHANGE_FILE), index=False)
    return _record(raw, DAC_EXCHANGE_FILE, source="dac_exchange", method="http")


def _copy_iati(
    raw: Path, *, archive: Path, publishers: tuple[str, ...]
) -> list[RawFile]:
    """Put each publisher's archived activity XML under `external/iati/<publisher>/`.

    Raises:
        SourceUnavailableError: when the archive holds no XML for a publisher.
    """
    files: list[RawFile] = []
    for publisher in publishers:
        origins = sorted((archive / publisher).glob("*.xml"))
        if not origins:
            raise SourceUnavailableError(
                f"The IATI archive {archive} holds no XML for publisher {publisher!r}."
            )
        for origin in origins:
            relative = IATI_DIR / publisher / origin.name
            _place(origin, _target(raw, relative))
            files.append(
                _record(raw, relative, source=f"iati_{publisher}", method="manual")
            )
    return files
