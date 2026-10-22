"""The fetch stage: every source into the build's `raw/` directory, and `raw_manifest`.

Fetch owns every network call of the build. It writes each source to its place in
the raw layout under `<release_dir>/raw/` and records one `raw_manifest` row per
file, with its sha256. Online, it fetches into `raw.partial/` and swaps that in
whole once every source has arrived, so a failed fetch leaves the previous `raw/`
in place. Offline, it makes no network call. It checks that `raw/` holds exactly
the files the previous `raw_manifest` records, each with its recorded sha256, and
returns that manifest unchanged.
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict

import httpx
import polars as pl

from climate_finance.build.errors import StageInputMissingError
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import RAW_MANIFEST, sha256_file

if TYPE_CHECKING:
    from climate_finance.build.config import BuildConfig, LocalFile
    from climate_finance.build.stages import StageContext, StageOutput

CRS_FILE = Path("oecd/CRS.parquet")
type Perspective = Literal["recipient", "provider"]

CRDF_FILES: dict[Perspective, Path] = {
    "recipient": Path("oecd/crdf_recipient.parquet"),
    "provider": Path("oecd/crdf_provider.parquet"),
}
MULTISYSTEM_FILE = Path("oecd/multisystem.parquet")
IMPUTED_SHARES_NAME = "Imputed_multilateral_shares.xlsx"
IMPUTED_SHARES_FILE = Path("oecd") / IMPUTED_SHARES_NAME
GCF_FILE = Path("external/gcf/projects.json")
IATI_DIR = Path("external/iati")
DAC_EXCHANGE_FILE = Path("dac_exchange.parquet")
ADAPTATION_LABELS_FILE = Path("labels/adaptation_labels.csv")

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


def run_fetch(context: StageContext) -> StageOutput:
    """Fetch every source into the raw directory and write `raw_manifest`.

    Raises:
        SourceUnavailableError: when a source cannot be fetched, including a bulk
            CRS download that Cloudflare challenges (the message carries the
            `register_bulk_file` call for a browser download); or, under
            `context.offline`, when there is no earlier `raw_manifest` or `raw/`
            does not match it.
    """
    raw = context.raw_dir
    if context.offline:
        try:
            recorded = context.read("fetch", RAW_MANIFEST.name).collect()
        except StageInputMissingError as error:
            raise SourceUnavailableError(
                f"An offline fetch checks raw/ against the raw_manifest of an "
                f"earlier fetch, and there is none at {error.path}; run the fetch "
                "stage online once."
            ) from error
        verify_raw(raw, recorded=recorded, config=context.config)
        return {RAW_MANIFEST.name: recorded}

    partial = raw.with_name(f"{raw.name}.partial")
    shutil.rmtree(partial, ignore_errors=True)
    with _http_client() as client:
        files = fetch_sources(partial, config=context.config, client=client)
    shutil.rmtree(raw, ignore_errors=True)
    partial.rename(raw)
    schema = {name: column.dtype for name, column in RAW_MANIFEST.columns.items()}
    return {RAW_MANIFEST.name: pl.DataFrame(files, schema=schema)}


def fetch_sources(
    raw: Path, *, config: BuildConfig, client: httpx.Client
) -> list[RawFile]:
    """Fetch every source into `raw` and describe each file written.

    The bulk CRS comes first, so a challenge stops the fetch before the long
    downloads.

    Raises:
        SourceUnavailableError: when a source cannot be fetched.
    """
    sources = config.sources
    crs = (
        _link_local(
            raw,
            local=sources.crs_file,
            relative=CRS_FILE,
            source="crs",
            version=sources.crs_version,
        )
        if sources.crs_file is not None
        else _download_crs(raw)
    )
    crdf = [
        _download_crdf(
            raw,
            perspective=perspective,
            years=config.years.crdf,
            pins=sources.crdf_etags,
        )
        for perspective in CRDF_FILES
    ]
    return [
        crs,
        *crdf,
        _download_multisystem(raw, years=config.years.crdf),
        _download_imputed_shares(raw),
        _download_gcf(raw, client=client),
        _download_dac_exchange(raw),
        *_copy_iati(
            raw, archive=sources.iati_archive, publishers=sources.iati_publishers
        ),
        *(
            [
                _link_local(
                    raw,
                    local=labels,
                    relative=ADAPTATION_LABELS_FILE,
                    source="adaptation_labels",
                )
            ]
            if (labels := _pinned_files(config).get("adaptation_labels"))
            else []
        ),
    ]


def expected_sources(config: BuildConfig) -> list[str]:
    """The `raw_manifest` source names a complete fetch under `config` records."""
    return [
        "crs",
        *(f"crdf_{perspective}" for perspective in CRDF_FILES),
        "multisystem",
        "imputed_shares",
        "gcf_projects",
        "dac_exchange",
        *(f"iati_{publisher}" for publisher in config.sources.iati_publishers),
        *(["adaptation_labels"] if config.jev_labels else []),
    ]


def _pinned_files(config: BuildConfig) -> dict[str, LocalFile]:
    """The local files `config` names for fetch to copy, by source name.

    The adaptation labels count only when `jev_labels` is on, as only then does a
    stage read them.
    """
    sources = config.sources
    pinned = {"crs": sources.crs_file}
    if config.jev_labels:
        pinned["adaptation_labels"] = sources.adaptation_labels
    return {source: local for source, local in pinned.items() if local is not None}


def verify_raw(raw: Path, *, recorded: pl.DataFrame, config: BuildConfig) -> None:
    """Check `raw` against the `raw_manifest` an earlier fetch wrote.

    Reads only local files.

    Raises:
        SourceUnavailableError: when the sources `recorded` holds differ from those
            `config` names, in either direction, or `recorded` names a CRS or
            adaptation label file other than the one `config` names, or when
            `raw` lacks a recorded
            file, holds an unrecorded one, or holds one whose sha256 differs from
            the recorded one. The message names every such source or file.
    """
    expected, held = set(expected_sources(config)), set(recorded["source"])
    if expected != held:
        raise SourceUnavailableError(
            f"The raw_manifest records no file for {sorted(expected - held)} and "
            f"records {sorted(held - expected)}, which build.toml does not name; "
            "run the fetch stage online."
        )
    for source, local in _pinned_files(config).items():
        if recorded.filter(source=source)["sha256"].to_list() != [local.sha256]:
            raise SourceUnavailableError(
                f"build.toml names the {_DESCRIPTIONS[source]} {local.path} (sha256 "
                f"{local.sha256}), and the raw_manifest records another; run the "
                "fetch stage online."
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
) -> RawFile:
    return RawFile(
        source=source,
        url=url,
        path=relative.as_posix(),
        sha256=sha256 or sha256_file(raw / relative),
        etag=etag,
        version=version,
        fetched_at=datetime.now(UTC),
        method=method,
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


_DESCRIPTIONS = {"crs": "CRS file", "adaptation_labels": "adaptation label file"}


def _link_local(
    raw: Path,
    *,
    local: LocalFile,
    relative: Path,
    source: str,
    version: str | None = None,
) -> RawFile:
    """Put a local file, such as the browser-downloaded bulk CRS, at `relative`,
    checking its sha256.

    Raises:
        SourceUnavailableError: when the file is missing or its sha256 differs.
    """
    if not local.path.is_file():
        raise SourceUnavailableError(
            f"build.toml names the {_DESCRIPTIONS[source]} {local.path}, which does "
            "not exist."
        )
    if (actual := sha256_file(local.path)) != local.sha256:
        raise SourceUnavailableError(
            f"{local.path} has sha256 {actual}, and build.toml expects {local.sha256}."
        )
    _place(local.path, _target(raw, relative))
    return _record(
        raw,
        relative,
        source=source,
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
        raw, relative, source=f"crdf_{perspective}", method="http", url=CRDF_URL
    )


def _download_multisystem(raw: Path, *, years: tuple[int, int]) -> RawFile:
    import oda_reader

    first, last = years
    frame = oda_reader.download_multisystem(
        start_year=first, end_year=last, pre_process=True, dotstat_codes=True
    )
    frame.to_parquet(_target(raw, MULTISYSTEM_FILE), index=False)
    return _record(raw, MULTISYSTEM_FILE, source="multisystem", method="http")


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
