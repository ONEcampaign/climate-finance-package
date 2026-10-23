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
from collections.abc import Collection
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import httpx
import polars as pl

from climate_finance.build.errors import StageInputMissingError
from climate_finance.build.readers.btr import MANIFEST as BTR_MANIFEST
from climate_finance.build.readers.btr import archive_files
from climate_finance.build.readers.ledgers.finances_one import (
    PUBLISHERS as FINANCES_ONE_PUBLISHERS,
)
from climate_finance.build.readers.ledgers.finances_one import snapshot_years
from climate_finance.build.readers.ledgers.gcf import (
    PUBLISHER as GCF_PUBLISHER,
)
from climate_finance.build.readers.ledgers.gcf import (
    READINESS_PUBLISHER,
)
from climate_finance.build.sources import finances_one, iati, imf
from climate_finance.build.sources.files import LEDGER_DIR, RawFile
from climate_finance.build.sources.gcf import (
    PROJECTS_FILE,
    READINESS_FILE,
    READINESS_SOURCE,
    download_gcf_projects,
    download_gcf_readiness,
)
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import RAW_MANIFEST, sha256_file

if TYPE_CHECKING:
    from climate_finance.build.config import BuildConfig, Ledgers, LocalFile
    from climate_finance.build.stages import StageContext, StageOutput

CRS_FILE = Path("oecd/CRS.parquet")
type Perspective = Literal["recipient", "provider"]

CRDF_FILES: dict[Perspective, Path] = {
    "recipient": Path("oecd/crdf_recipient.parquet"),
    "provider": Path("oecd/crdf_provider.parquet"),
}
# Recipient rows of the year before `years.crdf`, so the first share year has the
# two-year span of `measure.agency_shares`.
CRDF_SPAN_FILE = Path("oecd/crdf_span.parquet")
MULTISYSTEM_FILE = Path("oecd/multisystem.parquet")
IMPUTED_SHARES_NAME = "Imputed_multilateral_shares.xlsx"
IMPUTED_SHARES_FILE = Path("oecd") / IMPUTED_SHARES_NAME
GCF_FILE = PROJECTS_FILE
IATI_DIR = Path("external/iati")
BTR_DIR = Path("unfccc/btr1")
BTR_SOURCE = "unfccc_btr1"
DAC_EXCHANGE_FILE = Path("dac_exchange.parquet")
ADAPTATION_LABELS_FILE = Path("labels/adaptation_labels.csv")

CRDF_URL = "https://webfs.oecd.org/climate/"
_TIMEOUT = httpx.Timeout(30.0, read=300.0)


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
    downloads. The ledgers `config` names come last.

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
    first, last = config.years.crdf
    crdf = [
        _download_crdf(
            raw,
            perspective=perspective,
            years=(first, last),
            pins=sources.crdf_etags,
            relative=relative,
            source=f"crdf_{perspective}",
        )
        for perspective, relative in CRDF_FILES.items()
    ]
    span = _download_crdf(
        raw,
        perspective="recipient",
        years=(first - 1, first - 1),
        pins=sources.crdf_etags,
        relative=CRDF_SPAN_FILE,
        source="crdf_span",
    )
    return [
        crs,
        *crdf,
        span,
        _download_multisystem(raw, years=config.years.crdf),
        _download_imputed_shares(raw),
        *download_gcf_projects(raw, offline=False, client=client),
        _download_dac_exchange(raw),
        *_copy_iati(
            raw, archive=sources.iati_archive, publishers=sources.iati_publishers
        ),
        *(
            _copy_btr(raw, archive=sources.btr_archive)
            if sources.btr_archive is not None
            else []
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
        *(
            _fetch_ledgers(raw, ledgers=sources.ledgers, client=client)
            if sources.ledgers is not None
            else []
        ),
    ]


def expected_sources(config: BuildConfig) -> list[str]:
    """The `raw_manifest` source names a complete fetch under `config` records."""
    return [
        "crs",
        *(f"crdf_{perspective}" for perspective in CRDF_FILES),
        "crdf_span",
        "multisystem",
        "imputed_shares",
        "gcf_projects",
        "dac_exchange",
        *(f"iati_{publisher}" for publisher in config.sources.iati_publishers),
        *([BTR_SOURCE] if config.sources.btr_archive is not None else []),
        *(["adaptation_labels"] if config.jev_labels else []),
        *(
            _ledger_sources(config.sources.ledgers)
            if config.sources.ledgers is not None
            else []
        ),
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


def iati_ledger_publishers(publishers: Collection[str]) -> list[str]:
    """The ledger publishers read from IATI files, which are the rest of
    `publishers` once World Bank Finances One and the GCF's own files are set
    aside."""
    own = {*FINANCES_ONE_PUBLISHERS, GCF_PUBLISHER, READINESS_PUBLISHER}
    return [publisher for publisher in publishers if publisher not in own]


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
    raw: Path,
    *,
    perspective: Perspective,
    years: tuple[int, int],
    pins: dict[str, str],
    relative: Path,
    source: str,
) -> RawFile:
    """Write the `years` of one perspective of the CRDF, as oda-reader returns
    them, to `relative`.

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
    frame.to_parquet(_target(raw, relative), index=False)
    return _record(raw, relative, source=source, method="http", url=CRDF_URL)


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


def _copy_btr(raw: Path, *, archive: Path) -> list[RawFile]:
    """Put the BTR archive's manifest and every workbook it lists under
    `unfccc/btr1/`.

    Raises:
        SourceUnavailableError: when the manifest or a workbook it lists is missing.
    """
    origins = [archive / BTR_MANIFEST, *(path for path, _ in archive_files(archive))]
    files = []
    for origin in origins:
        relative = BTR_DIR / origin.name
        _place(origin, _target(raw, relative))
        files.append(_record(raw, relative, source=BTR_SOURCE, method="manual"))
    return files


def _fetch_ledgers(
    raw: Path, *, ledgers: Ledgers, client: httpx.Client
) -> list[RawFile]:
    """Put the ledgers `ledgers` names under `ledgers/`, from its archive if it has
    one. The GCF ledger reads the projects file, which every fetch writes. With the
    AfDB, whose ledger is in SDR, the IMF's US dollar rate of the SDR comes too.

    Raises:
        SourceUnavailableError: when a download fails or the archive lacks a file.
    """
    first, last = ledgers.years
    archive = ledgers.archive

    def archived(folder: Path) -> Path | None:
        return None if archive is None else archive / folder.relative_to(LEDGER_DIR)

    files: list[RawFile] = []
    if datasets := _finances_one_datasets(ledgers):
        files += finances_one.download_finances_one(
            raw,
            years=range(first, last + 1),
            datasets=datasets,
            offline=False,
            archive=archived(finances_one.FOLDER),
            client=client,
        )
    if publishers := iati_ledger_publishers(ledgers.publishers):
        files += iati.download_iati_ledgers(
            raw,
            publishers=publishers,
            offline=False,
            archive=archived(iati.FOLDER),
            client=client,
        )
    if READINESS_PUBLISHER in ledgers.publishers:
        files += download_gcf_readiness(
            raw, offline=False, archive=archived(READINESS_FILE.parent), client=client
        )
    if imf.XDR_PUBLISHERS & set(ledgers.publishers):
        files += imf.download_sdr_rates(
            raw, offline=False, archive=archived(imf.FOLDER), client=client
        )
    return files


def _ledger_sources(ledgers: Ledgers) -> list[str]:
    first, last = ledgers.years
    year_ends = snapshot_years(range(first, last + 1))
    return [
        *(
            finances_one.source_name(dataset, year)
            for dataset in _finances_one_datasets(ledgers)
            for year in year_ends
        ),
        *(
            iati.source_name(publisher)
            for publisher in iati_ledger_publishers(ledgers.publishers)
        ),
        *([READINESS_SOURCE] if READINESS_PUBLISHER in ledgers.publishers else []),
        *([imf.SOURCE] if imf.XDR_PUBLISHERS & set(ledgers.publishers) else []),
    ]


def _finances_one_datasets(ledgers: Ledgers) -> list[finances_one.Dataset]:
    return [
        FINANCES_ONE_PUBLISHERS[publisher]
        for publisher in ledgers.publishers
        if publisher in FINANCES_ONE_PUBLISHERS
    ]
