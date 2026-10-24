"""The fetch stage: every source into the build's raw directory, and `raw_manifest`.

Fetch owns every network call of the build. It writes each source to its place in
the raw layout and records one `raw_manifest` row per file, with its sha256, and
for the CRS, the CRDF and MULTI the years asked (`resolve_years`) and the years the
file holds of them (`held_in`). Every later stage reads the years asked that the
file holds, through `fetched_years`. Each bank ledger file records the ledger years
too, which later stages read through `ledger_years`. The raw layout sits in the
fetch stage's directory as `<release_dir>/fetch/raw/`, so the raw files and the
manifest that records them replace the previous ones in one directory swap. Online,
fetch downloads into `<release_dir>/raw.partial/`, which `seal_fetch` moves into the new
fetch directory, so a fetch that fails at any point leaves the previous files and
manifest in place. Offline, it makes no network call. It checks that the previous fetch
ran under the configured `SETTINGS`, other than the years, and that `raw/` holds exactly
the files the previous `raw_manifest` records, each with its recorded sha256. It
resolves the configured years against the years the previous fetch asked for, which must
cover them, and returns the manifest with the resolved years asked and the held years as
recorded, since the files are the bytes the online fetch checked. An offline fetch of
unchanged settings therefore writes the `stage.json` the previous fetch wrote, and one
of narrower years narrows every later stage to them.
"""

from __future__ import annotations

import shutil
from collections.abc import Collection
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import httpx
import polars as pl

from climate_finance.build.errors import (
    ConfigError,
    StageInputMissingError,
    StaleStageError,
)
from climate_finance.build.provenance import read_record, setting_value
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
    PROJECTS_SOURCE,
    READINESS_FILE,
    READINESS_SOURCE,
    download_gcf_projects,
    download_gcf_readiness,
)
from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import RAW_MANIFEST, sha256_file

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pandas as pd

    from climate_finance.build.config import BuildConfig, Ledgers, LocalFile, Years
    from climate_finance.build.stages import StageContext, StageOutput

# The build settings fetch reads. An offline fetch needs the previous fetch's,
# other than the years, which its `raw_manifest` years need only cover.
SETTINGS = (
    "jev_labels",
    "sources.adaptation_labels",
    "sources.crdf_etags",
    "sources.crs_file",
    "sources.crs_version",
    "sources.iati_archive",
    "sources.iati_publishers",
    "sources.ledgers",
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
# Sources read for a range of years, which their `raw_manifest` rows record, and
# their files.
YEAR_FILES = {
    "crs": CRS_FILE,
    **{f"crdf_{perspective}": path for perspective, path in CRDF_FILES.items()},
    "multisystem": MULTISYSTEM_FILE,
}
YEAR_SOURCES = tuple(YEAR_FILES)
type YearRange = tuple[int, int]
# The first year MULTI holds, as the OECD states no coverage for it: its SDMX
# availability endpoint returned HTTP 500, and a query of the DAC total for 1990 to
# 2024 returned 17,252 rows of 2011 to 2024 only (24 October 2026, oda-reader
# 1.12.0).
MULTISYSTEM_FIRST_YEAR = 2011
IMPUTED_SHARES_NAME = "Imputed_multilateral_shares.xlsx"
IMPUTED_SHARES_FILE = Path("oecd") / IMPUTED_SHARES_NAME
GCF_FILE = PROJECTS_FILE
IATI_DIR = Path("external/iati")
DAC_EXCHANGE_FILE = Path("dac_exchange.parquet")
ADAPTATION_LABELS_FILE = Path("labels/adaptation_labels.csv")

CRDF_URL = "https://webfs.oecd.org/climate/"
_TIMEOUT = httpx.Timeout(30.0, read=300.0)


def run_fetch(context: StageContext) -> StageOutput:
    """Fetch every source and return `raw_manifest`; `seal_fetch` places the files.

    Raises:
        SourceUnavailableError: when a source cannot be fetched, including a bulk
            CRS download that Cloudflare challenges (the message carries the
            `register_bulk_file` call for a browser download); or, under
            `context.offline`, when there is no earlier `raw_manifest` or `raw/`
            does not match it, or the earlier fetch ran under other `SETTINGS`,
            or the years it asked for do not cover the configured ones, or a raw
            file holds none of them.
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
            context.config.years,
            held=_recorded_years(recorded),
            recipient_first=held_years(context.raw_dir / CRDF_FILES["recipient"])[0],
            source="raw_manifest",
        )
        manifest = _with_years(recorded, years=years)
        read_years(manifest)
        return {RAW_MANIFEST.name: manifest}

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
    downloads, and the ledgers `config` names come last. The years asked come
    from `resolve_years`, against the years the CRS file and the CRDF folder hold,
    and `held_in` checks the files. The recipient CRDF file also holds the year
    before the first CRDF year, where the folder holds it, from which normalise
    builds `crdf_span`. Each ledger file records the ledger years.

    Raises:
        SourceUnavailableError: when a source cannot be fetched, or does not hold
            the configured years, or a file falls short of them.
    """
    import oda_reader

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
    listing = _crdf_years(oda_reader.list_crdf_files())
    years = resolve_years(
        config.years,
        held={"crs": held_years(raw / CRS_FILE), **listing},
        recipient_first=listing["crdf_recipient"][0],
        source="source",
    )
    first, last = years["crdf_recipient"]
    downloads: dict[Perspective, YearRange] = {
        "recipient": (max(first - 1, listing["crdf_recipient"][0]), last),
        "provider": years["crdf_provider"],
    }
    crdf = [
        _download_crdf(
            raw,
            perspective=perspective,
            years=downloads[perspective],
            pins=sources.crdf_etags,
        )
        for perspective in CRDF_FILES
    ]
    files = [
        crs,
        *crdf,
        _download_multisystem(raw, years=years["multisystem"]),
        _download_imputed_shares(raw),
        *download_gcf_projects(raw, offline=False, client=client),
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
        *(
            _fetch_ledgers(raw, ledgers=sources.ledgers, client=client)
            if sources.ledgers is not None
            else []
        ),
    ]
    held = held_in(raw, asked=years, crdf=listing)
    for file in files:
        if (name := file["source"]) in held:
            file["first_year"], file["last_year"] = years[name]
            file["held_first_year"], file["held_last_year"] = held[name]
    if sources.ledgers is not None:
        ledger_files = _ledger_files(sources.ledgers)
        for file in files:
            if file["source"] in ledger_files:
                file["first_year"], file["last_year"] = sources.ledgers.years
    return files


def expected_sources(config: BuildConfig) -> list[str]:
    """The `raw_manifest` source names a complete fetch under `config` records."""
    return [
        *YEAR_SOURCES,
        "imputed_shares",
        "gcf_projects",
        "dac_exchange",
        *(f"iati_{publisher}" for publisher in config.sources.iati_publishers),
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


def resolve_years(
    years: Years,
    *,
    held: Mapping[str, YearRange],
    recipient_first: int,
    source: Literal["source", "raw_manifest"],
) -> dict[str, YearRange]:
    """The years the build reads from each source in `YEAR_SOURCES`.

    The recipient-perspective CRDF and MULTI take `years.crdf`, or under `"all"`
    every year the recipient CRDF holds. The provider-perspective CRDF takes those
    of them it holds, and the CRS takes `years.crs`, or the first CRDF year to the
    last year the CRS holds. Where the recipient CRDF holds the year before the
    first CRDF year, normalise reads that year into `crdf_span` and the CRS starts
    then too, so the first share year's CRDF rows and CRS commitments cover the
    same years.

    Args:
        years: the configured years.
        held: the years each source holds, by `YEAR_SOURCES` name, or for an
            earlier fetch the years it asked for. The CRDF perspectives and the
            CRS are required; MULTI is checked when present.
        recipient_first: the first year the recipient-perspective CRDF holds: its
            workbook listing's, or the raw file's for an earlier fetch.
        source: what `held` describes, for the messages: `"source"` for the
            sources themselves, `"raw_manifest"` for an earlier fetch's record.

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
    crs_first = crdf[0] - 1 if recipient_first < crdf[0] else crdf[0]
    resolved = {
        "crs": years.crs or (crs_first, held["crs"][1]),
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


def held_in(
    raw: Path, *, asked: Mapping[str, YearRange], crdf: Mapping[str, YearRange]
) -> dict[str, YearRange]:
    """The years `asked` that the file of each source in `YEAR_SOURCES` under `raw`
    holds.

    A file may start after the first year asked where the source holds no earlier
    year: the CRDF perspectives none before the first year `crdf` (their workbook
    listing) holds, MULTI none before `MULTISYSTEM_FIRST_YEAR`. The CRS file is the
    source, so it holds every year asked.

    Raises:
        SourceUnavailableError: when a file starts later than that, ends before
            the last year asked, or lacks a year between, as a truncated download
            would. The message names every such file.
    """
    starts = {
        "crs": asked["crs"][0],
        **{name: span[0] for name, span in crdf.items()},
        "multisystem": MULTISYSTEM_FIRST_YEAR,
    }
    spans: dict[str, YearRange] = {}
    problems = []
    for name in YEAR_SOURCES:
        first, last = asked[name]
        held = set(
            pl.scan_parquet(raw / YEAR_FILES[name])
            .filter(pl.col("year").is_between(first, last))
            .select(pl.col("year").unique())
            .collect()["year"]
        )
        start = min(held | {max(first, starts[name])})
        if missing := sorted(set(range(start, last + 1)) - held):
            problems.append(
                f"{name} ({_span(asked[name])} asked) lacks "
                f"{', '.join(map(str, missing))}"
            )
        spans[name] = (start, last)
    if problems:
        raise SourceUnavailableError(
            f"Downloads fall short of the years the sources hold: {'; '.join(problems)}"
            "; run the fetch stage again."
        )
    return spans


def fetched_years(context: StageContext) -> dict[str, YearRange]:
    """The years the build reads from each source in `YEAR_SOURCES`, as the fetch
    stage's `raw_manifest` records them (`read_years`).

    Raises:
        StageInputMissingError: when the fetch stage wrote no `raw_manifest`.
        SourceUnavailableError: as `read_years` does.
    """
    return read_years(context.read("fetch", RAW_MANIFEST.name).collect())


def read_years(manifest: pl.DataFrame) -> dict[str, YearRange]:
    """The years asked that the file holds, for each source in `YEAR_SOURCES` of
    `manifest`, a `raw_manifest`.

    Raises:
        SourceUnavailableError: when it records no years asked or held for one of
            them, or the file holds none of the years asked.
    """
    asked = _recorded_years(manifest)
    held = _recorded_years(manifest, columns=("held_first_year", "held_last_year"))
    years = {
        name: (max(first, held[name][0]), min(last, held[name][1]))
        for name, (first, last) in asked.items()
    }
    if empty := [
        f"{name} ({_span(asked[name])} asked, {_span(held[name])} held)"
        for name, (first, last) in years.items()
        if first > last
    ]:
        raise SourceUnavailableError(
            f"The raw files hold none of the years build.toml asks for: "
            f"{', '.join(empty)}; set other years under [years]."
        )
    return years


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


def ledger_years(context: StageContext) -> YearRange:
    """The years of the bank ledgers, as the fetch stage's `raw_manifest` records
    them on each ledger file.

    Raises:
        ConfigError: when the build config names no ledgers.
        StageInputMissingError: when the fetch stage wrote no `raw_manifest`.
        SourceUnavailableError: when it records no single range of years over the
            ledger files.
    """
    ledgers = context.config.sources.ledgers
    if ledgers is None:
        raise ConfigError("build.toml names no ledgers under [sources.ledgers].")
    ranges = (
        context.read("fetch", RAW_MANIFEST.name)
        .filter(pl.col("source").is_in(sorted(_ledger_files(ledgers))))
        .select("first_year", "last_year")
        .unique()
        .collect()
    )
    if ranges.height != 1 or ranges.null_count().row(0) != (0, 0):
        raise SourceUnavailableError(
            "The raw_manifest records no single range of years for the ledgers; run "
            "the fetch stage online."
        )
    first, last = ranges.row(0)
    return first, last


def _recorded_years(
    recorded: pl.DataFrame, *, columns: tuple[str, str] = ("first_year", "last_year")
) -> dict[str, YearRange]:
    """The years `recorded` holds in `columns` for each source in `YEAR_SOURCES`.

    Raises:
        SourceUnavailableError: when it records none, or more than one range, for
            one of them.
    """
    if not set(columns) <= set(recorded.columns):
        recorded = recorded.with_columns(pl.lit(None).alias(name) for name in columns)
    ranges = (
        recorded.filter(pl.col("source").is_in(YEAR_SOURCES))
        .select("source", *columns)
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
            `config` names, in either direction, or `recorded` names a CRS or
            adaptation label file other than the one `config` names, or when `raw`
            lacks a recorded file, holds an unrecorded one, or holds one whose
            sha256 differs from the recorded one. The message names every such
            source or file.
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
        first_year=None,
        last_year=None,
        held_first_year=None,
        held_last_year=None,
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
    years: YearRange,
    pins: dict[str, str],
) -> RawFile:
    """Write the `years` of one perspective of the CRDF, as oda-reader returns
    them.

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


def _ledger_files(ledgers: Ledgers) -> set[str]:
    """The `raw_manifest` sources that hold the ledgers `ledgers` names: their
    downloads, and the GCF projects file when the GCF ledger is named."""
    gcf = [PROJECTS_SOURCE] if GCF_PUBLISHER in ledgers.publishers else []
    return {*_ledger_sources(ledgers), *gcf}


def _finances_one_datasets(ledgers: Ledgers) -> list[finances_one.Dataset]:
    return [
        FINANCES_ONE_PUBLISHERS[publisher]
        for publisher in ledgers.publishers
        if publisher in FINANCES_ONE_PUBLISHERS
    ]
