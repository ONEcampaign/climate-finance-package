"""One stage run by hand on inputs a test writes: the build.toml config, a context
that records no provenance, and the registry entry that writes the stage."""

import shutil
from collections.abc import Collection, Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance.build import write_stage
from climate_finance.build.config import BuildConfig, Ledgers, load_config
from climate_finance.build.fetch import (
    CONVERSION_FACTORS_FILE,
    YEAR_SOURCES,
    _ledger_files,
)
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs, normalise_reflows
from climate_finance.build.provenance import Provenance
from climate_finance.build.stages import (
    REGISTRY,
    StageContext,
    StageDef,
    raw_dir,
    table_path,
)
from climate_finance.schema import RAW_MANIFEST

BUILD_TOML = Path(__file__).parent / "fixtures" / "build.toml"
OECD = Path(__file__).parent / "fixtures" / "raw" / "oecd"
SNAPSHOT = Path(__file__).parent / "fixtures" / "snapshot"
# The Jev model build.toml pins, which the price table must know.
JEV_MODEL = load_config(BUILD_TOML).jev.model


def stage_context(
    release_dir: Path, *, config: BuildConfig | None = None, **switches: bool
) -> StageContext:
    """An offline context for `release_dir` under `config` (build.toml by default)
    with `switches` set. It records no provenance, so a stage reads inputs written
    without a `stage.json`."""
    base = load_config(BUILD_TOML) if config is None else config
    return StageContext(
        release="r",
        release_dir=release_dir,
        config=base.model_copy(update=switches),
        offline=True,
    )


def registered(name: str) -> StageDef:
    """The registry entry of stage `name`."""
    return next(entry for entry in REGISTRY if entry.name == name)


def run_stage(name: str, context: StageContext) -> dict[str, str]:
    """Run and write stage `name` in `context`; return its tables' sha256."""
    stage = registered(name)
    return write_stage(stage, stage.run(context), context.release_dir)


def write_inputs(
    release_dir: Path, stage: str, tables: Mapping[str, pl.DataFrame]
) -> None:
    """Write `tables` as `stage`'s output, as given and without a `stage.json`."""
    for name, frame in tables.items():
        path = table_path(release_dir, stage, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)


def write_recorded(
    release_dir: Path,
    stage: str,
    frames: Mapping[str, pl.DataFrame],
    *,
    config: BuildConfig,
    reads: tuple[str, ...] = (),
) -> None:
    """Write `frames` as `stage`'s output with its `stage.json`, recording that it
    read `reads`, so a later stage run through the runner accepts it."""
    entry = registered(stage)
    provenance = Provenance(
        stage=stage, release_dir=release_dir, config=config, settings=entry.settings
    )
    for read in reads:
        provenance.read_stage(read)
    write_stage(entry, dict(frames), release_dir, provenance=provenance)


def write_fetch(
    release_dir: Path, raw_manifest: pl.DataFrame, *, config: BuildConfig
) -> None:
    """Write `raw_manifest` as the fetch stage's output with its `stage.json`, and
    the snapshot's conversion factors in `raw/`, which assemble ships."""
    write_recorded(release_dir, "fetch", {"raw_manifest": raw_manifest}, config=config)
    factors = raw_dir(release_dir) / CONVERSION_FACTORS_FILE
    factors.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(SNAPSHOT / "conversion_factors.parquet", factors)


def year_rows(
    *,
    crdf: tuple[int, int] = (2015, 2024),
    crs: tuple[int, int] = (2010, 2025),
    ledgers: Ledgers | None = None,
) -> pl.DataFrame:
    """`raw_manifest` rows recording `crdf` for each CRDF perspective and MULTI and
    `crs` for the CRS, asked and held, plus `ledger_rows(ledgers)`, for files no
    test reads."""
    rows = pl.DataFrame(
        [
            _row(source, first=first, last=last, held=True)
            for source in YEAR_SOURCES
            for first, last in [crs if source == "crs" else crdf]
        ],
        schema=RAW_MANIFEST.dtypes,
    )
    return rows if ledgers is None else pl.concat([rows, ledger_rows(ledgers)])


def ledger_rows(ledgers: Ledgers, *, skip: Collection[str] = ()) -> pl.DataFrame:
    """`raw_manifest` rows of the files that hold the ledgers `ledgers` names, less
    the sources in `skip`, each recording the ledger years as fetch does, for files
    no test reads."""
    first, last = ledgers.years
    return pl.DataFrame(
        [
            _row(source, first=first, last=last, held=False)
            for source in sorted(_ledger_files(ledgers) - set(skip))
        ],
        schema=RAW_MANIFEST.dtypes,
    )


def _row(source: str, *, first: int, last: int, held: bool) -> dict[str, object]:
    return {
        "source": source,
        "path": f"{source}.parquet",
        "sha256": "0" * 64,
        "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
        "method": "manual",
        "first_year": first,
        "last_year": last,
        "held_first_year": first if held else None,
        "held_last_year": last if held else None,
    }


def write_fetched_years(
    release_dir: Path,
    *,
    crdf: tuple[int, int] = (2015, 2024),
    crs: tuple[int, int] = (2010, 2025),
    ledgers: Ledgers | None = None,
) -> None:
    """Write the fetch stage's `raw_manifest` as `year_rows` alone."""
    rows = year_rows(crdf=crdf, crs=crs, ledgers=ledgers)
    write_inputs(release_dir, "fetch", {"raw_manifest": rows})


def write_normalised(release_dir: Path) -> None:
    """Write the normalise stage's `crs`, `reflows` and `crdf`, both perspectives,
    from the raw OECD fixtures, and no other table, and a fetch `raw_manifest`
    recording the years they were read for."""
    write_fetched_years(release_dir)
    raw_crs = pl.scan_parquet(OECD / "CRS.parquet")
    crs = normalise_crs(raw_crs, years=(2010, 2025)).collect()
    reflows = normalise_reflows(raw_crs, years=(2010, 2025)).collect()
    crdf = pl.concat(
        normalise_crdf(
            pl.scan_parquet(OECD / f"crdf_{perspective}.parquet"),
            perspective=perspective,
            crs=crs.lazy(),
        )
        for perspective in ("recipient", "provider")
    ).collect()
    write_inputs(
        release_dir, "normalise", {"crs": crs, "reflows": reflows, "crdf": crdf}
    )
