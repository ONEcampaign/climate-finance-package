"""One stage run by hand on inputs a test writes: the build.toml config, a context
that records no provenance, and the registry entry that writes the stage."""

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance.build import write_stage
from climate_finance.build.config import BuildConfig, load_config
from climate_finance.build.fetch import YEAR_SOURCES
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.stages import REGISTRY, StageContext, StageDef, table_path
from climate_finance.schema import RAW_MANIFEST

BUILD_TOML = Path(__file__).parents[1] / "build.toml"
OECD = Path(__file__).parent / "fixtures" / "raw" / "oecd"


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


def year_rows(
    *, crdf: tuple[int, int] = (2015, 2024), crs: tuple[int, int] = (2010, 2025)
) -> pl.DataFrame:
    """`raw_manifest` rows recording `crdf` for each CRDF perspective and MULTI and
    `crs` for the CRS, asked and held, for files no test reads."""
    return pl.DataFrame(
        [
            {
                "source": source,
                "path": f"{source}.parquet",
                "sha256": "0" * 64,
                "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
                "method": "manual",
                "first_year": first,
                "last_year": last,
                "held_first_year": first,
                "held_last_year": last,
            }
            for source in YEAR_SOURCES
            for first, last in [crs if source == "crs" else crdf]
        ],
        schema=RAW_MANIFEST.dtypes,
    )


def write_fetched_years(
    release_dir: Path,
    *,
    crdf: tuple[int, int] = (2015, 2024),
    crs: tuple[int, int] = (2010, 2025),
) -> None:
    """Write the fetch stage's `raw_manifest` as `year_rows` alone."""
    write_inputs(release_dir, "fetch", {"raw_manifest": year_rows(crdf=crdf, crs=crs)})


def write_normalised(release_dir: Path) -> None:
    """Write the normalise stage's `crs` and `crdf`, both perspectives, from the raw
    OECD fixtures, and no other table, and a fetch `raw_manifest` recording the
    years they were read for."""
    write_fetched_years(release_dir)
    crs = normalise_crs(
        pl.scan_parquet(OECD / "CRS.parquet"), years=(2010, 2025)
    ).collect()
    crdf = pl.concat(
        normalise_crdf(
            pl.scan_parquet(OECD / f"crdf_{perspective}.parquet"),
            perspective=perspective,
            crs=crs.lazy(),
        )
        for perspective in ("recipient", "provider")
    ).collect()
    write_inputs(release_dir, "normalise", {"crs": crs, "crdf": crdf})
