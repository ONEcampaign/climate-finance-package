"""One stage run by hand on inputs a test writes: the build.toml config, a context
that records no provenance, and the registry entry that writes the stage."""

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from climate_finance.build import write_stage
from climate_finance.build.config import BuildConfig, load_config
from climate_finance.build.normalise.crdf import normalise_crdf
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.stages import REGISTRY, StageContext, StageDef, table_path

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


def write_normalised(release_dir: Path) -> None:
    """Write the normalise stage's `crs` and `crdf`, both perspectives, from the raw
    OECD fixtures, and no other table."""
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
