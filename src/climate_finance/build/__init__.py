"""The build: fetch the sources, run the stages, write a release.

Needs the [build] extra. The stage writer sorts every table by its schema's sort key,
then by its remaining flat columns, so two builds from the same inputs give the same
sha256 per table.
"""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path

_BUILD_EXTRA = ("typer",)
if _missing := [name for name in _BUILD_EXTRA if find_spec(name) is None]:
    raise ImportError(
        "climate_finance.build needs the [build] extra "
        f"(missing: {', '.join(_missing)}). "
        "Install it with: pip install 'climate-finance[build]'"
    )

import polars as pl  # noqa: E402

from climate_finance.build.config import BuildConfig, load_config  # noqa: E402
from climate_finance.build.errors import BuildError, StageOutputError  # noqa: E402
from climate_finance.build.stages import (  # noqa: E402
    REGISTRY,
    Stage,
    StageContext,
    StageDef,
    StageOutput,
    table_path,
)
from climate_finance.schema import (  # noqa: E402
    OPTIONAL_STAGE_TABLES,
    TableSchema,
    sha256_file,
    validate,
)


@dataclass(frozen=True)
class BuildManifest:
    """What a build wrote: sha256 per table, by stage."""

    release: str
    release_dir: Path
    tables: Mapping[str, Mapping[str, str]]


def write_table(frame: pl.DataFrame, table: TableSchema, path: Path) -> str:
    """Write `frame` in schema column order, fully sorted; return its sha256."""
    ordered = frame.select(list(table.columns))
    tiebreak = [
        name
        for name, dtype in ordered.schema.items()
        if name not in table.sort_key and not dtype.is_nested()
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered.sort(
        [*table.sort_key, *tiebreak], nulls_last=True, maintain_order=True
    ).write_parquet(path, compression="zstd", statistics=True)
    return sha256_file(path)


def write_stage(
    stage: StageDef, output: StageOutput, release_dir: Path
) -> dict[str, str]:
    """Validate every table `stage` returned, then write them all.

    Raises:
        StageOutputError: when the returned tables differ from the declared ones,
            other than by leaving out a table in `OPTIONAL_STAGE_TABLES`.
        SchemaError: when a table fails its schema.
    """
    optional = OPTIONAL_STAGE_TABLES.get(stage.name, frozenset())
    missing = sorted(set(stage.tables) - set(output) - optional)
    unexpected = sorted(set(output) - set(stage.tables))
    if missing or unexpected:
        raise StageOutputError(
            f"Stage {stage.name!r} returned unexpected tables {unexpected} "
            f"and left out {missing}"
        )
    frames = {
        name: output[name].lazy().collect() for name in stage.tables if name in output
    }
    for name, frame in frames.items():
        validate(stage.tables[name], frame)
    return {
        name: write_table(
            frame, stage.tables[name], table_path(release_dir, stage.name, name)
        )
        for name, frame in frames.items()
    }


def run_stages(
    registry: Sequence[StageDef],
    *,
    release: str,
    release_dir: Path,
    config: BuildConfig,
    stages: Collection[str] | None = None,
    offline: bool = False,
) -> BuildManifest:
    """Run the selected stages of `registry` in registry order.

    Raises:
        BuildError: when `stages` names a stage the registry lacks.
    """
    names = [stage.name for stage in registry]
    selected = set(names) if stages is None else set(stages)
    if unknown := sorted(selected - set(names)):
        raise BuildError(f"Unknown stages {unknown}; the stages are {names}")
    context = StageContext(
        release=release, release_dir=release_dir, config=config, offline=offline
    )
    tables = {
        stage.name: write_stage(stage, stage.run(context), release_dir)
        for stage in registry
        if stage.name in selected
    }
    return BuildManifest(release=release, release_dir=release_dir, tables=tables)


def build(
    release: str,
    *,
    config: Path | BuildConfig = Path("build.toml"),
    out_dir: Path = Path("releases"),
    stages: Sequence[Stage] | None = None,
    offline: bool = False,
) -> BuildManifest:
    """Build `release` into `<out_dir>/<release>/<stage>/`.

    `stages` runs a subset, reading earlier stages' tables from disk. `offline=True`
    (or `offline = true` in the config) refuses network access.
    """
    build_config = config if isinstance(config, BuildConfig) else load_config(config)
    return run_stages(
        REGISTRY,
        release=release,
        release_dir=out_dir / release,
        config=build_config,
        stages=None if stages is None else [Stage(stage) for stage in stages],
        offline=offline or build_config.offline,
    )


__all__ = [
    "BuildConfig",
    "BuildManifest",
    "Stage",
    "StageContext",
    "build",
    "load_config",
    "run_stages",
    "write_stage",
    "write_table",
]
