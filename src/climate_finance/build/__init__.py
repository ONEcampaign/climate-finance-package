"""The build: fetch the sources, run the stages, write a release.

Needs the [build] extra. The stage writer sorts every table by its schema's sort key,
then by its remaining flat columns, so two builds from the same inputs give the same
sha256 per table. It writes a stage to a temporary directory and moves it into place
once every table is written, so a stage that fails leaves the last good one intact. A
build that died while moving a stage into place left `<stage>.old`, and the next
build restores or removes it before any stage runs.
"""

import shutil
import tempfile
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
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
    stage_dir,
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
    stage: StageDef,
    output: StageOutput,
    release_dir: Path,
    *,
    seal: Callable[[Mapping[str, pl.DataFrame], Mapping[str, str], Path], None]
    | None = None,
) -> dict[str, str]:
    """Validate every table `stage` returned, then write them all.

    The tables go to a temporary directory beside the stage's own. `seal`, given
    the tables, their sha256 and that directory, may check them and add files
    before the directory replaces `<release_dir>/<stage>/`. Nothing replaces it
    when validation, writing or `seal` raises.

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
    release_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{stage.name}-", dir=release_dir))
    try:
        sha256 = {
            name: write_table(frame, stage.tables[name], staging / f"{name}.parquet")
            for name, frame in frames.items()
        }
        if seal is not None:
            seal(frames, sha256, staging)
        _replace_dir(staging, stage_dir(release_dir, stage.name))
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return sha256


def _replace_dir(source: Path, target: Path) -> None:
    """Move `source` to `target`, removing what `target` held."""
    if target.exists():
        old = _old_dir(target)
        target.rename(old)
        source.rename(target)
        shutil.rmtree(old)
    else:
        source.rename(target)


def _old_dir(target: Path) -> Path:
    """Where `_replace_dir` keeps `target` while its replacement moves in."""
    return target.with_name(f"{target.name}.old")


def recover_stage_dirs(registry: Sequence[StageDef], release_dir: Path) -> None:
    """Undo a stage swap a build died in the middle of.

    A `<stage>.old` beside a missing stage directory is the last good stage, and moves
    back. Beside a stage directory it is the stage that was replaced, and goes.
    """
    for stage in registry:
        target = stage_dir(release_dir, stage.name)
        old = _old_dir(target)
        if not old.exists():
            continue
        if target.exists():
            shutil.rmtree(old)
        else:
            old.rename(target)


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
    recover_stage_dirs(registry, release_dir)
    context = StageContext(
        release=release, release_dir=release_dir, config=config, offline=offline
    )
    tables = {
        stage.name: write_stage(
            stage,
            stage.run(context),
            release_dir,
            seal=None if stage.seal is None else partial(stage.seal, context),
        )
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
