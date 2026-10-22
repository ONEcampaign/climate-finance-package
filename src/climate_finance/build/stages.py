"""The stage registry: each stage's function and the tables it writes, in run order.

A stage function takes a `StageContext` and returns its tables by name. It reads
earlier stages' tables through `StageContext.read`, and the runner validates, sorts
and writes what it returns to `<out_dir>/<release>/<stage>/<table>.parquet`. A
stage's `seal` runs on the written tables before they replace the stage's directory.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from pathlib import Path

import polars as pl

from climate_finance.build import allocate, fetch, label, measure, release
from climate_finance.build.config import BuildConfig
from climate_finance.build.errors import StageInputMissingError
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link import gap_fill, review, run_link
from climate_finance.build.normalise import run_normalise
from climate_finance.schema import STAGE_TABLES, TableSchema

type StageOutput = Mapping[str, pl.DataFrame | pl.LazyFrame]


class Stage(StrEnum):
    """Build stages, in run order."""

    FETCH = "fetch"
    NORMALISE = "normalise"
    LINK = "link"
    ALLOCATE = "allocate"
    FILL = "fill"
    REVIEW = "review"
    LABEL = "label"
    MEASURE = "measure"
    ASSEMBLE = "assemble"


def stage_dir(release_dir: Path, stage: str) -> Path:
    """Where `stage` writes its tables."""
    return release_dir / stage


def table_path(release_dir: Path, stage: str, table: str) -> Path:
    """Where `stage` writes `table`."""
    return stage_dir(release_dir, stage) / f"{table}.parquet"


def raw_dir(release_dir: Path) -> Path:
    """Where fetch writes the raw source files that normalise reads."""
    return release_dir / "raw"


@dataclass(frozen=True)
class StageContext:
    """What every stage function receives.

    The runner builds one context per build, so every stage that asks Jev shares
    one `jev` client and one cost cap.
    """

    release: str
    release_dir: Path
    config: BuildConfig
    offline: bool

    @property
    def raw_dir(self) -> Path:
        """Where fetch writes the raw source files that normalise reads."""
        return raw_dir(self.release_dir)

    @cached_property
    def jev(self) -> JevClient:
        """The build's Jev client, made on first use from `config.jev`."""
        jev = self.config.jev
        return JevClient(
            model=jev.model,
            cost_cap_usd=jev.cost_cap_usd,
            cache_dir=jev.cache_dir,
            offline=self.offline,
        )

    def read(self, stage: str, table: str) -> pl.LazyFrame:
        """Scan a table an earlier stage wrote.

        Raises:
            StageInputMissingError: when the table has not been written.
        """
        path = table_path(self.release_dir, stage, table)
        if not path.exists():
            raise StageInputMissingError(stage=stage, table=table, path=path)
        return pl.scan_parquet(path)


type Seal = Callable[
    [StageContext, Mapping[str, pl.DataFrame], Mapping[str, str], Path], None
]


@dataclass(frozen=True)
class StageDef:
    """One registry entry: a stage name, its function and the tables it writes.

    `seal` takes the context, the validated tables, their sha256 and the temporary
    directory they were written to. It may raise to refuse them or add files.
    """

    name: str
    run: Callable[[StageContext], StageOutput]
    tables: Mapping[str, TableSchema]
    seal: Seal | None = None


def _entry(
    stage: Stage,
    run: Callable[[StageContext], StageOutput],
    *,
    seal: Seal | None = None,
) -> StageDef:
    return StageDef(name=stage, run=run, tables=STAGE_TABLES[stage], seal=seal)


REGISTRY: tuple[StageDef, ...] = (
    _entry(Stage.FETCH, fetch.run_fetch),
    _entry(Stage.NORMALISE, run_normalise),
    _entry(Stage.LINK, run_link),
    _entry(Stage.ALLOCATE, allocate.run_allocate),
    _entry(Stage.FILL, gap_fill.run_fill),
    _entry(Stage.REVIEW, review.run_review),
    _entry(Stage.LABEL, label.run_label),
    _entry(Stage.MEASURE, measure.run_measure),
    _entry(Stage.ASSEMBLE, release.assemble, seal=release.seal_release),
)
