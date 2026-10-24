"""The stage registry: each stage's function and the tables it writes, in run order.

A stage function takes a `StageContext` and returns its tables by name. It reads
earlier stages' tables through `StageContext.read`, and the runner validates, sorts
and writes what it returns to `<out_dir>/<release>/<stage>/<table>.parquet`, with
the stage's `stage.json` (see `provenance`). A stage's `seal` runs on the written
tables before they replace the stage's directory. Each entry declares the build
settings its stage, and its `seal`, read from `StageContext.config`, as dotted
paths, and the stage's `stage.json` records their values.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from pathlib import Path

import polars as pl

from climate_finance.build import allocate, fetch, label, measure, release
from climate_finance.build.config import RELEASE_SETTINGS, BuildConfig
from climate_finance.build.errors import StageInputMissingError
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link import gap_fill, review, run_link
from climate_finance.build.normalise import run_normalise
from climate_finance.build.provenance import Provenance
from climate_finance.schema import STAGE_TABLES, TableSchema, scan_table

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
    """Where fetch writes the raw source files that normalise reads, beside the
    `raw_manifest` that records them."""
    return stage_dir(release_dir, Stage.FETCH) / fetch.RAW


@dataclass(frozen=True)
class StageContext:
    """What every stage function receives.

    The runner builds one context per stage and sets `provenance`, which records
    the stages the stage reads, and `jev_client`, which returns the build's one Jev
    client, so every stage that asks Jev shares one client and one cost cap.
    """

    release: str
    release_dir: Path
    config: BuildConfig
    offline: bool
    provenance: Provenance | None = None
    jev_client: Callable[[], JevClient] | None = None

    @property
    def raw_dir(self) -> Path:
        """Where fetch writes the raw source files that normalise reads.

        Raises:
            StaleStageError: when the fetch stage is stale.
        """
        if self.provenance is not None:
            self.provenance.read_stage(Stage.FETCH)
        return raw_dir(self.release_dir)

    @cached_property
    def jev(self) -> JevClient:
        """The build's Jev client from `jev_client`, or without one a client made
        on first use from `config.jev`."""
        if self.jev_client is not None:
            return self.jev_client()
        return make_jev_client(self.config, offline=self.offline)

    def read(self, stage: str, table: str) -> pl.LazyFrame:
        """Scan a table an earlier stage wrote.

        Raises:
            StageInputMissingError: when the table has not been written.
            StaleStageError: when `stage`, or a stage it was built from, is stale.
        """
        path = table_path(self.release_dir, stage, table)
        if not path.exists():
            raise StageInputMissingError(stage=stage, table=table, path=path)
        if self.provenance is not None:
            self.provenance.read_stage(stage)
        return scan_table(path)


def make_jev_client(config: BuildConfig, *, offline: bool) -> JevClient:
    """A Jev client for `config.jev`."""
    jev = config.jev
    return JevClient(
        model=jev.model,
        cost_cap_usd=jev.cost_cap_usd,
        cache_dir=jev.cache_dir,
        offline=offline,
    )


type Seal = Callable[
    [StageContext, Mapping[str, pl.DataFrame], Mapping[str, str], Path], None
]


@dataclass(frozen=True)
class StageDef:
    """One registry entry: a stage name, its function, the tables it writes and
    the build settings it reads.

    `seal` takes the context, the validated tables, their sha256 and the temporary
    directory they were written to. It may raise to refuse them or add files.
    """

    name: str
    run: Callable[[StageContext], StageOutput]
    tables: Mapping[str, TableSchema]
    seal: Seal | None = None
    settings: tuple[str, ...] = ()


def _entry(
    stage: Stage,
    run: Callable[[StageContext], StageOutput],
    *,
    seal: Seal | None = None,
    settings: tuple[str, ...] = (),
) -> StageDef:
    return StageDef(
        name=stage, run=run, tables=STAGE_TABLES[stage], seal=seal, settings=settings
    )


REGISTRY: tuple[StageDef, ...] = (
    _entry(
        Stage.FETCH, fetch.run_fetch, seal=fetch.seal_fetch, settings=fetch.SETTINGS
    ),
    _entry(
        Stage.NORMALISE,
        run_normalise,
        settings=("recoveries", "sources.btr_archive", "sources.ledgers"),
    ),
    _entry(
        Stage.LINK,
        run_link,
        settings=(
            "embed",
            "embedding.model",
            "embedding.revision",
            "jev.band",
            "jev.model",
            "jev_links",
            "key_repair",
            "recoveries",
            "skip_eib_routes",
            "sources.btr_archive",
            "sources.iati_publishers",
            "sources.review_decisions",
        ),
    ),
    _entry(
        Stage.ALLOCATE, allocate.run_allocate, settings=("years.crs_last_complete",)
    ),
    _entry(
        Stage.FILL,
        gap_fill.run_fill,
        settings=(
            "gap_fill",
            "sources.iati_publishers",
            "sources.ledgers",
            "years.crs_last_complete",
        ),
    ),
    _entry(
        Stage.REVIEW,
        review.run_review,
        settings=("jev.band", "review", "sources.ledgers", "sources.review_decisions"),
    ),
    _entry(
        Stage.LABEL,
        label.run_label,
        settings=("gap_fill", "jev.model", "jev_labels", "years.crs_last_complete"),
    ),
    _entry(
        Stage.MEASURE,
        measure.run_measure,
        settings=("years.crs_last_complete",),
    ),
    _entry(
        Stage.ASSEMBLE,
        release.assemble,
        seal=release.seal_release,
        settings=(
            "embedding.model",
            "embedding.revision",
            "jev.model",
            *RELEASE_SETTINGS,
        ),
    ),
)
