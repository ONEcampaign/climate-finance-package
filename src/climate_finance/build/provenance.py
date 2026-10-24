"""What each stage output was built from, and the check that refuses a stale one.

Every stage directory holds `stage.json`, a `StageRecord`: the sha256 of each table
the stage wrote, the identity of each earlier stage it read, and the value of each
build setting it read. A stage's identity is the sha256 of its `stage.json`, so the
fetch stage's identity covers its `raw_manifest`, and through it the raw files.

The runner gives each stage a `Provenance`, which records reads as they happen:
`StageContext.read` and `StageContext.raw_dir` record the stage read, and
`Provenance.config` records every setting read through it. A stage read must be
current: each setting it recorded must equal the build config's, and each stage it
read must still have the identity it recorded, and be current itself. Otherwise the
read raises `StaleStageError` naming the stage to rerun.
"""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, ConfigDict, JsonValue
from pydantic_core import to_jsonable_python

from climate_finance.build.errors import StaleStageError
from climate_finance.schema import sha256_file

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from climate_finance.build.config import BuildConfig

STAGE_RECORD = "stage.json"


class StageRecord(BaseModel):
    """A stage's `stage.json`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    stage: str
    tables: dict[str, str]
    inputs: dict[str, str]
    settings: dict[str, JsonValue]


def record_path(release_dir: Path, stage: str) -> Path:
    """Where `stage` keeps its `stage.json`."""
    return release_dir / stage / STAGE_RECORD


def identity(release_dir: Path, stage: str) -> str:
    """The sha256 of `stage`'s `stage.json`."""
    return sha256_file(record_path(release_dir, stage))


class Provenance:
    """The stages and settings one stage reads while it runs."""

    def __init__(self, *, stage: str, release_dir: Path, config: BuildConfig) -> None:
        self.stage = stage
        self.release_dir = release_dir
        self.inputs: dict[str, str] = {}
        self.settings: dict[str, JsonValue] = {}
        self._config = config
        self._current: set[str] = set()

    @property
    def config(self) -> BuildConfig:
        """The build config, recording each setting read through it."""
        # A stand-in that reads like the config and records the setting paths read.
        return cast("BuildConfig", _Recording(self._config, "", self._setting))

    def read_stage(self, stage: str) -> None:
        """Record a read of `stage`'s output.

        Reading the stage's own earlier output records nothing.

        Raises:
            StaleStageError: when `stage`, or a stage it was built from, is stale.
        """
        if stage == self.stage or stage in self.inputs:
            return
        self._check_current(stage)
        self.inputs[stage] = identity(self.release_dir, stage)

    def record(self, tables: Mapping[str, str]) -> StageRecord:
        """The `stage.json` of the stage's output, with `tables` their sha256."""
        return StageRecord(
            stage=self.stage,
            tables=dict(sorted(tables.items())),
            inputs=dict(sorted(self.inputs.items())),
            settings=dict(sorted(self.settings.items())),
        )

    def _setting(self, path: str, value: JsonValue) -> None:
        self.settings[path] = value

    def _check_current(self, stage: str) -> None:
        if stage in self._current:
            return
        path = record_path(self.release_dir, stage)
        if not path.exists():
            raise StaleStageError(
                stage=stage, reason=f"has no {STAGE_RECORD}, so its inputs are unknown"
            )
        record = StageRecord.model_validate_json(path.read_text())
        # Earlier stages first, so the error names the first stage to rerun.
        for upstream, recorded in record.inputs.items():
            self._check_current(upstream)
            if identity(self.release_dir, upstream) != recorded:
                raise StaleStageError(
                    stage=stage, reason=f"was built from another {upstream!r} output"
                )
        for setting, recorded in record.settings.items():
            now = to_jsonable_python(reduce(getattr, setting.split("."), self._config))
            if now != recorded:
                raise StaleStageError(
                    stage=stage,
                    reason=f"was built with {setting} = {recorded}, and the build "
                    f"config sets {now}",
                )
        self._current.add(stage)


class _Recording:
    """A read-through view of a settings model that reports each setting read.

    A field holding a model gives another view; any other field reports its value.
    Any other attribute, such as a method, reports every field of the model.
    """

    def __init__(
        self,
        model: BaseModel,
        prefix: str,
        report: Callable[[str, JsonValue], None],
    ) -> None:
        self._model = model
        self._prefix = prefix
        self._report = report

    def __getattr__(self, name: str) -> object:
        value = getattr(self._model, name)
        fields = type(self._model).model_fields
        if name not in fields:
            for field in fields:
                self._report(
                    self._prefix + field,
                    to_jsonable_python(getattr(self._model, field)),
                )
            return value
        if isinstance(value, BaseModel):
            return _Recording(value, f"{self._prefix}{name}.", self._report)
        self._report(self._prefix + name, to_jsonable_python(value))
        return value
