"""What each stage output was built from, and the check that refuses a stale one.

Every stage directory holds `stage.json`, a `StageRecord`: the package version and
the sha256 of the installed package's files (`source_sha256`), the sha256 of each
table the stage wrote, the identity of each earlier stage it read, and the value of
each build setting the stage declares in the registry. A setting that names a file
a stage reads (`config.FILE_SETTINGS`) records the sha256 of the file. A stage's
identity is the sha256 of its `stage.json`, so the fetch stage's identity covers
its `raw_manifest`, and through it the raw files.

The runner gives each stage a `Provenance`, and `StageContext.read` and
`StageContext.raw_dir` record each stage read through it. A stage read must be
current: the stage must have been built by the installed package, each setting it
recorded must equal the build config's, and each stage it read must still have the
identity it recorded, and be current itself. Otherwise the read raises
`StaleStageError` naming the stage to rerun.

`build_identity` collects what a release records about the code and environment that
built it: the package source digest, the Python and library versions, the digest of
the whole environment, and the identity of each stage the release was built from.
"""

from __future__ import annotations

import hashlib
import platform
import re
from contextlib import suppress
from functools import cache, reduce
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError
from pydantic_core import to_jsonable_python

import climate_finance
from climate_finance.build.config import FILE_SETTINGS
from climate_finance.build.errors import StaleStageError
from climate_finance.schema import BuildIdentity, sha256_file

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

    from climate_finance.build.config import BuildConfig

STAGE_RECORD = "stage.json"
# The libraries whose versions a release records, those its results depend on.
DEPENDENCIES = (
    "duckdb",
    "numpy",
    "oda-reader",
    "openpyxl",
    "pandas",
    "polars",
    "pyarrow",
    "pydantic",
    "pydeflate",
    "rapidfuzz",
    "resolvekit",
    "scikit-learn",
    "scipy",
    "sentence-transformers",
    "splink",
    "tokenizers",
    "torch",
    "transformers",
)


class StageRecord(BaseModel):
    """A stage's `stage.json`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    stage: str
    package_version: str
    source_sha256: str
    tables: dict[str, str]
    inputs: dict[str, str]
    settings: dict[str, JsonValue]


def record_path(release_dir: Path, stage: str) -> Path:
    """Where `stage` keeps its `stage.json`."""
    return release_dir / stage / STAGE_RECORD


def identity(release_dir: Path, stage: str) -> str:
    """The sha256 of `stage`'s `stage.json`."""
    return sha256_file(record_path(release_dir, stage))


def read_record(release_dir: Path, stage: str) -> StageRecord:
    """`stage`'s `stage.json`.

    Raises:
        StaleStageError: when it is missing or the installed package cannot read
            it.
    """
    path = record_path(release_dir, stage)
    if not path.exists():
        raise StaleStageError(
            stage=stage, reason=f"has no {STAGE_RECORD}, so its inputs are unknown"
        )
    try:
        return StageRecord.model_validate_json(path.read_text(encoding="utf-8"))
    except ValidationError as error:
        raise StaleStageError(
            stage=stage,
            reason=f"has a {STAGE_RECORD} that climate-finance "
            f"{climate_finance.__version__} cannot read",
        ) from error


def setting_value(config: BuildConfig, setting: str) -> JsonValue:
    """The value of `setting`, a dotted path such as `years.crdf`, in `config`.

    The value of a setting in `FILE_SETTINGS` is the sha256 of the file it names,
    or None while there is no file.

    Raises:
        AttributeError: when `config` has no such setting.
    """
    value: object = reduce(getattr, setting.split("."), config)
    if setting in FILE_SETTINGS:
        assert isinstance(value, Path)
        return sha256_file(value) if value.exists() else None
    return to_jsonable_python(value)


@cache
def source_sha256() -> str:
    """The sha256 over the path and sha256 of every file of the installed
    `climate_finance` package, compiled bytecode and hidden files left out."""
    root = Path(climate_finance.__file__).parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        skipped = any(
            part == "__pycache__" or part.startswith(".") for part in relative.parts
        )
        if path.is_file() and not skipped:
            digest.update(f"{relative.as_posix()}\0{sha256_file(path)}\n".encode())
    return digest.hexdigest()


def environment_sha256() -> str:
    """The sha256 over the sorted `name==version` lines of every installed
    distribution, with names lower-cased and `-`, `_` and `.` runs joined by `-`."""
    lines = {
        f"{re.sub(r'[-_.]+', '-', name).lower()}=={dist.version}"
        for dist in metadata.distributions()
        if (name := dist.metadata["Name"])
    }
    return hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()


def _installed_versions(names: Iterable[str]) -> dict[str, str]:
    """The version of each of `names` that is installed."""
    versions = {}
    for name in names:
        with suppress(metadata.PackageNotFoundError):
            versions[name] = metadata.version(name)
    return versions


def build_identity(release_dir: Path, *, stages: Iterable[str]) -> BuildIdentity:
    """The code and environment building the release in `release_dir`, with the
    identity of each of `stages` that has written a `stage.json`."""
    return BuildIdentity(
        package_source_sha256=source_sha256(),
        python=platform.python_version(),
        environment_sha256=environment_sha256(),
        dependencies=_installed_versions(DEPENDENCIES),
        stage_identities={
            stage: identity(release_dir, stage)
            for stage in stages
            if record_path(release_dir, stage).exists()
        },
    )


class Provenance:
    """The stages one stage reads while it runs, and the settings it declares."""

    def __init__(
        self,
        *,
        stage: str,
        release_dir: Path,
        config: BuildConfig,
        settings: Collection[str],
    ) -> None:
        self.stage = stage
        self.release_dir = release_dir
        self.inputs: dict[str, str] = {}
        self.settings = {
            setting: setting_value(config, setting) for setting in sorted(settings)
        }
        self._config = config
        self._current: set[str] = set()

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
            package_version=climate_finance.__version__,
            source_sha256=source_sha256(),
            tables=dict(sorted(tables.items())),
            inputs=dict(sorted(self.inputs.items())),
            settings=self.settings,
        )

    def _check_current(self, stage: str) -> None:
        if stage in self._current:
            return
        record = read_record(self.release_dir, stage)
        # Earlier stages first, so the error names the first stage to rerun.
        for upstream, recorded in record.inputs.items():
            self._check_current(upstream)
            if identity(self.release_dir, upstream) != recorded:
                raise StaleStageError(
                    stage=stage, reason=f"was built from another {upstream!r} output"
                )
        built_by = (record.package_version, record.source_sha256)
        if built_by != (climate_finance.__version__, source_sha256()):
            raise StaleStageError(
                stage=stage,
                reason=f"was built by climate-finance {record.package_version} "
                f"with package files other than those of the installed "
                f"{climate_finance.__version__}",
            )
        for setting, recorded in record.settings.items():
            try:
                now = setting_value(self._config, setting)
            except AttributeError:
                raise StaleStageError(
                    stage=stage,
                    reason=f"was built with {setting} = {recorded}, which the build "
                    "config no longer sets",
                ) from None
            if now != recorded:
                raise StaleStageError(
                    stage=stage,
                    reason=f"was built with {setting} = {recorded}, and the build "
                    f"config sets {now}",
                )
        self._current.add(stage)
