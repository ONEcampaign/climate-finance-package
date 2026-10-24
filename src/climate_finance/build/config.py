"""`BuildConfig`, read from `build.toml`. Relative paths resolve against the file."""

import tomllib
from pathlib import Path
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    ValidationError,
    ValidationInfo,
)

from climate_finance.build.errors import ConfigError


def _resolve(path: Path, info: ValidationInfo) -> Path:
    root = (info.context or {}).get("root")
    if path.is_absolute() or root is None:
        return path
    return root / path


ConfigPath = Annotated[Path, AfterValidator(_resolve)]


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Years(_Section):
    """Inclusive first and last years read from each source."""

    crdf: tuple[int, int]
    crs: tuple[int, int]


class Sources(_Section):
    """Pinned source versions and local source paths."""

    crs_version: str
    crdf_etags: dict[str, str]
    btr_archive: ConfigPath
    ledger_archive: ConfigPath
    review_decisions: ConfigPath


class Jev(_Section):
    """The pinned Jev model, its per-build cost cap, the band it adjudicates."""

    model: str
    cost_cap_usd: float
    band: tuple[float, float]
    cache_dir: ConfigPath


class Embedding(_Section):
    """The pinned embedding model, reserved for a later release."""

    model: str
    revision: str


class BuildConfig(_Section):
    """Every build setting."""

    offline: bool = False
    key_repair: bool = True
    recoveries: bool = True
    years: Years
    sources: Sources
    jev: Jev
    embedding: Embedding


def load_config(path: Path) -> BuildConfig:
    """Read `path` as a `BuildConfig`.

    Raises:
        ConfigError: when the file is missing or a key is missing or invalid.
    """
    try:
        raw: dict[str, Any] = tomllib.loads(path.read_text())
    except FileNotFoundError as error:
        raise ConfigError(f"No build config at {path}") from error
    try:
        return BuildConfig.model_validate(raw, context={"root": path.parent})
    except ValidationError as error:
        raise ConfigError(f"{path}: {error}") from error
