"""`BuildConfig`, read from `build.toml`. Relative paths resolve against the file."""

import tomllib
from pathlib import Path
from typing import Annotated, Any, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    model_validator,
)

from climate_finance.build.errors import ConfigError


def _resolve(path: Path, info: ValidationInfo) -> Path:
    root = (info.context or {}).get("root")
    if path.is_absolute() or root is None:
        return path
    return root / path


ConfigPath = Annotated[Path, AfterValidator(_resolve)]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Years(_Section):
    """Inclusive first and last years read from each source.

    Fetch also reads the recipient CRDF of the year before `crdf`, which gives the
    first agency share year its two-year span.

    `crs_last_complete` is the last CRS year every provider has reported; the CRS
    years after it hold early reports only. Disbursement observed over time and
    the modelled line's CRS series end there.
    """

    crdf: tuple[int, int]
    crs: tuple[int, int]
    crs_last_complete: int

    @model_validator(mode="after")
    def _complete_year_inside_crs(self) -> Self:
        first, last = self.crs
        if not first <= self.crs_last_complete <= last:
            raise ValueError(
                f"crs_last_complete {self.crs_last_complete} lies outside the CRS "
                f"years {first} to {last}"
            )
        return self


class LocalFile(_Section):
    """A source file downloaded by hand, and the sha256 it must have."""

    path: ConfigPath
    sha256: Sha256


class Ledgers(_Section):
    """The ledgers the review stage checks bank links against.

    Each name in `publishers` is a ledger: `ibrd` and `ida` read World Bank
    Finances One, `gcf` the GCF projects file fetch always writes, `gcf_readiness`
    the GCF readiness list, and any other name the files of that publisher on the
    IATI Bulk Data Service. `years` are the first and last calendar years read and
    checked. With `archive`, a directory laid out as `raw/ledgers/`, fetch copies the
    files from it and downloads none.
    """

    years: tuple[int, int]
    publishers: tuple[str, ...]
    archive: ConfigPath | None = None


class Sources(_Section):
    """Pinned source versions and local source paths.

    `crs_file` names a browser-downloaded bulk CRS parquet; without it, fetch
    downloads the file with oda-reader. `crdf_etags`, when not empty, pins every
    CRDF workbook fetch reads to the ETag it maps the workbook's name to.
    `iati_archive` holds one directory of activity XML per publisher, and fetch
    copies those of `iati_publishers`. Without `ledgers`, fetch and normalise skip
    the bank ledgers. `adaptation_labels` names Weikmans et al.'s human-coded
    adaptation re-evaluation, which fetch copies into `raw/` when `jev_labels` is on
    and which fits the cut on Jev's answers. `btr_archive` holds the BTR workbooks
    a person downloaded with their `manifest.json`; without it the build has no
    BTR rows and the release no reconciliation.
    """

    crs_version: str
    crs_file: LocalFile | None = None
    crdf_etags: dict[str, str]
    iati_archive: ConfigPath
    iati_publishers: tuple[str, ...]
    btr_archive: ConfigPath | None = None
    ledgers: Ledgers | None = None
    review_decisions: ConfigPath
    adaptation_labels: LocalFile | None = None


class Jev(_Section):
    """The pinned Jev model, its per-build cost cap, the band it adjudicates."""

    model: str
    cost_cap_usd: float
    band: tuple[float, float]
    cache_dir: ConfigPath


class Embedding(_Section):
    """The pinned embedding model behind the blocking's embedding neighbours, and
    the directory that caches its vectors."""

    model: str
    revision: str
    cache_dir: ConfigPath


class BuildConfig(_Section):
    """Every build setting.

    `skip_eib_routes` names EIB recovery routes the link stage leaves out when
    `recoveries` is on. A release builds with none; an end-to-end test sets one to
    measure what that route adds. `jev_labels` has the label stage ask Jev about
    linked records and fit its adaptation cut on `sources.adaptation_labels`.
    `jev_links` has the link stage ask Jev about the classifier's pairs in
    `jev.band` and about each Splink model's calibration sample, and link what the
    calibrated Splink models score.
    `embed` adds embedding neighbours to the link stage's blocking, which needs the
    `embed` extra unless `embedding.cache_dir` holds every vector.
    """

    offline: bool = False
    key_repair: bool = True
    recoveries: bool = True
    skip_eib_routes: tuple[str, ...] = ()
    gap_fill: bool = False
    review: bool = False
    jev_labels: bool = False
    jev_links: bool = False
    embed: bool = False
    years: Years
    sources: Sources
    jev: Jev
    embedding: Embedding

    @model_validator(mode="after")
    def _jev_labels_need_human_labels(self) -> Self:
        if self.jev_labels and self.sources.adaptation_labels is None:
            raise ValueError("jev_labels needs sources.adaptation_labels")
        return self


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
