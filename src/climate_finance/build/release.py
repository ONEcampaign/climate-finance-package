"""The assemble stage and release packing."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from climate_finance.build.stages import StageContext, StageOutput


def run_assemble(context: StageContext) -> StageOutput:
    """Project stage tables into the release tables and write `release.json`."""
    raise NotImplementedError("slice S10a")


def pack_release(release_dir: Path, *, out_path: Path) -> Path:
    """Pack a release as `.tar.zst` with its sources and licences file."""
    raise NotImplementedError("slice S10b")


def cut_release(
    release: str, *, config: Path, out_dir: Path, waive_precision: bool = False
) -> Path:
    """Build, gate and pack `release`; record a precision waiver in release.json."""
    raise NotImplementedError("slice S10b")


def upload_release(pack: Path, *, target: str) -> str:
    """Upload a packed release to `target`; return its URL."""
    raise NotImplementedError("slice S10b")
