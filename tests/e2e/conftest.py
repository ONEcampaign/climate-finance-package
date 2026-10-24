"""The archive release, built once per session for the e2e tests that read it.

Key repair and recoveries are off. The raw directory is seeded from the archive in
CF_E2E_DATA with a matching `raw_manifest` (`archive_build.seed_raw`), so the fetch
stage runs offline over every year the archive's recipient CRDF holds.
"""

import os
from pathlib import Path

import pytest
from archive_build import CRS_ARCHIVE, CRS_SHA256, seed_raw

from climate_finance.build import build, load_config
from climate_finance.build.config import BuildConfig, LocalFile

REPO = Path(__file__).parents[2]
RELEASE = "e2e"


@pytest.fixture(scope="session")
def release(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the archive offline and return the release directory."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    base = load_config(REPO / "build.toml")
    config = BuildConfig.model_validate(
        base.model_dump()
        | {
            "key_repair": False,
            "recoveries": False,
            "sources": base.sources.model_dump()
            | {
                "crs_file": LocalFile(path=archive / CRS_ARCHIVE, sha256=CRS_SHA256),
                "iati_archive": archive / "external/iati",
            },
        }
    )
    out_dir = tmp_path_factory.mktemp("build")
    seed_raw(out_dir / RELEASE, archive=archive, config=config)
    # Slow: runs every stage over 5.1 million CRS records.
    build(RELEASE, config=config, out_dir=out_dir, offline=True)
    return out_dir / RELEASE / "assemble"
