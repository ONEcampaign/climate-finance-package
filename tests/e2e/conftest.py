"""The archive release, built once per session for the e2e tests that read it.

Key repair and recoveries are off. The raw directory is seeded from the archive in
CF_E2E_DATA with a matching `raw_manifest` (`archive_build.seed_raw`), so the fetch
stage runs offline over every year the archive's recipient CRDF holds.
"""

import os
from pathlib import Path

import pytest
from archive_build import archive_config, seed_raw

from climate_finance.build import build

RELEASE = "e2e"


@pytest.fixture(scope="session")
def release(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the archive offline and return the release directory."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=False)
    out_dir = tmp_path_factory.mktemp("build")
    seed_raw(out_dir / RELEASE, archive=archive, config=config)
    # Slow: runs every stage over 5.1 million CRS records.
    build(RELEASE, config=config, out_dir=out_dir, offline=True)
    return out_dir / RELEASE / "assemble"
