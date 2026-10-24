"""The archive release, built once per session for the e2e tests that read it.

Key repair and recoveries are off.
"""

import os
from pathlib import Path

import pytest
from archive_build import archive_config, seed_release

from climate_finance.build import build

RELEASE = "e2e"


@pytest.fixture(scope="session")
def release(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the archive offline and return the release directory."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=False)
    out_dir = tmp_path_factory.mktemp("build")
    release_dir = out_dir / RELEASE
    seed_release(release_dir, archive=archive, config=config)
    # Slow: runs every stage over 3.8 million CRS records.
    build(RELEASE, config=config, out_dir=out_dir, offline=True)
    return release_dir / "assemble"
