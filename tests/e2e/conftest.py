"""The archive release, built once per session for the e2e tests that read it.

Key repair and recoveries are off. The raw directory is seeded from the archive in
CF_E2E_DATA with a matching `raw_manifest`, so the fetch stage runs offline.
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from climate_finance.build import build, load_config
from climate_finance.build.config import BuildConfig, LocalFile
from climate_finance.build.stages import raw_dir
from climate_finance.schema import RAW_MANIFEST, sha256_file

REPO = Path(__file__).parents[2]
RELEASE = "e2e"
CRS_SHA256 = "d24840a5fad50e718da4eb5831c3e1e333e7c5ef6b46c158e3510c7497518f06"
# Archive path, relative to CF_E2E_DATA/raw, by raw-layout path and source.
ARCHIVE_FILES = {
    ("oecd/CRS.parquet", "crs"): "oecd/CRS_v20261006.parquet",
    ("oecd/crdf_recipient.parquet", "crdf_recipient"): "oecd/crdf_recipient.parquet",
    ("oecd/crdf_provider.parquet", "crdf_provider"): "oecd/crdf_provider.parquet",
    ("oecd/multisystem.parquet", "multisystem"): "oecd/multisystem.parquet",
    (
        "oecd/Imputed_multilateral_shares.xlsx",
        "imputed_shares",
    ): "oecd/Imputed_multilateral_shares.xlsx",
    ("external/gcf/projects.json", "gcf_projects"): "external/gcf_api/projects.json",
    ("dac_exchange.parquet", "dac_exchange"): "dac_exchange.parquet",
}


def _seed_raw(raw: Path, *, archive: Path, publishers: tuple[str, ...]) -> pl.DataFrame:
    """Symlink the archived sources into `raw` and describe them as fetch would."""
    placed = [
        (archive / origin, path, source)
        for (path, source), origin in ARCHIVE_FILES.items()
    ]
    for publisher in publishers:
        placed.extend(
            (origin, f"external/iati/{publisher}/{origin.name}", f"iati_{publisher}")
            for origin in sorted((archive / "external/iati" / publisher).glob("*.xml"))
        )
    rows = []
    for origin, path, source in placed:
        target = raw / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(origin)
        rows.append(
            {
                "source": source,
                "url": None,
                "path": path,
                "sha256": sha256_file(origin),
                "etag": None,
                "version": "v20261006" if source == "crs" else None,
                "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
                "method": "manual",
            }
        )
    columns = {name: column.dtype for name, column in RAW_MANIFEST.columns.items()}
    return pl.DataFrame(rows, schema=columns)


@pytest.fixture(scope="session")
def release(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the archive offline and return the release directory."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    base = load_config(REPO / "build.toml")
    crs = archive / ARCHIVE_FILES[("oecd/CRS.parquet", "crs")]
    config = BuildConfig.model_validate(
        base.model_dump()
        | {
            "key_repair": False,
            "recoveries": False,
            "sources": base.sources.model_dump()
            | {
                "crs_file": LocalFile(path=crs, sha256=CRS_SHA256),
                "iati_archive": archive / "external/iati",
            },
        }
    )
    out_dir = tmp_path_factory.mktemp("build")
    release_dir = out_dir / RELEASE
    manifest = _seed_raw(
        raw_dir(release_dir), archive=archive, publishers=config.sources.iati_publishers
    )
    (release_dir / "fetch").mkdir()
    manifest.write_parquet(release_dir / "fetch" / "raw_manifest.parquet")
    # Slow: runs every stage over 3.8 million CRS records.
    build(RELEASE, config=config, out_dir=out_dir, offline=True)
    return release_dir / "assemble"
