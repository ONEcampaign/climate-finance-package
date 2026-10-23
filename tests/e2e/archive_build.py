"""Set-up shared by the e2e tests that build the archive offline.

The raw directory is seeded from the archive in CF_E2E_DATA with a matching
`raw_manifest`, so the fetch stage runs offline.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance.build import load_config
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
    ("oecd/crdf_span.parquet", "crdf_span"): "oecd/crdf_span.parquet",
    ("oecd/multisystem.parquet", "multisystem"): "oecd/multisystem.parquet",
    (
        "oecd/Imputed_multilateral_shares.xlsx",
        "imputed_shares",
    ): "oecd/Imputed_multilateral_shares.xlsx",
    ("external/gcf/projects.json", "gcf_projects"): "external/gcf_api/projects.json",
    ("dac_exchange.parquet", "dac_exchange"): "dac_exchange.parquet",
}


def seed_raw(raw: Path, *, archive: Path, publishers: tuple[str, ...]) -> pl.DataFrame:
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


def archive_config(archive: Path, *, key_repair: bool, recoveries: bool) -> BuildConfig:
    """`build.toml` with the archive's CRS and IATI files as its sources, and no
    BTR archive."""
    base = load_config(REPO / "build.toml")
    crs = archive / ARCHIVE_FILES[("oecd/CRS.parquet", "crs")]
    return BuildConfig.model_validate(
        base.model_dump()
        | {
            "key_repair": key_repair,
            "recoveries": recoveries,
            # The archive builds run offline with no Jev cache; the Jev step has its
            # own measurement.
            "jev_links": False,
            "sources": base.sources.model_dump()
            | {
                "crs_file": LocalFile(path=crs, sha256=CRS_SHA256),
                "iati_archive": archive / "external/iati",
                # The BTR link runs in its own e2e file, on the BTR archive.
                "btr_archive": None,
            },
        }
    )


def seed_release(release_dir: Path, *, archive: Path, config: BuildConfig) -> None:
    """Lay the archive out as `release_dir`'s raw directory, with its manifest."""
    manifest = seed_raw(
        raw_dir(release_dir), archive=archive, publishers=config.sources.iati_publishers
    )
    (release_dir / "fetch").mkdir()
    manifest.write_parquet(release_dir / "fetch" / "raw_manifest.parquet")
