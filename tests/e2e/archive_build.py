"""The archived sources laid out as fetch would, so an archive build runs offline.

The recipient-perspective CRDF joins the archive's 2015 to 2024 file and its 2000
to 2014 file into one raw file. Every other source links to its archive file. The
`raw_manifest` records the years `resolve_years` gives for the configured years
against the years the files hold.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from normalised import record_fetch

from climate_finance.build.config import BuildConfig
from climate_finance.build.fetch import (
    CRDF_FILES,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    GCF_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
    held_years,
    resolve_years,
)
from climate_finance.build.stages import raw_dir
from climate_finance.schema import RAW_MANIFEST, sha256_file

CRS_ARCHIVE = Path("oecd/CRS_v20261006.parquet")
CRS_SHA256 = "d24840a5fad50e718da4eb5831c3e1e333e7c5ef6b46c158e3510c7497518f06"
# The archive's recipient-perspective CRDF files, earliest years first.
RECIPIENT_ARCHIVES = (
    Path("oecd/crdf_recipient_2000_2014.parquet"),
    Path("oecd/crdf_recipient.parquet"),
)


def seed_raw(release_dir: Path, *, archive: Path, config: BuildConfig) -> None:
    """Lay the files of `archive` (CF_E2E_DATA/raw) out under `release_dir`'s raw
    directory and record them as an earlier fetch under `config`."""
    raw = raw_dir(release_dir)
    recipient = raw / CRDF_FILES["recipient"]
    recipient.parent.mkdir(parents=True, exist_ok=True)
    pl.concat(
        [pl.scan_parquet(archive / path) for path in RECIPIENT_ARCHIVES],
        how="vertical_relaxed",
    ).sink_parquet(recipient)
    links = {
        "crs": [(CRS_ARCHIVE, CRS_FILE)],
        "crdf_provider": [(CRDF_FILES["provider"], CRDF_FILES["provider"])],
        "multisystem": [(MULTISYSTEM_FILE, MULTISYSTEM_FILE)],
        "imputed_shares": [(IMPUTED_SHARES_FILE, IMPUTED_SHARES_FILE)],
        "gcf_projects": [(Path("external/gcf_api/projects.json"), GCF_FILE)],
        "dac_exchange": [(DAC_EXCHANGE_FILE, DAC_EXCHANGE_FILE)],
        **{
            f"iati_{publisher}": [
                (origin.relative_to(archive), IATI_DIR / publisher / origin.name)
                for origin in sorted((archive / IATI_DIR / publisher).glob("*.xml"))
            ]
            for publisher in config.sources.iati_publishers
        },
    }
    placed = [("crdf_recipient", CRDF_FILES["recipient"])]
    for source, files in links.items():
        for origin, relative in files:
            target = raw / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(archive / origin)
            placed.append((source, relative))
    years = resolve_years(
        config.years,
        held={
            "crs": held_years(raw / CRS_FILE),
            **{
                f"crdf_{perspective}": held_years(raw / path)
                for perspective, path in CRDF_FILES.items()
            },
        },
        source="source",
    )
    manifest = pl.DataFrame(
        [
            {
                "source": source,
                "path": relative.as_posix(),
                # build.toml pins the CRS file's sha256, so it is not hashed again.
                "sha256": (
                    CRS_SHA256 if source == "crs" else sha256_file(raw / relative)
                ),
                "version": config.sources.crs_version if source == "crs" else None,
                "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
                "method": "manual",
                "first_year": years[source][0] if source in years else None,
                "last_year": years[source][1] if source in years else None,
            }
            for source, relative in placed
        ],
        schema=RAW_MANIFEST.dtypes,
    )
    record_fetch(release_dir, manifest, config=config)
