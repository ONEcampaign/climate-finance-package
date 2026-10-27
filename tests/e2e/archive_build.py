"""The archived sources laid out as fetch would, so an archive build runs offline.

The archive holds the CRDF and MULTI as two files each: the years from 2015, and the
earlier years an online fetch of every year reads (recipient CRDF and MULTI from
2000, provider CRDF from 2013). `seed_raw` joins each pair into one raw file and
links every other source to its archive file. The `raw_manifest` records the years
`resolve_years` asks for the configured years against the years the files hold, and
the years of them `held_in` finds in each file (MULTI from 2011). `archive_config`
is `build.toml` with the archive's CRS and IATI files as its sources.
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from normalised import record_fetch

from climate_finance.build.config import BuildConfig, LocalFile, load_config
from climate_finance.build.fetch import (
    CRDF_FILES,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
    held_in,
    held_years,
    resolve_years,
)
from climate_finance.build.readers.gcf import PROJECTS_FILE
from climate_finance.build.stages import raw_dir
from climate_finance.schema import RAW_MANIFEST, sha256_file

REPO = Path(__file__).parents[2]
CRS_ARCHIVE = Path("oecd/CRS_v20261006.parquet")
CRS_SHA256 = "d24840a5fad50e718da4eb5831c3e1e333e7c5ef6b46c158e3510c7497518f06"
# Each joined source's raw file and its archive files, earliest years first.
JOINED = {
    "crdf_recipient": (
        CRDF_FILES["recipient"],
        (Path("oecd/crdf_recipient_2000_2014.parquet"), CRDF_FILES["recipient"]),
    ),
    "crdf_provider": (
        CRDF_FILES["provider"],
        (Path("oecd/crdf_provider_2013_2014.parquet"), CRDF_FILES["provider"]),
    ),
    "multisystem": (
        MULTISYSTEM_FILE,
        (Path("oecd/multisystem_2000_2014.parquet"), MULTISYSTEM_FILE),
    ),
}


def archive_config(archive: Path, *, key_repair: bool, recoveries: bool) -> BuildConfig:
    """`build.toml` with the CRS and IATI files of `archive` (CF_E2E_DATA/raw) as its
    sources, and `key_repair` and `recoveries` set."""
    base = load_config(REPO / "build.toml")
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
                "crs_file": LocalFile(path=archive / CRS_ARCHIVE, sha256=CRS_SHA256),
                "iati_archive": archive / "external/iati",
            },
        }
    )


def seed_raw(release_dir: Path, *, archive: Path, config: BuildConfig) -> None:
    """Lay the files of `archive` (CF_E2E_DATA/raw) out under `release_dir`'s raw
    directory and record them as an earlier fetch under `config`."""
    raw = raw_dir(release_dir)
    placed = []
    for source, (relative, origins) in JOINED.items():
        (raw / relative).parent.mkdir(parents=True, exist_ok=True)
        pl.concat(
            [pl.scan_parquet(archive / origin) for origin in origins],
            how="vertical_relaxed",
        ).sink_parquet(raw / relative)
        placed.append((source, relative))
    links = {
        "crs": [(CRS_ARCHIVE, CRS_FILE)],
        "imputed_shares": [(IMPUTED_SHARES_FILE, IMPUTED_SHARES_FILE)],
        "gcf_projects": [(Path("external/gcf_api/projects.json"), PROJECTS_FILE)],
        "dac_exchange": [(DAC_EXCHANGE_FILE, DAC_EXCHANGE_FILE)],
        **{
            f"iati_{publisher}": [
                (origin.relative_to(archive), IATI_DIR / publisher / origin.name)
                for origin in sorted((archive / IATI_DIR / publisher).glob("*.xml"))
            ]
            for publisher in config.sources.iati_publishers
        },
    }
    for source, files in links.items():
        for origin, relative in files:
            target = raw / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(archive / origin)
            placed.append((source, relative))
    crdf = {
        f"crdf_{perspective}": held_years(raw / path)
        for perspective, path in CRDF_FILES.items()
    }
    years = resolve_years(
        config.years,
        held={"crs": held_years(raw / CRS_FILE), **crdf},
        recipient_first=crdf["crdf_recipient"][0],
        source="source",
    )
    held = held_in(raw, asked=years, crdf=crdf)
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
                "held_first_year": held[source][0] if source in held else None,
                "held_last_year": held[source][1] if source in held else None,
            }
            for source, relative in placed
        ],
        schema=RAW_MANIFEST.dtypes,
    )
    record_fetch(release_dir, manifest, config=config)
