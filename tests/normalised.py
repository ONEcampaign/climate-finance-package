"""Hand-built rows in the normalised `crdf` and `crs` schemas, the raw fixtures in
the raw layout normalise reads, and the fetch record of raw files laid out by hand.

Codes are real DAC codes; ids, titles and amounts are invented.
"""

import shutil
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance import schema
from climate_finance.build import write_table
from climate_finance.build.config import BuildConfig
from climate_finance.build.fetch import (
    CRDF_FILES,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    GCF_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
)
from climate_finance.build.provenance import Provenance, record_path
from climate_finance.build.stages import REGISTRY, Stage, table_path
from climate_finance.schema import sha256_file

RAW_FIXTURES = Path(__file__).parent / "fixtures" / "raw"
# The publishers the IATI fixtures hold.
IATI_PUBLISHERS = ("caf", "eib", "gcf", "gef")

_CRDF_DEFAULTS: dict[str, object] = {
    "perspective": "recipient",
    "provider_code": 8,
    "agency_code": None,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 248,
    "channel_code": None,
    "bi_multi": 1,
    "purpose_code": 23210,
    "flow_code": 10,
    "objective": "mitigation",
    "rio_adaptation": 0,
    "rio_mitigation": 2,
    "climate_usd": 1e6,
    "adaptation_usd": 0.0,
    "overlap_usd": 0.0,
    "climate_share": None,
    "id_scheme": None,
    "repeat_of": None,
    "parent_row_id": None,
}
_CRS_DEFAULTS: dict[str, object] = {
    "donor_code": 8,
    "agency_code": 1,
    "crs_id": None,
    "project_number": None,
    "title": None,
    "recipient_code": 248,
    "purpose_code": 23210,
    "channel_code": None,
    "bi_multi": 1,
    "flow_code": 11,
    "commitment_usd": None,
    "disbursement_usd": None,
    "grant_equiv_usd": None,
    "commitment_date": None,
    "rio_adaptation": 0,
    "rio_mitigation": 2,
    "text_hash": None,
}


def crdf_rows(*rows: dict[str, object]) -> pl.DataFrame:
    """CRDF rows; each needs `row_id` and `year`, and `face_usd` defaults to climate."""
    filled = []
    for row in rows:
        full = _CRDF_DEFAULTS | {"vintage": row["year"]} | row
        full.setdefault("face_usd", full["climate_usd"])
        full.setdefault("mitigation_usd", full["climate_usd"])
        filled.append(full)
    return _frame(schema.CRDF, filled)


def crs_records(*records: dict[str, object]) -> pl.DataFrame:
    """CRS records; each needs `record_id` and `year`."""
    return _frame(schema.CRS, [_CRS_DEFAULTS | record for record in records])


def named_records(
    records: Mapping[str, Mapping[str, object]], **defaults: object
) -> pl.LazyFrame:
    """CRS records by record id, each field `defaults` sets or `crs_records`
    defaults where the record leaves it out."""
    return crs_records(
        *({**defaults, "record_id": name, **fields} for name, fields in records.items())
    ).lazy()


def _frame(table: schema.TableSchema, rows: list[dict[str, object]]) -> pl.DataFrame:
    frame = pl.DataFrame(rows, schema=table.dtypes)
    schema.validate(table, frame)
    return frame


def write_raw(raw: Path, *, crdf_years: tuple[int, int] = (2015, 2024)) -> pl.DataFrame:
    """Lay the raw fixtures out under `raw` as fetch would, with a euro DAC rate
    of 0.8, and return their `raw_manifest`, which records the years an online
    fetch of `crdf_years` resolves, with CRS years to 2025 and provider-perspective
    CRDF years from 2013, as asked and held."""
    oecd = RAW_FIXTURES / "oecd"
    files = [
        ("crs", oecd / "CRS.parquet", CRS_FILE),
        *(
            (f"crdf_{perspective}", oecd / f"crdf_{perspective}.parquet", target)
            for perspective, target in CRDF_FILES.items()
        ),
        (
            "multisystem",
            RAW_FIXTURES / "multisystem" / "multisystem_2022_2023.parquet",
            MULTISYSTEM_FILE,
        ),
        (
            "imputed_shares",
            RAW_FIXTURES / "imputed_shares" / "Imputed_multilateral_shares.xlsx",
            IMPUTED_SHARES_FILE,
        ),
        ("gcf_projects", RAW_FIXTURES / "gcf" / "projects.json", GCF_FILE),
        *(
            (f"iati_{publisher}", xml, IATI_DIR / publisher / xml.name)
            for publisher in IATI_PUBLISHERS
            for xml in sorted((RAW_FIXTURES / "iati" / publisher).glob("*.xml"))
        ),
    ]
    for _, origin, target in files:
        (raw / target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(origin, raw / target)
    pl.DataFrame(
        {"iso_code": ["EUI"] * 12, "year": range(2015, 2027), "exchange_rate": 0.8}
    ).write_parquet(raw / DAC_EXCHANGE_FILE)
    files.append(("dac_exchange", raw / DAC_EXCHANGE_FILE, DAC_EXCHANGE_FILE))
    first, last = crdf_years
    years = {
        "crs": (first, 2025),
        "crdf_recipient": crdf_years,
        "crdf_provider": (max(first, 2013), last),
        "multisystem": crdf_years,
    }
    return pl.DataFrame(
        [
            {
                "source": source,
                "path": target.as_posix(),
                "sha256": sha256_file(raw / target),
                "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
                "method": "manual",
                "first_year": first,
                "last_year": last,
                "held_first_year": first,
                "held_last_year": last,
            }
            for source, _, target in files
            for first, last in [years.get(source, (None, None))]
        ],
        schema=schema.RAW_MANIFEST.dtypes,
    )


def record_fetch(
    release_dir: Path, manifest: pl.DataFrame, *, config: BuildConfig
) -> None:
    """Write `manifest` as the fetch stage's `raw_manifest`, beside raw files laid
    out by hand, with the `stage.json` a fetch under `config` writes, so an offline
    fetch accepts them."""
    [fetch] = (stage for stage in REGISTRY if stage.name == Stage.FETCH)
    path = table_path(release_dir, fetch.name, schema.RAW_MANIFEST.name)
    sha256 = write_table(manifest, schema.RAW_MANIFEST, path)
    provenance = Provenance(
        stage=fetch.name,
        release_dir=release_dir,
        config=config,
        settings=fetch.settings,
    )
    record = provenance.record({schema.RAW_MANIFEST.name: sha256})
    record_path(release_dir, fetch.name).write_text(
        record.model_dump_json(indent=2) + "\n"
    )
