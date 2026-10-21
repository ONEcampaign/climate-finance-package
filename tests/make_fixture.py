"""Write the synthetic release snapshot in tests/fixtures/snapshot/.

Run `uv run python tests/make_fixture.py [out_dir]`. The snapshot is schema-valid and
covers six providers, a Bi_Multi 2 row, Bi_Multi 4 rows, an id-less multilateral row,
a modelled-line provider, every status and every link method. Amounts are invented.
"""

import itertools
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from climate_finance import __version__
from climate_finance.build import write_table
from climate_finance.schema import (
    LINK_METHODS,
    RELEASE_TABLES,
    ReleaseManifest,
    TableEntry,
    validate,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "snapshot"
RELEASE = "fixture"

# DAC codes: Germany, France, Norway, Japan, IDA, AfDB; IFC has only a modelled line.
DEU, FRA, NOR, JPN, IDA, AFDB, IFC = 5, 4, 8, 701, 905, 913, 903
IDA_CH, AFDB_CH = 44002, 46002
KEN, BGD, PER, UNS = 248, 666, 543, 998
NO_RECORD = "no_disbursement_record"
# FRA-1's project pays earlier tranches; AFDB-2 is the id-less multilateral row.
PROJECT_DISBURSING = {"FRA-1"}
ID_LESS = {"AFDB-2"}

# (row_id, provider, agency, recipient, channel, bi_multi, year, rio_adaptation,
#  rio_mitigation, climate_share, face_usd, status)
COMMITMENT_SPECS = [
    ("DEU-1", DEU, 52, KEN, None, 1, 2021, 2, 0, None, 40e6, "disbursed"),
    ("DEU-2", DEU, 52, BGD, None, 1, 2022, 1, 1, None, 25e6, "partly_disbursed"),
    ("DEU-3", DEU, 52, UNS, IDA_CH, 2, 2021, 1, 1, None, 60e6, "disbursed"),
    ("FRA-1", FRA, 1, PER, None, 1, 2021, 0, 2, None, 30e6, NO_RECORD),
    ("FRA-2", FRA, 1, KEN, None, 1, 2022, 1, 0, None, 12e6, "pending_review"),
    ("NOR-1", NOR, 1, BGD, None, 1, 2021, 2, 1, None, 18e6, "disbursed"),
    ("NOR-2", NOR, 1, PER, None, 3, 2022, 0, 1, None, 9e6, NO_RECORD),
    ("JPN-1", JPN, 2, BGD, None, 1, 2021, 0, 2, None, 80e6, "partly_disbursed"),
    ("JPN-2", JPN, 2, KEN, None, 1, 2022, 1, 1, None, 50e6, "disbursed"),
    ("IDA-1", IDA, 1, KEN, IDA_CH, 4, 2021, None, None, 0.45, 150e6, "disbursed"),
    (
        "IDA-2",
        IDA,
        1,
        BGD,
        IDA_CH,
        4,
        2022,
        None,
        None,
        0.30,
        200e6,
        "partly_disbursed",
    ),
    (
        "AFDB-1",
        AFDB,
        1,
        KEN,
        AFDB_CH,
        4,
        2021,
        None,
        None,
        0.60,
        70e6,
        "partly_disbursed",
    ),
    ("AFDB-2", AFDB, 1, PER, AFDB_CH, 4, 2022, None, None, 0.50, 45e6, "not_traceable"),
]

# Share of climate value disbursed, by status.
DISBURSED_SHARE = {
    "disbursed": 0.95,
    "partly_disbursed": 0.5,
    "pending_review": 0.3,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}


def _climate_split(
    face: float, rio_a: int | None, rio_m: int | None, share: float | None
) -> tuple[float, float, float, float]:
    """(climate, adaptation, mitigation, overlap) in USD."""
    if share is not None:
        climate = face * share
        return climate, climate * 0.5, climate * 0.5, 0.0
    adaptation = face if rio_a else 0.0
    mitigation = face if rio_m else 0.0
    overlap = face if rio_a and rio_m else 0.0
    return adaptation + mitigation - overlap, adaptation, mitigation, overlap


def build_tables() -> dict[str, pl.DataFrame]:
    """Every release table of the synthetic snapshot."""
    methods = itertools.cycle(LINK_METHODS)
    commitment_rows: list[dict[str, object]] = []
    disbursement_rows: list[dict[str, object]] = []
    objective_rows: list[dict[str, object]] = []

    for (
        row_id, provider, agency, recipient, channel, bi_multi, year, rio_a, rio_m,
        share, face, status,
    ) in COMMITMENT_SPECS:  # fmt: skip
        has_ids = row_id not in ID_LESS
        climate, adaptation, mitigation, overlap = _climate_split(
            face, rio_a, rio_m, share
        )
        # Each disbursing commitment draws enough records that every link method
        # appears once across the fixture.
        n_records = 3 if DISBURSED_SHARE[status] else 0
        total = climate * DISBURSED_SHARE[status]
        allocations = [total / n_records] * n_records if n_records else []
        low = 0.0
        for index, allocated in enumerate(allocations):
            method = next(methods)
            probability = 1.0 if index == 0 else 0.85
            low += allocated if probability >= 0.9 else 0.0
            record_id = f"{row_id}-R{index}"
            disbursement_rows.append(
                {
                    "row_id": row_id,
                    "record_id": record_id,
                    "year": year + index,
                    "provider_code": provider,
                    "agency_code": agency,
                    "recipient_code": recipient,
                    "channel_code": channel,
                    "bi_multi": bi_multi,
                    "purpose_code": 41010,
                    "flow_code": 11 if provider in (DEU, NOR) else 13,
                    "project_number": f"PN-{row_id}",
                    "title": f"Climate project {row_id}",
                    "allocated_usd": allocated,
                    "grant_equiv_usd": allocated if index else None,
                    "inherited_rio_adaptation": rio_a,
                    "inherited_rio_mitigation": rio_m,
                    "record_rio_adaptation": rio_a if index else None,
                    "record_rio_mitigation": rio_m,
                    "marker_source": "commitment" if index else "record",
                    "climate_share": share,
                    "link_method": method,
                    "link_probability": probability,
                    "rule": "dated" if index == 0 else "fifo",
                    "disbursement_source": "crs",
                    "ledger_check": "confirmed" if provider == IDA else None,
                }
            )
            # Bank climate components carry CRDF objectives, not Rio markers.
            labels = (
                (("adaptation", rio_a, "marker"), ("mitigation", rio_m, "marker"))
                if share is None
                else (("adaptation", 2, "crdf"), ("mitigation", 2, "crdf"))
            )
            objective_rows.extend(
                {
                    "record_id": record_id,
                    "objective": objective,
                    "level": level,
                    "label_source": source,
                    "probability": None,
                }
                for objective, level, source in labels
            )
        commitment_rows.append(
            {
                "row_id": row_id,
                "vintage": year,
                "perspective": "recipient" if bi_multi == 4 else "provider",
                "year": year,
                "provider_code": provider,
                "agency_code": agency,
                "recipient_code": recipient,
                "channel_code": channel,
                "bi_multi": bi_multi,
                "purpose_code": 41010,
                "flow_code": 11,
                "crs_id": f"{year}00{row_id}" if has_ids else None,
                "project_number": f"PN-{row_id}" if has_ids else None,
                "rio_adaptation": rio_a,
                "rio_mitigation": rio_m,
                "climate_share": share,
                "face_usd": face,
                "climate_usd": climate,
                "adaptation_usd": adaptation,
                "mitigation_usd": mitigation,
                "overlap_usd": overlap,
                "status": status,
                "disbursed_usd": total,
                "disbursed_low_usd": low,
                "disbursed_high_usd": min(climate, total * 1.1),
                "project_disbursing": row_id in PROJECT_DISBURSING,
                "repeat_of": None,
                "parent_row_id": None,
            }
        )

    used = {row["link_method"] for row in disbursement_rows}
    if missing := set(LINK_METHODS) - used:
        raise ValueError(f"fixture misses link methods {sorted(missing)}")

    # A bank-ledger gap fill: no CRS link, so no method or probability.
    disbursement_rows.append(
        {
            **disbursement_rows[-1],
            "record_id": "AFDB-1-LEDGER",
            "year": 2023,
            "allocated_usd": 1e6,
            "link_method": None,
            "link_probability": None,
            "rule": "bank_ledger",
            "disbursement_source": "bank_ledger",
            "ledger_check": None,
        }
    )
    # A Jev objective label on one record without markers.
    objective_rows.append(
        {
            "record_id": "JPN-1-R1",
            "objective": "adaptation",
            "level": 1,
            "label_source": "jev",
            "probability": 0.72,
        }
    )

    tables = {
        "commitments": commitment_rows,
        "disbursements": disbursement_rows,
        "objectives": objective_rows,
        "agency_shares": [
            {
                "channel_code": channel,
                "year": year,
                "objective": objective,
                "share": share,
                "source": source,
            }
            for channel, base in ((IDA_CH, 0.35), (AFDB_CH, 0.5))
            for year in (2021, 2022)
            for objective, weight in (("adaptation", 0.4), ("mitigation", 0.6))
            for source, share in (("oecd", base * weight), ("one", base * weight * 0.9))
        ],
        "modelled": [
            {
                "provider_code": IFC,
                "year": year,
                "recipient_code": recipient,
                "low_usd": low,
                "high_usd": high,
                "basis": "annual_report",
            }
            for year, recipient, low, high in (
                (2021, None, 460e6, 970e6),
                (2022, None, 500e6, 1050e6),
                (2022, PER, 30e6, 63e6),
            )
        ],
        "core_contributions": [
            {
                "provider_code": provider,
                "channel_code": channel,
                "year": year,
                "contribution_usd": amount,
            }
            for provider, channel, amount in (
                (DEU, IDA_CH, 60e6),
                (FRA, AFDB_CH, 25e6),
                (NOR, IDA_CH, 15e6),
            )
            for year in (2021, 2022)
        ],
        "btr": [
            {
                "row_id": f"BTR-{party}-{year}",
                "party": party,
                "year": year,
                "recipient": "Kenya",
                "recipient_code": KEN,
                "title": f"Adaptation programme {party} {year}",
                "amount_domestic": amount,
                "currency": currency,
                "face_usd": amount * rate,
                "grant_equiv_usd": amount * rate,
                "status": "disbursed",
                "channel": "bilateral",
                "funding_source": "ODA",
                "instrument": "grant",
                "support_type": "adaptation",
                "sector": "agriculture",
                "additional_information": None,
                "is_aggregate": False,
                "source_file": f"{party}-BTR1.xlsx",
            }
            for party, currency, rate, amount in (
                ("DEU", "EUR", 1.1, 20e6),
                ("NOR", "NOK", 0.1, 150e6),
            )
            for year in (2021, 2022)
        ],
        "disbursement_profile": [
            {
                "group": "all",
                "years_since": years_since,
                "cumulative_share": share,
                "vintages": 9 - years_since,
            }
            for years_since, share in enumerate(
                (0.23, 0.31, 0.39, 0.46, 0.52, 0.58, 0.63, 0.67, 0.70)
            )
        ],
    }
    frames = {
        name: pl.from_dicts(rows, schema=_polars_schema(name))
        for name, rows in tables.items()
    }
    frames["quality"] = _quality(frames["commitments"], frames["disbursements"])
    return frames


def _polars_schema(table: str) -> pl.Schema:
    columns = RELEASE_TABLES[table].columns
    return pl.Schema({name: column.dtype for name, column in columns.items()})


def _quality(commitments: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
    linked = disbursements.group_by("provider_code", "year").agg(
        linked_usd=pl.col("allocated_usd").sum(),
        linked_high_probability_usd=pl.col("allocated_usd")
        .filter(pl.col("link_probability") >= 0.8)
        .sum(),
        grant_equiv_coverage=pl.col("grant_equiv_usd").is_not_null().mean(),
    )
    return (
        commitments.group_by("provider_code", "year")
        .agg(
            commitments=pl.len().cast(pl.Int32),
            climate_usd=pl.col("climate_usd").sum(),
        )
        .join(linked, on=["provider_code", "year"], how="left")
        .with_columns(
            pl.col("linked_usd", "linked_high_probability_usd").fill_null(0.0),
            model_labelled_usd=pl.lit(0.0),
            report_labelled_usd=pl.col("climate_usd"),
        )
        .select(list(RELEASE_TABLES["quality"].columns))
    )


def write_fixture(out_dir: Path) -> ReleaseManifest:
    """Write every release table and `release.json` to `out_dir`."""
    entries = {}
    for name, frame in build_tables().items():
        table = RELEASE_TABLES[name]
        validate(table, frame)
        sha256 = write_table(frame, table, out_dir / f"{name}.parquet")
        entries[name] = TableEntry(sha256=sha256, rows=frame.height)
    manifest = ReleaseManifest(
        release=RELEASE,
        created_at=datetime(2026, 10, 21, tzinfo=UTC),
        package_version=__version__,
        tables=entries,
        source_versions={"crs": "fixture", "crdf": "fixture"},
        model_versions={"jev": "jev-1.14.0"},
        gates={},
        precision_gate_waived=False,
    )
    (out_dir / "release.json").write_text(manifest.model_dump_json(indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else FIXTURE_DIR
    written = write_fixture(target)
    print(f"wrote {len(written.tables)} tables to {target}")
