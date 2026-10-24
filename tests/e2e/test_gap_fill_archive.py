"""The archive build with the fill on, through assemble: the GCF and GEF IATI routes
against the prototype's 2021 to 2022 buckets, the overlap test, the bank-ledger
fill's conservation and the release's fill rows. Needs CF_E2E_DATA pointing at the
scoping `data/`."""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_raw
from normalised import record_fetch

from climate_finance.build import build
from climate_finance.build.allocate import counted_rows
from climate_finance.build.config import BuildConfig, Ledgers
from climate_finance.build.fetch import _ledger_files
from climate_finance.build.link.gap_fill import FUND_PUBLISHERS
from climate_finance.build.sources import finances_one, iati, imf
from climate_finance.build.stages import raw_dir, table_path
from climate_finance.schema import RAW_MANIFEST, RELEASE_TABLES, release_invariants

pytestmark = pytest.mark.slow

YEARS = (2021, 2022)
# Every ledger bank the archive holds files for. The World Bank snapshots run to
# 31 December 2022, so the ledgers cover 2021 and 2022.
LEDGERS = Ledgers(
    years=YEARS,
    publishers=("ibrd", "ida", "asdb", "afdb", "iadb", "isdb", "ebrd", "bcie", "gcf"),
)
BANKS = Path("banks")
# Each IATI ledger publisher's archived files, relative to CF_E2E_DATA/raw/banks.
# The IsDB's trade-finance file (ITFC) is left out of scope.
IATI_FILES = {
    "asdb": "bank-routes-a/iati/asdb-*.xml",
    "afdb": "bank-routes-a/iati/afdb-*.xml",
    "iadb": "bank-routes-a/iati/iadb-*.xml",
    "isdb": "bank-routes-b/isdb-activities-isdb-?.xml",
    "ebrd": "bank-routes-b/ebrd-activity.xml",
    "bcie": "bank-routes-b/bcie-activity.xml",
}
# The IMF's US dollar rate of the SDR, which converts the AfDB's ledger.
SDR_DIR = "imf"
# The bank-years that pass the overlap test on the archive, by DAC provider code:
# IBRD, IDA, IDB, AfDB, AfDF and GCF in both years, IsDB in 2022. ADB's ledger is
# 12% and 11% short of the CRS, EBRD's CRS carries no project number on 61% to 67%
# of its disbursement, IsDB's 2021 CRS on any, and no CABEI key matches.
PASSING = {
    (code, year) for code in (901, 905, 909, 913, 914, 1016) for year in YEARS
} | {(976, 2022)}
# USD bn of 2021 to 2022 climate value per status for the GCF and GEF rows the
# prototype's cascade leaves not traceable (238 GCF rows, USD 3.03bn; 573 GEF rows,
# USD 0.78bn), as its IATI route sets them.
TARGET_BN = {
    "gcf": {
        "disbursed": 0.0,
        "partly_disbursed": 2.2115,
        "no_disbursement_record": 0.6100,
        "not_traceable": 0.2050,
    },
    "gef": {
        "disbursed": 0.0399,
        "partly_disbursed": 0.2196,
        "no_disbursement_record": 0.1965,
        "not_traceable": 0.3245,
    },
}
# The prototype adds GEF's cumulative disbursements as published and the build reads
# them as rises of their running peak, which moves eight GEF rows below 0.9.
RUNNING_PEAK_MOVED_BN = {"disbursed": -0.0083, "partly_disbursed": 0.0083}
TOLERANCE_BN = 0.001
CONSERVATION_USD = 1e-6


@pytest.fixture(scope="module")
def release_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The archive built offline through assemble with the fill on, key repair and
    recoveries off as in the prototype's cascade, and every archived ledger."""
    archive = Path(os.environ["CF_E2E_DATA"]) / "raw"
    config = archive_config(archive, key_repair=False, recoveries=False)
    config = config.model_copy(
        update={
            "gap_fill": True,
            "sources": config.sources.model_copy(update={"ledgers": LEDGERS}),
        }
    )
    out = tmp_path_factory.mktemp("gap_fill")
    release = out / "r"
    seed_raw(release, archive=archive, config=config)
    _seed_ledgers(release, archive=archive / BANKS, config=config)
    # Slow: normalises 3.8 million CRS records and the bank ledgers, then runs every
    # stage, about two minutes.
    return build("r", config=config, out_dir=out, offline=True).release_dir


def _seed_ledgers(release: Path, *, archive: Path, config: BuildConfig) -> None:
    """Import the archived ledgers into the raw directory and its manifest, as a
    fetch with `ledgers.archive` under `config` would, each ledger file recording
    the ledger years."""
    layout = release / "ledger_archive"
    for name in finances_one.snapshot_years(range(YEARS[0], YEARS[1] + 1)):
        for dataset in ("DS00975", "DS00976"):
            snapshot = f"{dataset}_31-Dec-{name}.json"
            (layout / "finances_one").mkdir(parents=True, exist_ok=True)
            (layout / "finances_one" / snapshot).symlink_to(
                archive / "bank-routes-a" / snapshot
            )
    for publisher, pattern in IATI_FILES.items():
        (layout / "iati" / publisher).mkdir(parents=True)
        for origin in sorted(archive.glob(pattern)):
            (layout / "iati" / publisher / origin.name).symlink_to(origin)
    raw = raw_dir(release)
    files = [
        *finances_one.download_finances_one(
            raw,
            years=range(YEARS[0], YEARS[1] + 1),
            offline=True,
            archive=layout / "finances_one",
        ),
        *iati.download_iati_ledgers(
            raw, publishers=list(IATI_FILES), offline=True, archive=layout / "iati"
        ),
        *imf.download_sdr_rates(raw, offline=True, archive=archive / SDR_DIR),
    ]
    manifest_path = table_path(release, "fetch", "raw_manifest")
    ledger_files = sorted(_ledger_files(LEDGERS))
    manifest = pl.concat(
        [
            pl.read_parquet(manifest_path),
            pl.DataFrame(files, schema=RAW_MANIFEST.dtypes),
        ]
    ).with_columns(
        pl.when(pl.col("source").is_in(ledger_files))
        .then(pl.lit(year))
        .otherwise(pl.col(column))
        .cast(RAW_MANIFEST.dtypes[column])
        .alias(column)
        for column, year in zip(("first_year", "last_year"), LEDGERS.years, strict=True)
    )
    record_fetch(release, manifest, config=config)


def _tables(release_dir: Path, stage: str, *tables: str) -> list[pl.DataFrame]:
    return [pl.read_parquet(table_path(release_dir, stage, t)) for t in tables]


def test_gcf_and_gef_rows_reproduce_the_prototype_buckets(release_dir: Path) -> None:
    crdf = counted_rows(
        pl.scan_parquet(table_path(release_dir, "normalise", "crdf"))
    ).filter(
        pl.col("year").is_between(*YEARS),
        pl.col("provider_code").is_in(list(FUND_PUBLISHERS)),
    )
    before, after = (
        pl.scan_parquet(table_path(release_dir, stage, "commitments")).select(
            "row_id", "status"
        )
        for stage in ("allocate", "fill")
    )
    routed = (
        crdf.join(before.filter(status="not_traceable"), on="row_id")
        .drop("status")
        .join(after, on="row_id")
        .with_columns(
            publisher=pl.col("provider_code").replace_strict(
                FUND_PUBLISHERS, return_dtype=pl.String
            )
        )
        .collect()
    )
    totals = routed.group_by("publisher").agg(
        rows=pl.len(), usd_bn=pl.col("climate_usd").sum() / 1e9
    )
    by_status = {
        publisher: dict(
            routed.filter(publisher=publisher)
            .group_by("status")
            .agg(pl.col("climate_usd").sum() / 1e9)
            .iter_rows()
        )
        for publisher in FUND_PUBLISHERS.values()
    }

    assert dict(totals.select("publisher", "rows").iter_rows()) == {
        "gcf": 238,
        "gef": 573,
    }
    expected = TARGET_BN | {
        "gef": {
            status: usd_bn + RUNNING_PEAK_MOVED_BN.get(status, 0.0)
            for status, usd_bn in TARGET_BN["gef"].items()
        }
    }
    for publisher, statuses in expected.items():
        got = {status: by_status[publisher].get(status, 0.0) for status in statuses}
        assert got == pytest.approx(statuses, abs=TOLERANCE_BN), publisher


def test_fills_leave_crs_allocations_alone_and_conserve_every_amount(
    release_dir: Path,
) -> None:
    allocated, status = _tables(release_dir, "allocate", "allocations", "commitments")
    filled, commitments, overlap = _tables(
        release_dir, "fill", "allocations", "commitments", "bank_year_overlap"
    )
    fills = filled.filter(pl.col("disbursement_source") != "crs")

    assert filled.filter(disbursement_source="crs").equals(allocated)
    assert set(fills["disbursement_source"]) == {"iati", "bank_ledger"}
    assert (fills["rule"] == fills["disbursement_source"]).all()
    # Every ledger fill sits in a bank-year that passes the overlap test.
    ledger_fills = (
        fills.filter(disbursement_source="bank_ledger")
        .join(status.select("row_id"), on="row_id")
        .join(
            counted_rows(pl.scan_parquet(table_path(release_dir, "normalise", "crdf")))
            .select("row_id", "provider_code")
            .collect(),
            on="row_id",
        )
        .join(
            overlap.select("provider_code", "year", "passed"),
            on=["provider_code", "year"],
            how="left",
        )
    )
    assert ledger_fills.height > 0
    assert ledger_fills["passed"].fill_null(False).all()
    # Each commitment's disbursed amount is its allocations, within its face value.
    sums = commitments.join(
        filled.filter(pl.col("row_id").is_not_null(), pl.col("rule") != "excess")
        .group_by("row_id")
        .agg(allocated=pl.col("allocated_usd").sum()),
        on="row_id",
        how="left",
    ).with_columns(pl.col("allocated").fill_null(0.0))
    gap = (sums["disbursed_usd"] - sums["allocated"]).abs()
    assert gap.max() < CONSERVATION_USD
    over = sums.filter(pl.col("face_usd") > 0).select(
        pl.col("disbursed_usd") - pl.col("face_usd")
    )
    assert over.to_series().max() < CONSERVATION_USD
    # The climate value disbursed plus the climate share of what is not equals the
    # climate value of every commitment.
    share = pl.col("climate_usd") / pl.col("face_usd")
    identity = sums.filter(pl.col("face_usd") > 0).select(
        (
            pl.col("disbursed_usd") * share
            + (pl.col("face_usd") - pl.col("disbursed_usd")) * share
            - pl.col("climate_usd")
        ).abs()
    )
    assert identity.to_series().max() < CONSERVATION_USD


def test_bank_years_pass_the_overlap_test_where_ledger_and_crs_agree(
    release_dir: Path,
) -> None:
    (overlap,) = _tables(release_dir, "fill", "bank_year_overlap")

    passed = set(overlap.filter("passed").select("provider_code", "year").iter_rows())

    assert passed == PASSING


def test_the_release_carries_each_fill_with_its_commitment_facts(
    release_dir: Path,
) -> None:
    tables = {
        name: pl.read_parquet(release_dir / "assemble" / f"{name}.parquet")
        for name in RELEASE_TABLES
    }
    disbursements = tables["disbursements"]
    fills = disbursements.filter(pl.col("disbursement_source") != "crs")
    (filled,) = _tables(release_dir, "fill", "allocations")
    expected = filled.filter(
        pl.col("disbursement_source") != "crs", pl.col("row_id").is_not_null()
    )

    assert release_invariants(tables) == []
    assert fills.height == expected.height > 0
    assert fills["allocated_usd"].sum() == pytest.approx(
        expected["allocated_usd"].sum(), abs=CONSERVATION_USD
    )
    assert fills["link_method"].is_null().all()
    assert set(fills["marker_source"]) <= {"commitment", "components"}
