"""Fetch through allocate on the archived source files, with recoveries off.

The fetch is offline: the archive files take their places in the raw layout, with a
`raw_manifest` that records them. One build runs with key repair off and pins the
2021 to 2022 statuses; a second reuses its fetch and normalise tables, runs link and
allocate with key repair on, and pins the no-record value the rules move.
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from climate_finance.build import Stage, build, write_table
from climate_finance.build.config import BuildConfig, LocalFile, load_config
from climate_finance.build.fetch import (
    CRDF_FILES,
    CRDF_SPAN_FILE,
    CRS_FILE,
    DAC_EXCHANGE_FILE,
    GCF_FILE,
    IATI_DIR,
    IMPUTED_SHARES_FILE,
    MULTISYSTEM_FILE,
)
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.stages import raw_dir, table_path
from climate_finance.schema import KEY_REPAIR_METHODS, RAW_MANIFEST, sha256_file

pytestmark = pytest.mark.slow

BUILD_TOML = Path(__file__).parents[2] / "build.toml"
CRS_ARCHIVE = Path("oecd/CRS_v20261006.parquet")
CRS_SHA256 = "d24840a5fad50e718da4eb5831c3e1e333e7c5ef6b46c158e3510c7497518f06"
YEARS = (2021, 2022)
# USD bn of 2021 to 2022 climate value per status.
TOLERANCE_BN = 0.1
# The reference leaves a repeat row's allocation on the repeat; the build moves it
# to the first row, and the 2021 to 2022 first rows of EIB repeats leave not
# traceable.
FIRST_ROWS_MOVED_BN = {
    "disbursed": 0.18,
    "partly_disbursed": 0.76,
    "no_disbursement_record": 0.0,
    "not_traceable": -0.94,
}
# The reference keys World Bank disbursements on their crs ids, which name projects;
# the build's `wb_loan` tier keys them on their loan or credit numbers.
LOAN_TIER_MOVED_BN = {
    "disbursed": 1.60,
    "partly_disbursed": 1.18,
    "no_disbursement_record": -2.78,
    "not_traceable": 0.0,
}
# The reference links by key alone. The truth-group classifier (`link.score`) links
# disbursements the keys left to commitments, which moves value out of no record,
# under the reference's counting and under the release rule.
CLASSIFIER_MOVED_BN = {
    "disbursed": 0.17,
    "partly_disbursed": -0.02,
    "no_disbursement_record": -0.15,
    "not_traceable": 0.0,
}
CLASSIFIER_RELEASE_MOVED_BN = {
    "disbursed": 0.17,
    "partly_disbursed": -0.02,
    "no_disbursement_record": -0.15,
    "not_traceable": 0.0,
}
# With aggregate children counted and their parents left out (the release rule).
RELEASE_RULE_BN = {
    "disbursed": 56.37,
    "partly_disbursed": 106.01,
    "no_disbursement_record": 32.07,
    "not_traceable": 35.12,
}
# No-record value the key-repair rules move to partly or fully disbursed, counted
# as the reference counts, by provider category (USD bn). The `wb_loan` tier already
# reaches the World Bank commitments the rules would, so the rules move no IBRD or
# IDA value. Both builds carry classifier links. In the build without the rules, the
# classifier already lifts USD 0.097 bn of bilateral rows out of no record through
# commitments the rules reach, so the rules move that much less.
REPAIR_MOVED_BN = {"bilateral": 0.87, "banks": 2.02}
REPAIR_TOLERANCE_BN = 0.05
_CATEGORY = {
    "DAC member": "bilateral",
    "Non-DAC member": "bilateral",
    "Multilateral development bank": "banks",
    "Other multilateral": "other_multilateral",
    "Private donor": "private",
}


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def config(data: Path) -> BuildConfig:
    config = load_config(BUILD_TOML)
    sources = config.sources.model_copy(
        update={
            "crs_file": LocalFile(path=data / "raw" / CRS_ARCHIVE, sha256=CRS_SHA256),
            "iati_archive": data / "raw" / "external" / "iati",
        }
    )
    return config.model_copy(
        update={"key_repair": False, "recoveries": False, "sources": sources}
    )


def _lay_out_raw(release_dir: Path, *, archive: Path, config: BuildConfig) -> None:
    """Link the archive files into the raw layout and record them as a fetch would."""
    places = {
        "crs": [(CRS_ARCHIVE, CRS_FILE)],
        **{
            f"crdf_{perspective}": [(path, path)]
            for perspective, path in CRDF_FILES.items()
        },
        "crdf_span": [(CRDF_SPAN_FILE, CRDF_SPAN_FILE)],
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
    raw = raw_dir(release_dir)
    rows = []
    for source, files in places.items():
        for origin, relative in files:
            target = raw / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(archive / origin)
            rows.append(
                {
                    "source": source,
                    "path": relative.as_posix(),
                    "sha256": CRS_SHA256 if source == "crs" else sha256_file(target),
                    "version": config.sources.crs_version if source == "crs" else None,
                    "fetched_at": datetime(2026, 10, 21, tzinfo=UTC),
                    "method": "manual",
                }
            )
    manifest = pl.DataFrame(rows).with_columns(
        url=pl.lit(None, pl.String), etag=pl.lit(None, pl.String)
    )
    write_table(
        manifest, RAW_MANIFEST, table_path(release_dir, "fetch", RAW_MANIFEST.name)
    )


@pytest.fixture(scope="module")
def base(
    tmp_path_factory: pytest.TempPathFactory, data: Path, config: BuildConfig
) -> Path:
    # Slow: normalises 3.8 million CRS records and links and allocates them, about
    # a minute.
    out = tmp_path_factory.mktemp("allocate")
    _lay_out_raw(out / "base", archive=data / "raw", config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK, Stage.ALLOCATE]
    manifest = build("base", config=config, out_dir=out, stages=stages, offline=True)
    return manifest.release_dir


@pytest.fixture(scope="module")
def repaired(base: Path, config: BuildConfig) -> Path:
    release_dir = base.with_name("repaired")
    for stage in ("raw", "fetch", "normalise"):
        (release_dir / stage).parent.mkdir(parents=True, exist_ok=True)
        (release_dir / stage).symlink_to(base / stage, target_is_directory=True)
    build(
        "repaired",
        config=config.model_copy(update={"key_repair": True}),
        out_dir=base.parent,
        stages=[Stage.LINK, Stage.ALLOCATE],
        offline=True,
    )
    return release_dir


def _rows(release_dir: Path) -> pl.DataFrame:
    """2021 to 2022 recipient-perspective CRDF rows with their status; an aggregate
    parent left out of allocation has none."""
    crdf = pl.read_parquet(table_path(release_dir, "normalise", "crdf"))
    status = pl.read_parquet(table_path(release_dir, "allocate", "commitments"))
    return crdf.filter(
        pl.col("year").is_between(*YEARS), pl.col("perspective") == "recipient"
    ).join(status.select("row_id", "status"), on="row_id", how="left")


def _prototype_count(rows: pl.DataFrame) -> pl.DataFrame:
    """Rows as the reference counts them: aggregate children out, their parents in,
    and a parent not traceable, as it has no crs id."""
    return rows.filter(pl.col("parent_row_id").is_null()).with_columns(
        pl.col("status").fill_null("not_traceable")
    )


def _by_status(rows: pl.DataFrame) -> dict[str, float]:
    totals = rows.group_by("status").agg(pl.col("climate_usd").sum() / 1e9)
    return dict(totals.iter_rows())


def test_statuses_reproduce_the_reference_without_recoveries(
    data: Path, base: Path
) -> None:
    reference = pl.read_csv(
        data / "prototypes" / "a1-reference" / "a1_reference.csv",
        schema_overrides={"scope": pl.String},
    ).filter(
        pl.col("scope") == "2021-2022",
        pl.col("config") == "b_pnunique",
        pl.col("provider_type") == "all",
    )
    built = _by_status(_prototype_count(_rows(base)))
    assert set(built) == set(reference["status"])
    expected = {
        status: usd_bn
        + FIRST_ROWS_MOVED_BN[status]
        + LOAN_TIER_MOVED_BN[status]
        + CLASSIFIER_MOVED_BN[status]
        for status, usd_bn in reference.select("status", "usd_bn").iter_rows()
    }
    assert built == pytest.approx(expected, abs=TOLERANCE_BN)


def test_statuses_under_the_release_rule(base: Path) -> None:
    rows = _rows(base)
    # The parents that children stand in for are the rows without a status.
    assert (
        rows.filter(pl.col("status").is_null())["row_id"]
        .is_in(rows["parent_row_id"].drop_nulls().implode())
        .all()
    )
    built = _by_status(rows.drop_nulls("status"))
    expected = {
        status: usd_bn + CLASSIFIER_RELEASE_MOVED_BN[status]
        for status, usd_bn in RELEASE_RULE_BN.items()
    }
    assert built == pytest.approx(expected, abs=TOLERANCE_BN)


@pytest.mark.parametrize("build_name", ["base", "repaired"])
def test_linked_disbursement_is_conserved(
    build_name: str, request: pytest.FixtureRequest
) -> None:
    release_dir: Path = request.getfixturevalue(build_name)
    units = pl.scan_parquet(table_path(release_dir, "link", "units"))
    links = pl.scan_parquet(table_path(release_dir, "link", "links"))
    crs = trace_population(pl.scan_parquet(table_path(release_dir, "normalise", "crs")))
    allocations = pl.scan_parquet(table_path(release_dir, "allocate", "allocations"))
    linked = (
        units.join(
            links.filter(task="trace").select(unit_id="left_id").unique(),
            on="unit_id",
        )
        .select(record_id="record_ids")
        .explode("record_id")
        .join(crs.select("record_id", "disbursement_usd"), on="record_id")
        .filter(pl.col("disbursement_usd") > 0)
    )
    given = allocations.group_by("record_id").agg(pl.col("allocated_usd").sum())
    gap = (
        linked.join(given, on="record_id", how="left")
        .select(
            (pl.col("disbursement_usd") - pl.col("allocated_usd").fill_null(0.0))
            .abs()
            .max()
        )
        .collect()
        .item()
    )
    assert gap <= 1e-6
    assert allocations.join(linked, on="record_id", how="anti").collect().is_empty()


def test_key_repair_moves_no_record_value(
    data: Path, base: Path, repaired: Path
) -> None:
    categories = (
        pl.scan_parquet(data / "raw" / "oecd" / "crdf_recipient.parquet")
        .select("provider_code", "agency_code", "provider_type")
        .unique()
        .collect()
        .select(
            "provider_code",
            "agency_code",
            category=pl.col("provider_type").replace_strict(_CATEGORY),
        )
    )
    before = _prototype_count(_rows(base))
    after = _prototype_count(_rows(repaired)).select("row_id", status_after="status")
    moved = (
        before.filter(pl.col("status") == "no_disbursement_record")
        .join(after, on="row_id")
        .filter(pl.col("status_after") != "no_disbursement_record")
        .join(
            categories,
            on=["provider_code", "agency_code"],
            how="left",
            nulls_equal=True,
        )
    )
    assert set(moved["status_after"]) == {"disbursed", "partly_disbursed"}
    by_category = dict(
        moved.group_by("category").agg(pl.col("climate_usd").sum() / 1e9).iter_rows()
    )
    for category, usd_bn in REPAIR_MOVED_BN.items():
        assert by_category[category] == pytest.approx(
            usd_bn, abs=REPAIR_TOLERANCE_BN
        ), category

    links = pl.read_parquet(table_path(repaired, "link", "links"))
    assert set(KEY_REPAIR_METHODS) <= set(links["link_method"])


def test_units_rebuild_to_the_same_bits(base: Path, repaired: Path) -> None:
    # The two builds form units from one normalise output, in separate runs; a
    # group sum that adds in thread order changes the last bit of some amounts.
    assert sha256_file(table_path(base, "link", "units")) == sha256_file(
        table_path(repaired, "link", "units")
    )
