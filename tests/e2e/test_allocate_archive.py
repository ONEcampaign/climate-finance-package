"""Fetch through allocate on the archived source files, with recoveries off.

The fetch is offline. The archive files take their places in the raw layout, with a
`raw_manifest` that records them. One build runs with key repair off and pins the
2021 to 2022 statuses; a second reuses its fetch and normalise tables, runs link and
allocate with key repair on, and pins the no-record value the rules move.
"""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config, seed_raw

from climate_finance.build import Stage, build
from climate_finance.build.config import BuildConfig
from climate_finance.build.normalise.crs import trace_population
from climate_finance.build.stages import table_path
from climate_finance.schema import KEY_REPAIR_METHODS, sha256_file

pytestmark = pytest.mark.slow

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
# The build's key tiers link more than the reference's: the project-number tiers
# run for every agency, the crs-id tiers key an id that names several projects or
# countries by recipient (`crs_key`) and compare unpadded line suffixes
# (`canonical_crs_id`), and the project-number tiers compare numbers without `-` and
# `_` (`pn_key`). That moves value from partly disbursed to disbursed, under the
# reference's counting and under the release rule.
KEY_TIERS_MOVED_BN = {
    "disbursed": 0.30,
    "partly_disbursed": -0.28,
    "no_disbursement_record": -0.02,
    "not_traceable": 0.0,
}
KEY_TIERS_RELEASE_MOVED_BN = {
    "disbursed": 0.30,
    "partly_disbursed": -0.30,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
# Each project-number tier asks for one crs id over its own key (`Tier.unique_by`),
# which links US awards and other donors' lines out of no record. The rows it moves
# count the same under both rules.
UNIQUE_BY_MOVED_BN = {
    "disbursed": 0.14,
    "partly_disbursed": 0.17,
    "no_disbursement_record": -0.31,
    "not_traceable": 0.0,
}
# `pn_recipient_date` puts German disbursements on the line whose commitment date
# they repeat.
DATE_TIER_MOVED_BN = {
    "disbursed": 0.12,
    "partly_disbursed": -0.08,
    "no_disbursement_record": -0.04,
    "not_traceable": 0.0,
}
# The reference links by key alone. The truth-group classifier (`link.score`) links
# disbursements the keys left to commitments, which moves value out of no record.
CLASSIFIER_MOVED_BN = {
    "disbursed": 0.03,
    "partly_disbursed": 0.11,
    "no_disbursement_record": -0.15,
    "not_traceable": 0.0,
}
CLASSIFIER_RELEASE_MOVED_BN = {
    "disbursed": 0.03,
    "partly_disbursed": 0.12,
    "no_disbursement_record": -0.15,
    "not_traceable": 0.0,
}
# A record's own commitment fills before older commitments of its key group
# (`_own_commitments`), so where a donor files a new module or yearly grant under a
# reused crs id, the new record's money stays out of the older module's headroom.
# Mostly Dutch and British rows move from disbursed to partly disbursed, and own
# commitments with no record take their records' money.
OWN_FIRST_MOVED_BN = {
    "disbursed": -0.50,
    "partly_disbursed": 0.70,
    "no_disbursement_record": -0.21,
    "not_traceable": 0.0,
}
OWN_FIRST_RELEASE_MOVED_BN = {
    "disbursed": -0.50,
    "partly_disbursed": 0.63,
    "no_disbursement_record": -0.14,
    "not_traceable": 0.0,
}
# The figures above read CRDF 2015 to 2024 and CRS 2010 to 2025; each change below
# is measured with the changes above it in place, in USD bn to four places. The
# truth-group classifier labels pairs from `FIRST_MEASURED_YEAR` and reads each
# commitment's sibling lines (`sibling_lines`), so it links less disbursement into
# 2021 to 2022 rows.
CLASSIFIER_SIBLINGS_MOVED_BN = {
    "disbursed": -0.0217,
    "partly_disbursed": -0.0586,
    "no_disbursement_record": 0.0802,
    "not_traceable": 0.0,
}
CLASSIFIER_SIBLINGS_RELEASE_MOVED_BN = {
    "disbursed": -0.0216,
    "partly_disbursed": -0.0669,
    "no_disbursement_record": 0.0885,
    "not_traceable": 0.0,
}
# The build reads every year the sources hold: CRDF and CRS from 2000.
EVERY_YEAR_MOVED_BN = {
    "disbursed": 0.0001,
    "partly_disbursed": -0.0081,
    "no_disbursement_record": 0.0080,
    "not_traceable": 0.0,
}
# Allocation reads CRS records up to `crs_last_complete`, so the early 2025 reports
# no longer complete some 2021 to 2022 commitments.
LAST_COMPLETE_YEAR_MOVED_BN = {
    "disbursed": -0.1516,
    "partly_disbursed": 0.1433,
    "no_disbursement_record": 0.0084,
    "not_traceable": 0.0,
}
# An agency keying under `MIN_KEYED_DISBURSED` of its disbursement is not traceable,
# mostly the EBRD's and IFAD's rows.
DISBURSEMENT_SIDE_MOVED_BN = {
    "disbursed": 0.0,
    "partly_disbursed": 0.0,
    "no_disbursement_record": -5.6023,
    "not_traceable": 5.6023,
}
# No row receives a record from before its CRDF year, which moves only the release
# rule.
TIME_ORDER_MOVED_BN = {
    "disbursed": -0.0011,
    "partly_disbursed": 0.0011,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
# Only an EU contract passes its floor in the parent tier, in allocation and in the
# keys (`parent_floor_year`), and `crs_key` counts a missing project number as one
# more project. Together they move only the release rule.
PARENT_FLOOR_RELEASE_MOVED_BN = {
    "disbursed": 0.0,
    "partly_disbursed": -0.0024,
    "no_disbursement_record": 0.0024,
    "not_traceable": 0.0,
}
# A lettered record reaches its parent only in its recipient under its project
# number (`_parent_key`).
LETTERED_PARENT_MOVED_BN = {
    "disbursed": 0.0015,
    "partly_disbursed": -0.0068,
    "no_disbursement_record": 0.0053,
    "not_traceable": 0.0,
}
# A component award keys each component by purpose (`split_components`). The
# purpose tier's `unique_by` already reaches each component, so the split only stops
# links across components. The floor exemption under a disbursement's own crs id
# (`same_crs_id`) moves no status.
COMPONENT_AWARDS_MOVED_BN = {
    "disbursed": -0.0011,
    "partly_disbursed": -0.0020,
    "no_disbursement_record": 0.0031,
    "not_traceable": 0.0,
}
# The classifier links at the fixed cut of 0.40, below the best threshold of its
# training F1 in this build (0.43), which moves a little value from partly disbursed
# to disbursed.
FIXED_CUT_MOVED_BN = {
    "disbursed": 0.0003,
    "partly_disbursed": -0.0003,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
FIXED_CUT_RELEASE_MOVED_BN = {
    "disbursed": 0.0004,
    "partly_disbursed": -0.0004,
    "no_disbursement_record": 0.0,
    "not_traceable": 0.0,
}
# `sibling_lines` counts a commitment's sibling lines over every link-stage unit,
# which changes the classifier's links.
SIBLING_LINES_MOVED_BN = {
    "disbursed": -0.0081,
    "partly_disbursed": 0.0166,
    "no_disbursement_record": -0.0086,
    "not_traceable": 0.0,
}
SIBLING_LINES_RELEASE_MOVED_BN = {
    "disbursed": -0.0081,
    "partly_disbursed": 0.0166,
    "no_disbursement_record": -0.0085,
    "not_traceable": 0.0,
}
# An IBRD or IDA record fills the commitment on its own sector line before the
# loan's other lines (`_own_commitments`). The rows it moves count the same under
# both rules.
WORLD_BANK_OWN_LINE_MOVED_BN = {
    "disbursed": 0.1526,
    "partly_disbursed": -0.1803,
    "no_disbursement_record": 0.0277,
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
# classifier already lifts USD 0.08 bn of bilateral rows out of no record through
# commitments the rules reach, so the rules move that much less. A number committed
# in several countries links within the disbursement's recipient and purpose
# (`_programmes`); on the number alone a disbursement would reach more than
# `MAX_COMMITMENTS` commitments and link to none. US numbers of that kind carry
# USD 0.40 bn of the bilateral figure; the purpose tier's own guard
# (`Tier.unique_by`) already links USD 0.20 bn more US value in the build without
# the rules. A unit that disbursed no more than its own commitment is no
# disbursement unit to the rules, and a line's spare commitment (`_spare_lines`)
# joins the commitments a number reaches, so some US numbers reach more than
# `MAX_COMMITMENTS`. Between them the two take USD 0.07 bn of US rows out of the
# figure. Rounded AfDB project numbers are null, so `repair_title_recipient` also
# takes AfDB commitments that the rounded numbers reach, which carry USD 0.18 bn of
# the banks figure. A rounded AfDB number that also fits a spare line stands for two
# full numbers, so `repair_afdb_scientific` links it to none (USD 0.05 bn).
# `repair_stray_country` moves USD 0.01 bn of Canadian and Danish rows.
REPAIR_MOVED_BN = {"bilateral": 0.85, "banks": 2.12}
# The classifier links less into 2021 to 2022 rows, so the rules lift more bilateral
# value out of no record, and so do the CRDF and CRS years from 2000.
REPAIR_CLASSIFIER_SIBLINGS_BN = {"bilateral": 0.0324, "banks": 0.0}
REPAIR_EVERY_YEAR_BN = {"bilateral": 0.0091, "banks": 0.0}
# The EBRD's no-record rows become not traceable, leaving the rules less to move.
REPAIR_DISBURSEMENT_SIDE_BN = {"bilateral": 0.0, "banks": -0.3130}
# Key repair compares awards too (`split_components`), so its rules link within one
# component where the award was ambiguous across them.
REPAIR_COMPONENT_AWARDS_BN = {"bilateral": 0.1491, "banks": 0.0}
# With `sibling_lines` counted over every link-stage unit, the build without the
# rules holds less in no record, and the rules lift less bilateral value out of it.
REPAIR_SIBLING_LINES_BN = {"bilateral": -0.0085, "banks": 0.0}
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
    return archive_config(data / "raw", key_repair=False, recoveries=False)


@pytest.fixture(scope="module")
def base(
    tmp_path_factory: pytest.TempPathFactory, data: Path, config: BuildConfig
) -> Path:
    # Slow: normalises 5.1 million CRS records and links and allocates them.
    out = tmp_path_factory.mktemp("allocate")
    seed_raw(out / "base", archive=data / "raw", config=config)
    stages = [Stage.FETCH, Stage.NORMALISE, Stage.LINK, Stage.ALLOCATE]
    manifest = build("base", config=config, out_dir=out, stages=stages, offline=True)
    return manifest.release_dir


@pytest.fixture(scope="module")
def repaired(base: Path, config: BuildConfig) -> Path:
    release_dir = base.with_name("repaired")
    for stage in ("fetch", "normalise"):
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
    """2021 to 2022 recipient-perspective CRDF rows with their status. An aggregate
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
        + KEY_TIERS_MOVED_BN[status]
        + UNIQUE_BY_MOVED_BN[status]
        + DATE_TIER_MOVED_BN[status]
        + CLASSIFIER_MOVED_BN[status]
        + OWN_FIRST_MOVED_BN[status]
        + CLASSIFIER_SIBLINGS_MOVED_BN[status]
        + EVERY_YEAR_MOVED_BN[status]
        + LAST_COMPLETE_YEAR_MOVED_BN[status]
        + DISBURSEMENT_SIDE_MOVED_BN[status]
        + LETTERED_PARENT_MOVED_BN[status]
        + COMPONENT_AWARDS_MOVED_BN[status]
        + FIXED_CUT_MOVED_BN[status]
        + SIBLING_LINES_MOVED_BN[status]
        + WORLD_BANK_OWN_LINE_MOVED_BN[status]
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
        status: usd_bn
        + KEY_TIERS_RELEASE_MOVED_BN[status]
        + UNIQUE_BY_MOVED_BN[status]
        + DATE_TIER_MOVED_BN[status]
        + CLASSIFIER_RELEASE_MOVED_BN[status]
        + OWN_FIRST_RELEASE_MOVED_BN[status]
        + CLASSIFIER_SIBLINGS_RELEASE_MOVED_BN[status]
        + EVERY_YEAR_MOVED_BN[status]
        + LAST_COMPLETE_YEAR_MOVED_BN[status]
        + DISBURSEMENT_SIDE_MOVED_BN[status]
        + TIME_ORDER_MOVED_BN[status]
        + PARENT_FLOOR_RELEASE_MOVED_BN[status]
        + LETTERED_PARENT_MOVED_BN[status]
        + COMPONENT_AWARDS_MOVED_BN[status]
        + FIXED_CUT_RELEASE_MOVED_BN[status]
        + SIBLING_LINES_RELEASE_MOVED_BN[status]
        + WORLD_BANK_OWN_LINE_MOVED_BN[status]
        for status, usd_bn in RELEASE_RULE_BN.items()
    }
    assert built == pytest.approx(expected, abs=TOLERANCE_BN)


@pytest.mark.parametrize("build_name", ["base", "repaired"])
def test_linked_disbursement_is_conserved(
    build_name: str, request: pytest.FixtureRequest, config: BuildConfig
) -> None:
    release_dir: Path = request.getfixturevalue(build_name)
    units = pl.scan_parquet(table_path(release_dir, "link", "units"))
    links = pl.scan_parquet(table_path(release_dir, "link", "links"))
    # Allocation reads CRS records up to the last complete year, as `run_allocate`
    # does; later records are linked but give nothing.
    crs = trace_population(
        pl.scan_parquet(table_path(release_dir, "normalise", "crs"))
    ).filter(pl.col("year") <= config.years.crs_last_complete)
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
        moved_bn = (
            usd_bn
            + REPAIR_CLASSIFIER_SIBLINGS_BN[category]
            + REPAIR_EVERY_YEAR_BN[category]
            + REPAIR_DISBURSEMENT_SIDE_BN[category]
            + REPAIR_COMPONENT_AWARDS_BN[category]
            + REPAIR_SIBLING_LINES_BN[category]
        )
        assert by_category[category] == pytest.approx(
            moved_bn, abs=REPAIR_TOLERANCE_BN
        ), category

    links = pl.read_parquet(table_path(repaired, "link", "links"))
    assert set(KEY_REPAIR_METHODS) <= set(links["link_method"])


def test_units_rebuild_to_the_same_bits(base: Path, repaired: Path) -> None:
    # The two builds form units from one normalise output, in separate runs. A
    # group sum that adds in thread order changes the last bit of some amounts.
    assert sha256_file(table_path(base, "link", "units")) == sha256_file(
        table_path(repaired, "link", "units")
    )
