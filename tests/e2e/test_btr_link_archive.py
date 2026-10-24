"""The BTR-to-CRS link over the archived BTR1 workbooks and the bulk CRS, and the
reconciliation of party totals on the archive release. Needs CF_E2E_DATA pointing at
the scoping `data/`."""

import os
import shutil
from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build import release as assemble
from climate_finance.build.fetch import DAC_EXCHANGE_FILE
from climate_finance.build.link.btr import (
    LINK_PROBABILITY,
    btr_rows,
    calibrate,
    crs_records,
    link_btr,
    link_by_ids,
    linked_pairs,
    score_party,
)
from climate_finance.build.normalise import btr_at_dac_rates
from climate_finance.build.normalise.crs import normalise_crs
from climate_finance.build.readers.btr import estimate_party_weights, ingest_unfccc
from climate_finance.result import Reconciliation
from climate_finance.snapshot import Snapshot

pytestmark = pytest.mark.slow

# The prototype's party weights, by DAC provider code: 0.4 for the EU, Norway,
# Sweden, the Netherlands and Switzerland, 0.5 for Germany.
PROTOTYPE_WEIGHTS = {918: 0.4, 8: 0.4, 10: 0.4, 7: 0.4, 11: 0.4, 5: 0.5}


@pytest.fixture(scope="module")
def data() -> Path:
    return Path(os.environ["CF_E2E_DATA"])


@pytest.fixture(scope="module")
def btr(data: Path, tmp_path_factory: pytest.TempPathFactory) -> pl.LazyFrame:
    out = tmp_path_factory.mktemp("btr") / "btr.parquet"
    return pl.scan_parquet(ingest_unfccc(data / "raw" / "unfccc_btr1", out_path=out))


@pytest.fixture(scope="module")
def crs(data: Path) -> pl.LazyFrame:
    raw = pl.scan_parquet(data / "raw" / "oecd" / "CRS_v20261006.parquet")
    return normalise_crs(raw, years=(2021, 2022)).collect().lazy()


@pytest.fixture(scope="module")
def links(btr: pl.LazyFrame, crs: pl.LazyFrame) -> pl.DataFrame:
    # Slow: fits a Splink model for each of the 28 parties with CRS records.
    return link_btr(btr, crs=crs)


def test_uk_programme_numbers_find_their_programme_years(
    btr: pl.LazyFrame, crs: pl.LazyFrame
) -> None:
    rows = btr_rows(btr.filter(party="GBR"))
    by_id = link_by_ids(rows, records=crs_records(crs, rows=rows))
    programmes = rows.with_columns(
        programme=pl.col("title").str.extract(r"^\s*(\d{6})")
    ).drop_nulls("programme")
    found = programmes.join(by_id, on="row_id", how="semi")

    years = programmes.select("programme", "year").unique().height
    found_years = found.select("programme", "year").unique().height
    # Six of the seven not found have CRS records only in the other year. The
    # seventh, 300137 in 2022, has one record with no amount, which `normalise_crs`
    # drops. The prototype's join on the raw CRS finds it, giving 285.
    assert (years, found_years) == (291, 284)


def test_norway_model_agrees_with_its_ids(btr: pl.LazyFrame, crs: pl.LazyFrame) -> None:
    rows = btr_rows(btr.filter(party="NOR"))
    records = crs_records(crs, rows=rows)
    truth = link_by_ids(rows, records=records)
    best = calibrate(score_party(rows, records=records), truth=truth)
    checked = best.join(
        truth.with_columns(correct=pl.lit(value=True)),
        on=["row_id", "record_id"],
        how="left",
    ).join(truth, on="row_id", how="semi")
    linked = checked.filter(pl.col("probability") >= LINK_PROBABILITY)

    # 69 of the 1,234 are rows whose recipient is "Global", which keys as an
    # aggregate and so partners any CRS recipient; all 69 agree with the ids.
    assert linked.height == 1234
    assert linked["correct"].fill_null(value=False).mean() == pytest.approx(
        0.983, abs=0.001
    )


def test_links_cover_the_reported_dollars(
    btr: pl.LazyFrame, links: pl.DataFrame
) -> None:
    rows = btr.select("row_id", "party", "face_usd").collect()
    linked = rows.join(links.select(row_id="left_id").unique(), on="row_id", how="semi")
    share = {
        party: linked.filter(party=party)["face_usd"].sum()
        / rows.filter(party=party)["face_usd"].sum()
        for party in ("NOR", "DEU", "GBR")
    }

    # "Global" rows hold 30% of Norway's dollars and 69% of the UK's, and key as
    # aggregates. Germany's model links 0.528 of its dollars: its calibration map
    # puts 230 rows under the threshold, and labels at their weights are right 30% to
    # 50% of the time.
    assert linked["face_usd"].sum() / rows["face_usd"].sum() == pytest.approx(
        0.782, abs=0.001
    )
    assert share == pytest.approx({"NOR": 1.0, "DEU": 0.727, "GBR": 0.910}, abs=0.001)


def test_linked_pairs_give_germany_half_and_norway_forty_percent(
    btr: pl.LazyFrame, crs: pl.LazyFrame, links: pl.DataFrame
) -> None:
    weights = {
        row["party"]: row
        for row in estimate_party_weights(
            linked_pairs(links, btr=btr, crs=crs)
        ).to_dicts()
    }

    assert weights["DEU"]["significant"] == 0.5
    assert weights["NOR"]["significant"] == 0.4
    assert weights["EUU"]["significant"] == 0.4
    assert weights["SWE"]["significant"] == 0.4
    assert weights["NLD"]["significant"] == 0.4
    assert weights["NOR"]["principal_at_100"] == pytest.approx(0.996, abs=0.001)


@pytest.fixture(scope="module")
def reconciliation(
    release: Path,
    btr: pl.LazyFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    tmp_path_factory: pytest.TempPathFactory,
) -> Reconciliation:
    """The archive release with the BTR tables and the prototype's weights."""
    out = tmp_path_factory.mktemp("reconcile")
    for name in (*schema.RELEASE_TABLES, "release"):
        suffix = ".json" if name == "release" else ".parquet"
        if (release / f"{name}{suffix}").exists():
            shutil.copyfile(release / f"{name}{suffix}", out / f"{name}{suffix}")
    rows = btr_at_dac_rates(
        btr.collect(),
        exchange=pl.read_parquet(release.parent / "raw" / DAC_EXCHANGE_FILE),
    )
    rows.write_parquet(out / "btr.parquet")
    btr_links = assemble.btr_links(links.lazy(), crs=crs).collect()
    btr_links.write_parquet(out / "btr_links.parquet")
    assemble.btr_totals(rows.lazy(), links=btr_links.lazy()).collect().write_parquet(
        out / "btr_totals.parquet"
    )
    assemble.btr_crs_unlinked(
        crs, btr=rows.lazy(), links=links.lazy()
    ).collect().write_parquet(out / "btr_crs_unlinked.parquet")
    # The CRS of 2021 and 2022, so the years compared.
    assemble.total_flows(crs).collect().write_parquet(out / "total_flows.parquet")
    pl.DataFrame(
        {
            "provider_code": list(PROTOTYPE_WEIGHTS),
            "agency_code": None,
            "first_year": None,
            "last_year": None,
            "significant": list(PROTOTYPE_WEIGHTS.values()),
            "principal": 1.0,
            "both_significant": None,
            "case_by_case": False,
            "source": "prototype",
            "locator": "weights",
        },
        schema={
            name: column.dtype
            for name, column in schema.PROVIDER_COEFFICIENTS.columns.items()
        },
    ).write_parquet(out / "provider_coefficients.parquet")
    manifest = schema.ReleaseManifest.model_validate_json(
        (out / "release.json").read_text()
    )
    return Snapshot(path=out, manifest=manifest).reconcile(years=(2021, 2022))


@pytest.mark.parametrize(
    ("party", "year", "comparator", "ratio"),
    [
        # BTR amounts at the DAC rate; at the parties' own rates the EU reads 1.0032
        # and 1.0103, the Netherlands 1.0059, Switzerland 1.0118 and Germany 0.9385.
        ("EUU", 2021, "crdf_at_party_weight", 1.0025),
        ("EUU", 2022, "crdf_at_party_weight", 1.0093),
        ("NOR", 2021, "crs_disbursed_at_party_weight", 1.0028),
        ("SWE", 2021, "crs_disbursed_at_party_weight", 1.0016),
        ("NLD", 2021, "crs_disbursed_at_party_weight", 1.0056),
        ("CHE", 2022, "crs_disbursed_at_party_weight", 1.0119),
        # 6.2% under, which the prototype's report rounds to 0.94. Its CRDF at 100%
        # is the same USD 12,411m.
        ("DEU", 2022, "crdf_at_party_weight", 0.9376),
    ],
)
def test_party_totals_agree_at_the_prototype_weights(
    reconciliation: Reconciliation,
    party: str,
    year: int,
    comparator: str,
    ratio: float,
) -> None:
    rows = reconciliation.table.to_polars().filter(
        party=party, year=year, method="ONE", comparator=comparator
    )

    assert rows.height == 1
    assert rows["btr_over_comparator"].item() == pytest.approx(ratio, abs=0.0005)
