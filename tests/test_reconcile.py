"""`Snapshot.reconcile()` on the fixture release: BTR totals against OECD figures."""

from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.methods import OECD, ONE, MethodRange
from climate_finance.result import Reconciliation

SNAP = cf.load(path=FIXTURE_DIR)


def _row(reconciliation: Reconciliation, party: str, year: int) -> dict:
    rows = reconciliation.table.to_polars().filter(party=party, year=year)
    assert rows.height == 1
    return rows.row(0, named=True)


def test_germany_reads_against_its_own_weight() -> None:
    row = _row(SNAP.reconcile(parties="DEU"), "DEU", 2022)

    assert row["party_weight"] == 0.5
    assert row["weight_confirmed"] is True
    assert row["btr_face"] == pytest.approx(22e6)
    # DEU-2 is Significant on both objectives, face 25m.
    assert row["crdf_at_party_weight"] == pytest.approx(12.5e6)
    assert row["crdf_at_100"] == pytest.approx(25e6)
    # Same-year Bi_Multi 1 disbursements: 50.67m Principal and 16.67m Significant.
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(
        50.666667e6 + 0.5 * 16.666667e6, rel=1e-6
    )
    # ONE counts DEU-2's disbursed 12.5m at 0.4.
    assert row["traced_climate_value"] == pytest.approx(5e6)
    assert row["btr_over_crdf_at_party_weight"] == pytest.approx(22 / 12.5)


def test_core_contributions_stay_out_of_the_comparators() -> None:
    # DEU-3 (60m) and the Bi_Multi 2 disbursements are core contributions.
    row = _row(SNAP.reconcile(parties="DEU"), "DEU", 2021)

    assert row["crdf_at_100"] == pytest.approx(40e6)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(50.666667e6, rel=1e-6)


def test_a_party_without_a_weight_has_no_weighted_comparators() -> None:
    row = _row(SNAP.reconcile(parties="NOR"), "NOR", 2021)

    assert row["party_weight"] is None
    assert row["crdf_at_party_weight"] is None
    assert row["crs_disbursed_at_party_weight"] is None
    assert row["crdf_at_100"] == pytest.approx(18e6)


def test_each_method_gives_its_traced_value() -> None:
    table = SNAP.reconcile(
        parties=["DEU"], years=[2022], method=MethodRange(methods=(ONE, OECD))
    ).table.to_polars()

    assert dict(table.select("method", "traced_climate_value").iter_rows()) == {
        "ONE": pytest.approx(5e6),
        "OECD": pytest.approx(12.5e6),
    }


def _commitments_climate_share(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        climate_share=pl.when(pl.col("row_id") == "DEU-2")
        .then(0.3)
        .otherwise(pl.col("climate_share"))
    )


def test_climate_component_rows_stay_out_of_the_crdf_comparators(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "r", edits={"commitments": _commitments_climate_share}
    )

    row = _row(cf.load(path=release).reconcile(parties="DEU"), "DEU", 2022)

    assert row["crdf_at_100"] is None
    assert row["traced_climate_value"] is None


def _norway_reports_commitments(frame: pl.DataFrame) -> pl.DataFrame:
    committed = frame.filter(row_id="BTR-NOR-2021").with_columns(
        row_id=pl.lit("BTR-NOR-2021-C"), status=pl.lit("committed"), face_usd=20e6
    )
    unknown = frame.filter(row_id="BTR-NOR-2022").with_columns(status=None)
    return pl.concat(
        [frame.filter(~pl.col("row_id").is_in(["BTR-NOR-2022"])), committed, unknown]
    )


def test_a_party_year_reads_on_its_main_status(tmp_path: Path) -> None:
    release = copy_release(tmp_path / "r", edits={"btr": _norway_reports_commitments})

    reconciliation = cf.load(path=release).reconcile(parties="NOR")

    committed = _row(reconciliation, "NOR", 2021)
    assert committed["status"] == "committed"
    assert committed["btr_face"] == pytest.approx(20e6)
    # A year whose rows carry no status reads on all of them.
    unknown = _row(reconciliation, "NOR", 2022)
    assert unknown["status"] is None
    assert unknown["btr_face"] == pytest.approx(15e6)


def test_unlinked_rows_are_unfccc_only() -> None:
    reconciliation = SNAP.reconcile(parties=["DEU"])

    assert _row(reconciliation, "DEU", 2021)["unfccc_only_usd"] == 0
    assert _row(reconciliation, "DEU", 2022)["unfccc_only_usd"] == pytest.approx(22e6)
    linked = reconciliation.rows("DEU", 2021)
    assert linked["record_id"].to_list() == ["DEU-B-R1", "DEU-B-R2"]
    assert linked["crs_project_number"].to_list() == ["300123-101", "300123-105"]
    assert not linked["unfccc_only"].any()
    unlinked = reconciliation.rows("DEU", 2022)
    assert unlinked.select("row_id", "record_id", "unfccc_only").rows() == [
        ("BTR-DEU-2022", None, True)
    ]
