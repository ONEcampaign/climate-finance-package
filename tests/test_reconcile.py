"""`Snapshot.reconcile()` on the fixture release: BTR totals against OECD figures."""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.build.release import btr_totals
from climate_finance.errors import WithheldTableError
from climate_finance.methods import OECD, ONE, MethodRange
from climate_finance.result import Reconciliation
from climate_finance.schema import ReleaseManifest

SNAP = cf.load(path=FIXTURE_DIR)
# DEU 2022's same-year Bi_Multi 1 disbursements: 50.67m Principal and 16.67m
# Significant, at Germany's weight of 0.5.
DEU_2022_DISBURSED = 50.666667e6 + 0.5 * 16.666667e6


def _row(
    reconciliation: Reconciliation,
    party: str,
    year: int,
    status: str | None = "any",
) -> dict:
    rows = reconciliation.table.to_polars().filter(party=party, year=year)
    if status != "any":
        rows = rows.filter(pl.col("status").eq_missing(status))
    assert rows.height == 1
    return rows.row(0, named=True)


def _btr_release(target: Path, edit: Callable[[pl.DataFrame], pl.DataFrame]) -> Path:
    """A copy of the fixture release with `btr` edited and `btr_totals` summed from
    the edited rows, as the build sums them."""
    btr = edit(pl.read_parquet(FIXTURE_DIR / "btr.parquet"))
    links = pl.scan_parquet(FIXTURE_DIR / "btr_links.parquet")
    totals = btr_totals(btr.lazy(), links=links).collect()
    return copy_release(
        target, edits={"btr": lambda _: btr, "btr_totals": lambda _: totals}
    )


def test_germany_reads_against_its_own_weight() -> None:
    row = _row(SNAP.reconcile(parties="DEU"), "DEU", 2022)

    assert row["party_weight"] == 0.5
    assert row["party_principal"] == 1.0
    assert row["weight_source"] == "Fixture survey, p. 8"
    assert row["btr_face"] == pytest.approx(22e6)
    # DEU-2, face 25m, is Significant on both objectives, which Germany counts in
    # full.
    assert row["crdf_at_party_weight"] == pytest.approx(25e6)
    assert row["crdf_at_100"] == pytest.approx(25e6)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(
        DEU_2022_DISBURSED, rel=1e-6
    )
    # ONE counts DEU-2's disbursed 12.5m at 0.4.
    assert row["traced_climate_value"] == pytest.approx(5e6)
    assert row["btr_over_crdf_at_party_weight"] == pytest.approx(22 / 25)
    # Germany reports commitments, so it reads against the CRDF first and not
    # against disbursements.
    assert row["status"] == "committed"
    assert row["comparator"] == "crdf_at_party_weight"
    assert row["btr_over_comparator"] == pytest.approx(22 / 25)
    assert row["thin_comparator"] is False
    assert row["btr_over_crdf_at_100"] == pytest.approx(22 / 25)
    assert row["btr_over_crs_disbursed_at_party_weight"] is None


def test_a_disbursed_row_reads_against_disbursements_only() -> None:
    row = _row(SNAP.reconcile(parties="NOR"), "NOR", 2021)

    assert row["status"] == "disbursed"
    assert row["comparator"] == "crs_disbursed_at_party_weight"
    assert row["crdf_at_100"] == pytest.approx(18e6)
    assert row["btr_over_crdf_at_100"] is None
    assert row["btr_over_traced_climate_value"] is None
    assert row["btr_over_comparator"] == pytest.approx(
        row["btr_face"] / row["crs_disbursed_at_party_weight"]
    )
    assert row["btr_over_comparator"] == row["btr_over_crs_disbursed_at_party_weight"]


def test_grant_equivalent_reads_against_the_crs_grant_equivalent() -> None:
    row = _row(SNAP.reconcile(parties="DEU"), "DEU", 2022)

    # The fixture's grant equivalent is 0.9 of each disbursement.
    assert row["crs_grant_equivalent_at_party_weight"] == pytest.approx(
        0.9 * DEU_2022_DISBURSED, rel=1e-6
    )
    assert row["btr_grant_equivalent_over_crs_grant_equivalent"] == pytest.approx(
        22e6 / (0.9 * DEU_2022_DISBURSED), rel=1e-6
    )


def test_core_contributions_stay_out_of_the_comparators() -> None:
    # DEU-3 (60m) and the Bi_Multi 2 disbursements are core contributions.
    row = _row(SNAP.reconcile(parties="DEU"), "DEU", 2021)

    assert row["crdf_at_100"] == pytest.approx(40e6)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(50.666667e6, rel=1e-6)


def test_a_case_by_case_party_reads_against_its_default() -> None:
    # Norway assesses each activity, with 0.3 for a Significant one whose share is
    # unknown and no default for a Principal one, which counts in full. NOR-1 (18m)
    # is Principal.
    row = _row(SNAP.reconcile(parties="NOR"), "NOR", 2021)

    assert (row["party_weight"], row["party_principal"]) == (0.3, None)
    assert row["crdf_at_party_weight"] == pytest.approx(18e6)


def _without_a_default(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        significant=pl.when(pl.col("provider_code") != 8).then("significant")
    )


def test_a_party_without_a_weight_has_no_weighted_comparators(
    tmp_path: Path,
) -> None:
    release = copy_release(
        tmp_path / "release", edits={"provider_coefficients": _without_a_default}
    )
    row = _row(cf.load(path=release).reconcile(parties="NOR"), "NOR", 2021)

    assert row["party_weight"] is None
    assert row["crdf_at_party_weight"] is None
    assert row["crs_disbursed_at_party_weight"] is None
    # Norway's own comparator is null, so its ratio says nothing.
    assert row["btr_over_comparator"] is None
    assert row["thin_comparator"] is None
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
    """Norway commits a 20m loan in 2021 beside the grant it disbursed, as Japan
    does, and gives its 2022 grant no status."""
    committed = frame.filter(row_id="BTR-NOR-2021").with_columns(
        row_id=pl.lit("BTR-NOR-2021-C"),
        title=pl.lit("Loan"),
        status=pl.lit("committed"),
        face_usd_at_dac=20e6,
    )
    unknown = frame.filter(row_id="BTR-NOR-2022").with_columns(status=None)
    return pl.concat(
        [frame.filter(~pl.col("row_id").is_in(["BTR-NOR-2022"])), committed, unknown]
    )


def test_each_status_reads_on_its_own_row(tmp_path: Path) -> None:
    release = _btr_release(tmp_path / "r", _norway_reports_commitments)

    reconciliation = cf.load(path=release).reconcile(parties="NOR")

    committed = _row(reconciliation, "NOR", 2021, "committed")
    assert committed["btr_face"] == pytest.approx(20e6)
    assert committed["comparator"] == "crdf_at_party_weight"
    assert committed["btr_over_crdf_at_100"] == pytest.approx(20 / 18)
    disbursed = _row(reconciliation, "NOR", 2021, "disbursed")
    assert disbursed["btr_face"] == pytest.approx(15e6)
    assert disbursed["btr_over_crdf_at_100"] is None
    # Rows without a status read on their own row, against every comparator.
    unknown = _row(reconciliation, "NOR", 2022)
    assert unknown["status"] is None
    assert unknown["btr_face"] == pytest.approx(15e6)
    assert unknown["comparator"] is None
    assert unknown["btr_over_crdf_at_100"] is not None


def _norway_misses_amounts(frame: pl.DataFrame) -> pl.DataFrame:
    face = pl.when(pl.col("row_id") == "BTR-NOR-2021").then(None).otherwise(0.0)
    grant_equivalent = (
        pl.when(pl.col("row_id") == "BTR-NOR-2022")
        .then(None)
        .otherwise(pl.col(name))
        .alias(name)
        for name in ("grant_equiv_usd", "grant_equiv_usd_at_dac")
    )
    return frame.with_columns(face_usd=face, face_usd_at_dac=face).with_columns(
        *grant_equivalent
    )


def test_a_missing_btr_amount_stays_null_and_a_reported_zero_stays_zero(
    tmp_path: Path,
) -> None:
    release = _btr_release(tmp_path / "r", _norway_misses_amounts)

    reconciliation = cf.load(path=release).reconcile(parties="NOR")

    no_face = _row(reconciliation, "NOR", 2021)
    assert no_face["btr_face"] is None
    assert no_face["btr_grant_equivalent"] == pytest.approx(15e6)
    assert no_face["unfccc_only_usd"] is None
    assert no_face["btr_over_crdf_at_100"] is None
    zero_face = _row(reconciliation, "NOR", 2022)
    assert zero_face["btr_face"] == 0
    assert zero_face["btr_grant_equivalent"] is None
    assert zero_face["unfccc_only_usd"] == 0


def test_a_release_with_no_btr_rows_reconciles_to_an_empty_table(
    tmp_path: Path,
) -> None:
    def clear(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.clear()

    release = copy_release(
        tmp_path / "r",
        edits=dict.fromkeys(
            ("btr", "btr_links", "btr_totals", "btr_crs_unlinked"), clear
        ),
    )

    assert cf.load(path=release).reconcile().table.to_polars().is_empty()


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


def test_unlinked_btr_money_splits_by_funding_source(tmp_path: Path) -> None:
    def oof_loan(frame: pl.DataFrame) -> pl.DataFrame:
        loan = frame.filter(row_id="BTR-DEU-2022").with_columns(
            row_id=pl.lit("BTR-DEU-2022-L"),
            funding_source=pl.lit("OOF (ICF)"),
            face_usd_at_dac=5e6,
        )
        return pl.concat([frame, loan])

    row = _row(
        cf.load(path=_btr_release(tmp_path / "r", oof_loan)).reconcile(parties="DEU"),
        "DEU",
        2022,
    )

    assert row["unfccc_only_usd"] == pytest.approx(27e6)
    assert row["unfccc_only_oda_usd"] == pytest.approx(22e6)
    assert row["unfccc_only_oof_usd"] == pytest.approx(5e6)
    assert row["unfccc_only_other_usd"] == 0


def test_oecd_only_reads_the_status_flow_of_unlinked_crs_records(
    tmp_path: Path,
) -> None:
    def unlinked(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            amount_usd=pl.when(pl.col("flow") == "commitments").then(7e6).otherwise(3e6)
        )

    release = copy_release(tmp_path / "r", edits={"btr_crs_unlinked": unlinked})
    reconciliation = cf.load(path=release).reconcile()

    assert _row(reconciliation, "DEU", 2022)["oecd_only_usd"] == pytest.approx(7e6)
    assert _row(reconciliation, "NOR", 2022)["oecd_only_usd"] == pytest.approx(3e6)


def test_a_comparator_under_10m_is_thin(tmp_path: Path) -> None:
    def small(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            face_usd=pl.when(pl.col("row_id") == "DEU-2")
            .then(0.8e6)
            .otherwise(pl.col("face_usd"))
        )

    release = copy_release(tmp_path / "r", edits={"commitments": small})
    row = _row(cf.load(path=release).reconcile(parties="DEU"), "DEU", 2022)

    # DEU-2 is Significant on both objectives, which Germany counts in full.
    assert row["crdf_at_party_weight"] == pytest.approx(0.8e6)
    assert row["btr_over_comparator"] == pytest.approx(27.5)
    assert row["thin_comparator"] is True


def test_btr_amounts_read_at_the_dac_rate(tmp_path: Path) -> None:
    # Germany converts 20m euros at 1/1.1 a dollar; at a DAC rate of 1.0 the same
    # euros are 20m dollars.
    def at_dac(frame: pl.DataFrame) -> pl.DataFrame:
        germany = pl.col("party") == "DEU"
        return frame.with_columns(
            dac_rate=pl.when(germany).then(1.0).otherwise(pl.col("dac_rate")),
            face_usd_at_dac=pl.when(germany)
            .then(pl.col("amount_domestic"))
            .otherwise(pl.col("face_usd_at_dac")),
        )

    release = _btr_release(tmp_path / "r", at_dac)
    row = _row(cf.load(path=release).reconcile(parties="DEU"), "DEU", 2022)

    assert row["btr_face"] == pytest.approx(20e6)
    assert row["btr_face_reported"] == pytest.approx(22e6)
    assert row["party_rate"] == pytest.approx(1 / 1.1)
    assert row["dac_rate"] == 1.0
    assert row["btr_over_crdf_at_party_weight"] == pytest.approx(20 / 25)


def test_grant_equivalent_counts_rows_without_a_status_or_a_face_value(
    tmp_path: Path,
) -> None:
    # Germany reports a loan's grant equivalent on its own row with no face value,
    # and three more with no status.
    def germany(frame: pl.DataFrame) -> pl.DataFrame:
        base = frame.filter(row_id="BTR-DEU-2022")
        extra = [
            base.with_columns(
                row_id=pl.lit(f"BTR-DEU-2022-{n}"),
                status=pl.lit(status, pl.String),
                face_usd=0.0,
                face_usd_at_dac=0.0,
                grant_equiv_usd=amount,
                grant_equiv_usd_at_dac=amount,
            )
            for n, status, amount in (("G", "committed", 3e6), ("N", None, 5e6))
        ]
        return pl.concat([frame, *extra])

    release = _btr_release(tmp_path / "r", germany)
    reconciliation = cf.load(path=release).reconcile(parties="DEU", years=[2022])

    table = reconciliation.table.to_polars()
    assert table["btr_grant_equivalent"].sum() == pytest.approx(22e6 + 3e6 + 5e6)
    assert _row(reconciliation, "DEU", 2022, None)["btr_face"] == 0


def test_a_release_without_btr_rows_reconciles_from_its_totals(
    tmp_path: Path,
) -> None:
    release = copy_release(tmp_path / "r")
    manifest = ReleaseManifest.model_validate_json(
        (release / "release.json").read_text()
    )
    withheld = {"btr": "Build it.", "btr_links": "Build it."}
    published = manifest.model_copy(
        update={
            "tables": {
                name: entry
                for name, entry in manifest.tables.items()
                if name not in withheld
            },
            "withheld_tables": withheld,
        }
    )
    (release / "release.json").write_text(published.model_dump_json())
    for name in withheld:
        (release / f"{name}.parquet").unlink()

    reconciliation = cf.load(path=release).reconcile()

    assert reconciliation.table.to_polars().equals(SNAP.reconcile().table.to_polars())
    with pytest.raises(WithheldTableError, match="btr"):
        reconciliation.rows("DEU", 2021)
