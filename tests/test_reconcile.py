"""`Release.reconcile()` on the fixture release: BTR totals against OECD figures."""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.build.release import btr_totals
from climate_finance.errors import ReleaseIntegrityError, WithheldTableError
from climate_finance.methods import OECD, ONE, MethodRange
from climate_finance.result import Reconciliation
from climate_finance.schema import ReleaseManifest

SNAP = cf.load(path=FIXTURE_DIR)
# DEU 2022's same-year Bi_Multi 1 disbursements: 60.67m Principal, 10m of it from
# the unlinked DEU-UNLINKED, and 16.67m from DEU-2, Significant on both objectives,
# which Germany counts in full, above its Significant weight of 0.5.
DEU_2022_DISBURSED = 60.666667e6 + 16.666667e6


def _row(
    reconciliation: Reconciliation,
    party: str,
    year: int,
    status: str | None = "any",
) -> dict:
    rows = reconciliation.table.filter(party=party, year=year)
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


@pytest.mark.parametrize("germany", ["Germany", " germany ", "DEU", 5])
def test_providers_name_a_party_in_any_form(germany: str | int) -> None:
    reconciliation = SNAP.reconcile(providers=germany)
    rows = reconciliation.rows(provider=germany, year=2021)

    assert set(reconciliation.table["party"]) == {"DEU"}
    assert set(rows["party"]) == {"DEU"}


def test_rows_name_the_provider_and_keep_the_btr_recipient() -> None:
    rows = SNAP.reconcile().rows(provider="DEU", year=2021)
    btr = pl.read_parquet(FIXTURE_DIR / "btr.parquet").filter(
        provider_code=5, year=2021
    )

    assert rows.columns[:3] == ["party", "provider", "provider_code"]
    assert set(rows["provider"]) == {"Germany"}
    assert set(rows["recipient"]) == set(btr["recipient"])


def test_years_take_one_year_and_none_takes_every_year() -> None:
    assert set(SNAP.reconcile(years=2022).table["year"]) == {2022}
    assert set(SNAP.reconcile().table["year"]) == {2021, 2022}


@pytest.mark.parametrize(
    "call",
    [
        lambda: SNAP.reconcile(providers="EIB"),
        lambda: SNAP.reconcile().rows(provider="EIB", year=2021),
        lambda: SNAP.quality(providers=["Germany", "EIB"]),
    ],
    ids=["reconcile", "rows", "quality"],
)
def test_an_agency_is_refused_naming_its_provider(call: Callable[[], object]) -> None:
    with pytest.raises(
        ValueError,
        match="'EIB' is the European Investment Bank, an agency of EU Institutions",
    ) as raised:
        call()

    assert "='EU Institutions'" in str(raised.value)


def test_an_unknown_provider_is_refused_with_suggestions() -> None:
    with pytest.raises(cf.UnknownProviderError) as raised:
        SNAP.reconcile(providers="Germny")

    assert "Germany" in raised.value.suggestions


def test_germany_reads_against_its_own_weight() -> None:
    row = _row(SNAP.reconcile(providers="DEU"), "DEU", 2022)

    assert row["party_weight"] == 0.5
    assert row["party_principal"] == 1.0
    assert row["weight_source"] == "Fixture survey, p. 8"
    assert row["coefficient_basis"] == "provider"
    assert (row["crdf_method_share"], row["crs_method_share"]) == (0.0, 0.0)
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
    row = _row(SNAP.reconcile(providers="NOR"), "NOR", 2021)

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
    row = _row(SNAP.reconcile(providers="DEU"), "DEU", 2022)

    # The fixture's grant equivalent is 0.9 of each disbursement.
    assert row["crs_grant_equivalent_at_party_weight"] == pytest.approx(
        0.9 * DEU_2022_DISBURSED, rel=1e-6
    )
    assert row["btr_grant_equivalent_over_crs_grant_equivalent"] == pytest.approx(
        22e6 / (0.9 * DEU_2022_DISBURSED), rel=1e-6
    )


def test_core_contributions_stay_out_of_the_comparators() -> None:
    # DEU-3 (60m) and the Bi_Multi 2 disbursements are core contributions.
    row = _row(SNAP.reconcile(providers="DEU"), "DEU", 2021)

    assert row["crdf_at_100"] == pytest.approx(40e6)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(50.666667e6, rel=1e-6)


def test_a_case_by_case_party_reads_against_its_default() -> None:
    # Norway assesses each activity, with 0.3 for a Significant one whose share is
    # unknown and no default for a Principal one, which counts in full at PROVIDER's
    # 1.0. NOR-1 (18m, 2021) is Principal and NOR-2 (9m, 2022) Significant.
    reconciliation = SNAP.reconcile(providers="NOR")
    row = _row(reconciliation, "NOR", 2021)

    assert (row["party_weight"], row["party_principal"]) == (0.3, None)
    assert row["coefficient_basis"] == "default"
    assert row["crdf_at_party_weight"] == pytest.approx(18e6)
    # A Principal row rests on PROVIDER's 1.0 though the basis is `default`.
    assert (row["crdf_method_share"], row["crs_method_share"]) == (1.0, 1.0)
    assert _row(reconciliation, "NOR", 2022)["crdf_method_share"] == 0.0


def _france_reports(frame: pl.DataFrame) -> pl.DataFrame:
    """France reports Germany's BTR totals."""
    france = frame.filter(party="DEU").with_columns(
        party=pl.lit("FRA"), provider_code=pl.lit(4, pl.Int32)
    )
    return pl.concat([frame, france])


def test_an_agency_reads_against_its_own_coefficients(tmp_path: Path) -> None:
    # France counts a Significant activity at 0.3, and its agency 1 assesses each
    # activity case by case, so its Significant commitment FRA-2 (12m) and its
    # Significant 2021 disbursement (12m) count at PROVIDER's 0.4.
    release = copy_release(tmp_path / "r", edits={"btr_totals": _france_reports})
    reconciliation = cf.load(path=release).reconcile(providers="FRA")
    row = _row(reconciliation, "FRA", 2022)

    assert row["party_weight"] == 0.3
    assert row["crdf_at_party_weight"] == pytest.approx(0.4 * 12e6)
    assert row["btr_over_crdf_at_party_weight"] == pytest.approx(22 / 4.8)
    disbursed = _row(reconciliation, "FRA", 2021)
    assert disbursed["crs_disbursed_at_party_weight"] == pytest.approx(0.4 * 12e6)
    # The fixture's grant equivalent is 0.9 of each disbursement.
    assert disbursed["crs_grant_equivalent_at_party_weight"] == pytest.approx(
        0.9 * 0.4 * 12e6
    )


def _french_agency_2(frame: pl.DataFrame) -> pl.DataFrame:
    """A copy of France's 2022 rows of agency 1 under agency 2, which has no row of
    its own and takes France's 0.3."""
    agency_2 = frame.filter(provider_code=4, agency_code=1, year=2022).with_columns(
        agency_code=pl.lit(2, pl.Int32)
    )
    if "row_id" in frame.columns:
        agency_2 = agency_2.with_columns(pl.col("row_id") + "-AGENCY-2")
    return pl.concat([frame, agency_2])


def _a_loan_at_agency_1(frame: pl.DataFrame) -> pl.DataFrame:
    """France's 2022 grant equivalent of agency 1 halved, as a loan's would be."""
    loan = (
        (pl.col("provider_code") == 4)
        & (pl.col("agency_code") == 1)
        & (pl.col("year") == 2022)
        & (pl.col("flow") == "grant_equivalent")
    )
    return frame.with_columns(
        amount_usd=pl.when(loan).then(pl.col("amount_usd") / 2).otherwise("amount_usd")
    )


def test_a_mixed_basis_party_shows_its_method_shares(tmp_path: Path) -> None:
    # FRA-2 (12m) and France's Significant 2022 disbursement (4.8m) count at
    # PROVIDER's 0.4 under agency 1 and at France's 0.3 under agency 2, so 0.4 of
    # every 0.7 rests on PROVIDER's coefficients. Agency 1's grant equivalent is a
    # loan's 2.16m against agency 2's 4.32m, so 0.864m of 2.16m rests on them.
    release = copy_release(
        tmp_path / "r",
        edits={
            "btr_totals": _france_reports,
            "commitments": _french_agency_2,
            "total_flows": lambda frame: _a_loan_at_agency_1(_french_agency_2(frame)),
        },
    )
    row = _row(cf.load(path=release).reconcile(providers="FRA"), "FRA", 2022)

    assert row["coefficient_basis"] == "provider"
    assert row["crdf_at_party_weight"] == pytest.approx(0.7 * 12e6)
    assert row["crdf_method_share"] == pytest.approx(4 / 7)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(0.7 * 4.8e6)
    assert row["crs_method_share"] == pytest.approx(4 / 7)
    assert row["crs_grant_equivalent_method_share"] == pytest.approx(0.4)


def _overlapping_coefficients(frame: pl.DataFrame) -> pl.DataFrame:
    """A second all-agency row for Germany from 2020, overlapping its first."""
    later = frame.filter(provider_code=5).with_columns(
        first_year=pl.lit(2020, pl.Int16), significant=0.9
    )
    return pl.concat([frame, later])


def test_overlapping_coefficient_rows_refuse_to_reconcile(tmp_path: Path) -> None:
    # The build refuses such a table; an edited release must not double Germany's
    # comparators instead.
    release = copy_release(
        tmp_path / "r", edits={"provider_coefficients": _overlapping_coefficients}
    )

    with pytest.raises(
        ReleaseIntegrityError,
        match="5 None 2020: covers 2020 to every later year",
    ):
        _ = cf.load(path=release).reconcile(providers="DEU").table


def _without_a_default(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        significant=pl.when(pl.col("provider_code") != 8).then("significant")
    )


def test_a_case_by_case_party_without_a_default_reads_at_provider_coefficients(
    tmp_path: Path,
) -> None:
    # Norway assesses each activity and here states no default, so its Significant
    # NOR-2 (9m) counts at PROVIDER's 0.4, as `spending` counts it.
    release = copy_release(
        tmp_path / "release", edits={"provider_coefficients": _without_a_default}
    )
    row = _row(cf.load(path=release).reconcile(providers="NOR"), "NOR", 2022)

    assert row["party_weight"] is None
    assert row["coefficient_basis"] == "case_by_case"
    assert row["crdf_at_party_weight"] == pytest.approx(0.4 * 9e6)
    assert row["btr_over_comparator"] == pytest.approx(
        row["btr_face"] / row["crs_disbursed_at_party_weight"]
    )


def test_a_party_without_a_coefficient_row_reads_at_provider_coefficients(
    tmp_path: Path,
) -> None:
    # Without its row Germany weighs as `spending` weighs it: DEU-2 (25m), Significant
    # on both objectives, counts at PROVIDER's 0.4, and so does DEU-2's 16.67m
    # disbursed.
    release = copy_release(
        tmp_path / "release",
        edits={
            "provider_coefficients": lambda frame: frame.filter(
                pl.col("provider_code") != 5
            )
        },
    )
    row = _row(cf.load(path=release).reconcile(providers="DEU"), "DEU", 2022)

    assert (row["party_weight"], row["coefficient_basis"]) == (None, None)
    assert row["crdf_at_party_weight"] == pytest.approx(0.4 * 25e6)
    assert row["crs_disbursed_at_party_weight"] == pytest.approx(
        60.666667e6 + 0.4 * 16.666667e6, rel=1e-6
    )
    assert (row["crdf_method_share"], row["crs_method_share"]) == (1.0, 1.0)
    assert row["btr_over_comparator"] == pytest.approx(22 / 10)


def test_each_method_gives_its_traced_value() -> None:
    table = SNAP.reconcile(
        providers=["DEU"], years=[2022], method=MethodRange(methods=(ONE, OECD))
    ).table

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

    row = _row(cf.load(path=release).reconcile(providers="DEU"), "DEU", 2022)

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

    reconciliation = cf.load(path=release).reconcile(providers="NOR")

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

    reconciliation = cf.load(path=release).reconcile(providers="NOR")

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

    assert cf.load(path=release).reconcile().table.is_empty()


def test_unlinked_rows_are_unfccc_only() -> None:
    reconciliation = SNAP.reconcile(providers=["DEU"])

    assert _row(reconciliation, "DEU", 2021)["unfccc_only_usd"] == 0
    assert _row(reconciliation, "DEU", 2022)["unfccc_only_usd"] == pytest.approx(22e6)
    linked = reconciliation.rows(provider="DEU", year=2021)
    assert linked["record_id"].to_list() == ["DEU-B-R1", "DEU-B-R2"]
    assert linked["crs_project_number"].to_list() == ["300123-101", "300123-105"]
    assert not linked["unfccc_only"].any()
    unlinked = reconciliation.rows(provider="DEU", year=2022)
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
        cf.load(path=_btr_release(tmp_path / "r", oof_loan)).reconcile(providers="DEU"),
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


@pytest.mark.parametrize(("face", "ratio"), [(0.8e6, 27.5), (0.0, None)])
def test_a_comparator_under_10m_is_thin(
    tmp_path: Path, face: float, ratio: float | None
) -> None:
    def small(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            face_usd=pl.when(pl.col("row_id") == "DEU-2")
            .then(face)
            .otherwise(pl.col("face_usd"))
        )

    release = copy_release(tmp_path / "r", edits={"commitments": small})
    row = _row(cf.load(path=release).reconcile(providers="DEU"), "DEU", 2022)

    # DEU-2 is Significant on both objectives, which Germany counts in full. A
    # zero comparator gives no ratio.
    assert row["crdf_at_party_weight"] == pytest.approx(face)
    assert row["btr_over_comparator"] == pytest.approx(ratio)
    assert row["btr_over_crdf_at_party_weight"] == pytest.approx(ratio)
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
    row = _row(cf.load(path=release).reconcile(providers="DEU"), "DEU", 2022)

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
    reconciliation = cf.load(path=release).reconcile(providers="DEU", years=[2022])

    table = reconciliation.table
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

    assert reconciliation.table.equals(SNAP.reconcile().table)
    with pytest.raises(WithheldTableError, match="btr"):
        reconciliation.rows(provider="DEU", year=2021)
