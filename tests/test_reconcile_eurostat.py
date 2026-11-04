"""`Release.reconcile_eurostat()`: Eurostat's `sdg_13_50` lines against the release."""

import warnings
from pathlib import Path

import polars as pl
import pytest
from make_fixture import FIXTURE_DIR
from releases import copy_release

import climate_finance as cf
from climate_finance.errors import CoverageWarning, InvalidInputError
from climate_finance.methods import ONE
from climate_finance.result import EurostatReconciliation
from climate_finance.spec import Flow, View

SNAP = cf.load(path=FIXTURE_DIR)
# The release side's method, spelled out here so a change to the query's shows.
EU_AS_PROVIDER = ONE.replace(eu="provider")


def _line(reconciliation: EurostatReconciliation, geo: str, year: int) -> dict:
    rows = reconciliation.table.filter(geo=geo, year=year)
    assert rows.height == 1
    return rows.row(0, named=True)


def _release_value(
    release: cf.Release,
    *,
    origin: str,
    flow: Flow,
    providers: int | cf.Agency,
    year: int,
    view: View = "provider",
) -> float:
    """The EUR climate value of `spending`'s rows of `origin` under the release
    side's method."""
    rows = release.spending(
        providers=providers,
        years=year,
        flow=flow,
        view=view,
        currency="EUR",
        method=EU_AS_PROVIDER,
    ).rows()
    return float(rows.filter(origin=origin)["value"].sum())


@pytest.mark.parametrize(
    ("provider", "year", "flow"),
    [(5, 2021, "commitments"), (5, 2022, "disbursements"), (4, 2022, "disbursements")],
)
def test_a_member_reads_its_bilateral_commitments_and_its_contributions_by_basis(
    provider: int, year: int, flow: Flow
) -> None:
    # 2021 counts core contributions as committed, and from 2022 as provided.
    line = _line(SNAP.reconcile_eurostat(), {5: "DE", 4: "FR"}[provider], year)
    bilateral = _release_value(
        SNAP, origin="commitment", flow="commitments", providers=provider, year=year
    )
    multilateral = _release_value(
        SNAP, origin="attribution", flow=flow, providers=provider, year=year
    )

    assert (
        line["basis"]
        == {
            "commitments": "commitments",
            "disbursements": "bilateral_committed_multilateral_provided",
        }[flow]
    )
    assert line["release_bilateral_eur"] == pytest.approx(bilateral)
    assert line["release_multilateral_eur"] == pytest.approx(multilateral)
    assert line["release_eur"] == pytest.approx(bilateral + multilateral)
    assert line["gap_eur"] == pytest.approx(line["eurostat_eur"] - line["release_eur"])
    assert line["gap_share"] == pytest.approx(line["gap_eur"] / line["eurostat_eur"])


def test_germany_2022_counts_contributions_provided_not_committed() -> None:
    line = _line(SNAP.reconcile_eurostat(providers="Germany"), "DE", 2022)
    committed = _release_value(
        SNAP, origin="attribution", flow="commitments", providers=5, year=2022
    )

    # The fixture's 2022 contributions differ by flow, so the basis decides.
    assert line["release_multilateral_eur"] != pytest.approx(committed)


def _through_the_eib(frame: pl.DataFrame) -> pl.DataFrame:
    """A copy of every row of IDA's channel (44002) under the EIB's (42004): in
    `core_contributions` Germany's contributions, and in the share tables IDA's
    shares."""
    eib = frame.filter(channel_code=44002).with_columns(
        channel_code=pl.lit(42004, pl.Int32)
    )
    return pl.concat([frame, eib])


def test_a_members_contributions_to_the_eib_stay_in_the_eibs_line(
    tmp_path: Path,
) -> None:
    release = cf.load(
        path=copy_release(
            tmp_path / "release",
            edits=dict.fromkeys(
                ("core_contributions", "agency_shares", "agency_spending"),
                _through_the_eib,
            ),
        )
    )
    through_eib = release.spending(
        providers=5,
        years=2022,
        view="provider",
        currency="EUR",
        method=EU_AS_PROVIDER,
    ).rows()

    # The provider view attributes Germany's contributions to the EIB, and the
    # comparison leaves them out, since the EIB's own commitments make its line.
    assert through_eib.filter(origin="attribution", channel_code=42004).height
    assert (
        _line(release.reconcile_eurostat(), "DE", 2022)["release_multilateral_eur"]
        == _line(SNAP.reconcile_eurostat(), "DE", 2022)["release_multilateral_eur"]
    )


def test_the_table_leaves_out_eu27_and_names_each_line() -> None:
    table = SNAP.reconcile_eurostat().table

    assert table.select("geo", "year").rows() == [
        ("DE", 2021),
        ("DE", 2022),
        ("EIB", 2022),
        ("FR", 2021),
        ("FR", 2022),
    ]
    assert table.columns == [
        "geo",
        "provider",
        "provider_code",
        "agency",
        "agency_code",
        "year",
        "basis",
        "eurostat_eur",
        "obs_flag",
        "release_bilateral_eur",
        "release_multilateral_eur",
        "release_eur",
        "gap_eur",
        "gap_share",
    ]
    eib = _line(SNAP.reconcile_eurostat(), "EIB", 2022)
    assert (eib["provider"], eib["agency"]) == (
        "EU Institutions",
        "European Investment Bank",
    )
    # The example holds no EIB rows, so the whole line is a gap.
    assert (eib["release_eur"], eib["gap_share"]) == (0.0, 1.0)


def _eu_institutions_rows(frame: pl.DataFrame) -> pl.DataFrame:
    """France's 2022 commitment under each of EU Institutions' agencies 1 to 4."""
    france = frame.filter(provider_code=4, year=2022)
    copies = [
        france.with_columns(
            provider_code=pl.lit(918, pl.Int32),
            agency_code=pl.lit(agency, pl.Int32),
            row_id=pl.col("row_id") + f"-EUI-{agency}",
        )
        for agency in (1, 2, 3, 4)
    ]
    return pl.concat([frame, *copies])


def _with_comm(frame: pl.DataFrame) -> pl.DataFrame:
    comm = frame.filter(geo="EIB").with_columns(
        geo=pl.lit("COMM"), agency_code=pl.lit(None, pl.Int32)
    )
    return pl.concat([frame, comm]).sort("geo", "year")


def test_the_commission_counts_agencies_1_and_2_and_the_eib_agency_3(
    tmp_path: Path,
) -> None:
    release = cf.load(
        path=copy_release(
            tmp_path / "release",
            edits={
                "commitments": _eu_institutions_rows,
                "eurostat_climate_finance": _with_comm,
            },
        )
    )
    reconciliation = release.reconcile_eurostat(providers="EU Institutions")
    eib = _release_value(
        release,
        origin="commitment",
        flow="commitments",
        view="recipient",
        providers=cf.Agency(provider=918, agency=3),
        year=2022,
    )

    assert eib > 0
    assert _line(reconciliation, "EIB", 2022)["release_bilateral_eur"] == eib
    # Agency 4 counts in neither line, and neither line attributes contributions.
    comm = _line(reconciliation, "COMM", 2022)
    assert comm["release_bilateral_eur"] == pytest.approx(2 * eib)
    assert comm["release_multilateral_eur"] == 0.0


@pytest.mark.parametrize(
    ("providers", "lines"),
    [
        ("Germany", {"DE"}),
        ("DEU", {"DE"}),
        (5, {"DE"}),
        (["France", "EIB"], {"FR", "EIB"}),
        ("EU Institutions", {"EIB"}),
    ],
)
def test_providers_name_lines_in_any_form(
    providers: str | int | list[str], lines: set[str]
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        table = SNAP.reconcile_eurostat(providers=providers).table

    assert set(table["geo"]) == lines


@pytest.mark.parametrize(
    ("providers", "message"),
    [
        ("Japan", "holds no line in the years asked for Japan"),
        ("European Commission", "holds no line in the years asked for European Com"),
        # A member's agency counts in the member's line, which is its total.
        (
            cf.Agency(provider=5, agency=52),
            "its line for Germany is the member's total, so name 'Germany'",
        ),
    ],
)
def test_a_provider_without_a_line_warns(
    providers: str | cf.Agency, message: str
) -> None:
    with pytest.warns(CoverageWarning, match="sdg_13_50") as caught:
        table = SNAP.reconcile_eurostat(providers=["France", providers]).table

    assert set(table["geo"]) == {"FR"}
    assert message in str(caught[0].message)
    assert "France" not in str(caught[0].message)


def test_an_unknown_provider_is_refused() -> None:
    with pytest.raises(cf.UnknownProviderError):
        SNAP.reconcile_eurostat(providers="Germny")


def test_years_outside_the_compared_ones_warn() -> None:
    with pytest.warns(CoverageWarning) as caught:
        table = SNAP.reconcile_eurostat(years=[2019, 2022]).table

    assert str(caught[0].message) == (
        "the release holds Eurostat figures to compare for 2021 to 2022; "
        "year 2019 returns no rows"
    )
    assert set(table["year"]) == {2022}


def test_an_unknown_keyword_is_refused() -> None:
    with pytest.raises(InvalidInputError, match="method"):
        SNAP.reconcile_eurostat(method="ONE")  # ty: ignore[unknown-argument]


def _germany_2024(frame: pl.DataFrame) -> pl.DataFrame:
    return pl.concat(
        [
            frame,
            frame.filter(geo="DE", year=2022).with_columns(year=pl.lit(2024, pl.Int16)),
        ]
    ).sort("geo", "year")


def test_the_release_side_is_null_in_a_year_its_tables_hold_no_rows(
    tmp_path: Path,
) -> None:
    release = cf.load(
        path=copy_release(
            tmp_path / "release", edits={"eurostat_climate_finance": _germany_2024}
        )
    )
    line = _line(release.reconcile_eurostat(providers="Germany"), "DE", 2024)

    assert line["eurostat_eur"] == 38e6
    assert line["release_bilateral_eur"] is None
    assert line["release_multilateral_eur"] is None
    assert line["gap_eur"] is None


def test_a_reconciliation_prints_its_lines_years_and_table() -> None:
    reconciliation = SNAP.reconcile_eurostat()
    title = "Eurostat reconciliation: lines DE, EIB, FR; years 2021 to 2022; EUR"

    assert repr(reconciliation) == f"{title}\n{reconciliation.table!r}"
    assert reconciliation._repr_html_().startswith(f"<p>{title}</p><div>")
