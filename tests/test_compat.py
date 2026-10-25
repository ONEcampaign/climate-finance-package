"""`ClimateData`, the deprecated 1.x wrapper, against `spending()` on the fixture."""

import subprocess
import sys
import warnings
from collections.abc import Callable

import pandas as pd
import polars as pl
import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance.compat import ClimateData, set_climate_finance_data_path
from climate_finance.methods import Method

RELEASE = cf.load(path=FIXTURE_DIR)
YEARS = range(2021, 2025)


def _climate_data(**kwargs: object) -> ClimateData:
    with pytest.warns(DeprecationWarning, match=r"spending\(\.\.\.\)"):
        return ClimateData(years=YEARS, release=RELEASE, **kwargs)


def _expected(*parts: tuple[str, str, cf.Result]) -> pd.DataFrame:
    """`spending()` tables with the `flow` and `source` columns `get_data` adds."""
    frame = pl.concat(
        result.table.with_columns(flow=pl.lit(flow), source=pl.lit(source))
        for source, flow, result in parts
    )
    return frame.to_pandas()


def test_get_data_equals_spending_for_each_flow() -> None:
    data = (
        _climate_data(providers=[5, "France"], recipients="Kenya")
        .load_spending_data(flows=["gross_disbursements", "commitments"])
        .get_data()
    )

    def spending(flow: str) -> cf.Result:
        return RELEASE.spending(
            providers=[5, "France"], recipients="Kenya", years=YEARS, flow=flow
        )

    expected = _expected(
        ("OECD_CRS", "gross_disbursements", spending("disbursements")),
        ("OECD_CRS", "commitments", spending("commitments")),
    )
    assert len(data)
    pd.testing.assert_frame_equal(data, expected)


@pytest.mark.parametrize(
    ("configure", "load", "spending"),
    [
        (lambda data: data, {"methodology": "OECD"}, {"method": cf.methods.OECD}),
        (
            lambda data: data.set_custom_spending_methodology((0.5, 0.8), False),
            {"methodology": "custom"},
            {"method": Method(significant=0.5, principal=0.8, highest_marker=False)},
        ),
        (
            lambda data: data.set_only_oda(),
            {"source": "OECD_CRDF_DONOR"},
            {"oda_only": True, "view": "provider"},
        ),
    ],
)
def test_1x_settings_map_onto_spending_arguments(
    configure: Callable[[ClimateData], ClimateData],
    load: dict[str, str],
    spending: dict[str, object],
) -> None:
    climate_data = configure(_climate_data())
    data = climate_data.load_spending_data(**load).get_data()

    source = load.get("source", "OECD_CRS")
    expected = _expected(
        (source, "gross_disbursements", RELEASE.spending(years=YEARS, **spending))
    )
    pd.testing.assert_frame_equal(data, expected)


def test_the_methodology_argument_overrides_the_set_methods_as_in_1x() -> None:
    data = _climate_data().set_oecd_spending_methodology().load_spending_data()

    expected = _expected(
        ("OECD_CRS", "gross_disbursements", RELEASE.spending(years=YEARS))
    )
    pd.testing.assert_frame_equal(data.get_data(), expected)


def test_importing_the_package_warns_only_when_a_1x_name_is_used() -> None:
    quiet = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::DeprecationWarning",
            "-c",
            "import climate_finance",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert quiet.returncode == 0, quiet.stderr

    with pytest.warns(DeprecationWarning, match="climate_finance.ClimateData"):
        from climate_finance import ClimateData as Imported

    assert Imported is ClimateData


def test_constructing_climate_data_warns_with_the_replacement() -> None:
    with pytest.warns(DeprecationWarning, match="goes in climate-finance 3.0"):
        ClimateData(years=YEARS, release=RELEASE)


@pytest.mark.parametrize(
    ("call", "replacement"),
    [
        (
            lambda: _climate_data().load_spending_data(flows="net_disbursements"),
            "gross disbursements only",
        ),
        (lambda: _climate_data().load_spending_data(source="UNFCCC"), "reconcile"),
        (lambda: _climate_data().as_share_of_total(), "share_of_total"),
        (lambda: ClimateData.available_flows(), "flows.list"),
        (
            lambda: set_climate_finance_data_path("data"),
            "CLIMATE_FINANCE_CACHE",
        ),
    ],
)
def test_1x_calls_without_an_equivalent_name_the_replacement(
    call: Callable[[], object], replacement: str
) -> None:
    with pytest.raises(NotImplementedError, match=replacement):
        call()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"prices": "constant"}, "base year"),
        ({"base_year": 2022}, "base year"),
        ({"currency": "JPY"}, "currency"),
    ],
)
def test_choices_1x_refused_are_still_refused(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message), warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ClimateData(years=YEARS, release=RELEASE, **kwargs)


def test_custom_methodology_needs_its_coefficients_first() -> None:
    with pytest.raises(ValueError, match="set_custom_spending_methodology"):
        _climate_data().load_spending_data(methodology="custom")


def test_get_data_before_loading_raises_as_in_1x() -> None:
    with pytest.raises(AttributeError, match="No data has been loaded"):
        _climate_data().get_data()
