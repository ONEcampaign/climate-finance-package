"""`ClimateData`, the deprecated 1.x wrapper, against `spending()` on the fixture."""

import subprocess
import sys
import warnings
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance import compat
from climate_finance._removed import REMOVED_MODULES
from climate_finance.compat import ClimateData, set_climate_finance_data_path
from climate_finance.errors import (
    ClimateFinanceDeprecationWarning,
    CoverageWarning,
    RemovedError,
    RemovedModuleError,
)
from climate_finance.methods import Method
from climate_finance.spec import Flow, SpecInputs

RELEASE = cf.load(path=FIXTURE_DIR)
YEARS = range(2021, 2025)
# The fixture's commitments end in 2022, two years before its disbursements.
UNCOVERED_COMMITMENTS = "years 2023 to 2024 return no rows"


def _climate_data(**kwargs: Any) -> ClimateData:
    with pytest.warns(ClimateFinanceDeprecationWarning, match=r"spending\(\.\.\.\)"):
        return ClimateData(years=YEARS, release=RELEASE, **kwargs)


def _expected(*parts: tuple[str, str, cf.Result]) -> pd.DataFrame:
    """`spending()` tables with the `flow` and `source` columns `get_data` adds."""
    frame = pl.concat(
        result.table.with_columns(flow=pl.lit(flow), source=pl.lit(source))
        for source, flow, result in parts
    )
    return frame.to_pandas()


def test_get_data_equals_traced_spending_for_each_flow() -> None:
    climate_data = _climate_data(providers=[5, "France"], recipients="Kenya")
    with pytest.warns(CoverageWarning, match=UNCOVERED_COMMITMENTS):
        data = climate_data.load_spending_data(
            flows=["gross_disbursements", "commitments", "grant_equivalent"],
            source="OECD_CRDF_CRS",
        ).get_data()

    def spending(flow: Flow) -> cf.Result:
        return RELEASE.spending(
            providers=[5, "France"],
            recipients="Kenya",
            years=YEARS,
            flow=flow,
            untraced=False,
        )

    with pytest.warns(CoverageWarning, match=UNCOVERED_COMMITMENTS):
        commitments = spending("commitments")
    expected = _expected(
        ("OECD_CRDF_CRS", "gross_disbursements", spending("disbursements")),
        ("OECD_CRDF_CRS", "commitments", commitments),
        ("OECD_CRDF_CRS", "grant_equivalent", spending("grant_equivalent")),
    )
    assert len(data)
    pd.testing.assert_frame_equal(data, expected)


@pytest.mark.parametrize(
    ("configure", "load", "spending"),
    [
        (
            lambda data: data,
            {"methodology": "OECD", "source": "OECD_CRDF", "flows": "commitments"},
            {"method": cf.methods.OECD, "flow": "commitments"},
        ),
        (
            lambda data: data.set_custom_spending_methodology((0.5, 0.8), False),
            {"methodology": "custom", "source": "OECD_CRDF_CRS"},
            {"method": Method(significant=0.5, principal=0.8, highest_marker=False)},
        ),
        (
            lambda data: data.set_only_oda(),
            {"source": "OECD_CRDF_DONOR", "flows": "commitments"},
            {"oda_only": True, "view": "provider", "flow": "commitments"},
        ),
    ],
)
# The fixture's commitments end before YEARS do, which the commitment rows warn of.
@pytest.mark.filterwarnings("ignore::climate_finance.errors.CoverageWarning")
def test_1x_settings_map_onto_spending_arguments(
    configure: Callable[[ClimateData], ClimateData],
    load: dict[str, str],
    spending: SpecInputs,
) -> None:
    climate_data = configure(_climate_data())
    data = climate_data.load_spending_data(**load).get_data()

    inputs: SpecInputs = {**spending, "years": YEARS, "untraced": False}
    flow = load.get("flows", "gross_disbursements")
    expected = _expected((load["source"], flow, RELEASE.spending(**inputs)))
    pd.testing.assert_frame_equal(data, expected)


def test_the_methodology_argument_overrides_the_set_methods_as_in_1x() -> None:
    data = (
        _climate_data()
        .set_oecd_spending_methodology()
        .load_spending_data(source="OECD_CRDF_CRS")
    )

    expected = _expected(
        (
            "OECD_CRDF_CRS",
            "gross_disbursements",
            RELEASE.spending(years=YEARS, untraced=False),
        )
    )
    pd.testing.assert_frame_equal(data.get_data(), expected)


def test_the_data_path_is_where_climate_data_downloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `load` stands in for the download, so the test holds whatever an index entry
    # or a cached release must carry.
    loads: list[dict[str, object]] = []

    def load(**kwargs: object) -> cf.Release:
        loads.append(kwargs)
        return RELEASE

    monkeypatch.setattr(compat, "load", load)
    monkeypatch.setattr(compat, "_cache_dir", None)

    with pytest.warns(ClimateFinanceDeprecationWarning, match="cache_dir="):
        set_climate_finance_data_path(tmp_path)
    with pytest.warns(ClimateFinanceDeprecationWarning):
        ClimateData(years=YEARS).load_spending_data(source="OECD_CRDF_CRS")

    assert loads == [{"cache_dir": tmp_path.resolve(), "refresh": False}]


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
        encoding="utf-8",
        check=False,
    )
    assert quiet.returncode == 0, quiet.stderr

    with pytest.warns(DeprecationWarning, match=r"use climate_finance.load\(\)"):
        from climate_finance import ClimateData as Imported

    assert Imported is ClimateData


def test_constructing_climate_data_warns_with_the_replacement() -> None:
    with pytest.warns(DeprecationWarning, match="goes in 3.0"):
        ClimateData(years=YEARS, release=RELEASE)


@pytest.mark.parametrize(
    ("call", "removed", "replacement"),
    [
        (
            lambda: _climate_data().load_spending_data(flows="net_disbursements"),
            "flows='net_disbursements'",
            "spending(flow='net_disbursements')",
        ),
        # 1.x's default source, whose meaning 2.0 changed.
        (
            lambda: _climate_data().load_spending_data(),
            "load_spending_data() without source=, which 1.x read as source='OECD_CRS'",
            "source='OECD_CRDF_CRS'",
        ),
        (
            lambda: _climate_data().load_spending_data(source="OECD_CRS"),
            "source='OECD_CRS'",
            "source='OECD_CRDF_CRS'",
        ),
        (
            lambda: _climate_data().load_spending_data(source="OECD_CRS_ALLOCABLE"),
            "source='OECD_CRS_ALLOCABLE'",
            "source='OECD_CRDF_CRS'",
        ),
        (
            lambda: _climate_data().load_spending_data(
                source="OECD_CRDF_CRS_ALLOCABLE"
            ),
            "source='OECD_CRDF_CRS_ALLOCABLE'",
            "spending(untraced=False)",
        ),
        (
            lambda: _climate_data().load_spending_data(source="OECD_CRDF"),
            "source='OECD_CRDF' with flows='gross_disbursements'",
            "source='OECD_CRDF_CRS'",
        ),
        (
            lambda: _climate_data().load_spending_data(
                source="OECD_CRDF_DONOR",
                flows=["commitments", "grant_equivalent"],
            ),
            "source='OECD_CRDF_DONOR' with flows='grant_equivalent'",
            "view='provider'",
        ),
        (
            lambda: _climate_data().load_spending_data(source="UNFCCC"),
            "source='UNFCCC'",
            "reconcile()",
        ),
        (lambda: compat.get_unfccc_data(), "get_unfccc_data", "reconcile()"),
        (
            lambda: _climate_data().as_share_of_total(),
            "as_share_of_total",
            "share_of_total()",
        ),
        (lambda: ClimateData.available_flows(), "available_flows", "flows.list()"),
    ],
)
def test_1x_choices_of_changed_meaning_or_none_name_the_2_0_call(
    call: Callable[[], object], removed: str, replacement: str
) -> None:
    with pytest.raises(RemovedError) as refused:
        call()

    assert refused.value.removed == removed
    assert replacement in refused.value.replacement


@pytest.mark.parametrize("module", sorted(REMOVED_MODULES))
def test_a_removed_1x_module_names_its_replacement(module: str) -> None:
    with pytest.raises(RemovedModuleError) as refused:
        import_module(f"climate_finance.{module}.anything")

    assert refused.value.removed == f"climate_finance.{module}"
    assert isinstance(refused.value, ImportError)


def test_importing_a_removed_module_from_the_package_raises_the_same_error() -> None:
    with pytest.raises(RemovedModuleError):
        from climate_finance import unfccc  # noqa: F401


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"prices": "constant"}, "base year"),
        ({"base_year": 2022}, "base year"),
        ({"currency": "JPY"}, "currency"),
    ],
)
def test_choices_1x_refused_are_still_refused(
    kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message), warnings.catch_warnings():
        warnings.simplefilter("ignore", ClimateFinanceDeprecationWarning)
        ClimateData(years=YEARS, release=RELEASE, **kwargs)


def test_custom_methodology_needs_its_coefficients_first() -> None:
    with pytest.raises(ValueError, match="set_custom_spending_methodology"):
        _climate_data().load_spending_data(methodology="custom")


def test_get_data_without_pandas_names_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    climate_data = _climate_data().load_spending_data(source="OECD_CRDF_CRS")
    monkeypatch.setitem(sys.modules, "pandas", None)

    with pytest.raises(ImportError, match=r"climate-finance\[pandas\]"):
        climate_data.get_data()


def test_get_data_before_loading_raises_as_in_1x() -> None:
    with pytest.raises(AttributeError, match="No data has been loaded"):
        _climate_data().get_data()
