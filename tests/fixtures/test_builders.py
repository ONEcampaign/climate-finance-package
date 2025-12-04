import pandas as pd
from climate_finance.common.schema import ClimateSchema
from tests.fixtures.builders import build_crs_data, build_contributions_data, build_spending_shares


def test_build_crs_data_creates_valid_dataframe():
    """Test that CRS builder creates DataFrame with required columns."""
    df = build_crs_data(
        years=[2020],
        providers=[4],
        n_rows=5
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert ClimateSchema.YEAR in df.columns
    assert ClimateSchema.PROVIDER_CODE in df.columns
    assert ClimateSchema.ADAPTATION in df.columns
    assert ClimateSchema.MITIGATION in df.columns


def test_build_crs_data_respects_marker_inputs():
    """Test that builder uses provided marker values."""
    df = build_crs_data(
        adaptation_markers=[2, 2, 2],
        mitigation_markers=[0, 0, 0],
        n_rows=3
    )

    assert all(df[ClimateSchema.ADAPTATION] == 2)
    assert all(df[ClimateSchema.MITIGATION] == 0)


def test_build_contributions_data_creates_valid_dataframe():
    """Test that contributions builder creates valid DataFrame."""
    df = build_contributions_data(years=[2020, 2021], providers=[4], n_rows=6)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 6
    assert ClimateSchema.AGENCY_CODE in df.columns
    assert ClimateSchema.USD_COMMITMENT in df.columns


def test_build_spending_shares_creates_valid_dataframe():
    """Test that spending shares builder creates valid DataFrame."""
    df = build_spending_shares(agencies=[901], climate_share=0.3, n_rows=3)

    assert isinstance(df, pd.DataFrame)
    assert ClimateSchema.SHARE in df.columns
    assert all(df[ClimateSchema.SHARE] > 0.25)
    assert all(df[ClimateSchema.SHARE] < 0.35)
