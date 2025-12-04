"""Integration test to demonstrate all conftest fixtures work together."""
import pandas as pd
import pytest
from climate_finance.common.schema import ClimateSchema


def test_all_fixtures_work_together(
    tmp_data_path,
    sample_crs_row,
    sample_crs_df,
    sample_contributions_df,
    climate_data_instance,
):
    """Test that all fixtures can be used together in a single test."""
    # Test tmp_data_path
    assert tmp_data_path.exists()

    # Test sample_crs_row
    assert sample_crs_row[ClimateSchema.YEAR] == 2020

    # Test sample_crs_df
    assert len(sample_crs_df) == 20
    assert ClimateSchema.ADAPTATION in sample_crs_df.columns

    # Test sample_contributions_df
    assert len(sample_contributions_df) == 10
    assert ClimateSchema.AGENCY_CODE in sample_contributions_df.columns

    # Test climate_data_instance
    assert climate_data_instance.years == [2020, 2021]

    # Demonstrate that fixtures produce valid data structures
    assert isinstance(sample_crs_df, pd.DataFrame)
    assert isinstance(sample_contributions_df, pd.DataFrame)


def test_sample_crs_df_can_be_filtered(sample_crs_df):
    """Test that sample_crs_df can be filtered like real data."""
    # Filter by year
    df_2020 = sample_crs_df[sample_crs_df[ClimateSchema.YEAR] == 2020]
    assert len(df_2020) > 0
    assert all(df_2020[ClimateSchema.YEAR] == 2020)

    # Filter by provider
    df_france = sample_crs_df[sample_crs_df[ClimateSchema.PROVIDER_CODE] == 4]
    assert len(df_france) > 0
    assert all(df_france[ClimateSchema.PROVIDER_CODE] == 4)


def test_sample_contributions_df_has_valid_structure(sample_contributions_df):
    """Test that sample_contributions_df has the structure needed for multilateral tests."""
    required_columns = [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.USD_COMMITMENT,
    ]

    for col in required_columns:
        assert col in sample_contributions_df.columns, f"Missing column: {col}"

    # Check data types
    assert sample_contributions_df[ClimateSchema.YEAR].dtype in [int, "int64", "int32"]
    assert sample_contributions_df[ClimateSchema.USD_COMMITMENT].dtype in [
        float,
        "float64",
    ]


def test_mock_prevents_network_calls(mock_oda_data_download):
    """Test that the mock fixture prevents actual network calls."""
    from oda_data import ODAData

    # Create an ODAData instance (normally would try to download)
    oda = ODAData(years=[2020])

    # Call load_indicator (normally would download data)
    # With the mock, this should do nothing
    oda.load_indicator(indicators=["test_indicator"])

    # Call get_data (normally would return downloaded data)
    # With the mock, this should return an empty DataFrame
    result = oda.get_data()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0  # Empty because it's mocked
