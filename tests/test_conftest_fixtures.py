"""Test to verify conftest fixtures are working correctly."""
import pandas as pd
from climate_finance.common.schema import ClimateSchema


def test_tmp_data_path_fixture(tmp_data_path):
    """Test that tmp_data_path fixture creates a valid directory."""
    assert tmp_data_path.exists()
    assert tmp_data_path.is_dir()


def test_sample_crs_row_fixture(sample_crs_row):
    """Test that sample_crs_row fixture returns valid data."""
    assert isinstance(sample_crs_row, dict)
    assert ClimateSchema.YEAR in sample_crs_row
    assert ClimateSchema.PROVIDER_CODE in sample_crs_row
    assert sample_crs_row[ClimateSchema.YEAR] == 2020
    assert sample_crs_row[ClimateSchema.PROVIDER_CODE] == 4


def test_sample_crs_df_fixture(sample_crs_df):
    """Test that sample_crs_df fixture returns valid DataFrame."""
    assert isinstance(sample_crs_df, pd.DataFrame)
    assert len(sample_crs_df) == 20
    assert ClimateSchema.YEAR in sample_crs_df.columns
    assert ClimateSchema.PROVIDER_CODE in sample_crs_df.columns
    assert ClimateSchema.ADAPTATION in sample_crs_df.columns
    assert ClimateSchema.MITIGATION in sample_crs_df.columns
    # Check that providers are in the expected set
    assert all(sample_crs_df[ClimateSchema.PROVIDER_CODE].isin([4, 12]))
    # Check that years are in the expected set
    assert all(sample_crs_df[ClimateSchema.YEAR].isin([2020, 2021]))


def test_sample_contributions_df_fixture(sample_contributions_df):
    """Test that sample_contributions_df fixture returns valid DataFrame."""
    assert isinstance(sample_contributions_df, pd.DataFrame)
    assert len(sample_contributions_df) == 10
    assert ClimateSchema.YEAR in sample_contributions_df.columns
    assert ClimateSchema.PROVIDER_CODE in sample_contributions_df.columns
    assert ClimateSchema.AGENCY_CODE in sample_contributions_df.columns
    assert ClimateSchema.USD_COMMITMENT in sample_contributions_df.columns
    # Check that providers are in the expected set
    assert all(sample_contributions_df[ClimateSchema.PROVIDER_CODE].isin([4, 12]))
    # Check that agencies are in the expected set
    assert all(sample_contributions_df[ClimateSchema.AGENCY_CODE].isin([901, 905]))


def test_climate_data_instance_fixture(climate_data_instance):
    """Test that climate_data_instance fixture returns valid ClimateData object."""
    from climate_finance import ClimateData

    assert isinstance(climate_data_instance, ClimateData)
    # Check that the instance has expected attributes
    assert climate_data_instance.years == [2020, 2021]
    assert climate_data_instance.providers == [4, 12]
    assert climate_data_instance.currency == "USD"
    assert climate_data_instance.prices == "current"


def test_mock_oda_data_download_fixture(mock_oda_data_download):
    """Test that mock_oda_data_download fixture is applied."""
    # This fixture doesn't return anything, it just sets up the mock
    # If the fixture runs without error, it's working
    assert True
