"""Test the basic fixtures from conftest.py"""
from climate_finance.common.schema import ClimateSchema


def test_tmp_data_path_fixture(tmp_data_path):
    """Verify tmp_data_path fixture creates a directory."""
    assert tmp_data_path.exists()
    assert tmp_data_path.is_dir()
    assert tmp_data_path.name == "test_data"


def test_sample_crs_row_fixture(sample_crs_row):
    """Verify sample_crs_row fixture has required fields."""
    assert ClimateSchema.YEAR in sample_crs_row
    assert ClimateSchema.PROVIDER_CODE in sample_crs_row
    assert ClimateSchema.RECIPIENT_CODE in sample_crs_row
    assert ClimateSchema.ADAPTATION in sample_crs_row
    assert ClimateSchema.MITIGATION in sample_crs_row
    assert ClimateSchema.USD_COMMITMENT in sample_crs_row
    assert ClimateSchema.USD_DISBURSEMENT in sample_crs_row

    # Verify values
    assert sample_crs_row[ClimateSchema.YEAR] == 2020
    assert sample_crs_row[ClimateSchema.PROVIDER_CODE] == 4
    assert sample_crs_row[ClimateSchema.ADAPTATION] == 2  # Principal
    assert sample_crs_row[ClimateSchema.MITIGATION] == 1  # Significant
