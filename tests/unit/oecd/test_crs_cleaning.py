import pandas as pd
import pytest
from climate_finance.common.schema import ClimateSchema
from climate_finance.oecd.cleaning_tools.tools import (
    keep_only_allocable_aid,
    replace_missing_climate_with_zero,
    clean_adaptation_and_mitigation_columns,
    fix_crs_year_encoding,
    convert_flows_millions_to_units,
)
from tests.fixtures.builders import build_crs_data


def test_keep_only_allocable_aid_filters_correctly():
    """Test that allocable aid filter removes non-allocable flows."""
    # Create test data with mix of allocable and non-allocable
    df = build_crs_data(n_rows=10)

    # Add FLOW_MODALITY column with mix of allocable and non-allocable types
    df[ClimateSchema.FLOW_MODALITY] = ["A02", "B01", "B03", "X99", "Y99",
                                         "A02", "C01", "D01", "Z99", "E01"]

    result = keep_only_allocable_aid(df)

    # Should only keep allocable flows
    assert len(result) < len(df)
    # Should have exactly 7 rows (A02 twice, B01, B03, A02, C01, D01, E01)
    assert len(result) == 7
    # All rows should have allocable flow modalities
    allocable_types = ["A02", "B01", "B03", "B031", "B032", "B033", "B04", "C01", "D01", "D02", "E01"]
    assert all(result[ClimateSchema.FLOW_MODALITY].isin(allocable_types))


def test_allocable_filter_preserves_all_columns():
    """Test that allocable filter doesn't drop required columns."""
    df = build_crs_data(n_rows=5)

    # Add FLOW_MODALITY column with all allocable types
    df[ClimateSchema.FLOW_MODALITY] = "A02"

    # Store original columns
    original_columns = set(df.columns)

    result = keep_only_allocable_aid(df)

    # Should preserve all original columns
    assert set(result.columns) == original_columns
    assert ClimateSchema.YEAR in result.columns
    assert ClimateSchema.PROVIDER_CODE in result.columns
    assert ClimateSchema.ADAPTATION in result.columns
    assert ClimateSchema.MITIGATION in result.columns
    assert ClimateSchema.USD_COMMITMENT in result.columns


def test_keep_only_allocable_aid_returns_empty_if_no_allocable():
    """Test that filter returns empty DataFrame if no allocable flows."""
    df = build_crs_data(n_rows=3)

    # Add only non-allocable types
    df[ClimateSchema.FLOW_MODALITY] = ["X99", "Y99", "Z99"]

    result = keep_only_allocable_aid(df)

    # Should return empty DataFrame
    assert len(result) == 0
    # But should still have the same columns
    assert set(result.columns) == set(df.columns)


def test_keep_only_allocable_aid_handles_all_allocable_types():
    """Test that filter recognizes all defined allocable types."""
    allocable_types = ["A02", "B01", "B03", "B031", "B032", "B033", "B04", "C01", "D01", "D02", "E01"]

    # Create DataFrame with each allocable type
    df = build_crs_data(n_rows=len(allocable_types))
    df[ClimateSchema.FLOW_MODALITY] = allocable_types

    result = keep_only_allocable_aid(df)

    # All rows should be retained
    assert len(result) == len(allocable_types)
    # Verify each type is present
    for aid_type in allocable_types:
        assert aid_type in result[ClimateSchema.FLOW_MODALITY].values


def test_replace_missing_climate_with_zero():
    """Test that missing values are replaced with '0' string."""
    df = pd.DataFrame({
        ClimateSchema.ADAPTATION: [1, 2, "nan", 0],
        ClimateSchema.MITIGATION: [2, "nan", 1, "nan"],
        ClimateSchema.VALUE: [1000, 2000, 3000, 4000]
    })

    # Replace missing in adaptation column
    result = replace_missing_climate_with_zero(df, ClimateSchema.ADAPTATION)

    # "nan" string should be replaced with "0"
    assert "nan" not in result[ClimateSchema.ADAPTATION].values
    assert "0" in result[ClimateSchema.ADAPTATION].astype(str).values
    # Other columns should remain unchanged
    assert "nan" in result[ClimateSchema.MITIGATION].values


def test_clean_adaptation_and_mitigation_columns_fills_na():
    """Test that NaN values in climate columns are filled with 0."""
    df = pd.DataFrame({
        ClimateSchema.ADAPTATION: [1, pd.NA, 2, pd.NA],
        ClimateSchema.MITIGATION: [pd.NA, 1, pd.NA, 2],
        ClimateSchema.VALUE: [1000, 2000, 3000, 4000]
    })

    result = clean_adaptation_and_mitigation_columns(df)

    # No NaN should remain in climate columns
    assert result[ClimateSchema.ADAPTATION].isna().sum() == 0
    assert result[ClimateSchema.MITIGATION].isna().sum() == 0
    # NaN should be replaced with 0
    assert 0 in result[ClimateSchema.ADAPTATION].values
    assert 0 in result[ClimateSchema.MITIGATION].values


def test_fix_crs_year_encoding_removes_bom():
    """Test that year encoding fix removes BOM characters."""
    df = pd.DataFrame({
        ClimateSchema.YEAR: ["\ufeff2020", "2021", "\ufeff2022"],
        ClimateSchema.VALUE: [1000, 2000, 3000]
    })

    result = fix_crs_year_encoding(df)

    # BOM character should be removed
    assert "\ufeff" not in result[ClimateSchema.YEAR].str.cat()
    assert "2020" in result[ClimateSchema.YEAR].values
    assert "2021" in result[ClimateSchema.YEAR].values
    assert "2022" in result[ClimateSchema.YEAR].values


def test_convert_flows_millions_to_units():
    """Test that flow values are correctly converted from millions to units."""
    df = pd.DataFrame({
        ClimateSchema.USD_COMMITMENT: [1.5, 2.0, 3.5],  # millions
        ClimateSchema.USD_DISBURSEMENT: [1.0, 1.5, 2.5],  # millions
        ClimateSchema.YEAR: [2020, 2021, 2022]
    })

    flow_columns = [ClimateSchema.USD_COMMITMENT, ClimateSchema.USD_DISBURSEMENT]
    result = convert_flows_millions_to_units(df, flow_columns)

    # Values should be multiplied by 1e6
    assert result[ClimateSchema.USD_COMMITMENT].iloc[0] == pytest.approx(1.5e6)
    assert result[ClimateSchema.USD_COMMITMENT].iloc[1] == pytest.approx(2.0e6)
    assert result[ClimateSchema.USD_DISBURSEMENT].iloc[0] == pytest.approx(1.0e6)


def test_convert_flows_millions_preserves_other_columns():
    """Test that conversion only affects specified flow columns."""
    df = pd.DataFrame({
        ClimateSchema.USD_COMMITMENT: [1.5, 2.0],
        ClimateSchema.YEAR: [2020, 2021],
        ClimateSchema.PROVIDER_CODE: [4, 12]
    })

    result = convert_flows_millions_to_units(df, [ClimateSchema.USD_COMMITMENT])

    # Only USD_COMMITMENT should change
    assert result[ClimateSchema.USD_COMMITMENT].iloc[0] == pytest.approx(1.5e6)
    # Other columns should remain unchanged
    assert result[ClimateSchema.YEAR].iloc[0] == 2020
    assert result[ClimateSchema.PROVIDER_CODE].iloc[0] == 4


def test_keep_only_allocable_aid_resets_index():
    """Test that the filter properly resets the index."""
    df = build_crs_data(n_rows=5)
    df[ClimateSchema.FLOW_MODALITY] = ["A02", "X99", "B01", "Y99", "C01"]

    # Set a non-standard index
    df.index = [10, 20, 30, 40, 50]

    result = keep_only_allocable_aid(df)

    # Index should be reset to 0, 1, 2
    assert list(result.index) == [0, 1, 2]
    assert len(result) == 3


def test_allocable_filter_with_missing_flow_modality():
    """Test behavior when FLOW_MODALITY column has NaN values."""
    df = build_crs_data(n_rows=4)
    df[ClimateSchema.FLOW_MODALITY] = ["A02", pd.NA, "B01", pd.NA]

    result = keep_only_allocable_aid(df)

    # NaN values should be filtered out (not in allocable list)
    assert len(result) == 2
    assert result[ClimateSchema.FLOW_MODALITY].isna().sum() == 0


def test_clean_adaptation_and_mitigation_preserves_valid_values():
    """Test that valid marker values are preserved after cleaning."""
    df = pd.DataFrame({
        ClimateSchema.ADAPTATION: [0, 1, 2, pd.NA, 1],
        ClimateSchema.MITIGATION: [2, pd.NA, 0, 1, 2],
        ClimateSchema.VALUE: [1000, 2000, 3000, 4000, 5000]
    })

    result = clean_adaptation_and_mitigation_columns(df)

    # Valid values should be preserved
    assert 1 in result[ClimateSchema.ADAPTATION].values
    assert 2 in result[ClimateSchema.ADAPTATION].values
    assert 2 in result[ClimateSchema.MITIGATION].values
    # Total number of rows should remain the same
    assert len(result) == 5
