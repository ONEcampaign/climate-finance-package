import pandas as pd
from climate_finance.common.schema import ClimateSchema
from climate_finance.common.analysis_tools import (
    get_providers_filter,
    get_recipients_filter,
    filter_providers,
)
from tests.fixtures.builders import build_crs_data


def test_get_providers_filter_with_single_provider():
    """Test that get_providers_filter returns correct tuple for single provider."""
    result = get_providers_filter(provider_codes=[4])

    # Should return a tuple (column_name, operator, values)
    assert isinstance(result, tuple)
    assert len(result) == 3
    assert result[0] == "donor_code"  # column name
    assert result[1] == "in"  # operator
    assert result[2] == [4]  # values


def test_get_providers_filter_with_multiple_providers():
    """Test that get_providers_filter returns correct tuple for multiple providers."""
    result = get_providers_filter(provider_codes=[4, 12, 6])

    assert isinstance(result, tuple)
    assert result[2] == [4, 12, 6]


def test_get_providers_filter_with_none_returns_none():
    """Test that providers=None returns None in the values."""
    result = get_providers_filter(provider_codes=None)

    assert result[2] is None


def test_get_recipients_filter_works():
    """Test that get_recipients_filter returns correct tuple."""
    result = get_recipients_filter(recipient_codes=[50])

    assert isinstance(result, tuple)
    assert result[0] == "recipient_code"
    assert result[1] == "in"
    assert result[2] == [50]


def test_filter_providers_with_single_provider():
    """Test filtering DataFrame by a single provider code."""
    df = build_crs_data(providers=[4, 12, 6], n_rows=30)

    filtered = filter_providers(df, provider_codes=[4])

    assert all(filtered[ClimateSchema.PROVIDER_CODE] == 4)
    assert len(filtered) < len(df)


def test_filter_providers_with_multiple_providers():
    """Test filtering DataFrame by multiple provider codes."""
    df = build_crs_data(providers=[4, 12, 6], n_rows=30)

    filtered = filter_providers(df, provider_codes=[4, 12])

    assert all(filtered[ClimateSchema.PROVIDER_CODE].isin([4, 12]))
    assert not any(filtered[ClimateSchema.PROVIDER_CODE] == 6)


def test_filter_providers_with_none_returns_all():
    """Test that providers=None returns all data."""
    df = build_crs_data(providers=[4, 12], n_rows=20)

    filtered = filter_providers(df, provider_codes=None)

    assert len(filtered) == len(df)


# Tests for groupby and aggregation functions
from climate_finance.core.tools import groupby_sum  # noqa: E402


def test_groupby_sum_aggregates_values_correctly():
    """Test that groupby_sum correctly aggregates values."""
    df = pd.DataFrame([
        {ClimateSchema.YEAR: 2020, ClimateSchema.PROVIDER_CODE: 4, ClimateSchema.VALUE: 100},
        {ClimateSchema.YEAR: 2020, ClimateSchema.PROVIDER_CODE: 4, ClimateSchema.VALUE: 200},
        {ClimateSchema.YEAR: 2021, ClimateSchema.PROVIDER_CODE: 4, ClimateSchema.VALUE: 300},
    ])

    result = groupby_sum(
        df,
        groupby=[ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE],
    )

    # 2020 should have 300, 2021 should have 300
    assert len(result) == 2
    assert result[result[ClimateSchema.YEAR] == 2020][ClimateSchema.VALUE].iloc[0] == 300
    assert result[result[ClimateSchema.YEAR] == 2021][ClimateSchema.VALUE].iloc[0] == 300


def test_groupby_sum_preserves_non_numeric_columns():
    """Test that groupby_sum preserves non-numeric groupby columns."""
    df = pd.DataFrame([
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.PROVIDER_CODE: 4,
            ClimateSchema.PROVIDER_NAME: "France",
            ClimateSchema.VALUE: 100,
        },
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.PROVIDER_CODE: 4,
            ClimateSchema.PROVIDER_NAME: "France",
            ClimateSchema.VALUE: 200,
        },
    ])

    result = groupby_sum(
        df,
        groupby=[ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE, ClimateSchema.PROVIDER_NAME],
    )

    assert result[ClimateSchema.PROVIDER_NAME].iloc[0] == "France"
