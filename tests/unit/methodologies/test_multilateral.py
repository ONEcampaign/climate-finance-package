import pandas as pd
import pytest
from climate_finance.common.schema import ClimateSchema
from climate_finance.core.tools import data_to_share, calculate_imputations, merge_spending_and_contributions


def test_data_to_share_calculates_correct_proportions():
    """Test that data_to_share converts spending amounts to shares."""
    # Create spending data for one agency: 300K climate, 700K non-climate
    data = pd.DataFrame([
        {
            ClimateSchema.AGENCY_CODE: 901,
            ClimateSchema.AGENCY_NAME: "World Bank",
            ClimateSchema.YEAR: 2020,
            ClimateSchema.INDICATOR: "climate_adaptation",
            ClimateSchema.VALUE: 300000,
        },
        {
            ClimateSchema.AGENCY_CODE: 901,
            ClimateSchema.AGENCY_NAME: "World Bank",
            ClimateSchema.YEAR: 2020,
            ClimateSchema.INDICATOR: "not_climate_relevant",
            ClimateSchema.VALUE: 700000,
        },
    ])

    result = data_to_share(
        data,
        groupby=[ClimateSchema.YEAR, ClimateSchema.AGENCY_CODE, ClimateSchema.INDICATOR],
        shareby=[ClimateSchema.YEAR, ClimateSchema.AGENCY_CODE],
        rolling_years=1,
    )

    climate_share = result[
        result[ClimateSchema.INDICATOR] == "climate_adaptation"
    ][ClimateSchema.VALUE].iloc[0]

    # Climate share should be 300K / 1M = 0.3
    assert climate_share == pytest.approx(0.3, rel=0.001)


def test_calculate_imputations_multiplies_shares_by_contributions():
    """Test that imputations = contributions × shares."""
    # Create contributions: France gives 10M to World Bank
    contributions = pd.DataFrame([
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.PROVIDER_CODE: 4,
            ClimateSchema.PROVIDER_NAME: "France",
            ClimateSchema.CHANNEL_CODE: 901,
            ClimateSchema.CHANNEL_NAME: "World Bank",
            ClimateSchema.VALUE: 10000000,
            ClimateSchema.FLOW_TYPE: "commitments",
        }
    ])

    # Create shares: World Bank spends 25% on climate
    shares = pd.DataFrame([
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.CHANNEL_CODE: 901,
            ClimateSchema.INDICATOR: "climate_adaptation",
            ClimateSchema.VALUE: 0.25,
            ClimateSchema.FLOW_TYPE: "commitments",
        }
    ])

    # Merge contributions and shares
    merged = merge_spending_and_contributions(
        spending_data=shares,
        contributions_data=contributions
    )

    result = calculate_imputations(merged)

    # Imputation should be 10M × 0.25 = 2.5M
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(2500000, rel=0.01)
    assert result[ClimateSchema.INDICATOR].iloc[0] == "climate_adaptation"
    assert result[ClimateSchema.PROVIDER_CODE].iloc[0] == 4


def test_calculate_imputations_handles_multiple_providers():
    """Test imputations work with multiple providers to same agency."""
    contributions = pd.DataFrame([
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.PROVIDER_CODE: 4,
            ClimateSchema.PROVIDER_NAME: "France",
            ClimateSchema.CHANNEL_CODE: 901,
            ClimateSchema.CHANNEL_NAME: "World Bank",
            ClimateSchema.VALUE: 10000000,
            ClimateSchema.FLOW_TYPE: "commitments",
        },
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.PROVIDER_CODE: 12,
            ClimateSchema.PROVIDER_NAME: "United Kingdom",
            ClimateSchema.CHANNEL_CODE: 901,
            ClimateSchema.CHANNEL_NAME: "World Bank",
            ClimateSchema.VALUE: 15000000,
            ClimateSchema.FLOW_TYPE: "commitments",
        },
    ])

    shares = pd.DataFrame([
        {
            ClimateSchema.YEAR: 2020,
            ClimateSchema.CHANNEL_CODE: 901,
            ClimateSchema.INDICATOR: "climate_adaptation",
            ClimateSchema.VALUE: 0.20,
            ClimateSchema.FLOW_TYPE: "commitments",
        }
    ])

    # Merge contributions and shares
    merged = merge_spending_and_contributions(
        spending_data=shares,
        contributions_data=contributions
    )

    result = calculate_imputations(merged)

    # France: 10M × 0.20 = 2M, UK: 15M × 0.20 = 3M, Total = 5M
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(5000000, rel=0.01)
    assert len(result) == 2
