import pytest
from climate_finance import ClimateData
from climate_finance.common.schema import ClimateSchema


@pytest.mark.integration
def test_climate_data_loads_spending_with_oecd_methodology(setup_test_data):
    """Test full workflow: Create ClimateData → load with OECD → get results."""
    climate_data = ClimateData(
        years=[2020, 2021],
        providers=[4, 12],
        currency="USD",
        prices="current",
    )

    climate_data.load_spending_data(
        methodology="OECD",
        source="OECD_CRS",
        flows="gross_disbursements",
    )

    result = climate_data.get_data()

    # Should have data
    assert len(result) > 0
    # Should have climate indicators
    assert ClimateSchema.INDICATOR in result.columns
    # Should only have specified providers
    assert all(result[ClimateSchema.PROVIDER_CODE].isin([4, 12]))
    # Should only have specified years
    assert all(result[ClimateSchema.YEAR].isin([2020, 2021]))


@pytest.mark.integration
def test_one_methodology_produces_lower_totals_than_oecd(setup_test_data):
    """Test that ONE methodology (with 0.4 discount) produces lower totals."""
    # Load with OECD methodology
    climate_data_oecd = ClimateData(
        years=[2020, 2021],
        providers=[4, 12],
        currency="USD",
        prices="current",
    )
    climate_data_oecd.load_spending_data(
        methodology="OECD",
        source="OECD_CRS",
        flows="gross_disbursements",
    )
    result_oecd = climate_data_oecd.get_data()

    # Load with ONE methodology
    climate_data_one = ClimateData(
        years=[2020, 2021],
        providers=[4, 12],
        currency="USD",
        prices="current",
    )
    climate_data_one.load_spending_data(
        methodology="ONE",
        source="OECD_CRS",
        flows="gross_disbursements",
    )
    result_one = climate_data_one.get_data()

    # ONE total should be less than OECD total (due to 0.4 coefficient)
    total_oecd = result_oecd[ClimateSchema.VALUE].sum()
    total_one = result_one[ClimateSchema.VALUE].sum()

    assert total_one < total_oecd
    # ONE should be roughly 70-90% of OECD (depends on significant vs principal mix)
    assert total_one > total_oecd * 0.7
    assert total_one < total_oecd * 0.95
