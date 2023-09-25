from unittest.mock import patch

import pytest

from climate_finance.oecd.get_oecd_data import get_oecd_bilateral


@patch("climate_finance.oecd.get_oecd_data.get_crs_allocable_spending")
@patch("climate_finance.oecd.get_oecd_data.check_and_filter_parties")
@patch("climate_finance.oecd.get_oecd_data.base_oecd_transform_markers_into_indicators")
@patch.dict(
    "climate_finance.oecd.get_oecd_data.BILATERAL_CLIMATE_METHODOLOGY",
    {"oecd_bilateral": lambda d: None},
)
def test_get_oecd_bilateral(mock_transform, mock_filter, mock_get_crs):
    # Test with default parameters
    get_oecd_bilateral(2000, 2020)
    mock_get_crs.assert_called_once_with(
        start_year=2000, end_year=2020, force_update=False
    )
    mock_filter.assert_called_once()

    # Test with specific party
    get_oecd_bilateral(2000, 2020, party="party1")
    mock_filter.assert_called_with(mock_get_crs.return_value, "party1")

    # Test with update_data=True
    get_oecd_bilateral(2000, 2020, update_data=True)
    mock_get_crs.assert_called_with(start_year=2000, end_year=2020, force_update=True)

    # Test with invalid methodology
    with pytest.raises(ValueError):
        get_oecd_bilateral(
            2000,
            2020,
            methodology="invalid_methodology",
        )
