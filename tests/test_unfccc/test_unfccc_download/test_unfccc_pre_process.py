import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from climate_finance.unfccc.download.pre_process import (
    clean_unfccc,
    map_channel_names_to_oecd_codes,
)


def test_clean_unfccc():
    # Define a mock DataFrame
    df = pd.DataFrame(
        {
            "value": ["100", "200", "300", "100"],
            "Year": ["2020", "2021", "2022", "2023"],
            "Type of support": ["Support1", "Support2", "Support3", "Support4"],
            "currency": ["(USD)", "EUR", "JPY", "GBP"],
        }
    )

    # Call the function
    result = clean_unfccc(df)

    # Check the result
    assert isinstance(result, pd.DataFrame)
    assert "value" in result.columns
    assert "year" in result.columns
    assert "status" not in result.columns
    assert "funding_source" not in result.columns

    # Check the data cleaning
    assert result["value"].dtype == "float64"
    assert result["year"].dtype == "Int32"
    assert list(result.currency) == ["USD", "EUR", "JPY", "GBP"]


@patch(
    "climate_finance.unfccc.download.pre_process.generate_channel_mapping_dictionary",
    return_value={"UNFCCC": 1, "World Bank IBRD": 2, "IBRD": 3},
)
@patch("climate_finance.unfccc.download.pre_process.add_channel_names")
def test_map_channel_names_to_oecd_codes(
    mock_generate_channel_mapping_dictionary, mock_add_channel_names
):
    # Define a mock DataFrame
    df = pd.DataFrame(
        {
            "channel_name": [
                "UNFCCC",
                "World Bank IBRD",
                "IBRD",
            ]
        }
    )

    # Call the function
    map_channel_names_to_oecd_codes(df, "channel_name", None)

    # Check if the mocked functions were called
    mock_generate_channel_mapping_dictionary.assert_called()
    mock_add_channel_names.assert_called()
