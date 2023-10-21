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
