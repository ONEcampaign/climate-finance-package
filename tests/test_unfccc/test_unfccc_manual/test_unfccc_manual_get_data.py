import pandas as pd
import pytest
from unittest.mock import patch, Mock
from climate_finance.unfccc.manual.get_data import (
    _table7x_pipeline,
    table7_pipeline,
    table7a_pipeline,
    table7b_pipeline,
)
from climate_finance.unfccc.manual.pre_process import (
    clean_table7,
    clean_table7a,
    clean_table7b,
)

# Mock Data
mock_br_data = {
    "CountryA": {
        "Table 7_2021": "Mock DataFrame A 2021",
        "Table 7(a)_2021": "Mock DataFrame A 2021(a)",
    },
    "CountryB": {
        "Table 7_2022": "Mock DataFrame B 2022",
        "Table 7(b)_2022": "Mock DataFrame B 2022(b)",
    },
}


def mock_clean_func(df, country, year):
    return pd.DataFrame([f"Cleaned {df} for {country} {year}"])


@patch(
    "climate_finance.unfccc.manual.get_data.load_br_files_tables7",
    return_value=mock_br_data,
)
@patch(
    "climate_finance.unfccc.manual.get_data.clean_table7", side_effect=mock_clean_func
)
def test_table7x_pipeline(mock_clean, mock_load_br_files):
    result = _table7x_pipeline("/mock/path", "Table 7", mock_clean_func)
    expected = pd.DataFrame(
        [
            "Cleaned Mock DataFrame A 2021 for CountryA 2021",
            "Cleaned Mock DataFrame B 2022 for CountryB 2022",
        ]
    )
    pd.testing.assert_frame_equal(result, expected)


@patch("climate_finance.unfccc.manual.get_data._table7x_pipeline")
def test_table7_pipeline(mock_pipeline):
    table7_pipeline("/mock/path")
    mock_pipeline.assert_called_once_with("/mock/path", "Table 7", clean_table7)


@patch("climate_finance.unfccc.manual.get_data._table7x_pipeline")
def test_table7a_pipeline(mock_pipeline):
    table7a_pipeline("/mock/path")
    mock_pipeline.assert_called_once_with("/mock/path", "Table 7(a)", clean_table7a)


@patch("climate_finance.unfccc.manual.get_data._table7x_pipeline")
def test_table7b_pipeline(mock_pipeline):
    table7b_pipeline("/mock/path")
    mock_pipeline.assert_called_once_with("/mock/path", "Table 7(b)", clean_table7b)
