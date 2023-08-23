import json

import pandas as pd
import pytest
from climate_finance.unfccc.manual.pre_process import (
    clean_column_string,
    clean_table7,
    clean_table7a,
    clean_table7b,
    clean_table_7_columns,
    find_heading_row,
    find_last_row,
    rename_table_7a_columns,
    rename_table_7b_columns,
    reshape_table_7,
    reshape_table_7a,
    reshape_table_7b,
    table7a_heading_mapping,
)
from unittest.mock import mock_open, patch


def test_clean_column_string_basic_replacements():
    assert clean_column_string("lc") == "l"
    assert clean_column_string("cd") == "c"
    assert clean_column_string("inge") == "ing"
    assert clean_column_string("rf") == "r"


def test_clean_column_string_advanced_replacements():
    assert clean_column_string("fundsh") == "funds"
    assert clean_column_string("fundg") == "fund"
    assert clean_column_string("fundsg") == "funds"
    assert clean_column_string("channels:") == "channels"


def test_clean_column_string_multiple_replacements():
    assert clean_column_string("cd/lc") == "c/l"
    assert clean_column_string("123channels: fundsh") == "channels funds"


def test_clean_column_string_edge_cases():
    assert clean_column_string(None) == "None"
    assert clean_column_string("") == ""


# Test DataFrame
df_test = pd.DataFrame(
    {
        "A_test": ["apple", "banana", "cherry", "date", "apple"],
        "B": ["grape", "kiwi", "lemon", "mango", "orange"],
    }
)


def test_find_heading_row():
    assert find_heading_row(df_test, "date") == 3
    assert find_heading_row(df_test, "banana") == 1


def test_find_last_row():
    assert find_last_row(df_test, "apple") == 5
    assert find_last_row(df_test, "cherry") == 3


# Sample DataFrame for clean_table_7_columns
df_table7 = pd.DataFrame(
    {
        "A": ["x", "y"],
        "B": ["USD-Header1", "val1"],
        "C": ["USD-Header2", "val2"],
        "D": ["USD-Header3", "val3"],
        "E": ["USD-Header4", "val4"],
        "F": ["USD-Header5", "val5"],
        "G": ["EUR-Header6", "val6"],
        "H": ["EUR-Header7", "val7"],
        "I": ["EUR-Header8", "val8"],
    }
)


def test_clean_table_7_columns():
    cleaned_df = clean_table_7_columns(df_table7, "USD", "EUR")
    expected_cols = [
        "channel",
        "USD_USD-Header_val",
        "USD_USD-Header_val",
        "USD_USD-Header_val",
        "USD_USD-Header_val",
        "USD_USD-Header_val",
        "EUR_EUR-Header_val",
        "EUR_EUR-Header_val",
        "EUR_EUR-Header_val",
    ]
    assert list(cleaned_df.columns) == expected_cols


# Sample DataFrame for rename_table_7a_columns and rename_table_7b_columns
df_table7a = pd.DataFrame(columns=list(range(10)))
df_table7b = pd.DataFrame(columns=list(range(9)))


def test_rename_table_7a_columns():
    renamed_df = rename_table_7a_columns(df_table7a, "USD", "EUR")
    expected_cols = [
        "channel",
        "USD_Core",
        "EUR_Core",
        "USD_Climate-specific",
        "EUR_Climate-specific",
        "status",
        "funding_source",
        "financial_instrument",
        "type_of_support",
        "sector",
    ]
    assert list(renamed_df.columns) == expected_cols


def test_rename_table_7b_columns():
    renamed_df = rename_table_7b_columns(df_table7b, "USD", "EUR")
    expected_cols = [
        "recipient",
        "USD_Climate-specific",
        "EUR_Climate-specific",
        "status",
        "funding_source",
        "financial_instrument",
        "type_of_support",
        "sector",
        "additional_information",
    ]
    assert list(renamed_df.columns) == expected_cols


# Sample DataFrames for reshape_table_7
df_7 = pd.DataFrame(
    {"channel": ["apple", "banana"], "USD_A": [1, 2], "USD_B": [3, 4], "EUR_C": [5, 6]}
)


def test_reshape_table_7():
    reshaped_df = reshape_table_7(df_7)
    assert "currency" in reshaped_df.columns
    assert "indicator" in reshaped_df.columns
    assert len(reshaped_df) == 6  # 3 columns * 2 rows


# Sample DataFrames for reshape_table_7a and reshape_table_7b
df_7a = pd.DataFrame(
    {
        "recipient": ["apple", "banana"],
        "channel": ["hola", "bola"],
        "sector": [1, 2],
        "financial_instrument": ["A", "B"],
        "type_of_support": ["X", "Y"],
        "status": ["A", "B"],
        "funding_source": ["X", "Y"],
        "USD_A": ["A", "B"],
    }
)

df_7b = pd.DataFrame(
    {
        "recipient": ["apple", "banana"],
        "sector": [1, 2],
        "financial_instrument": ["A", "B"],
        "type_of_support": ["X", "Y"],
        "USD_A": ["A", "B"],
        "funding_source": ["X", "Y"],
        "additional_information": ["A", "B"],
        "status": ["A", "B"],
    }
)


def test_reshape_table_7a():
    reshaped_df = reshape_table_7a(df_7a)
    assert "currency" in reshaped_df.columns
    assert "indicator" in reshaped_df.columns
    assert len(reshaped_df) == 4


def test_reshape_table_7b():
    reshaped_df = reshape_table_7b(df_7b)
    assert "currency" in reshaped_df.columns
    assert "indicator" in reshaped_df.columns
    assert len(reshaped_df) == 2  # 1 column * 2 rows


# Sample DataFrame
df_table7a_mapping = pd.DataFrame({"channel": ["apple", "banana", "cherry"]})

# Mocked mapping data
mocked_mapping_data = {"apple": "fruit", "banana": "fruit", "cherry": "fruit"}


@patch(
    target="builtins.open",
    new_callable=mock_open,
    read_data=json.dumps(mocked_mapping_data),
)
def test_table7a_heading_mapping(mock_file):
    mapped_df = table7a_heading_mapping(df_table7a_mapping)
    assert "channel_type" in mapped_df.columns
    assert all(mapped_df["channel_type"] == "fruit")


# Sample DataFrame for clean_table7
df_7_test = pd.DataFrame(
    {
        "A": list(range(10)),
        "B": ["non-relevant"] * 3
        + ["allocation channels"]
        + [
            "USD-Header1",
            "USD-Header2",
            "USD-Header3",
            "USD-Header4",
            "USD-Header5",
            "bbrev",
        ],
        "C": list(range(10)),
        "C3": list(range(10)),
        "C4": list(range(10)),
        "C5": list(range(10)),
        "C6": list(range(10)),
    }
)


@patch("climate_finance.unfccc.manual.pre_process.find_heading_row", return_value=0)
@patch("climate_finance.unfccc.manual.pre_process.find_last_row", return_value=2)
@patch("climate_finance.unfccc.manual.pre_process.clean_table_7_columns")
@patch("climate_finance.unfccc.manual.pre_process.reshape_table_7")
def test_clean_table7(
    mock_reshape, mock_clean_columns, mock_find_last_row, mock_find_heading_row
):
    clean_table7(df_7_test, "SampleCountry", 2023)
    mock_clean_columns.assert_called_once()
    mock_reshape.assert_called_once()


# Sample DataFrame for clean_table7a
df_7a_test = pd.DataFrame(
    {
        "A": list(range(10)),
        "B": ["non-relevant"] * 3
        + ["donor funding"]
        + [
            "USD-Header1",
            "USD-Header2",
            "USD-Header3",
            "USD-Header4",
            "USD-Header5",
            "total contributions through",
        ],
        "C": list(range(10)),
        "C2": list(range(10)),
        "C3": list(range(10)),
        "C4": list(range(10)),
        "C5": list(range(10)),
        "C6": list(range(10)),
    }
)


@patch("climate_finance.unfccc.manual.pre_process.find_heading_row", side_effect=[3, 9])
@patch("climate_finance.unfccc.manual.pre_process.find_last_row", return_value=9)
@patch("climate_finance.unfccc.manual.pre_process.rename_table_7a_columns")
@patch("climate_finance.unfccc.manual.pre_process.reshape_table_7a")
@patch("climate_finance.unfccc.manual.pre_process.table7a_heading_mapping")
def test_clean_table7a(
    mock_mapping,
    mock_reshape,
    mock_rename_columns,
    mock_find_last_row,
    mock_find_heading_row,
):
    clean_table7a(df_7a_test, "SampleCountry", 2023)
    mock_rename_columns.assert_called_once()
    mock_reshape.assert_called_once()
    mock_mapping.assert_called_once()


# Sample DataFrame for clean_table7b
df_7b_test = pd.DataFrame(
    {
        "A": list(range(10)),
        "B": ["non-relevant"] * 3
        + ["recipient"]
        + [
            "USD-Header1",
            "USD-Header2",
            "USD-Header3",
            "USD-Header4",
            "USD-Header5",
            "total contributions through",
        ],
        "C": list(range(10)),
        "C2": list(range(10)),
        "C3": list(range(10)),
        "C4": list(range(10)),
        "C5": list(range(10)),
        "C6": list(range(10)),
        "C7": list(range(10)),
    }
)


@patch("climate_finance.unfccc.manual.pre_process.find_heading_row", return_value=3)
@patch("climate_finance.unfccc.manual.pre_process.find_last_row", return_value=9)
@patch("climate_finance.unfccc.manual.pre_process.rename_table_7b_columns")
@patch("climate_finance.unfccc.manual.pre_process.reshape_table_7b")
def test_clean_table7b(
    mock_reshape, mock_rename_columns, mock_find_last_row, mock_find_heading_row
):
    clean_table7b(df_7b_test, "SampleCountry", 2023)
    mock_rename_columns.assert_called_once()
    mock_reshape.assert_called_once()
