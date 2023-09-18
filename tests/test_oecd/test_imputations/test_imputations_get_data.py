import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from climate_finance.oecd.imputations.get_data import (
    _log_notes,
    _read_and_clean_excel_sheets,
    _merge_dataframes,
    _add_channel_codes,
    _reorder_imputations_columns,
    _add_climate_value_columns,
    _clean_df,
    _download_excel_file,
    download_file,
    get_oecd_multilateral_climate_imputations,
)


# Mock logger
@patch("climate_finance.oecd.imputations.get_data.logger")
def test_log_notes(mock_logger):
    df = pd.DataFrame({"notes": ["0", "Latest update: 2022-01-01"]})
    _log_notes(df)
    mock_logger.info.assert_called_once_with("Latest update: 2022-01-01")


# Mock ExcelFile and _clean_df function
@patch("climate_finance.oecd.imputations.get_data._clean_df", return_value="cleaned_df")
def test_read_and_clean_excel_sheets(mock_clean_df):
    mock_excel_file = MagicMock()
    mock_excel_file.sheet_names = ["Notes", "2022", "2021"]
    dfs = _read_and_clean_excel_sheets(mock_excel_file)
    assert dfs == ["cleaned_df", "cleaned_df"]


# Test _merge_dataframes function
def test_merge_dataframes():
    dfs = [
        pd.DataFrame({"year": [2022], "oecd_climate_total": [100]}),
        pd.DataFrame({"year": [2021], "oecd_climate_total": [50]}),
    ]
    result = _merge_dataframes(dfs)
    expected = pd.DataFrame({"year": [2022, 2021], "oecd_climate_total": [100, 50]})
    pd.testing.assert_frame_equal(result, expected)


# Mock generate_channel_mapping_dictionary function
@patch(
    "climate_finance.oecd.imputations.get_data.generate_channel_mapping_dictionary",
    return_value={"channel1": "code1"},
)
def test_add_channel_codes(mock_mapping):
    data = pd.DataFrame({"channel": ["channel1", "channel2"]})
    result = _add_channel_codes(data)
    expected = pd.DataFrame(
        {"channel": ["channel1", "channel2"], "oecd_channel_code": ["code1", None]}
    )
    pd.testing.assert_frame_equal(result, expected)


# Test _reorder_imputations_columns function
def test_reorder_imputations_columns():
    data = pd.DataFrame(
        {
            "year": [2022],
            "oecd_channel_code": ["code1"],
            "oecd_channel_name": ["channel1"],
            "acronym": ["acr1"],
            "flow_type": ["usd_commitment"],
            "type": ["type1"],
            "reporting_method": ["method1"],
            "converged_reporting": ["yes"],
            "other_column": ["other"],
        }
    )
    result = _reorder_imputations_columns(data)
    expected = pd.DataFrame(
        {
            "year": [2022],
            "oecd_channel_code": ["code1"],
            "oecd_channel_name": ["channel1"],
            "acronym": ["acr1"],
            "flow_type": ["usd_commitment"],
            "type": ["type1"],
            "reporting_method": ["method1"],
            "converged_reporting": ["yes"],
            "other_column": ["other"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


# Test _add_climate_value_columns function
def test_add_climate_value_columns():
    data = pd.DataFrame(
        {
            "oecd_climate_total": [100],
            "oecd_climate_total_share": [1],
            "oecd_mitigation_share": [0.5],
            "oecd_adaptation_share": [0.5],
            "oecd_cross_cutting_share": [0],
        }
    )
    result = _add_climate_value_columns(data)
    expected = pd.DataFrame(
        {
            "oecd_climate_total": [100],
            "oecd_climate_total_share": [1],
            "oecd_mitigation_share": [0.5],
            "oecd_adaptation_share": [0.5],
            "oecd_cross_cutting_share": [0],
            "oecd_mitigation": [50.0],
            "oecd_adaptation": [50.0],
            "oecd_cross_cutting": [0.0],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


mock1 = pd.DataFrame(
    {
        "f1": ["A0", "A1", "A", "B"],
        "f2": ["c0", "c1", "channel1", "channel2"],
        "acronym": ["A0", "A1", "A", "B"],
        "channel": ["c0", "c1", "channel1", "channel2"],
        "type": ["t0", "t1", "type1", "type2"],
        "oecd_climate_total": [0, 1, 100, 200],
        "oecd_climate_total_share": [0, 1, 0.5, 0.1],
        "reporting_method": ["m0", "m1", "method1", "method2"],
        "converged_reporting": ["0", "1", "yes", "no"],
    }
)

mock2 = pd.DataFrame(
    {
        "f1": ["A0", "A1", "A", "B"],
        "f2": ["c0", "c1", "channel1", "channel2"],
        "acronym": ["A0", "A1", "A", "B"],
        "channel": ["c0", "c1", "channel1", "channel2"],
        "type": ["t0", "t1", "type1", "type2"],
        "oecd_climate_total": [0, 1, 100, 200],
        "oecd_climate_total_share": [0, 1, 0.5, 0.1],
        "oecd_mitigation_share": [0, 1, 0.5, 0.2],
        "oecd_adaptation_share": [0, 1, 0.5, 0.3],
        "oecd_cross_cutting_share": [0, 1, 0.5, 0.1],
        "reporting_method": ["m0", "m1", "method1", "method2"],
        "converged_reporting": ["0", "1", "yes", "no"],
    }
)


def test__clean_df():
    result1 = _clean_df(mock1, 2021)
    result2 = _clean_df(mock2, 2022)

    assert "year" in result1.columns
    assert "year" in result2.columns

    assert "flow_type" in result1.columns
    assert "flow_type" in result2.columns


@patch("climate_finance.oecd.imputations.get_data.fetch_file_from_url_selenium")
@patch("climate_finance.oecd.imputations.get_data.pd.ExcelFile", return_value="file")
def test__download_excel_file(mock_fetch_file, mock_excel_file):
    result = _download_excel_file()
    assert result == "file"
    # check that the function was called with the correct url
    mock_fetch_file.assert_called_once()


@patch("climate_finance.oecd.imputations.get_data._download_excel_file")
@patch("climate_finance.oecd.imputations.get_data._read_and_clean_excel_sheets")
@patch("climate_finance.oecd.imputations.get_data._merge_dataframes")
@patch("climate_finance.oecd.imputations.get_data._add_channel_codes")
@patch("climate_finance.oecd.imputations.get_data._reorder_imputations_columns")
@patch("pandas.DataFrame.to_feather")
def test_download_file(
    mock_to_feather,
    mock_reorder_imputations_columns,
    mock_add_channel_codes,
    mock_merge_dataframes,
    mock_read_and_clean_excel_sheets,
    mock_download_excel_file,
):
    # Call the function
    download_file()

    # Assert that the mocked functions were called once
    mock_download_excel_file.assert_called_once()
    mock_read_and_clean_excel_sheets.assert_called_once()
    mock_merge_dataframes.assert_called_once()
    mock_add_channel_codes.assert_called_once()
    mock_reorder_imputations_columns.assert_called_once()


@patch("climate_finance.oecd.imputations.get_data.download_file")
@patch("climate_finance.oecd.imputations.get_data.pd.read_feather")
def test_get_oecd_multilateral_climate_imputations(
    mock_read_feather, mock_download_file
):
    mock_read_feather.return_value = mock1.assign(year=2019)
    result = get_oecd_multilateral_climate_imputations(
        start_year=2020, end_year=2022, update_data=True
    )
    assert isinstance(result, pd.DataFrame)

    assert len(result) == 0
