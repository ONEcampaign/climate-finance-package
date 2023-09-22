import os
import tempfile
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from climate_finance.unfccc.download.download_data import (
    SAVE_FILES_TO,
    _check_download_and_rename,
    _get_driver,
)
from climate_finance.unfccc.download.get_data import (
    _concat_files,
    _check_and_download,
    _check_path_and_br,
    get_unfccc_summary,
    get_unfccc_multilateral,
    get_unfccc_bilateral,
)


def test_get_driver():
    # Mock webdriver and ChromeDriverManager
    with mock.patch.object(
        webdriver, "Chrome", return_value="mocked_driver"
    ) as mock_webdriver, mock.patch.object(
        ChromeDriverManager, "install", return_value="mocked_manager"
    ) as mock_manager:
        # Call the function
        result = _get_driver("dummy_folder")

        # Assert that the result is as expected
        assert result == "mocked_driver"

        # Assert that the mocked functions were called correctly
        mock_manager.assert_called_once()
        assert mock_webdriver.call_args[1]["service"] == mock.ANY
        assert isinstance(mock_webdriver.call_args[1]["service"], Service)
        assert isinstance(
            mock_webdriver.call_args[1]["options"], webdriver.ChromeOptions
        )
        options = mock_webdriver.call_args[1]["options"]
        assert options.arguments == ["--no-sandbox", "--headless"]
        assert options.experimental_options["prefs"] == {
            "download.default_directory": f"{SAVE_FILES_TO}/dummy_folder"
        }
        # delete dummy_folder
        try:
            os.rmdir(f"{SAVE_FILES_TO}/dummy_folder")
        except OSError:
            pass


@patch("os.makedirs")
@patch("os.path.exists")
def test_get_driver_directory_check(mock_exists, mock_makedirs):
    # Mock webdriver and ChromeDriverManager
    with mock.patch.object(
        webdriver, "Chrome", return_value="mocked_driver"
    ) as mock_webdriver, mock.patch.object(
        ChromeDriverManager, "install", return_value="mocked_manager"
    ) as mock_manager:
        # Scenario 1: Directory exists
        mock_exists.return_value = True
        result = _get_driver("dummy_folder")
        assert result == "mocked_driver"
        mock_makedirs.assert_not_called()

        # Scenario 2: Directory doesn't exist
        mock_exists.return_value = False
        result = _get_driver("dummy_folder")
        assert result == "mocked_driver"
        mock_makedirs.assert_called_once()

        # delete dummy_folder
        try:
            os.rmdir(f"{SAVE_FILES_TO}/dummy_folder")
        except OSError:
            pass


def test_check_download_and_rename():
    # Create a temporary directory using the tempfile module
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a dummy excel file
        dummy_data = pd.DataFrame({"Party": ["party1", "party2"]})
        dummy_file = os.path.join(temp_dir, "FinancialSupportSummary.xlsx")
        dummy_data.to_excel(dummy_file, index=False)

        # Patch the SAVE_FILES_TO variable
        with patch(
            "climate_finance.unfccc.download.download_data.SAVE_FILES_TO", new=temp_dir
        ):
            # Test when base_name is in the file
            assert _check_download_and_rename("FinancialSupportSummary", "")

            assert os.path.exists(
                os.path.join(temp_dir, "FinancialSupportSummary.xlsx")
            )

            # Test when base_name is not in the file
            assert _check_download_and_rename("FinancialSupportSummary2", "") is False

            # Test when party is in the file
            assert _check_download_and_rename("FinancialSupportSummary", "", "party1")

            # Test when party is not in the file
            assert (
                _check_download_and_rename("T_FinancialSupportSummary", "", "party3")
                is False
            )


def test_concat_files():
    # Create dummy files in the temporary directory
    directory = Path(__file__).resolve().parent / "dummy_data"
    for i in range(3):
        pd.DataFrame({"col": [i]}).to_excel(directory / f"file{i}.xlsx", index=False)

    # Call the function
    result = (
        _concat_files(directory, "file").sort_values(by="col").reset_index(drop=True).
    )

    # Expected result
    expected = (
        pd.DataFrame({"col": [0, 1, 2]}).sort_values(by="col").reset_index(drop=True).
    )

    # Check if the resulting DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected)


def test_check_and_download():
    # Call the function with an empty DataFrame and force_download = True
    mock_download_function = lambda br, directory, party: None

    check = _check_and_download(
        pd.DataFrame(),
        True,
        mock_download_function,
        None,
        Path("dummy_directory"),
    )

    # Check if the download function was called
    assert check


def test_check_path_and_br():
    # Test with directory as string and party as string
    directory, party = _check_path_and_br("dummy_directory", "dummy_party")
    assert isinstance(directory, Path)
    assert isinstance(party, list)

    # Test with directory as pathlib.Path and party as list
    directory, party = _check_path_and_br(Path("dummy_directory"), ["dummy_party"])
    assert isinstance(directory, Path)
    assert isinstance(party, list)


@patch("climate_finance.unfccc.download.get_data._concat_files")
@patch("climate_finance.unfccc.download.get_data._check_and_download")
@patch("climate_finance.unfccc.download.get_data.check_unfccc_data")
@patch("climate_finance.unfccc.download.get_data.clean_unfccc")
@patch("climate_finance.unfccc.download.get_data.map_channel_names_to_oecd_codes")
@patch("climate_finance.unfccc.download.get_data._check_parties")
def test_get_unfccc_summary(
    mock_check_parties,
    mock_map_channel_names_to_oecd_codes,
    mock_clean_unfccc,
    mock_check_unfccc_data,
    mock_check_and_download,
    mock_concat_files,
):
    # Call the function
    get_unfccc_summary(
        2000,
        2020,
        [1, 2],
        "dummy_party",
        "dummy_directory",
        True,
    )

    # Check if the mocked functions were called correctly
    mock_check_and_download.assert_called_once()
    mock_check_unfccc_data.assert_called_once()

    get_unfccc_summary(
        2000,
        2020,
        [1, 2],
        None,
        "dummy_directory",
        True,
    )

    mock_check_parties.assert_called_once()


@patch("climate_finance.unfccc.download.get_data._concat_files")
@patch("climate_finance.unfccc.download.get_data._check_and_download")
@patch("climate_finance.unfccc.download.get_data.check_unfccc_data")
@patch("climate_finance.unfccc.download.get_data.clean_unfccc")
@patch("climate_finance.unfccc.download.get_data.map_channel_names_to_oecd_codes")
@patch("climate_finance.unfccc.download.get_data._check_parties")
def test_get_unfccc_multilateral(
    mock_check_parties,
    mock_map_channel_names_to_oecd_codes,
    mock_clean_unfccc,
    mock_check_unfccc_data,
    mock_check_and_download,
    mock_concat_files,
):
    # Call the function
    get_unfccc_multilateral(
        2000,
        2020,
        [1, 2],
        "dummy_party",
        "dummy_directory",
        True,
    )

    # Check if the mocked functions were called correctly
    mock_check_and_download.assert_called_once()
    mock_check_unfccc_data.assert_called_once()

    get_unfccc_multilateral(
        2000,
        2020,
        [1, 2],
        None,
        "dummy_directory",
        True,
    )

    mock_check_parties.assert_called_once()


@patch("climate_finance.unfccc.download.get_data._concat_files")
@patch("climate_finance.unfccc.download.get_data._check_and_download")
@patch("climate_finance.unfccc.download.get_data.check_unfccc_data")
@patch("climate_finance.unfccc.download.get_data.clean_unfccc")
@patch("climate_finance.unfccc.download.get_data.map_channel_names_to_oecd_codes")
@patch("climate_finance.unfccc.download.get_data._check_parties")
def test_get_unfccc_bilateral(
    mock_check_parties,
    mock_map_channel_names_to_oecd_codes,
    mock_clean_unfccc,
    mock_check_unfccc_data,
    mock_check_and_download,
    mock_concat_files,
):
    # Call the function
    get_unfccc_bilateral(
        2000,
        2020,
        [1, 2],
        "dummy_party",
        "dummy_directory",
        True,
    )

    # Check if the mocked functions were called correctly
    mock_check_and_download.assert_called_once()
    mock_check_unfccc_data.assert_called_once()

    get_unfccc_bilateral(
        2000,
        2020,
        [1, 2],
        None,
        "dummy_directory",
        True,
    )

    mock_check_parties.assert_called_once()
