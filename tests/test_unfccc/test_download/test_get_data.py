import os
import tempfile
from unittest import mock
from unittest.mock import patch

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from climate_finance.unfccc.download.get_data import (
    SAVE_FILES_TO,
    _check_download_and_rename,
    _get_driver,
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
        assert options.arguments == ["--no-sandbox"]
        assert options.experimental_options["prefs"] == {
            "download.default_directory": f"{SAVE_FILES_TO}/dummy_folder"
        }


def test_check_download_and_rename():
    # Create a temporary directory using the tempfile module
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a dummy excel file
        dummy_data = pd.DataFrame({"Party": ["party1", "party2"]})
        dummy_file = os.path.join(temp_dir, "FinancialSupportSummary.xlsx")
        dummy_data.to_excel(dummy_file, index=False)

        # Patch the SAVE_FILES_TO variable
        with patch(
            "climate_finance.unfccc.download.get_data.SAVE_FILES_TO", new=temp_dir
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
                _check_download_and_rename("FinancialSupportSummary", "", "party3")
                is False
            )
