from unittest.mock import patch

import pytest

from climate_finance.unfccc.download.download_data import (
    _check_download_and_rename,
    download_unfccc_summary,
    download_unfccc_bilateral,
    download_unfccc_multilateral,
)


@patch("os.path.exists")
@patch("os.rename")
def test_check_download_and_rename(mock_rename, mock_exists):
    # Scenario 1: File exists
    mock_exists.return_value = True
    assert _check_download_and_rename("base_name", "folder_name")
    mock_rename.assert_not_called()

    # Scenario 2: File doesn't exist
    mock_exists.return_value = False
    with pytest.raises(FileNotFoundError):
        _check_download_and_rename("base_name", "folder_name")
        mock_rename.assert_not_called()


@patch("climate_finance.unfccc.download.download_data.get_unfccc_export")
def test_download_unfccc_summary(mock_get_unfccc_export):
    download_unfccc_summary()
    mock_get_unfccc_export.assert_called_once()


@patch("climate_finance.unfccc.download.download_data.get_unfccc_export")
def test_download_unfccc_bilateral(mock_get_unfccc_export):
    download_unfccc_bilateral()
    mock_get_unfccc_export.assert_called_once()


@patch("climate_finance.unfccc.download.download_data.get_unfccc_export")
def test_download_unfccc_multilateral(mock_get_unfccc_export):
    download_unfccc_multilateral()
    mock_get_unfccc_export.assert_called_once()
