import pytest
from unittest.mock import Mock
from climate_finance.unfccc.manual.read_files import (
    _load_br_files,
    load_br_files_tables7,
)


# Mocking the ExcelFile object
class MockExcelFile:
    def __init__(self, sheets):
        self.sheet_names = sheets

    def parse(self, sheet):
        return f"Data from {sheet}"


# Test setup: fixtures
@pytest.fixture
def mock_glob(monkeypatch):
    mock_glob = Mock(return_value=["/path/to/file1.xlsx", "/path/to/file2.xlsx"])
    monkeypatch.setattr("climate_finance.unfccc.manual.read_files.glob.glob", mock_glob)
    return mock_glob


@pytest.fixture
def mock_excel(monkeypatch):
    def mock_return(file_path):
        if "file1" in file_path:
            return MockExcelFile(["Table 1", "Table 7", "Table 8"])
        elif "file2" in file_path:
            return MockExcelFile(["Table 4", "Table 7"])
        else:
            raise FileNotFoundError

    mock_excel = Mock(side_effect=mock_return)
    monkeypatch.setattr(
        "climate_finance.unfccc.manual.read_files.pd.ExcelFile", mock_excel
    )
    return mock_excel


# Test scenarios
def test_load_br_files_success(mock_glob, mock_excel):
    result = _load_br_files("/path/to", table_pattern="Table 7")
    expected = {
        "file1": {"Table 7": "Data from Table 7"},
        "file2": {"Table 7": "Data from Table 7"},
    }
    assert result == expected


def test_load_br_files_file_not_found(mock_glob, mock_excel):
    # Adjusting mock_glob to return a file that raises FileNotFoundError
    mock_glob.return_value = ["/path/to/nonexistent.xlsx"]

    result = _load_br_files("/path/to", table_pattern="Table 7")
    expected = {"nonexistent": {}}  # File not found, so no data returned
    assert result == expected


def test_load_br_files_no_matching_sheets(mock_glob, mock_excel):
    # Adjusting mock_glob to return different file paths
    mock_glob.return_value = ["/path/to/file3.xlsx"]

    # Adjusting mock_excel to return sheets that don't match the pattern
    mock_excel.side_effect = lambda x: MockExcelFile(["Table 1", "Table 2"])

    result = _load_br_files("/path/to", table_pattern="Table 7")
    expected = {"file3": {}}  # No files with matching sheets
    assert result == expected


def test_load_br_files_no_excel_files(mock_glob, mock_excel):
    # Adjusting mock_glob to return an empty list
    mock_glob.return_value = []

    result = _load_br_files("/path/to", table_pattern="Table 7")
    expected = {}  # No Excel files in directory
    assert result == expected


def test_load_br_files_tables7(mock_glob, mock_excel):
    result = load_br_files_tables7("/path/to")
    expected = {
        "file1": {"Table 7": "Data from Table 7"},
        "file2": {"Table 7": "Data from Table 7"},
    }
    assert result == expected
