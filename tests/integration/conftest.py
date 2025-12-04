import pandas as pd
import pytest
from pathlib import Path
from climate_finance import set_climate_finance_data_path


@pytest.fixture
def setup_test_data(tmp_data_path, monkeypatch):
    """Set up test environment with mocked data loading."""
    set_climate_finance_data_path(str(tmp_data_path))

    # Load the test CSV
    test_csv_path = Path(__file__).parent.parent / "fixtures" / "synthetic" / "crs_sample.csv"
    test_data = pd.read_csv(test_csv_path)

    # Mock the CRS data loading to return our test CSV
    def mock_get_crs(start_year, end_year, provider_code=None, recipient_code=None, force_update=False):
        # Filter by years if needed
        df = test_data.copy()
        if start_year is not None and end_year is not None:
            df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]

        # Filter by provider codes if specified
        if provider_code is not None:
            df = df[df['oecd_provider_code'].isin(provider_code)]

        # Filter by recipient codes if specified
        if recipient_code is not None:
            df = df[df['oecd_recipient_code'].isin(recipient_code)]

        return df

    # Patch the data loading function
    monkeypatch.setattr(
        "climate_finance.oecd.crs.get_data.get_crs",
        mock_get_crs,
    )

    return tmp_data_path
