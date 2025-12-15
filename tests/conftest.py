import pandas as pd
import pytest
from pathlib import Path
from climate_finance.common.schema import ClimateSchema


@pytest.fixture
def tmp_data_path(tmp_path):
    """Create a temporary data directory for tests."""
    data_path = tmp_path / "test_data"
    data_path.mkdir()
    return data_path


@pytest.fixture
def sample_crs_row():
    """A single row of CRS data with all required fields."""
    return {
        ClimateSchema.YEAR: 2020,
        ClimateSchema.PROVIDER_CODE: 4,
        ClimateSchema.PROVIDER_NAME: "France",
        ClimateSchema.RECIPIENT_CODE: 50,
        ClimateSchema.RECIPIENT_NAME: "Bangladesh",
        ClimateSchema.ADAPTATION: 2,  # Principal
        ClimateSchema.MITIGATION: 1,  # Significant
        ClimateSchema.USD_COMMITMENT: 1000000,
        ClimateSchema.USD_DISBURSEMENT: 800000,
        ClimateSchema.FLOW_TYPE: "gross_disbursements",
        ClimateSchema.CURRENCY: "USD",
        ClimateSchema.PRICES: "current",
    }


@pytest.fixture
def mock_oda_data_download(monkeypatch):
    """Mock external data downloads to avoid network calls in tests."""
    def mock_load_indicator(*args, **kwargs):
        # Mock load_indicator to do nothing
        pass

    def mock_get_data(*args, **kwargs):
        # Return empty DataFrame
        return pd.DataFrame()

    # Mock the methods that ODAData uses for data loading
    monkeypatch.setattr(
        "oda_data.ODAData.load_indicator",
        mock_load_indicator,
    )
    monkeypatch.setattr(
        "oda_data.ODAData.get_data",
        mock_get_data,
    )


@pytest.fixture
def climate_data_instance(tmp_data_path):
    """Create a basic ClimateData instance for testing."""
    from climate_finance import set_climate_finance_data_path, ClimateData

    set_climate_finance_data_path(str(tmp_data_path))

    return ClimateData(
        years=[2020, 2021],
        providers=[4, 12],
        currency="USD",
        prices="current",
    )


@pytest.fixture
def sample_crs_df():
    """Sample CRS DataFrame for testing."""
    from tests.fixtures.builders import build_crs_data

    return build_crs_data(
        years=[2020, 2021],
        providers=[4, 12],
        recipients=[50, 64],
        n_rows=20,
    )


@pytest.fixture
def sample_contributions_df():
    """Sample contributions DataFrame for testing."""
    from tests.fixtures.builders import build_contributions_data

    return build_contributions_data(
        years=[2020, 2021],
        providers=[4, 12],
        agencies=[901, 905],
        n_rows=10,
    )


@pytest.fixture
def setup_test_data(tmp_data_path, monkeypatch):
    """Set up test environment with mocked data loading."""
    from climate_finance import set_climate_finance_data_path

    set_climate_finance_data_path(str(tmp_data_path))

    # Load the test CSV
    test_csv_path = Path(__file__).parent / "fixtures" / "synthetic" / "crs_sample.csv"
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
