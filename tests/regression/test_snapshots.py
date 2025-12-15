import json
import pytest
from climate_finance import ClimateData
from climate_finance.common.schema import ClimateSchema

# Try to import pytest-snapshot plugin
try:
    import pytest_snapshot  # noqa: F401
    HAS_SNAPSHOT = True
except ImportError:
    HAS_SNAPSHOT = False


@pytest.mark.skipif(not HAS_SNAPSHOT, reason="pytest-snapshot not installed. Install dev dependencies with: uv sync --group dev")
@pytest.mark.integration
@pytest.mark.slow
def test_france_2020_output_snapshot(snapshot, setup_test_data):
    """
    Regression test: Ensure France 2020 output doesn't change unexpectedly.

    Note: This test downloads real OECD CRS data (~1GB) on first run.
    The setup_test_data fixture attempts to mock data loading, but ClimateData
    bypasses the mock at a lower level. For production use, consider mocking
    at the oda_data level or using pre-downloaded data.
    """
    climate_data = ClimateData(
        years=[2020],
        providers=[4],
        currency="USD",
        prices="current",
    )

    climate_data.load_spending_data(
        methodology="ONE",
        source="OECD_CRS",
        flows="gross_disbursements",
    )

    result = climate_data.get_data()

    # Take key columns for snapshot
    snapshot_data = result[
        [ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE,
         ClimateSchema.INDICATOR, ClimateSchema.VALUE]
    ].to_dict()

    # Convert to JSON string for snapshot
    snapshot_json = json.dumps(snapshot_data, indent=2, sort_keys=True)

    # This will create a snapshot file on first run
    snapshot.assert_match(snapshot_json, "france_2020_one_methodology.json")
