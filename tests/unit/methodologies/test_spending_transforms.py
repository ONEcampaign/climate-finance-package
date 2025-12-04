import pandas as pd
import pytest
from climate_finance.common.schema import ClimateSchema
from climate_finance.methodologies.spending.crs import (
    process_crs_climate_indicators,
    transform_markers_into_indicators,
)
from tests.fixtures.builders import build_crs_data


def test_oecd_methodology_counts_significant_at_100_percent():
    """Test OECD methodology (1.0, 1.0) counts significant activities at 100%."""
    # Build test data: 1 significant adaptation activity worth 1M commitment (800K disbursement)
    df = build_crs_data(
        adaptation_markers=[1],  # Significant
        mitigation_markers=[0],  # Not targeted
        n_rows=1,
        commitment_amount=1000000,
    )

    # Process with OECD coefficients (1.0 for significant)
    result = process_crs_climate_indicators(
        df,
        percentage_significant=1.0,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Should count 100% of the disbursement (800K)
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(800000, rel=0.01)
    assert result[ClimateSchema.INDICATOR].iloc[0] == "climate_adaptation"


def test_one_methodology_counts_significant_at_40_percent():
    """Test ONE methodology (0.4, 1.0) discounts significant activities to 40%."""
    # Build test data: 1 significant adaptation activity worth 1M commitment (800K disbursement)
    df = build_crs_data(
        adaptation_markers=[1],  # Significant
        mitigation_markers=[0],  # Not targeted
        n_rows=1,
        commitment_amount=1000000,
    )

    # Process with ONE coefficients (0.4 for significant)
    result = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Should count 40% of the disbursement (800K * 0.4 = 320K)
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(320000, rel=0.01)
    assert result[ClimateSchema.INDICATOR].iloc[0] == "climate_adaptation"


def test_principal_activities_counted_at_100_percent_both_methodologies():
    """Test that principal activities are counted at 100% in both methodologies."""
    # Build test data: 1 principal mitigation activity worth 2M commitment (1.6M disbursement)
    df = build_crs_data(
        adaptation_markers=[0],  # Not targeted
        mitigation_markers=[2],  # Principal
        n_rows=1,
        commitment_amount=2000000,
    )

    # Test with OECD
    result_oecd = process_crs_climate_indicators(
        df,
        percentage_significant=1.0,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Test with ONE
    result_one = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Both should count 100% of the disbursement (1.6M)
    assert result_oecd[ClimateSchema.VALUE].sum() == pytest.approx(1600000, rel=0.01)
    assert result_one[ClimateSchema.VALUE].sum() == pytest.approx(1600000, rel=0.01)


def test_highest_marker_rule_assigns_to_mitigation_when_mitigation_higher():
    """Test highest marker rule assigns to mitigation when mitigation marker is higher."""
    # Build test data: adaptation=1 (significant), mitigation=2 (principal)
    df = build_crs_data(
        adaptation_markers=[1],
        mitigation_markers=[2],
        n_rows=1,
        commitment_amount=1000000,
    )

    result = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Should be assigned to mitigation (highest marker)
    assert result[ClimateSchema.INDICATOR].iloc[0] == "climate_mitigation"
    # Should count at principal level (100% of 800K disbursement)
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(800000, rel=0.01)


def test_highest_marker_rule_assigns_to_adaptation_when_adaptation_higher():
    """Test highest marker rule assigns to adaptation when adaptation marker is higher."""
    # Build test data: adaptation=2 (principal), mitigation=1 (significant)
    df = build_crs_data(
        adaptation_markers=[2],
        mitigation_markers=[1],
        n_rows=1,
        commitment_amount=1000000,
    )

    result = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Should be assigned to adaptation (highest marker)
    assert result[ClimateSchema.INDICATOR].iloc[0] == "climate_adaptation"
    # Should count at principal level (100%)
    assert result[ClimateSchema.VALUE].sum() == pytest.approx(800000, rel=0.01)


def test_highest_marker_rule_filters_out_equal_markers():
    """Test that process_crs_climate_indicators filters out equal markers (handled separately)."""
    # Build test data: adaptation=2, mitigation=2 (both principal)
    df = build_crs_data(
        adaptation_markers=[2],
        mitigation_markers=[2],
        n_rows=1,
        commitment_amount=1000000,
    )

    result = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Should return empty dataframe (equal markers are filtered out and handled separately)
    assert len(result) == 0


def test_cross_cutting_assigned_when_markers_equal():
    """Test that equal markers result in cross-cutting indicator using full transform."""
    # Build test data: adaptation=2, mitigation=2 (both principal)
    df = build_crs_data(
        adaptation_markers=[2],
        mitigation_markers=[2],
        n_rows=1,
        commitment_amount=1000000,
    )

    # Use the full transform function which handles cross-cutting
    result = transform_markers_into_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=True,
    )

    # Filter to cross-cutting rows
    cross_cutting = result[result[ClimateSchema.INDICATOR] == "climate_cross_cutting"]

    # Should have cross-cutting data
    assert len(cross_cutting) > 0
    # Should count at principal level (100%)
    assert cross_cutting[ClimateSchema.VALUE].sum() == pytest.approx(800000, rel=0.01)


def test_no_highest_marker_creates_separate_rows():
    """Test that disabling highest marker creates separate adaptation and mitigation rows."""
    # Build test data: adaptation=2, mitigation=1
    df = build_crs_data(
        adaptation_markers=[2],
        mitigation_markers=[1],
        n_rows=1,
        commitment_amount=1000000,
    )

    result = process_crs_climate_indicators(
        df,
        percentage_significant=0.4,
        percentage_principal=1.0,
        highest_marker=False,
    )

    # Should create 2 rows: one for adaptation, one for mitigation
    assert len(result) == 2
    adaptation_row = result[result[ClimateSchema.INDICATOR] == "climate_adaptation"]
    mitigation_row = result[result[ClimateSchema.INDICATOR] == "climate_mitigation"]

    # Adaptation at 100% (principal=800000), mitigation at 40% (significant=800000*0.4=320000)
    assert adaptation_row[ClimateSchema.VALUE].sum() == pytest.approx(800000, rel=0.01)
    assert mitigation_row[ClimateSchema.VALUE].sum() == pytest.approx(320000, rel=0.01)
