"""Test that required imports work correctly in test environment."""


def test_can_import_climate_schema():
    """Verify ClimateSchema can be imported."""
    from climate_finance.common.schema import ClimateSchema

    assert ClimateSchema is not None
    assert hasattr(ClimateSchema, 'YEAR')
    assert hasattr(ClimateSchema, 'PROVIDER_CODE')
    assert hasattr(ClimateSchema, 'ADAPTATION')
    assert hasattr(ClimateSchema, 'MITIGATION')


def test_can_import_pandas():
    """Verify pandas can be imported."""
    import pandas as pd

    assert pd is not None


def test_can_import_pathlib():
    """Verify pathlib can be imported."""
    from pathlib import Path

    assert Path is not None
