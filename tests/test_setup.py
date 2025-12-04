"""Basic test to verify pytest setup works."""


def test_pytest_setup():
    """Verify pytest is configured correctly."""
    assert True


def test_can_import_pytest():
    """Verify pytest can be imported."""
    import pytest
    assert pytest is not None
