import pytest

from climate_finance.core.validation import validate_prices_and_base_year


def test_validate_prices_and_base_year():
    # Test for when prices is "constant" and base_year is None
    with pytest.raises(ValueError):
        validate_prices_and_base_year("constant", None)

    # Test for when prices is "current" and base_year is not None
    with pytest.raises(ValueError):
        validate_prices_and_base_year("current", 2022)

    # Test for valid input when prices is "constant" and base_year is not None
    assert validate_prices_and_base_year("constant", 2022) is None

    # Test for valid input when prices is "current" and base_year is None
    assert validate_prices_and_base_year("current", None) is None
