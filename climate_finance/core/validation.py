from climate_finance.config import logger
from climate_finance.core.enums import ValidSources


def validate_prices_and_base_year(prices: str, base_year: int | None) -> None:
    """
    Args:
        prices (str): The type of prices being used. Can be either "constant" or "current".
        base_year (int | None): The base year for calculating prices. Can be an integer or None.

    Raises:
        ValueError: If prices is "constant" but base_year is None.
        ValueError: If prices is "current" but base_year is not None.

    """
    if prices == "constant" and base_year is None:
        raise ValueError("You must provide a base year when using constant prices")

    if prices == "current" and base_year is not None:
        raise ValueError("You cannot provide a base year when using current prices")


def validate_methodology(methodology: str, custom_methodology: bool) -> None:
    """
    Args:
        methodology (str): The methodology to validate.
        custom_methodology (bool): Flag indicating whether a custom methodology
        is being used.
    """
    # Validate OECD
    if methodology == "OECD":
        if custom_methodology:
            logger.warning(
                "You had set a custom methodology. This will be overwritten "
                "by the OECD methodology"
            )

    # Validate ONE
    elif methodology == "ONE":
        if custom_methodology:
            logger.warning(
                "You had set a custom methodology. This will be overwritten "
                "by the ONE methodology"
            )

    # Check if a custom methodology has been set when using the custom methodology
    elif methodology == "custom" and not custom_methodology:
        raise ValueError(
            "You must set the custom methodology using the "
            "set_custom_spending_methodology method"
        )


def validate_source(source: list[str | ValidSources]) -> None:
    """
    Args:
        source (str): The source to validate.
    """
    if "OECD_CRDF_DONOR" in source:
        logger.info(f"Methodology settings are ignored for OECD CRDF data")


def validate_list_of_str(values: str | list, valid_enum) -> list:
    """
    Args:
        values: Either a string or a list of strings to be validated.
        valid_enum: A function or enum that will be used to validate each element of the input.

    Returns:
        A list containing the validated values.

    """
    if isinstance(values, str):
        return [valid_enum(values)]
    if isinstance(values, list):
        return [valid_enum(f) for f in values]
