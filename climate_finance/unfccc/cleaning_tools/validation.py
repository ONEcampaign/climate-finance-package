import pandas as pd

from climate_finance.config import logger


def _check_brs(data: pd.DataFrame, br: list[int]) -> None:
    """
    Function to check that the right BRs were included in the data.
    Args:
        data: The data to check
        br: The list of BRs to check

    Returns:
        None

    """
    if isinstance(br, int) or isinstance(br, str):
        br = [str(br)]

    try:
        # Get the list of BRs in the data
        data_brs = data["Data source"].unique()
    except KeyError:
        data_brs = data["Data Source"].unique()

    # Check that the right BRs were included
    missing_brs = [b for b in br if f"BR_{b}" not in data_brs]

    # If there are missing BRs, log a message
    if len(missing_brs) > 0:
        logger.info(
            f"The available data did not include the following BRs {missing_brs}.\n"
            "To make sure they are included, redownload the data"
        )


def _check_years(data: pd.DataFrame, start_year: int, end_year: int) -> None:
    """
    Function to check that the right years were included in the data.
    Args:
        data: The data to check
        start_year: The start year to check
        end_year: The end year to check

    Returns:
        None

    """

    # Get the list of years in the data
    try:
        data_years = data["Year"].unique()
    except KeyError:
        data_years = data["year"].unique()

    # Check that the right years were included
    missing_years = [y for y in range(start_year, end_year + 1) if y not in data_years]

    # If there are missing years, log a message
    if len(missing_years) > 0:
        logger.info(
            f"The available data did not include the following years {missing_years}.\n"
            "To make sure they are included, redownload the data"
        )


def _check_parties(data: pd.DataFrame, party: str | list[str]) -> None:
    """
    Function to check that the right parties were included in the data.
    Args:
        data: The data to check
        party: The party(ies) to check

    Returns:
        None

    """

    # Get the list of parties in the data
    try:
        data_parties = data["Party"].unique()
    except KeyError:
        data_parties = data["party"].unique()

    # Check that the right parties were included
    missing_parties = [p for p in party if p not in data_parties]

    # If there are missing parties, log a message
    if len(missing_parties) > 0:
        logger.info(
            f"The available data did not include the following parties {missing_parties}.\n"
            "To make sure they are included, redownload the data"
        )


def check_unfccc_data(
    df: pd.DataFrame,
    party: str | list[str],
    br: str | list[int],
    start_year: int,
    end_year: int,
) -> None:
    """
    Function to check that the right data was included in the UNFCCC data.
    Args:
        df: The data to check
        party: The party(ies) to check
        br: The BR(s) to check
        start_year: The start year to check
        end_year: The end year to check
    """

    # Check that the right BRs were included
    if br is not None:
        _check_brs(data=df, br=br)

    # Check that the right years were included
    _check_years(data=df, start_year=start_year, end_year=end_year)

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        _check_parties(data=df, party=party)
