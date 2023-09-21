import glob
import pathlib

import pandas as pd

from climate_finance.config import ClimateDataPath, logger
from climate_finance.unfccc.cleaning_tools.tools import (
    BILATERAL_COLUMNS,
    MULTILATERAL_COLUMNS,
)
from climate_finance.unfccc.cleaning_tools.validation import (
    _check_brs,
    _check_years,
    _check_parties,
    check_unfccc_data,
)
from climate_finance.unfccc.download.download_data import (
    download_unfccc_bilateral,
    download_unfccc_multilateral,
)
from climate_finance.unfccc.download.pre_process import (
    clean_unfccc,
    map_channel_names_to_oecd_codes,
)


def _concat_files(directory: pathlib.Path, filename: str) -> pd.DataFrame:
    """
    Function to concatenate multiple files from a given directory that match a filename pattern.

    Args:
    directory (str): The directory path where files are located.
    filename (str): The pattern to match filenames.

    Returns:
    df (pd.DataFrame): The concatenated dataframe.
    """
    # Get list of files matching the filename pattern
    files = [file for file in glob.glob(f"{directory}/*") if filename in file]

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Iterate through the list of files and concatenate them into a single DataFrame
    for file in files:
        df = pd.concat([df, pd.read_excel(directory / file)])

    return df


def get_unfccc_summary(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Function to get the UNFCCC summary data.

    Args:
        start_year: the start year of the data
        end_year: the end year of the data

    Returns:
    df (pd.DataFrame): The UNFCCC summary data.
    """
    # read file
    df = pd.read_excel(
        ClimateDataPath.raw_data / "unfccc_summary" / "FinancialSupportSummary.xlsx"
    )

    df = df.pipe(clean_unfccc)

    return df.query(f"{start_year} <= year <= {end_year}").reset_index(drop=True)


def get_unfccc_multilateral(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path
    | str = ClimateDataPath.raw_data / "unfccc_data_interface_files",
    force_download: bool = False,
) -> pd.DataFrame:
    """
    Function to get the UNFCCC multilateral data.

    Args:
        start_year: the start year of the data
        end_year: the end year of the data
        br: the list of Biennial Reports to include in the data
        party: The party(ies) to include in the data. If non, all available parties
        that have been downloaded will be included. If none have been downloaded,
        all available parties will be downloaded.
        directory: The directory where the data is stored.
        force_download: If True, the data will be downloaded even if it already exists.

    Returns:
    df (pd.DataFrame): The UNFCCC multilateral data.
    """

    # Check that the directory is a pathlib.Path, if not convert it
    if isinstance(directory, str):
        directory = pathlib.Path(directory)

    # Check that parties are a list if passed as string
    if isinstance(party, str):
        party = [party]

    df = _concat_files(
        directory=directory / "unfccc_multilateral",
        filename="FinancialContributionsMultilateral.xlsx",
    )

    # Check that data was returned. If not, log a message.
    if len(df) == 0:
        logger.info(f"No data found. Downloading data from UNFCCC website.")

    # If force_download is True, log a message
    if force_download:
        logger.info(f"Downloading data from UNFCCC website.")

    # Download the data if no data was found or force_download is True
    if len(df) == 0 or force_download:
        download_unfccc_multilateral(br=br, directory=directory)

    # Check that the right data is included
    check_unfccc_data(
        df=df, party=party, br=br, start_year=start_year, end_year=end_year
    )

    df = (
        df.pipe(clean_unfccc)
        .pipe(map_channel_names_to_oecd_codes, channel_names_column="channel")
        .query(f"{start_year} <= year <= {end_year}")
    )

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        df = df.query(f"party in {party}")

    return df.reset_index(drop=True).filter(MULTILATERAL_COLUMNS)


def get_unfccc_bilateral(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path
    | str = ClimateDataPath.raw_data / "unfccc_data_interface_files",
    force_download: bool = False,
) -> pd.DataFrame:
    """
    Function to get the UNFCCC bilateral data.
    Args:

        start_year: the start year of the data
        end_year: the end year of the data
        br: the list of Biennial Reports to include in the data
        party: The party(ies) to include in the data. If non, all available parties
        that have been downloaded will be included. If none have been downloaded,
        all available parties will be downloaded.
        directory: The directory where the data is stored.
        force_download: If True, the data will be downloaded even if it already exists.

    Returns:
        df (pd.DataFrame): The UNFCCC bilateral data.

    """

    # Check that the directory is a pathlib.Path, if not convert it
    if isinstance(directory, str):
        directory = pathlib.Path(directory)

    # Check that parties are a list if passed as string
    if isinstance(party, str):
        party = [party]

    # Load any data already in that directory
    df = _concat_files(
        directory=directory / "unfccc_bilateral",
        filename="FinancialContributionsBilateralOther.xlsx",
    )

    # Check that data was returned. If not, log a message.
    if len(df) == 0:
        logger.info(f"No data found. Downloading data from UNFCCC website.")

    # If force_download is True, log a message
    if force_download:
        logger.info(f"Downloading data from UNFCCC website.")

    # Download the data if no data was found or force_download is True
    if len(df) == 0 or force_download:
        download_unfccc_bilateral(br=br, party=None, directory=directory)

    # Check that the right data is included
    check_unfccc_data(
        df=df, party=party, br=br, start_year=start_year, end_year=end_year
    )

    # Filter the data for the requested years
    df = df.pipe(clean_unfccc).query(f"{start_year} <= year <= {end_year}")

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        df = df.query(f"party in {party}")

    return df.reset_index(drop=True).filter(BILATERAL_COLUMNS)
