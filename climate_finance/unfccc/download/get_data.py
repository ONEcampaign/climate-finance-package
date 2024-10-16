import glob
import pathlib
from functools import partial

import pandas as pd

from climate_finance.config import ClimateDataPath, logger
from climate_finance.unfccc.cleaning_tools.tools import (
    BILATERAL_COLUMNS,
    MULTILATERAL_COLUMNS,
    SUMMARY_COLUMNS,
)
from climate_finance.unfccc.cleaning_tools.validation import (
    _check_parties,
    check_unfccc_data,
)
from climate_finance.unfccc.download.download_data import (
    download_unfccc_bilateral,
    download_unfccc_multilateral,
    download_unfccc_summary,
    PARTY_ID,
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


def _check_and_download(
    df: pd.DataFrame,
    force_download: bool,
    download_function: callable,
    br: list | None,
    directory: pathlib.Path,
    party: list | None = None,
) -> bool:
    # Check that data was returned. If not, log a message.
    if len(df) == 0:
        logger.info(f"No data found. Downloading data from UNFCCC website.")

    # If force_download is True, log a message
    if force_download:
        logger.info(f"Downloading data from UNFCCC website.")

    # Download the data if no data was found or force_download is True
    if len(df) == 0 or force_download:
        download_function(br=br, directory=directory, party=party)
        return True


def _check_path_and_br(
    directory: str | pathlib.Path, party: list | str | None
) -> tuple[pathlib.Path, list | None]:
    """
    Function to check that the directory and party arguments are valid.
    Args:
        directory: The directory where the data is stored.
        party: The party(ies) to include in the data.

    Returns:
        A tuple of the validated directory and party arguments.
    """

    # Check that the directory is a pathlib.Path, if not convert it
    if isinstance(directory, str):
        directory = pathlib.Path(directory)

    # Check that parties are a list if passed as string
    if isinstance(party, str):
        party = [party]

    return directory, party


def get_unfccc_summary(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path | str = ClimateDataPath.raw_data
    / "unfccc_data_interface_files",
    force_download: bool = False,
) -> pd.DataFrame:
    """
    Function to get the UNFCCC summary data.

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
        df (pd.DataFrame): The UNFCCC summary data.
    """

    # Check that the directory and party arguments are valid
    directory, party = _check_path_and_br(directory, party)

    # define read function
    load_summary = partial(
        _concat_files,
        directory=directory / "unfccc_summary",
        filename="FinancialSupportSummary.xlsx",
    )

    # Load any data already in that directory
    df = load_summary()

    # Check that data was returned and download if necessary (including if force download is True)
    if _check_and_download(
        df=df,
        br=br,
        download_function=download_unfccc_summary,
        force_download=force_download,
        directory=directory,
    ):
        df = load_summary()

    # Check that the right data is included
    check_unfccc_data(
        df=df, party=party, br=br, start_year=start_year, end_year=end_year
    )

    # Clean and filter the data for the requested years
    df = (
        df.pipe(clean_unfccc)
        .query(f"{start_year} <= year <= {end_year}")
        .reset_index(drop=True)
    )

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        df = df.query(f"party in {party}")
    else:
        _check_parties(df, list(PARTY_ID))

    return df.filter(SUMMARY_COLUMNS)


def get_unfccc_multilateral(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path | str = ClimateDataPath.raw_data
    / "unfccc_data_interface_files",
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
    # Check that the directory and party arguments are valid
    directory, party = _check_path_and_br(directory, party)

    # define read function
    load_multi = partial(
        _concat_files,
        directory=directory / "unfccc_multilateral",
        filename="FinancialContributionsMultilateral.xlsx",
    )

    # Load any data already in that directory
    df = load_multi()

    # Check that data was returned and download if necessary (including if force download is True)
    if _check_and_download(
        df=df,
        br=br,
        download_function=download_unfccc_multilateral,
        force_download=force_download,
        directory=directory,
    ):
        df = load_multi()

    # Check that the right data is included
    check_unfccc_data(
        df=df, party=party, br=br, start_year=start_year, end_year=end_year
    )

    # Clean and filter the data for the requested years
    df = (
        df.pipe(clean_unfccc)
        .pipe(map_channel_names_to_oecd_codes, channel_names_column="channel")
        .query(f"{start_year} <= year <= {end_year}")
    )

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        df = df.query(f"party in {party}")
    else:
        _check_parties(df, list(PARTY_ID))

    return df.reset_index(drop=True).filter(MULTILATERAL_COLUMNS)


def get_unfccc_bilateral(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path | str = ClimateDataPath.raw_data
    / "unfccc_data_interface_files",
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

    # Check that the directory and party arguments are valid
    directory, party = _check_path_and_br(directory, party)

    # define read function
    load_bilateral = partial(
        _concat_files,
        directory=directory / "unfccc_bilateral",
        filename="FinancialContributionsBilateralOther.xlsx",
    )

    # Load any data already in that directory
    df = load_bilateral()

    # Check that data was returned and download if necessary (including if force download is True)
    if _check_and_download(
        df=df,
        br=br,
        download_function=download_unfccc_bilateral,
        force_download=force_download,
        directory=directory,
        party=party,
    ):
        df = load_bilateral()

    # Check that the right data is included
    check_unfccc_data(
        df=df, party=party, br=br, start_year=start_year, end_year=end_year
    )

    # Filter the data for the requested years
    df = df.pipe(clean_unfccc).query(f"{start_year} <= year <= {end_year}")

    # Check that the right parties were included (if specific parties requested)
    if party is not None:
        df = df.query(f"party in {party}")
    else:
        _check_parties(df, list(PARTY_ID))

    return df.reset_index(drop=True).filter(BILATERAL_COLUMNS)
