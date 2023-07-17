import glob
import pathlib

import pandas as pd

from climate_finance.config import ClimateDataPath
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


def get_unfccc_multilateral(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Function to get the UNFCCC multilateral data.

    Args:
        start_year: the start year of the data
        end_year: the end year of the data

    Returns:
    df (pd.DataFrame): The UNFCCC multilateral data.
    """
    df = _concat_files(
        directory=ClimateDataPath.raw_data / "unfccc_multilateral",
        filename="FinancialContributionsMultilateral.xlsx",
    )

    df = (
        df.pipe(clean_unfccc)
        .pipe(map_channel_names_to_oecd_codes, channel_names_column="channel")
        .query(f"{start_year} <= year <= {end_year}")
    )

    return df.reset_index(drop=True)


def get_unfccc_bilateral(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Function to get the UNFCCC bilateral data.
    Args:
        start_year: the start year of the data
        end_year: the end year of the data

    Returns:
        df (pd.DataFrame): The UNFCCC bilateral data.

    """
    df = _concat_files(
        directory=ClimateDataPath.raw_data / "unfccc_bilateral",
        filename="FinancialContributionsBilateral.xlsx",
    )

    df = df.pipe(clean_unfccc).query(f"{start_year} <= year <= {end_year}")

    return df.reset_index(drop=True)
