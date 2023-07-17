import glob
import pathlib

import pandas as pd

from climate_finance.config import ClimateDataPath
from climate_finance.unfccc.download.pre_process import clean_unfccc


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


def get_unfccc_summary() -> pd.DataFrame:
    """
    Function to get the UNFCCC summary data.

    Returns:
    df (pd.DataFrame): The UNFCCC summary data.
    """
    # read file
    df = pd.read_excel(
        ClimateDataPath.raw_data / "unfccc_summary" / "FinancialSupportSummary.xlsx"
    )

    df = df.pipe(clean_unfccc)

    return df


def get_unfccc_multilateral() -> pd.DataFrame:
    """
    Function to get the UNFCCC multilateral data.

    Returns:
    df (pd.DataFrame): The UNFCCC multilateral data.
    """
    df = _concat_files(
        directory=ClimateDataPath.raw_data / "unfccc_multilateral",
        filename="FinancialContributionsMultilateral.xlsx",
    )

    df = df.pipe(clean_unfccc)

    return df


def get_unfccc_bilateral(start_year: int, end_year: int) -> pd.DataFrame:
    df = _concat_files(
        directory=ClimateDataPath.raw_data / "unfccc_bilateral",
        filename="FinancialContributionsBilateral.xlsx",
    )

    df = df.pipe(clean_unfccc).query(f"{start_year} <= year <= {end_year}")

    return df.reset_index(drop=True)
