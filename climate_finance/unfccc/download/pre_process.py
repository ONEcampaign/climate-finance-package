import glob
import pathlib

import pandas as pd
from bblocks import clean_numeric_series
from climate_finance.config import ClimateDataPath

from climate_finance.unfccc.cleaning_tools import (
    clean_currency,
    clean_status,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
)

COLUMN_MAPPING: dict = {
    "Party": "country",
    "Status": "status",
    "Funding source": "funding_source",
    "Financial instrument": "financial_instrument",
    "Contribution type": "indicator",
    "Allocation category": "channel",
    "Type of support": "type_of_support",
    "Sector": "sector",
    "Contribution": "value",
    "Currency": "currency",
    "Year": "year",
    "Data source": "br",
    "Recipient country/region": "recipient",
    "Project/programme/activity": "activity",
}


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


def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to rename dataframe columns based on a predefined mapping.

    Args:
    df (pd.DataFrame): The original dataframe.

    Returns:
    df (pd.DataFrame): The dataframe with renamed columns.
    """

    return df.rename(columns=COLUMN_MAPPING)


def clean_unfccc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to clean a dataframe.

    Args:
    df (pd.DataFrame): The original dataframe.

    Returns:
    df (pd.DataFrame): The cleaned dataframe.
    """

    # Pipeline
    df = (
        df.pipe(_rename_cols)
        .pipe(clean_currency)
        .assign(
            value=lambda d: clean_numeric_series(d.value),
            year=lambda d: d.year.astype("Int32"),
        )
        .dropna(subset=["value"])
        .pipe(fill_type_of_support_gaps)
        .pipe(harmonise_type_of_support)
    )

    # Try to clean status
    try:
        df = df.pipe(clean_status)
    except AttributeError:
        # If status not present, pass
        pass

    return df.reset_index(drop=True)


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
