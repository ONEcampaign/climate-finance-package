from pathlib import Path

import pandas as pd
from bblocks import clean_numeric_series
from oda_data.get_data.common import fetch_file_from_url_selenium

from climate_finance.config import logger, ClimateDataPath
from climate_finance.oecd.cleaning_tools.tools import convert_flows_millions_to_units
from climate_finance.unfccc.cleaning_tools.channels import (
    generate_channel_mapping_dictionary,
)

FILE_URL: str = (
    "https://webfs.oecd.org/climate/Imputed_multilateral_shares_climate.xlsx"
)

FILE_PATH: Path = (
    ClimateDataPath.raw_data / "oecd_multilateral_climate_imputations.feather"
)


def _log_notes(df: pd.DataFrame) -> None:
    """
    Log the latest update date from the notes sheet.
    Args:
        df: The notes sheet.

    Returns:
        None
    """

    logger.info(f"{df.iloc[1].values[0]}")


def _read_and_clean_excel_sheets(excel_file: pd.ExcelFile) -> list[pd.DataFrame]:
    """
    Read and clean the excel sheets from the excel file.
    Args:
        excel_file: The excel file to read and clean.

    Returns:
        A list of cleaned dataframes.

    """
    dfs = []
    for sheet in excel_file.sheet_names:
        if sheet == "Notes":
            _log_notes(excel_file.parse(sheet))
            continue
        dfs.append(_clean_df(excel_file.parse(sheet), int(sheet)))
    return dfs


def _merge_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge the dataframes from the excel file.

    Args:
        dfs: The list of dataframes to merge.

    Returns:
        A merged dataframe.
    """
    return (
        pd.concat(dfs, ignore_index=True)
        .sort_values(by=["year", "oecd_climate_total"], ascending=(False, False))
        .reset_index(drop=True)
    )


def _add_channel_codes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add channel codes to the dataframe.
    Args:
        data: The dataframe to add channel codes to.

    Returns:
        The dataframe with channel codes added.

    """
    # Generate channel mapping dictionary
    mapping = generate_channel_mapping_dictionary(
        raw_data=data,
        channel_names_column="channel",
        export_missing_path=ClimateDataPath.raw_data
        / "oecd_multilateral_climate_imputations_channels_not_mapped.csv",
    )

    # Map channel names
    data["oecd_channel_code"] = data.channel.map(mapping)

    return data


def _reorder_imputations_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder the columns in the imputations dataframe.
    Args:
        data: The dataframe to reorder.

    Returns:
        The reordered dataframe.

    """
    # reorder
    return data.set_index(
        [
            "year",
            "oecd_channel_code",
            "oecd_channel_name",
            "acronym",
            "flow_type",
            "type",
            "reporting_method",
            "converged_reporting",
        ]
    ).reset_index()


def _add_climate_value_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds new columns to the DataFrame containing calculated climate values.
    The function calculates new columns based on existing columns that contain shares.

    Args:
        data (pd.DataFrame): The input DataFrame containing climate data.
        value_columns (list): List of existing column names that contain shares.

    Returns:
        pd.DataFrame: DataFrame with new columns containing calculated climate values.
    """
    # Identify the columns which contain share
    share_columns = [c for c in data.columns if c.endswith("_share")]

    # A list of columns for which values will be calculated. This excludes the total column.
    to_value = [
        c
        for c in share_columns
        if c in data.columns and c != "oecd_climate_total_share"
    ]

    # If there are valid columns, calculate the values based on the share
    if len(to_value) > 0:
        for column in to_value:
            data[f"{column[:-6]}"] = (
                data.oecd_climate_total / data.oecd_climate_total_share * data[column]
            )
        return data

    # If there are no valid columns, return the original dataframe
    return data


def _clean_df(data: pd.DataFrame, year: int) -> pd.DataFrame:
    """Cleans an individual dataframe from the imputed multilateral shares file.

    Args:
        data (pd.DataFrame): Dataframe to clean.

    """

    # Get rid of the first column (blank) and the first two rows (metadata)
    data = data.iloc[2:, 2:]

    # Rename columns
    try:
        data.columns = [
            "acronym",
            "channel",
            "type",
            "oecd_climate_total",
            "oecd_climate_total_share",
            "reporting_method",
            "converged_reporting",
        ]
    except ValueError:
        data.columns = [
            "acronym",
            "channel",
            "type",
            "oecd_climate_total",
            "oecd_climate_total_share",
            "oecd_mitigation_share",
            "oecd_adaptation_share",
            "oecd_cross_cutting_share",
            "reporting_method",
            "converged_reporting",
        ]

    # Identify the numeric columns
    numeric_cols = [
        c
        for c in data.columns
        if c
        in [
            "oecd_climate_total",
            "oecd_climate_total_share",
            "oecd_mitigation_share",
            "oecd_adaptation_share",
            "oecd_cross_cutting_share",
        ]
    ]

    # clean numeric columns (remove non-numeric characters to make them floats)
    for column in numeric_cols:
        data[column] = clean_numeric_series(data[column])

    # Convert flows to millions
    data = data.pipe(
        convert_flows_millions_to_units, flow_columns=["oecd_climate_total"]
    )

    # Add climate value columns
    data = _add_climate_value_columns(data)

    return data.assign(year=int(year), flow_type="usd_commitment")


def _download_excel_file() -> pd.ExcelFile:
    """
    Download the excel file from the OECD website.
    Returns:
        pd.ExcelFile: The excel file.

    """
    # Get the file using Selenium
    logger.info(f"Downloading file from {FILE_URL}")
    file = fetch_file_from_url_selenium(FILE_URL)

    # log success
    logger.info(f"File downloaded successfully from {FILE_URL}")

    return pd.ExcelFile(file)


def download_file() -> None:
    """Download the file from the OECD website."""

    # Download the file
    master_file = _download_excel_file()

    # Extract the dataframes
    dfs = _read_and_clean_excel_sheets(master_file)

    # merge dataframes
    data = _merge_dataframes(dfs)

    # add channel codes
    data = _add_channel_codes(data)

    # rename columns
    data = data.rename(columns={"channel": "oecd_channel_name"})

    # reorder columns
    data = _reorder_imputations_columns(data)

    data.to_feather(FILE_PATH)


def get_oecd_multilateral_climate_imputations(
    start_year: int = 2017,
    end_year: int = 2021,
    force_update: bool = False,
) -> pd.DataFrame:
    """Get a clean, merged DataFrame of the OECD multilateral imputations.
    This dataset contains both imputations and shares of spending.

    Args:
        start_year: starting year for the data
        end_year: ending year for the data. If not available, a warning is raised.
        force_update: whether to update the data or not. If the
        data is not available locally, it will be downloaded regardless of this
        parameter.

    """

    if force_update or not FILE_PATH.exists():
        download_file()

    return pd.read_feather(FILE_PATH).query(f"year.between({start_year}, {end_year})")
