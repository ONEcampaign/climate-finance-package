import pathlib

import pandas as pd
from dateutil.utils import today
from oda_data.get_data.common import fetch_file_from_url_selenium

from climate_finance.config import logger
from climate_finance.oecd.climate_analysis.tools import OECD_CLIMATE_INDICATORS
from climate_finance.oecd.imputations.get_data import _log_notes


def get_file_url(year: int, url: str) -> str:
    """Get the file url for the given year.

    Args:
        year: The year to get the file url for.
        url: The base url for the file.

    Returns:
        The file url.

    """
    return f"{url}{year}.xlsx"


def download_excel_file(latest_year: int, base_url: str) -> pd.ExcelFile:
    """
    Download the excel file from the OECD website.
    Returns:
        pd.ExcelFile: The excel file.

    """
    # Get the file using Selenium
    url = get_file_url(latest_year, base_url)
    try:
        logger.info(f"Downloading file from {url}")
        file = fetch_file_from_url_selenium(url)
    except IndexError:
        logger.info(f"File not found for {latest_year}")
        latest_year -= 1
        url = get_file_url(latest_year)
        file = fetch_file_from_url_selenium(url)

    # log success
    logger.info(f"File downloaded successfully from {url}")

    return pd.ExcelFile(file)


def read_excel_sheets(excel_file: pd.ExcelFile) -> list[pd.DataFrame]:
    """
    Read the excel sheets from the excel file.
    Args:
        excel_file: The excel file to read.

    Returns:
        A list of cleaned dataframes.

    """
    dfs = []
    for sheet in excel_file.sheet_names:
        if sheet == "Notes":
            _log_notes(excel_file.parse(sheet))
            continue
        dfs.append(excel_file.parse(sheet))
    return dfs


def convert_thousands_to_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the columns that are in thousands to units.

    Args:
        df: The dataframe to convert the columns for.

    Returns:
        The dataframe with the columns converted.

    """

    # Identify the columns that are in thousands
    usd_thousands_cols = df.columns[df.columns.str.contains("_usd_thousand")]

    # For each column, convert to units
    for col in usd_thousands_cols:
        df[col] *= 1e3

    # Rename the columns
    df = df.rename(
        columns={col: col.replace("_usd_thousand", "") for col in usd_thousands_cols}
    )

    return df


def set_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the data types for the dataframe.
    Args:
        df: The dataframe to set the data types for.

    Returns:
        The dataframe with the data types set.

    """
    # Convert int column to "Int32" or similar
    int_data_types = {
        "year": "Int32",
        "provider_code": "Int32",
        "agency_code": "Int32",
        "recipient_code": "Int32",
        "channel_of_delivery_code": "Int32",
        "purpose_code": "Int32",
        "type_of_finance": "Int32",
        "coal_related_financing": "Int16",
    }

    # Convert float columns to "float64" or similar
    float_data_types = {
        "adaptation_related_development_finance_commitment_current": "float64",
        "mitigation_related_development_finance_commitment_current": "float64",
        "overlap_commitment_current": "float64",
        "climate_related_development_finance_commitment_current": "float64",
        "share_of_the_underlying_commitment_when_available": "float64",
    }

    # Convert categorical columns to "category"
    categorical_data_types = {
        "provider": "category",
        "provider_type": "category",
        "provider_detailed": "category",
        "provider_code": "category",
        "extending_agency": "category",
        "recipient": "category",
        "recipient_region": "category",
        "recipient_income_group_oecd_classification": "category",
        "concessionality": "category",
        "climate_objective_applies_to_rio_marked_data_only_or_climate_component": "category",
        "adaptation_objective_applies_to_rio_marked_data_only": "category",
        "mitigation_objective_applies_to_rio_marked_data_only": "category",
        "channel_of_delivery": "category",
        "sector_detailed": "category",
        "sub_sector": "category",
        "development_cooperation_modality": "category",
        "financial_instrument": "category",
        "methodology": "category",
        "gender": "category",
    }

    # Set data types by column
    for col in df.columns:
        df[col] = df[col].astype(
            (int_data_types | float_data_types | categorical_data_types).get(col, "str")
        )

    return df.reset_index(drop=True)


def clean_df(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans an individual dataframe from the imputed multilateral shares file.

    Args:
        data (pd.DataFrame): Dataframe to clean.

    """

    # convert all column names to lower case and remove spaces and special characters
    data.columns = (
        data.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace(r"[°(),%]", "", regex=True)
        .str.replace(r"_{2,}", "_", regex=True)
    )

    # Drop any columns that contain an integer
    data = data.drop(columns=data.columns[data.columns.str.contains(r"\d")])

    # Convert thousands to units
    data = convert_thousands_to_units(data)

    # Convert data types
    data = set_data_types(data)

    return data


def download_file(
    base_url: str,
    save_to_path: pathlib.Path,
    latest_year: int = today().year - 2,
) -> None:
    """Download the file from the OECD website."""
    # Download the file
    master_file = download_excel_file(
        latest_year=latest_year,
        base_url=base_url,
    )

    # Extract the dataframes
    dfs = read_excel_sheets(master_file)

    # merge dataframes
    data = pd.concat(dfs, ignore_index=True)

    # clean data
    data = clean_df(data)

    # Save file
    data.to_feather(save_to_path)


def rename_marker_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the marker columns to more standard names.

    Args:
        df: The dataframe to rename the columns for.

    Returns:
        The dataframe with the columns renamed.

    """
    # rename marker columns
    markers = {
        "adaptation_objective_applies_to_rio_marked_data_only": "climate_adaptation",
        "mitigation_objective_applies_to_rio_marked_data_only": "climate_mitigation",
        "adaptation_related_development_finance_commitment_current": "climate_adaptation_value",
        "mitigation_related_development_finance_commitment_current": "climate_mitigation_value",
    }

    return df.rename(columns=markers)


def rename_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the index columns to more standard names.

    Args:
        df: The dataframe to rename the columns for.

    Returns:
        The dataframe with the columns renamed.

    """
    # Define index columns and their new names
    idx_cols = {
        "provider": "party",
        "provider_code": "oecd_party_code",
        "channel_of_delivery_code": "oecd_channel_code",
        "channel_of_delivery": "oecd_channel_name",
    }

    return df.rename(columns=idx_cols)


def marker_columns_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the marker columns to numeric.

    The markers are converted to numeric values according to the following mapping:
    - Principal: 2
    - Significant: 1
    - Not targeted/Not screened: 0
    - Imputed multilateral contributions: 99

    Args:
        df: The dataframe to convert the marker columns for.

    Returns:
        The dataframe with the marker columns converted to numeric.

    """
    # markers to numeric
    markers_numeric = {
        "Principal": 2,
        "Significant": 1,
        "Not targeted/Not screened": 0,
        "Imputed multilateral contributions": 99,
        "Climate components": 100,
    }

    # Identify the marker columns
    marker_columns = ["climate_adaptation", "climate_mitigation"]

    # Convert the marker columns to numeric
    df[marker_columns] = df[marker_columns].replace(markers_numeric).astype("Int16")

    return df


def get_marker_data(df: pd.DataFrame, marker: str):
    """
    Get the marker data for a given marker.

    Args:
        df: The dataframe to get the marker data from.
        marker: The marker to get the data for.

    Returns:
        The marker data.

    """
    # Get adaptation
    return (
        df.loc[lambda d: d[marker] > 0]
        .copy()
        .assign(indicator=marker)
        .rename(columns={f"{marker}_value": "value"})
        .drop(columns=[marker])
    )


def load_or_download(base_url: str, save_to_path: str | pathlib.Path) -> pd.DataFrame:
    try:
        return pd.read_feather(save_to_path)
    except FileNotFoundError:
        download_file(base_url=base_url, save_to_path=save_to_path)
        return pd.read_feather(save_to_path)


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the columns of the dataframe. This means removing columns that are not needed
    and renaming the index columns. The "commitments" flow information is also added.

    Args:
        df: The dataframe to clean the columns for.

    Returns:
        The dataframe with the columns cleaned.

    """
    to_drop = [
        "climate_objective_applies_to_rio_marked_data_only_or_climate_component",
        "climate_adaptation",
        "climate_mitigation",
        "climate_related_development_finance_commitment_current",
        "climate_adaptation_value",
        "climate_mitigation_value",
        "overlap_commitment_current",
        "share_of_the_underlying_commitment_when_available",
    ]

    return (
        df.filter([c for c in df.columns if c not in to_drop])
        .assign(indicator=lambda d: d.indicator.map(OECD_CLIMATE_INDICATORS))
        .assign(flow_type="usd_commitment")
        .pipe(rename_index_columns)
    )
