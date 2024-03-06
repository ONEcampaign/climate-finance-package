import pathlib

import pandas as pd
from dateutil.utils import today
from oda_data.get_data.common import fetch_file_from_url_selenium

from climate_finance.common.schema import (
    CRS_MAPPING,
)
from climate_finance.config import logger
from climate_finance.core.dtypes import set_default_types
from climate_finance.methodologies.multilateral.tools import log_notes
from climate_finance.oecd.cleaning_tools.tools import (
    rename_crdf_marker_columns,
    marker_columns_to_numeric,
    clean_raw_crdf,
)


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
    url = get_file_url(year=latest_year, url=base_url)
    try:
        logger.info(f"Downloading file from {url}")
        file = fetch_file_from_url_selenium(url)
    except IndexError:
        logger.info(f"File not found for {latest_year}")
        latest_year -= 1
        url = get_file_url(year=latest_year, url=base_url)
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
            log_notes(excel_file.parse(sheet))
            continue
        dfs.append(excel_file.parse(sheet))
    return dfs


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
    data = clean_raw_crdf(data)

    # Save file
    data.to_feather(save_to_path)


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


def _load(save_to_path: str | pathlib.Path) -> pd.DataFrame:
    logger.info(f"Loadings CRDF data. This may take a while.")
    return (
        pd.read_feather(save_to_path)
        .rename(columns=CRS_MAPPING)
        .pipe(rename_crdf_marker_columns)
        .pipe(marker_columns_to_numeric)
        .pipe(set_default_types)
    )


def load_or_download(base_url: str, save_to_path: str | pathlib.Path) -> pd.DataFrame:
    try:
        return _load(save_to_path)
    except FileNotFoundError:
        download_file(base_url=base_url, save_to_path=save_to_path)
        return _load(save_to_path)
