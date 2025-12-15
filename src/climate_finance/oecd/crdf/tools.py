import io
import os
import pathlib
import tempfile
import time

import pandas as pd
from dateutil.utils import today
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from climate_finance.common.schema import (
    CRS_MAPPING,
)
from climate_finance.config import logger
from climate_finance.methodologies.multilateral.tools import log_notes
from climate_finance.oecd.cleaning_tools.tools import (
    rename_crdf_marker_columns,
    marker_columns_to_numeric,
    clean_raw_crdf,
)


def fetch_file_from_url_selenium(url: str) -> io.BytesIO:
    """
    Downloads a file from a specified URL using Selenium in headless mode
    and reads it into memory.

    Args:
        url: The URL to fetch the file from.

    Returns:
        A bytes object containing the file data.
    """
    # Create a temporary directory for downloads
    download_dir = tempfile.mkdtemp()

    # Set up Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_experimental_option(
        name="prefs",
        value={
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )

    # Get driver
    chrome = ChromeDriverManager().install()

    # Return driver with the options
    driver = webdriver.Chrome(service=Service(chrome), options=options)

    # define downloaded file
    downloaded_file = None
    try:
        driver.get(url)
        time.sleep(4)

        # Check for the completion of the download by monitoring the absence
        # of .crdownload files
        while any(
            file_name.endswith(".crdownload") for file_name in os.listdir(download_dir)
        ):
            time.sleep(1)  # Check every second

        # Once downloaded, read the file into memory
        downloaded_file = os.path.join(download_dir, os.listdir(download_dir)[0])

        with open(downloaded_file, "rb") as file:
            file_data = io.BytesIO(file.read())

    finally:
        driver.quit()
        if downloaded_file is not None:
            os.remove(downloaded_file)
        os.rmdir(download_dir)

    return file_data


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
        if sheet.lower() == "notes":
            log_notes(excel_file.parse(sheet))
            continue
        dfs.append(excel_file.parse(sheet))
    return dfs


def enforce_pyarrow_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures that a DataFrame uses pyarrow dtypes."""
    return df.convert_dtypes(dtype_backend="pyarrow")


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
    data = clean_raw_crdf(data).pipe(enforce_pyarrow_types)

    # Save file
    data.to_parquet(save_to_path)


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


def _load(
    save_to_path: str | pathlib.Path, filters: list[tuple] | None = None
) -> pd.DataFrame:
    logger.info("Loadings CRDF data. This may take a while.")
    return (
        pd.read_parquet(save_to_path, filters=filters)
        .rename(columns=CRS_MAPPING)
        .pipe(rename_crdf_marker_columns)
        .pipe(marker_columns_to_numeric)
        .pipe(enforce_pyarrow_types)
    )


def load_or_download(
    base_url: str, save_to_path: str | pathlib.Path, filters: list[tuple] | None = None
) -> pd.DataFrame:
    try:
        return _load(save_to_path, filters=filters)
    except FileNotFoundError:
        download_file(base_url=base_url, save_to_path=save_to_path)
        return _load(save_to_path, filters=filters)
