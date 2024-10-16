"""This script downloads data from the UNFCCC data interface."""

import fnmatch
import os
import pathlib
from time import sleep

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from climate_finance.config import ClimateDataPath, logger

# Set the path where the files will be saved
SAVE_FILES_TO: str = ClimateDataPath.raw_data / "unfccc_data_interface_files"

# Define the base URL for the UNFCCC data interface
UNFCCC_URL: str = "https://www4.unfccc.int/sites/br-di/Pages/FinancialSupport"

# Settings for the "summary" view of the data
SUMMARY_SETTINGS = {
    # The full URL to this section
    "url": f"{UNFCCC_URL}Summary.aspx",  # The ID of the dropdown menu
    "br_dropdown": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportSummaryControl"
        "_comboDataSource_Input"
    ),  # The XPath to the dropdown menu options, by BR
    "br_select": lambda d: (
        "//*["
        '@id="ctl00_PlaceHolderMain_cContentTableFinancialSupportSummaryControl'
        '_comboDataSource_DropDown"]/div/ul/'
        f"li[{d}]/label"
    ),  # The ID of the search button
    "search_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupport"
        "SummaryControl_rbApplyFilter_input"
    ),  # The ID of the export button
    "export_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupport"
        "SummaryControl_rbExport_input"
    ),
    # The name of the folder where the files will be saved
    "folder_name": "unfccc_summary",  # The name of the file that will be saved
    "file_name": "FinancialSupportSummary",  # The time to wait for the page to load
    "wait_time": 25,
}

# Settings for the "multilateral" view of the data
MULTILATERAL_SETTINGS = {
    # The full URL to this section
    "url": f"{UNFCCC_URL}.aspx?mode=1",  # The ID of the dropdown menu
    "br_dropdown": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupport"
        "Control_comboDataSource_Input"
    ),
    # The XPath to the dropdown menu options, by BR
    "br_select": lambda d: (
        "//*["
        '@id="ctl00_PlaceHolderMain_cContentTableFinancialSupportControl'
        '_comboDataSource_DropDown"]/div/ul/'
        f"li[{d}]/label"
    ),  # The ID of the search button
    "search_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_rbApplyFilter_input"
    ),
    # The ID of the export button
    "export_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_rbExport_input"
    ),
    # The name of the folder where the files will be saved
    "folder_name": "unfccc_multilateral",  # The name of the file that will be saved
    "file_name": "FinancialContributionsMultilateral",  # The time to wait for the page to load
    "wait_time": 20,
}

BILATERAL_SETTINGS = {
    # The full URL to this section
    "url": f"{UNFCCC_URL}.aspx?mode=2",  # The ID of the dropdown menu
    "br_dropdown": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupport"
        "Control_comboDataSource_Input"
    ),
    # The XPath to the dropdown menu options, by BR
    "br_select": lambda d: (
        "//*["
        '@id="ctl00_PlaceHolderMain_cContentTableFinancialSupportControl'
        '_comboDataSource_DropDown"]/div/ul/'
        f"li[{d}]/label"
    ),  # The ID of the party dropdown menu
    "party_dropdown": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_comboParties_Input"
    ),
    # The XPath to the party dropdown menu options, by party
    "party_select": lambda d: (
        f'//*[@id="ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_'
        f'comboParties_DropDown"]/div/ul/li[{d}]'
    ),
    # The XPath to the party dropdown menu options, for all parties
    "all_parties": (
        '//*[@id="ctl00_PlaceHolderMain_cContentTableFinancialSupport'
        'Control_comboParties_DropDown"]/div/div'
    ),  # The ID of the search button
    "search_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_rbApplyFilter_input"
    ),
    # The ID of the clear button
    "clear_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_rbClearFilter_input"
    ),
    # The ID of the export button
    "export_button": (
        "ctl00_PlaceHolderMain_cContentTableFinancialSupportControl_rbExport_input"
    ),
    # The name of the file that will be saved
    "file_name": "FinancialContributionsBilateralOther",
    # The name of the folder where the files will be saved
    "folder_name": "unfccc_bilateral",  # The time to wait for the page to load
    "wait_time": 25,
}

# The ID of each of the "parties" in the dropdown menu
PARTY_ID = {
    "Australia": 1,
    "Austria": 2,
    "Belarus": 3,
    "Belgium": 4,
    "Bulgaria": 5,
    "Canada": 6,
    "Croatia": 7,
    "Cyprus": 8,
    "Czechia": 9,
    "Denmark": 10,
    "Estonia": 11,
    "European Union (15)": 12,
    "European Union (28)": 13,
    "Finland": 14,
    "France": 15,
    "Germany": 16,
    "Greece": 17,
    "Hungary": 18,
    "Iceland": 19,
    "Ireland": 20,
    "Italy": 21,
    "Japan": 22,
    "Kazakhstan": 23,
    "Latvia": 24,
    "Liechtenstein": 25,
    "Lithuania": 26,
    "Luxembourg": 27,
    "Malta": 28,
    "Monaco": 29,
    "Netherlands": 30,
    "New Zealand": 31,
    "Norway": 32,
    "Poland": 33,
    "Portugal": 34,
    "Romania": 35,
    "Russian Federation": 36,
    "Slovakia": 37,
    "Slovenia": 38,
    "Spain": 39,
    "Sweden": 40,
    "Switzerland": 41,
    "Turkey": 42,
    "Ukraine": 43,
    "United Kingdom": 44,
    "United States of America": 45,
}


def _get_driver(folder: str) -> webdriver.chrome:
    """Get driver for Chrome. A folder name must be provided to save the files to."""

    # Create options
    options = webdriver.ChromeOptions()

    # check that download folder exists
    if not os.path.exists(f"{SAVE_FILES_TO}/{folder}"):
        try:
            os.makedirs(f"{SAVE_FILES_TO}/{folder}")
        except OSError:
            logger.error(
                f"The download folder must exist. Tried creating it"
                f"but it wasn't possible ({SAVE_FILES_TO}/{folder})"
            )

    else:
        logger.debug(f"Downloading data to {SAVE_FILES_TO}/{folder}")

    # Set download folder
    prefs = {"download.default_directory": f"{SAVE_FILES_TO}/{folder}"}
    # Add arguments and options to options
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")  # Run in headless mode

    options.add_experimental_option("prefs", prefs)

    # Get driver
    chrome = ChromeDriverManager().install()

    # Return driver with the options
    return webdriver.Chrome(service=Service(chrome), options=options)


def _select_party(driver: webdriver.chrome, settings: dict, donor: str) -> None:
    """Select the party from the dropdown menu."""

    # Get the party dropdown ID and the party ID
    dropdown_id = settings["party_dropdown"]
    party = settings["party_select"](PARTY_ID[donor])

    # Get the ID for the "all parties" option
    all_parties = settings["all_parties"]

    # Find dropdown element by its ID
    dropdown = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, dropdown_id))
    )

    # Click the dropdown to expand it
    dropdown.click()

    # Find dropdown element by its ID
    checkbox = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, all_parties))
    )

    # Click the dropdown to expand it
    checkbox.click()
    sleep(1)
    checkbox.click()

    # Find dropdown element by its ID
    checkbox = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, party))
    )
    # Click on it
    checkbox.click()


def _check_download_and_rename(
    base_name: str, folder_name: str, party: str = None
) -> bool:
    """Check if the file download was successful, and rename it if it was.
    If a party is provided, it will check that the file name contains the party name.

    This returns True if the file was downloaded successfully, and False if it was not.

    If the file was downloaded successfully, it will be renamed to the base name.

    """
    # Check if download successful
    if os.path.exists(f"{SAVE_FILES_TO}/{folder_name}/{base_name}.xlsx"):
        return True

    try:
        # Look for files that match the first part of the base name
        matching = [
            f
            for f in os.listdir(rf"{SAVE_FILES_TO}/{folder_name}")
            if fnmatch.fnmatch(f, f"{base_name.split('_')[1]}*.xlsx")
        ]
    except IndexError:
        # If the above fails, check for the full base name
        matching = [
            f
            for f in os.listdir(rf"{SAVE_FILES_TO}/{folder_name}")
            if fnmatch.fnmatch(f, f"{base_name}*.xlsx")
        ]

    # If there are no matching files, return False
    if len(matching) < 1:
        return False

    # There should only be one matching file, and we will rename it
    old_name = matching[0]
    new_name = f"{base_name}.xlsx"

    # If a party is provided, check that the file name contains the party name
    # If it does not, return False
    if party is not None:
        if party not in list(
            pd.read_excel(f"{SAVE_FILES_TO}/{folder_name}/{old_name}").Party.unique()
        ):
            return False

    try:
        os.rename(
            f"{SAVE_FILES_TO}/{folder_name}/{old_name}",
            f"{SAVE_FILES_TO}/{folder_name}/{new_name}",
        )
        logger.debug(f"Successfully downloaded {new_name}")
    except FileExistsError:
        print(f"File {new_name} already exists")
        pass

    return True


def _click_download(driver: webdriver.chrome, button_id: str, wait: int) -> None:
    """Click the export button and wait for the download to finish."""

    # Find export button and click
    export = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, button_id))
    )
    export.click()

    # Wait for the download to finish
    sleep(wait)


def _get_file(
    driver: webdriver.chrome,
    button_id: str,
    base_name: str,
    wait: int,
    folder_name: str,
    party: str = None,
) -> None:
    """Get the file from the website. If it fails, try again."""

    # Try to download the file
    _click_download(driver, button_id, wait)

    # Check if the download was successful
    if not _check_download_and_rename(base_name, folder_name=folder_name, party=party):
        logger.debug(f"Download for {party} not successful. Trying again.")
        _click_download(driver, button_id, wait)

    if not _check_download_and_rename(base_name, folder_name=folder_name):
        if party is None:
            logger.warning(f"Download failed after two tries ({base_name})")
        else:
            logger.warning(f"Download failed after two tries ({party})")


def _select_brs(driver, settings: dict, br: int | list) -> None:
    # Find dropdown element by its ID
    dropdown = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, settings["br_dropdown"]))
    )

    # Click the dropdown to expand it
    dropdown.click()

    # Select the requested BRs
    for br_version in br:
        checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, settings["br_select"](br_version)))
        )
        checkbox.click()


def _click_search(driver, settings: dict) -> None:
    # Click the search button
    search = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, settings["search_button"]))
    )
    search.click()

    # Wait for search to finish
    sleep(settings["wait_time"])


def get_unfccc_export(
    settings: dict,
    br: int | list = None,
    party: list[str] | None = None,
    directory: pathlib.Path = None,
) -> None:
    """Download the UNFCCC export file.

    Args:
        settings (dict): A dictionary of settings.
        br (int | list, optional): The BR version(s) to download. Defaults to None.
        If None, BRs 4 and 5 will be downloaded.
        party (list[str] | None, optional): The donor(s) to download. Defaults to None.
        If donors are specified, they will be downloaded one at a time.
        directory (pathlib.Path, optional): The directory where the data is stored.
    """
    global SAVE_FILES_TO

    if directory != SAVE_FILES_TO:
        SAVE_FILES_TO = directory

    # If no BRs are selected, select 4 and 5
    if br is None:
        br = [3, 4, 5]

    # If only one BR is selected, make it a list
    if isinstance(br, int):
        br = [br]

    # Get driver
    driver = _get_driver(folder=settings["folder_name"])

    # Get page
    driver.get(settings["url"])
    logger.debug(f"Getting {settings['url']}")

    if party is None:
        # Select BRs
        _select_brs(driver, settings=settings, br=br)

        # Click the search button
        _click_search(driver, settings=settings)

        # Download the file
        _get_file(
            driver,
            button_id=settings["export_button"],
            base_name=settings["file_name"],
            wait=settings["wait_time"],
            folder_name=settings["folder_name"],
        )
    else:
        for d_ in party:
            # Check if file already exists
            if _check_download_and_rename(
                base_name=f'{d_}_{settings["file_name"]}',
                folder_name=settings["folder_name"],
            ):
                # If it does exist, delete it so it can be downloaded again
                os.remove(
                    f"{SAVE_FILES_TO}/{settings['folder_name']}/{d_}_{settings['file_name']}.xlsx"
                )
                logger.debug(f"A file for {d_} alredy exists.")
                logger.debug(f"Deleted {d_}_{settings['file_name']}.xlsx")

            # Announce download
            logger.info(f"Downloading {d_}")

            # Select BRs
            _select_brs(driver, settings=settings, br=br)

            # Select party
            _select_party(driver, settings=settings, donor=d_)

            # Click the search button
            _click_search(driver, settings=settings)

            # Download the file
            _get_file(
                driver,
                button_id=settings["export_button"],
                base_name=f'{d_}_{settings["file_name"]}',
                wait=settings["wait_time"],
                folder_name=settings["folder_name"],
                party=d_,
            )

            # Refresh page
            driver.refresh()


def download_unfccc_summary(
    br: list = None,
    directory: pathlib.Path = ClimateDataPath.raw_data / "unfccc_data_interface_files",
    party=None,
) -> None:
    """Download the UNFCCC summary data.

    Args:
        br (list, optional): The BR version(s) to download. Defaults to None.
        party: The party(ies) to include in the data. By default all available are downloaded.
        If None, BRs 4 and 5 will be downloaded.

        directory (pathlib.Path, optional): The directory where the data is stored.

    """

    logger.info("Downloading UNFCCC summary data. This may take a while....")
    get_unfccc_export(settings=SUMMARY_SETTINGS, br=br, party=None, directory=directory)
    logger.info("Successfully downloaded UNFCCC summary data.")


def download_unfccc_bilateral(
    br: list = None,
    party: list[str] = None,
    directory: pathlib.Path = ClimateDataPath.raw_data / "unfccc_data_interface_files",
) -> None:
    """Download the UNFCCC bilateral data.

    Args:
        br (list, optional): The BR version(s) to download. Defaults to None.
        If None, BRs 4 and 5 will be downloaded.

        party (list[str], optional): The donor(s) to download. Defaults to None.
        If donors are specified, all will be downloaded, one at a time.

        directory (pathlib.Path, optional): The directory where the data is stored.

    """
    if party is None:
        party = list(PARTY_ID)

    logger.info("Downloading UNFCCC bilateral data. This may take a while....")
    get_unfccc_export(
        settings=BILATERAL_SETTINGS, br=br, party=party, directory=directory
    )
    logger.info("Successfully downloaded UNFCCC bilateral data.")


def download_unfccc_multilateral(
    br: list = None,
    directory: pathlib.Path = ClimateDataPath.raw_data / "unfccc_data_interface_files",
    party: None = None,
) -> None:
    """Download the UNFCCC multilateral data.

    Args:
        br (list, optional): The BR version(s) to download. Defaults to None.
        If None, BRs 4 and 5 will be downloaded.

        party (list[str], optional): By default, all parties are downloaded.

        directory (pathlib.Path, optional): The directory where the data is stored.

    """
    logger.info("Downloading UNFCCC multilateral data. This may take a while....")

    get_unfccc_export(
        settings=MULTILATERAL_SETTINGS, br=br, party=None, directory=directory
    )
    logger.info("Successfully downloaded UNFCCC multilateral data.")
