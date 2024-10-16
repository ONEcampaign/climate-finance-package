import pathlib
import re

import pandas as pd

from climate_finance.config import ClimateDataPath, logger
from climate_finance.unfccc.cleaning_tools.tools import BILATERAL_COLUMNS
from climate_finance.unfccc.manual.pre_process import (
    clean_table7,
    clean_table7a,
    clean_table7b,
)
from climate_finance.unfccc.manual.read_files import load_br_files_tables7


def _table7x_pipeline(
    folder_path: str | pathlib.Path, table_name: str, clean_func: callable
):
    """Create a single dataframe of table7 data"""
    br_data = load_br_files_tables7(folder_path=folder_path)

    data = []
    for country, table in br_data.items():
        years = [n.split("_")[1] for n in table.keys() if f"{table_name}_" in n]
        for year in years:
            df = table[f"{table_name}_{year}"]
            try:
                clean_df = clean_func(df=df, country=country, year=year)
                data.append(clean_df)
            except ValueError:
                print(f"Error cleaning table 7a for {country} {year}")

    return pd.concat(data, ignore_index=True)


def table7_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7 data"""
    return _table7x_pipeline(folder_path, "Table 7", clean_table7)


def table7a_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7a data"""
    return _table7x_pipeline(folder_path, "Table 7(a)", clean_table7a)


def table7b_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7b data"""
    return _table7x_pipeline(folder_path, "Table 7(b)", clean_table7b)


def get_unfccc_bilateral(
    start_year: int,
    end_year: int,
    br: list[int] = None,
    party: str | list[str] = None,
    directory: pathlib.Path | str = ClimateDataPath.raw_data / "br_files",
) -> pd.DataFrame:
    """
    Function to get the UNFCCC bilateral data.
    Args:
        start_year: the start year of the data
        end_year: the end year of the data
        br: the BR number(s) to include. If None, all BRs (for which there are folders)
        are included.
        party: the party(ies) to include. If None, all parties are included.
        directory: the directory where the BR files are located

    Returns:

    """

    logger.warning("This function is not yet fully implemented")

    if isinstance(directory, str):
        directory = pathlib.Path(directory)

    potential_folders = [
        folder.name for folder in directory.iterdir() if "br" in folder.name.lower()
    ]
    if len(potential_folders) < 1:
        raise ValueError("No BR folders found in directory")

    dfs = []

    for br_version in potential_folders:
        br_number = re.search(r"\d+\.?\d*", br_version).group()
        dfs.append(
            table7b_pipeline(folder_path=directory / br_version).assign(
                br=f"BR_{br_number}"
            )
        )

    return pd.concat(dfs, ignore_index=True).filter(BILATERAL_COLUMNS)
