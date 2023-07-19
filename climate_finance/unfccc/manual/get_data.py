import pathlib

import pandas as pd

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
                data.append(clean_func(df=df, country=country, year=year))
            except:
                print(f"Error cleaning table 7a for {country} {year}")

    return pd.concat(data, ignore_index=True)


def table7_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7 data"""
    return _table7x_pipeline(folder_path, "Table 7", clean_table7)


def table7a_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7 data"""
    return _table7x_pipeline(folder_path, "Table 7(a)", clean_table7a)


def table7b_pipeline(folder_path: str | pathlib.Path):
    """Create a single dataframe of table7 data"""
    return _table7x_pipeline(folder_path, "Table 7(b)", clean_table7b)
