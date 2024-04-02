"""This script deals with reading in the raw data from the UNFCCC biennial reports."""

import glob
import pathlib

import pandas as pd


def _load_br_files(
    folder_path: str | pathlib.Path, table_pattern: str
) -> dict[str, pd.DataFrame]:
    br_files: dict[str, pd.DataFrame] | dict = {}

    # Get all Excel files in the folder path
    files = [file for file in glob.glob(f"{folder_path}/*.xlsx")]

    for party in files:
        name = str(party).split("/")[-1].split(".")[0]
        br_files[name] = {}
        try:
            file = pd.ExcelFile(party)
        except FileNotFoundError:
            continue

        required = [c for c in file.sheet_names if table_pattern in c]
        for sheet in required:
            br_files[name][sheet] = file.parse(sheet)

    return br_files


def load_br_files_tables7(folder_path: str | pathlib.Path) -> dict[str, pd.DataFrame]:
    """
    Loads all "Tables 7" from the biennial reports for a given biennial report.
    This function wll look for all Excel files in the folder path and load them into
    a dictionary of DataFrames.
    """
    return _load_br_files(folder_path, table_pattern="Table 7")
