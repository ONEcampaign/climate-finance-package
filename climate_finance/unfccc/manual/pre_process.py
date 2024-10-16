import json
import re
from functools import partial

import pandas as pd
from bblocks import clean_numeric_series

from climate_finance import config
from climate_finance.unfccc.cleaning_tools.tools import (
    clean_recipient_names,
    rename_columns,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
    clean_status,
    clean_funding_source,
)
from climate_finance.unfccc.download.pre_process import clean_unfccc

TABLE7_COLUMNS: list[str] = [
    "status",
    "funding_source",
    "financial_instrument",
    "type_of_support",
    "channel",
    "sector",
    "recipient",
    "additional_information",
]


def clean_column_string(string: str):
    """Make a series of replacements to clean up the strings of column names

    Args:
        string (str): The string to clean

    Returns:
        str: The cleaned string
    """

    string = re.sub(r"\d+", "", str(string))

    replacements = {
        "lc": "l",
        "cd": "c",
        "inge": "ing",
        "rf": "r",
        "/ general,": "",
        "Climate-specific, _": "",
        "fundsh": "funds",
        "fundg": "fund",
        "fundsg": "funds",
        "channels:": "channels",
    }
    for old, new in replacements.items():
        string = string.replace(old, new)

    return string.strip("_")


def find_heading_row(df: pd.DataFrame, heading: str) -> int:
    """Find the row of the heading.

    Args:
        df (pd.DataFrame): The DataFrame to search.
        heading (str): The heading to search for.

    Returns:
        int: The row number of the heading.
    """
    col = df.columns[0]
    return df.loc[df[col].str.lower().fillna("").str.contains(heading)].index[0]


def find_last_row(df: pd.DataFrame, row_string: str) -> int:
    """Find the last row of the data.

    Args:
        df (pd.DataFrame): The DataFrame to search.
        row_string (str): The string to search for.

    Returns:
        int: The row number of the last row of the data.
    """
    col = df.columns[0]
    return df.loc[df[col].str.lower().fillna("").str.contains(row_string)].index[-1] + 1


def clean_table_7_columns(
    df: pd.DataFrame, first_currency: str, second_currency: str
) -> pd.DataFrame:
    """Clean the column names for table 7.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        first_currency (str): The first currency.
        second_currency (str): The second currency.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """

    # Create a new header using the first two rows
    header = df.iloc[:2].apply(lambda x: "_".join(x.fillna("").astype(str)), axis=0)

    # Create the first currency column names
    first_header = [f"{first_currency}_{clean_column_string(c)}" for c in header[1:6]]

    # Create the second currency column names
    second_header = [f"{second_currency}_{clean_column_string(c)}" for c in header[6:]]

    # Create the new column names
    df.columns = ["channel"] + first_header + second_header

    # Remove the first two rows
    df = df.iloc[2:].reset_index(drop=True)

    return df


def rename_table_7a_columns(
    df: pd.DataFrame, first_currency: str, second_currency: str
) -> pd.DataFrame:
    """Clean the column names for table 7a.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        first_currency (str): The first currency. This is extracted from the data in
        a previous step
        second_currency (str): The second currency. This is extracted from the data
        in a previous step

    Returns:
        pd.DataFrame: The cleaned DataFrame.

    """
    cols = df.columns

    columns = {
        cols[0]: "channel",
        cols[1]: f"{first_currency}_Core",
        cols[2]: f"{second_currency}_Core",
        cols[3]: f"{first_currency}_Climate-specific",
        cols[4]: f"{second_currency}_Climate-specific",
        cols[5]: "status",
        cols[6]: "funding_source",
        cols[7]: "financial_instrument",
        cols[8]: "type_of_support",
        cols[9]: "sector",
    }

    return df.rename(columns=columns)


def rename_table_7b_columns(
    df: pd.DataFrame, first_currency: str, second_currency: str
) -> pd.DataFrame:
    """Clean the column names for table 7b.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        first_currency (str): The first currency. This is extracted from the data in
        a previous step
        second_currency (str): The second currency. This is extracted from the data
        in a previous step

    Returns:
        pd.DataFrame: The cleaned DataFrame.

    """
    cols = df.columns
    columns = {
        cols[0]: "recipient",
        cols[1]: f"{first_currency}_Climate-specific",
        cols[2]: f"{second_currency}_Climate-specific",
        cols[3]: "status",
        cols[4]: "funding_source",
        cols[5]: "financial_instrument",
        cols[6]: "type_of_support",
        cols[7]: "sector",
        cols[8]: "additional_information",
    }

    return df.rename(columns=columns)


def reshape_table_7(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape the table 7 dataframes into a long format.
    Args:
        df: DataFrame to reshape

    Returns:
        Reshaped DataFrame
    """

    # Melt the dataframe
    df_ = df.melt(id_vars=["channel"], var_name="column", value_name="value")

    # Split the 'column' into currency and indicator
    df_[["currency", "indicator"]] = df_.column.str.split("_", expand=True)

    # Drop the column column
    return df_.drop(columns=["column"]).reset_index(drop=True)


def reshape_table_7x(df: pd.DataFrame, excluded_cols: list[str]) -> pd.DataFrame:
    """
    Reshape the table dataframes into a long format.
    Args:
        df: DataFrame to reshape
        excluded_cols: Columns to exclude from id_vars in the melt operation

    Returns:
        Reshaped DataFrame
    """

    # Melt the dataframe
    df_ = df.melt(
        id_vars=[c for c in TABLE7_COLUMNS if c not in excluded_cols],
        var_name="column",
        value_name="value",
    )

    # Split the 'column' into currency and indicator
    df_[["currency", "indicator"]] = df_.column.str.split("_", expand=True)
    return df_.drop(columns=["column"]).reset_index(drop=True)


# Partial function for table 7a
reshape_table_7a = partial(
    reshape_table_7x, excluded_cols=["recipient", "additional_information"]
)

# Partial function for table 7b
reshape_table_7b = partial(reshape_table_7x, excluded_cols=["channel"])


def table7a_heading_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map rows to the right category based on channels.

    Args:
        df (pd.DataFrame): DataFrame containing the channel column.

    Returns:
        pd.DataFrame: DataFrame with mapped channel types.
    """

    df["channel"] = df.channel.str.replace(f"[\d()+-]+|\.+", "", regex=True).str.strip()

    # read mapping from json
    with open(
        config.ClimateDataPath.unfccc_cleaning_tools / "unfccc_channel_mapping.json",
        "r",
    ) as f:
        mapping = json.load(f)

    df["channel_type"] = df.channel.map(mapping)

    # fix channel names
    df["channel"] = df.channel.str.replace(
        r"^(?:[a-z]+\s)?([A-Z].*)", r"\1", regex=True
    ).str.strip()

    return df


def clean_table7(df: pd.DataFrame, country: str, year: int) -> pd.DataFrame:
    """
    Process and clean table 7 data.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        country (str): Country associated with the data.
        year (int): Year associated with the data.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    # Find the index of the heading row
    heading_row = find_heading_row(df, "allocation channels")

    # Find the index of the last row
    last_row = find_last_row(df, "bbrev")

    # Find the name of the currencies
    first_currency = df.iloc[heading_row + 1, 1].split("-")[1].strip()
    second_currency = df.iloc[heading_row + 1, 6]

    # Reduce the dataframe to the relevant rows
    df = df.iloc[heading_row + 2 : last_row].reset_index(drop=True)

    # Clean the column names
    df = clean_table_7_columns(df, first_currency, second_currency)

    # Clean the channel names
    df.channel = df.channel.apply(clean_column_string).str.lstrip()

    # Reshape the dataframe
    df = reshape_table_7(df)

    # Clean the values
    df.value = clean_numeric_series(df.value)

    df.currency = df.currency.replace("USDb", "USD")

    # drop rows with no value
    df = df.dropna(subset=["value"])

    return df.assign(country=country, year=year)


def clean_table7a(df: pd.DataFrame, country: str, year: int) -> pd.DataFrame:
    """
    Process and clean table 7a data.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        country (str): Country associated with the data.
        year (int): Year associated with the data.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    # Find the index of the heading row
    heading_row = find_heading_row(df, "donor funding")

    # Find the index of the last row
    last_row = find_last_row(df, "abbreviations") - 1

    # Find the name of the currencies
    first_currency = df.iloc[heading_row + 2, 1].split("-")[1].strip()
    second_currency = df.iloc[heading_row + 2, 4]

    # find new heading row
    heading_row = find_heading_row(df, "total contributions through")

    # Reduce the dataframe to the relevant rows
    df = df.iloc[heading_row:last_row].reset_index(drop=True)

    # Clean the column names
    df = rename_table_7a_columns(df, first_currency, second_currency)

    # Reshape the dataframe
    df = reshape_table_7a(df)

    # Clean the values
    df.value = clean_numeric_series(df.value)

    # drop rows with no value
    df = df.dropna(subset=["value"])

    # Add channel type
    df = table7a_heading_mapping(df)

    return df.assign(country=country, year=year)


def clean_table7b(df: pd.DataFrame, country: str, year: int) -> pd.DataFrame:
    """
    Process and clean table 7b data.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        country (str): Country associated with the data.
        year (int): Year associated with the data.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    # Find the index of the heading row
    heading_row = find_heading_row(df, "recipient")

    # Find the index of the last row
    last_row = find_last_row(df, "abbreviations") - 1

    # Find the name of the currencies
    first_currency = df.iloc[heading_row + 2, 1].split("-")[1].strip()
    second_currency = df.iloc[heading_row + 2, 2]

    # find new heading row
    heading_row = find_heading_row(df, "total contributions")

    # Reduce the dataframe to the relevant rows
    df = df.iloc[heading_row:last_row].reset_index(drop=True)

    # Clean the column names
    df = rename_table_7b_columns(df, first_currency, second_currency)

    # Reshape the dataframe
    df = reshape_table_7b(df)

    # Clean the values
    df.value = clean_numeric_series(df.value)

    df = (
        df.pipe(rename_columns).dropna(subset=["value"]).pipe(harmonise_type_of_support)
    )

    # Try to clean status
    try:
        df = df.pipe(clean_status)
    except AttributeError:
        # If status not present, pass
        pass

    # Try to clean funding source
    try:
        df = df.pipe(clean_funding_source)
    except AttributeError:
        # If funding source not present, pass
        pass

    # Try to clean financial instrument
    try:
        df["financial_instrument"] = df["financial_instrument"].str.lower().str.strip()
    except AttributeError:
        # If financial instrument not present, pass
        pass

    # drop rows with no value
    df = df.dropna(subset=["value"])

    return df.assign(party=country, year=year)
