import re

import numpy as np
import pandas as pd
from bblocks import convert_id

CROSS_CUTTING = "Cross-cutting"
ADAPTATION = "Adaptation"
MITIGATION = "Mitigation"
OTHER = "Other"

COLUMN_MAPPING: dict = {
    "Party": "party",
    "country": "party",
    "Status": "status",
    "Funding source": "funding_source",
    "Financial instrument": "financial_instrument",
    "Contribution type": "indicator",
    "Contribution Type": "indicator",
    "Allocation channel": "channel_type",
    "Allocation Channel": "channel_type",
    "Allocation category": "channel",
    "Type of support": "type_of_support",
    "Sector": "sector",
    "Contribution": "value",
    "Currency": "currency",
    "Year": "year",
    "Data source": "br",
    "Data Source": "br",
    "Recipient country/region": "recipient",
    "Project/programme/activity": "activity",
    "additional_information": "activity",
}

STATUS_MAPPING: dict = {
    "provided": "disbursed",
    "disbursed": "disbursed",
    "pledged": "committed",
    "committed": "committed",
}

BILATERAL_COLUMNS: list = [
    "year",
    "party",
    "br",
    "indicator",
    "status",
    "funding_source",
    "financial_instrument",
    "type_of_support",
    "sector",
    "recipient",
    "activity",
    "currency",
    "value",
]

MULTILATERAL_COLUMNS: list = [
    "year",
    "party",
    "oecd_channel_code",
    "channel",
    "channel_type",
    "br",
    "indicator",
    "status",
    "funding_source",
    "financial_instrument",
    "type_of_support",
    "sector",
    "currency",
    "value",
]

SUMMARY_COLUMNS: list = [
    "year",
    "party",
    "br",
    "channel_type",
    "indicator",
    "type_of_support",
    "currency",
    "value",
]


def clean_currency(df: pd.DataFrame, currency_column: str = "currency") -> pd.DataFrame:
    """
    Function to clean the currency column.

    Args:
        df (pd.DataFrame): The original dataframe.
        currency_column: The name of the column to clean.

    Returns:
        df (pd.DataFrame): The dataframe with cleaned currency column.
    """

    # Extract currency codes from strings
    extracted_currency = df[currency_column].str.extract(r"\((.*?)\)")[0]

    # Create a mask for strings with length 3
    mask_len3 = df[currency_column].str.len() == 3

    # Use np.where to combine conditions and update the currency column
    df[currency_column] = np.where(mask_len3, df[currency_column], extracted_currency)

    return df


def fill_type_of_support_gaps(
    df: pd.DataFrame, support_type_column: str = "type_of_support"
) -> pd.DataFrame:
    """
    Function to fill missing values in the 'type_of_support' column.

    Args:
        df (pd.DataFrame): The original dataframe.
        support_type_column (str): The name of the column to fill.

    Returns:
        df (pd.DataFrame): The dataframe with filled 'type_of_support' column.
    """
    return df.assign(
        type_of_support=lambda d: d[support_type_column].fillna(CROSS_CUTTING)
    )


def harmonise_type_of_support(
    df: pd.DataFrame, type_of_support_column: str = "type_of_support"
) -> pd.DataFrame:
    """
    Function to harmonise values in the 'type_of_support' column.

    Args:
        df (pd.DataFrame): The original dataframe.
        type_of_support_column: The name of the column to harmonise.

    Returns:
        df (pd.DataFrame): The dataframe with harmonised 'type_of_support' column.
    """

    # Handle None values and convert to lowercase
    series = df[type_of_support_column].fillna("unknown").str.lower()

    # Create masks for each condition
    mask_cross_cutting = series.str.contains("cross-cutting")
    mask_adaptation = series.str.contains("adaptation")
    mask_mitigation = series.str.contains("mitigation")
    mask_other = series.str.contains("other")

    # Use np.select to conditionally assign new values
    conditions = [mask_cross_cutting, mask_adaptation, mask_mitigation, mask_other]
    choices = [CROSS_CUTTING, ADAPTATION, MITIGATION, OTHER]

    cleaned_series = (
        pd.Series(np.select(conditions, choices, default=series))
        .replace("unknown", pd.NA)
        .reset_index(drop=True)
    )

    # Update the DataFrame
    df = df.reset_index(drop=True)
    df[type_of_support_column] = cleaned_series

    return df


def fill_financial_instrument_gaps(
    df: pd.DataFrame,
    financial_instrument_column: str = "financial_instrument",
    default_value: str = "other",
) -> pd.DataFrame:
    """
    Function to fill missing values in the 'financial_instrument' column.

    Args:
        df (pd.DataFrame): The original dataframe.
        financial_instrument_column (str): The name of the column to fill.
        default_value (str): The value to fill the gaps with.
        The default value is 'other'.

    Returns:
        df (pd.DataFrame): The dataframe with filled 'financial_instrument' column.
    """
    return df.assign(
        financial_instrument=lambda d: d[financial_instrument_column].fillna(
            default_value
        )
    )


def clean_status(df: pd.DataFrame, status_column: str = "status") -> pd.DataFrame:
    """
    Function to clean the status column.

    Args:
        df (pd.DataFrame): The original dataframe.
        status_column (str): The name of the column to clean.

    Returns:
        df (pd.DataFrame): The dataframe with cleaned status column.

    """
    return df.assign(
        status=lambda d: d[status_column]
        .str.lower()
        .map(STATUS_MAPPING)
        .fillna(d.status)
        .fillna("unknown")
    )


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to rename dataframe columns based on a predefined mapping.

    Args:
        df (pd.DataFrame): The original dataframe.

    Returns:
        df (pd.DataFrame): The dataframe with renamed columns.
    """

    return df.rename(columns=COLUMN_MAPPING)


def clean_recipient_names(recipients_col: pd.Series) -> pd.Series:
    """Clean the recipient names to keep only the country names

    Args:
        recipients_col (pd.Series): The original recipients column

    Returns:
        pd.Series: The cleaned recipients column
    """

    # Keep only characters and spaces (including french accents)
    recipients_col = recipients_col.str.replace(
        rf"[^a-zA-Z\sô'éà]+", "", regex=True
    ).str.strip()

    # Add additional mapping
    additional_mapping = {"Other république Démocratique Du Congo": "DR Congo"}

    return convert_id(
        recipients_col,
        from_type="regex",
        to_type="name",
        additional_mapping=additional_mapping,
    )


def clean_funding_source(
    df: pd.DataFrame, funding_source_column: str = "funding_source"
) -> pd.DataFrame:
    """Clean the funding source column

    Args:
        df (pd.DataFrame): The original dataframe
        funding_source_column (str, optional): The name of the funding source column.
        Defaults to "funding_source".

    Returns:
        pd.DataFrame: The dataframe with cleaned funding source column
    """
    # Handle missing values and convert to lowercase
    df[funding_source_column] = df[funding_source_column].fillna("unknown").str.lower()

    # Handle 'other' cases and 'oda/oof'
    mask_other = df[funding_source_column].str.contains("other")
    mask_oda = df[funding_source_column].str.contains("oda")
    mask_oof = df[funding_source_column].str.contains("oof")
    mask_len3 = df[funding_source_column].str.len() == 3

    df[funding_source_column] = (
        df[funding_source_column]
        .mask(mask_len3, df[funding_source_column])
        .mask(mask_other & mask_oda & mask_oof, "oda/oof")
        .mask(mask_other & mask_oda & ~mask_oof, "oda")
        .mask(mask_other & ~mask_oda & mask_oof, "oof")
        .mask(mask_other & ~mask_oda & ~mask_oof, "other")
        .mask(mask_oda & mask_oof, "oda/oof")
        .mask(mask_oda & ~mask_oof, "oda")
        .mask(~mask_oda & mask_oof, "oof")
    )

    return df
