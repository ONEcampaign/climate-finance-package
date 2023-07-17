import re

import pandas as pd

CROSS_CUTTING = "Cross-cutting"
ADAPTATION = "Adaptation"
MITIGATION = "Mitigation"
OTHER = "Other"

COLUMN_MAPPING: dict = {
    "Party": "country",
    "Status": "status",
    "Funding source": "funding_source",
    "Financial instrument": "financial_instrument",
    "Contribution type": "indicator",
    "Allocation category": "channel",
    "Type of support": "type_of_support",
    "Sector": "sector",
    "Contribution": "value",
    "Currency": "currency",
    "Year": "year",
    "Data source": "br",
    "Recipient country/region": "recipient",
    "Project/programme/activity": "activity",
}

STATUS_MAPPING: dict = {
    "provided": "disbursed",
    "disbursed": "disbursed",
    "pledged": "committed",
    "committed": "committed",
}


def clean_currency(df: pd.DataFrame, currency_column: str = "currency") -> pd.DataFrame:
    """
    Function to clean the currency column.

    Args:
        df (pd.DataFrame): The original dataframe.
        currency_column: The name of the column to clean.

    Returns:
        df (pd.DataFrame): The dataframe with cleaned currency column.
    """

    def _extract_currency(x: str) -> str:
        if x is None:
            return x

        if len(x) == 3:
            return x

        match = re.findall("\((.*?)\)", x)
        if match:
            return match[0]

    df.currency = df[currency_column].apply(_extract_currency)

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

    def _clean_support(string: str) -> str | None:
        """Function to clean the type_of_support column.

        Args:
            string (str): The original string.

        Returns:
            string (str): The cleaned string.
        """
        if string is None:
            return string
        string = string.lower()
        if "cross-cutting" in string:
            return CROSS_CUTTING
        if "adaptation" in string:
            return ADAPTATION
        if "mitigation" in string:
            return MITIGATION
        if "other" in string:
            return OTHER
        return string

    return df.assign(
        type_of_support=lambda d: d[type_of_support_column].apply(_clean_support)
    )


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
