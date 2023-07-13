import re

import pandas as pd

CROSS_CUTTING = "Cross-cutting"
ADAPTATION = "Adaptation"
MITIGATION = "Mitigation"
OTHER = "Other"


def clean_currency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to clean the currency column.

    Args:
        df (pd.DataFrame): The original dataframe.

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

    df.currency = df.currency.apply(_extract_currency)

    return df


def fill_type_of_support_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to fill missing values in the 'type_of_support' column.

    Args:
        df (pd.DataFrame): The original dataframe.

    Returns:
        df (pd.DataFrame): The dataframe with filled 'type_of_support' column.
    """
    return df.assign(
        type_of_support=lambda d: d.type_of_support.fillna("Cross-cutting")
    )


def harmonise_type_of_support(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to harmonise values in the 'type_of_support' column.

    Args:
        df (pd.DataFrame): The original dataframe.

    Returns:
        df (pd.DataFrame): The dataframe with harmonised 'type_of_support' column.
    """

    def _clean_support(string: str) -> str | None:
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

    return df.assign(type_of_support=lambda d: d.type_of_support.apply(_clean_support))


def fill_financial_instrument(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to fill missing values in the 'financial_instrument' column.

    Args:
        df (pd.DataFrame): The original dataframe.

    Returns:
        df (pd.DataFrame): The dataframe with filled 'financial_instrument' column.
    """
    return df.assign(
        financial_instrument=lambda d: d.financial_instrument.fillna("other")
    )


def clean_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to clean the status column.

    Args:
        df (pd.DataFrame): The original dataframe.

    Returns:
        df (pd.DataFrame): The dataframe with cleaned status column.

    """
    status = {
        "provided": "disbursed",
        "disbursed": "disbursed",
        "pledged": "committed",
        "committed": "committed",
    }

    return df.assign(
        status=lambda d: d.status.str.lower()
        .map(status)
        .fillna(d.status)
        .fillna("unknown")
    )
