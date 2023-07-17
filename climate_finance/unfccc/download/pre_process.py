import pandas as pd
from bblocks import clean_numeric_series

from climate_finance.unfccc.cleaning_tools.tools import (
    clean_currency,
    clean_status,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
)

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


def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to rename dataframe columns based on a predefined mapping.

    Args:
    df (pd.DataFrame): The original dataframe.

    Returns:
    df (pd.DataFrame): The dataframe with renamed columns.
    """

    return df.rename(columns=COLUMN_MAPPING)


def clean_unfccc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to clean a dataframe.

    Args:
    df (pd.DataFrame): The original dataframe.

    Returns:
    df (pd.DataFrame): The cleaned dataframe.
    """

    # Pipeline
    df = (
        df.pipe(_rename_cols)
        .pipe(clean_currency)
        .assign(
            value=lambda d: clean_numeric_series(d.value),
            year=lambda d: d.year.astype("Int32"),
        )
        .dropna(subset=["value"])
        .pipe(fill_type_of_support_gaps)
        .pipe(harmonise_type_of_support)
    )

    # Try to clean status
    try:
        df = df.pipe(clean_status)
    except AttributeError:
        # If status not present, pass
        pass

    return df.reset_index(drop=True)
