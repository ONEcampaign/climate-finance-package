import pandas as pd
from bblocks import clean_numeric_series

from climate_finance.unfccc.cleaning_tools.channels import (
    add_channel_names,
    generate_channel_mapping_dictionary,
)
from climate_finance.unfccc.cleaning_tools.tools import (
    clean_currency,
    clean_status,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
    rename_columns,
    clean_funding_source,
)


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
        df.pipe(rename_columns)
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

    return df.reset_index(drop=True)


def map_channel_names_to_oecd_codes(
    df: pd.DataFrame, channel_names_column: str, export_missing_path: str | None = None
) -> pd.DataFrame:
    """
    Function to map channel names to OECD DAC codes.

    Args:
        df (pd.DataFrame): The original dataframe.
        channel_names_column (str): The name of the column with channel names.
        export_missing_path (str | None): The path to export a csv with missing channel names.

    Returns:
        df (pd.DataFrame): The dataframe with mapped channel names.
    """

    # Create a dictionary with channel names as keys and OECD DAC codes as values
    mapping = generate_channel_mapping_dictionary(
        raw_data=df,
        channel_names_column=channel_names_column,
        export_missing_path=export_missing_path,
    )

    # Create a new column with the mapped channel codes
    df["oecd_channel_code"] = df[channel_names_column].map(mapping).astype("Int32")

    df = df.pipe(
        add_channel_names,
        codes_column="oecd_channel_code",
        target_column="clean_channel_name",
    )

    return df
