import pandas as pd
from bblocks import clean_numeric_series
from oda_data import set_data_path
from oda_data.clean_data.channels import (
    add_channel_names,
    generate_channel_mapping_dictionary,
)

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.names import add_provider_agency_names
from climate_finance.unfccc.cleaning_tools.tools import (
    clean_currency,
    clean_status,
    fill_type_of_support_gaps,
    harmonise_type_of_support,
    rename_columns,
    clean_funding_source,
)

set_data_path(ClimateDataPath.raw_data)


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
    except KeyError:
        pass

    # Try to clean funding source
    try:
        df = df.pipe(clean_funding_source)
    except KeyError:
        pass

    # Try to clean financial instrument
    try:
        df["financial_instrument"] = df["financial_instrument"].str.lower().str.strip()
    except KeyError:
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

    # add names to the channel names column
    df = add_provider_agency_names(df)

    # create two sets of data to try to match
    df = df.assign(
        party_agency=lambda d: d.apply(
            lambda r: (
                r[ClimateSchema.PROVIDER_NAME]
                if str(r[ClimateSchema.PROVIDER_NAME]).lower().strip()
                == str(r[ClimateSchema.AGENCY_NAME]).lower().strip()
                else str(r[ClimateSchema.PROVIDER_NAME])
                + " "
                + str(r[ClimateSchema.AGENCY_NAME])
            ),
            axis=1,
        )
    )

    # Create a dictionary with channel names as keys and OECD DAC codes as values
    mapping_party_agency = generate_channel_mapping_dictionary(
        raw_data=df,
        channel_names_column="party_agency",
        export_missing_path=export_missing_path,
    )

    # Create a new column with the mapped channel codes
    df[ClimateSchema.CHANNEL_CODE] = (
        df["party_agency"].map(mapping_party_agency).astype("Int32")
    )

    df = df.pipe(
        add_channel_names,
        codes_column=ClimateSchema.CHANNEL_CODE,
        target_column=ClimateSchema.CHANNEL_NAME,
    )

    df = df.drop(columns=["party_agency"])

    return df
