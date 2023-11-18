from pathlib import Path

import pandas as pd
from oda_data import set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.common.schema import ClimateSchema
from climate_finance.oecd.crdf.tools import (
    download_file,
    load_or_download,
    marker_columns_to_numeric,
)
from climate_finance.oecd.imputed_multilateral.tools import check_and_filter_parties

FILE_PATH: Path = (
    ClimateDataPath.raw_data / "oecd_climate_recipient_perspective.feather"
)

set_data_path(ClimateDataPath.raw_data)

BASE_URL: str = "https://webfs.oecd.org/climate/RecipientPerspective/CRDF-RP-2000-"

UNIQUE_INDEX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.PURPOSE_CODE,
]

MULTI_COLUMNS: list = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_TYPE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.PROVIDER_DETAILED,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.AGENCY_NAME,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.RECIPIENT_NAME,
    ClimateSchema.RECIPIENT_REGION,
    ClimateSchema.RECIPIENT_INCOME,
    ClimateSchema.CONCESSIONALITY,
    ClimateSchema.CHANNEL_CODE_DELIVERY,
    ClimateSchema.CHANNEL_NAME_DELIVERY,
    ClimateSchema.SECTOR_NAME,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.PURPOSE_NAME,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.FINANCIAL_INSTRUMENT,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.PROJECT_TITLE,
    ClimateSchema.PROJECT_DESCRIPTION,
    ClimateSchema.GENDER,
    ClimateSchema.INDICATOR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.VALUE,
    ClimateSchema.TOTAL_VALUE,
    ClimateSchema.SHARE,
]


def add_imputed_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an imputed total column to the dataframe.
    This is done by dividing the climate related development finance commitment
    by the share of the underlying commitment.

    Args:
        df: The dataframe to add the imputed total to.

    Returns:
        The dataframe with the imputed total added.

    """

    # Add imputed total
    df[ClimateSchema.TOTAL_VALUE] = (
            df[ClimateSchema.CLIMATE_FINANCE_VALUE] / df[ClimateSchema.COMMITMENT_CLIMATE_SHARE]
    )

    return df


def get_marker_data_and_share(df: pd.DataFrame, marker: str):
    """
    Get the marker data for a given marker.

    Args:
        df: The dataframe to get the marker data from.
        marker: The marker to get the data for.

    Returns:
        The marker data.

    """

    return (
        df.loc[lambda d: d[marker] > 0]  # Only keep rows where the marker is > 0
        .copy()  # Make a copy of the dataframe
        .assign(indicator=marker)  # Add a column with the marker name
        .rename(columns={f"{marker}_value": ClimateSchema.VALUE})  # Rename the value column
        .drop(columns=[marker])  # Drop the marker column
        .assign(
            share=lambda d: d[ClimateSchema.VALUE] / d[ClimateSchema.TOTAL_VALUE]
        )  # Add a share column
        .drop(columns=[ClimateSchema.CLIMATE_FINANCE_VALUE])  # Drop the total column
    )


def get_overlap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the overlap data. In the recipient view, this is a specific column. It caputres
    where the same project is marked as both adaptation and mitigation.

    Args:
        df: The dataframe to get the overlap data from.

    Returns:
        The overlap data.

    """
    return (
        df.loc[
            lambda d: d[ClimateSchema.CROSS_CUTTING_VALUE] > 0
        ]  # Only where overlap is > 0
        .copy()  # Make a copy of the dataframe
        .assign(indicator=ClimateSchema.CROSS_CUTTING)  # Add a column with the marker name
        .rename(
            columns={ClimateSchema.CROSS_CUTTING_VALUE: ClimateSchema.VALUE}
        )  # Rename overlap
        .drop_duplicates(subset=UNIQUE_INDEX, keep="first")  # Drop duplicates
        .assign(
            share=lambda d: d[ClimateSchema.VALUE] / d[ClimateSchema.TOTAL_VALUE]
        )  # Add a share column
        .drop(columns=[ClimateSchema.CLIMATE_FINANCE_VALUE])  # Drop the total column
    )


def get_recipient_perspective(
    start_year: int,
    end_year: int,
    party: list[str] | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Get the provider perspective data from the OECD website. The data is read or downloaded
    and then reshaped to be in a 'longer' format where the different types of climate
    finance are indicators.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        force_update: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.

    Returns:

    """
    # Study years
    years = range(start_year, end_year + 1)

    # Check if data should be forced to update
    if force_update:
        download_file(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Try to load file
    df = load_or_download(base_url=BASE_URL, save_to_path=FILE_PATH)

    # Rename columns
    df = df.rename(
        columns={
            ClimateSchema.PROVIDER_NAME: f"{ClimateSchema.PROVIDER_NAME}_short",
            ClimateSchema.PROVIDER_DETAILED: ClimateSchema.PROVIDER_NAME,
        }
    )

    # Filter for years
    df = df.loc[lambda d: d.year.isin(years)]

    # filter for parties
    df = check_and_filter_parties(df, party=party, party_col=ClimateSchema.PROVIDER_NAME)

    # Convert markers to multilateral
    df = marker_columns_to_numeric(df)

    # Fix errors in recipient code
    df = df.replace({ClimateSchema.RECIPIENT_CODE: {"9998": "998"}})

    # Add flow type
    df = df.assign(flow_type="usd_commitment")

    return df
