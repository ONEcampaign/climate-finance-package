from pathlib import Path

import pandas as pd
from oda_data import set_data_path

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.climate_analysis.tools import check_and_filter_parties
from climate_finance.oecd.climate_related_activities.tools import (
    download_file,
    load_or_download,
    rename_marker_columns,
    marker_columns_to_numeric,
    clean_columns,
)

FILE_PATH: Path = (
    ClimateDataPath.raw_data / "oecd_climate_recipient_perspective.feather"
)

set_data_path(ClimateDataPath.raw_data)

BASE_URL: str = "https://webfs.oecd.org/climate/RecipientPerspective/CRDF-RP-2000-"

VALUES = [
    "climate_adaptation_value",
    "climate_mitigation_value",
    "overlap_commitment_current",
    "climate_related_development_finance_commitment_current",
    "share_of_the_underlying_commitment_when_available",
]

UNIQUE_INDEX = [
    "year",
    "provider_code",
    "agency_code",
    "crs_identification_n",
    "donor_project_n",
    "recipient_code",
    "purpose_code",
]

TOTAL_COL: str = "climate_related_development_finance_commitment_current"


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
    # Define the columns
    climate = "climate_related_development_finance_commitment_current"
    share = "share_of_the_underlying_commitment_when_available"

    # Add imputed total
    df["total_value"] = df[climate] / df[share]

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
        .rename(columns={f"{marker}_value": "value"})  # Rename the value column
        .drop(columns=[marker])  # Drop the marker column
        .assign(share=lambda d: d.value / d["total_value"])  # Add a share column
        .drop(columns=[TOTAL_COL])  # Drop the total column
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
        df.loc[lambda d: d.overlap_commitment_current > 0]  # Only where overlap is > 0
        .copy()  # Make a copy of the dataframe
        .assign(indicator="climate_cross_cutting")  # Add a column with the marker name
        .rename(columns={"overlap_commitment_current": "value"})  # Rename overlap
        .drop_duplicates(subset=UNIQUE_INDEX, keep="first")  # Drop duplicates
        .assign(share=lambda d: d.value / d["total_value"])  # Add a share column
        .drop(columns=[TOTAL_COL])  # Drop the total column
    )


def get_recipient_perspective(
    start_year: int,
    end_year: int,
    party: str | list[str] | None = None,
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

    # Filter for years
    df = df.loc[lambda d: d.year.isin(years)]

    # Rename markers
    df = rename_marker_columns(df)

    # Convert markers to multilateral
    df = marker_columns_to_numeric(df)

    # Drop duplicates
    df = df.drop_duplicates(keep="first")

    # Fix errors in recipient code
    df = df.replace({"recipient_code": {"998": "9998"}})

    # Add imputed total
    df = add_imputed_total(df)

    # Get dataframes for each marker
    adaptation = get_marker_data_and_share(df, marker="climate_adaptation")
    mitigation = get_marker_data_and_share(df, marker="climate_mitigation")
    overlap = get_overlap(df)

    # Combine dataframes
    data = pd.concat([adaptation, mitigation, overlap], ignore_index=True)

    # clean columns
    data = clean_columns(data)

    # Only values > 0
    data = data.loc[lambda d: d.value > 0]

    # Check parties
    data = check_and_filter_parties(data, party=party, party_col="party")

    return data
