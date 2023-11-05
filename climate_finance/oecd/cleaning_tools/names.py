import pandas as pd
from oda_data import read_crs

from climate_finance.config import ClimateDataPath
from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.cleaning_tools.tools import (
    rename_crs_columns,
    idx_to_str,
    set_crs_data_types,
)
from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)


def _create_names(
    crs_year: int, crs_idx: list[str], merge_idx: list[str], file_name: str
) -> None:
    # Get the CRS data for the year specified
    crs = (
        read_crs([crs_year])  # read the CRS
        .pipe(rename_crs_columns)  # rename the columns
        .drop_duplicates(subset=crs_idx)  # drop duplicates by index
        .filter(items=crs_idx)  # keep only the columns in the index
        .pipe(idx_to_str, idx=crs_idx)  # convert the index to strings
    )

    # Get the CRDF data for the year specified
    crdf = (
        get_recipient_perspective(start_year=crs_year, end_year=crs_year)
        .filter(items=crs_idx)  # keep only the columns in the index
        .drop_duplicates(subset=crs_idx)  # drop duplicates by index
        .pipe(idx_to_str, idx=merge_idx)
    )

    # Join the two datasets with different naming conventions
    crs = (
        pd.concat([crs, crdf], ignore_index=True)
        .drop_duplicates(subset=merge_idx)
        .reset_index(drop=True)
    )

    # Save the data
    crs.to_feather(
        ClimateDataPath.scripts / "oecd" / "cleaning_tools" / f"{file_name}.feather"
    )


def create_provider_agency_names(crs_year: int = 2021) -> None:
    # Define the index that will be used to identify unique names
    idx = [
        CrsSchema.PARTY_CODE,
        CrsSchema.AGENCY_CODE,
        CrsSchema.PARTY_NAME,
        CrsSchema.AGENCY_NAME,
    ]

    merge_idx = [CrsSchema.PARTY_CODE, CrsSchema.AGENCY_CODE]

    _create_names(
        crs_year=crs_year,
        crs_idx=idx,
        merge_idx=merge_idx,
        file_name="provider_agency_names",
    )


def create_provider_names(crs_year: int = 2021) -> None:
    # Define the index that will be used to identify unique names
    idx = [CrsSchema.PARTY_CODE, CrsSchema.PARTY_NAME]

    merge_idx = [CrsSchema.PARTY_CODE]

    _create_names(
        crs_year=crs_year,
        crs_idx=idx,
        merge_idx=merge_idx,
        file_name="provider_names",
    )


def create_recipient_names(crs_year: int = 2021) -> None:
    # Define the index that will be used to identify unique names
    idx = [CrsSchema.RECIPIENT_CODE, CrsSchema.RECIPIENT_NAME]

    merge_idx = [CrsSchema.RECIPIENT_CODE]

    _create_names(
        crs_year=crs_year,
        crs_idx=idx,
        merge_idx=merge_idx,
        file_name="recipient_names",
    )


def read_provider_agency_names() -> pd.DataFrame:
    return pd.read_feather(
        ClimateDataPath.scripts
        / "oecd"
        / "cleaning_tools"
        / f"provider_agency_names.feather"
    )


def read_provider_names() -> pd.DataFrame:
    return pd.read_feather(
        ClimateDataPath.scripts / "oecd" / "cleaning_tools" / f"provider_names.feather"
    )


def read_recipient_names() -> pd.DataFrame:
    return pd.read_feather(
        ClimateDataPath.scripts / "oecd" / "cleaning_tools" / f"recipient_names.feather"
    )


def _add_names(data: pd.DataFrame, names: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    data = data.pipe(idx_to_str, idx=idx)
    names = names.pipe(idx_to_str, idx=idx)

    # Add the names to the data
    data = data.merge(
        names,
        on=idx,
        how="left",
        suffixes=("", "_names"),
    )

    # drop any columns which contain the string "_crs_names"
    data = data.drop(columns=[c for c in data.columns if "_names" in c])

    return data.pipe(set_crs_data_types)


def add_provider_agency_names(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add the provider agency names to the data. This function needs both Provider and
    Agency codes in order to create matches.

    Args:
        data: The data to add the provider agency names to.
        crs_year: The year of the CRS data to use to get names. Defaults to 2021.

    Returns:
        The data with the provider agency names added.

    """
    names = read_provider_agency_names()

    return data.pipe(
        _add_names, names=names, idx=[CrsSchema.PARTY_CODE, CrsSchema.AGENCY_CODE]
    )


def add_provider_names(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add the provider agency names to the data. This function needs Provider
    codes in order to create matches.

    Args:
        data: The data to add the provider agency names to.
        crs_year: The year of the CRS data to use to get names. Defaults to 2021.

    Returns:
        pd.DataFrame: The data with the provider names added.

    """
    names = read_provider_names()

    return data.pipe(_add_names, names=names, idx=[CrsSchema.PARTY_CODE])


def add_recipient_names(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add the provider agency names to the data. This function needs Recipient
    codes in order to create matches.

    Args:
        data: The data to add the provider agency names to.
        crs_year: The year of the CRS data to use to get names. Defaults to 2021.

    Returns:
        pd.DataFrame: The data with the provider names added.

    """
    names = read_recipient_names()

    if CrsSchema.RECIPIENT_NAME in data.columns:
        data = data.drop(columns=CrsSchema.RECIPIENT_NAME)

    data = data.pipe(_add_names, names=names, idx=[CrsSchema.RECIPIENT_CODE])

    data = data.astype(
        {CrsSchema.RECIPIENT_NAME: "str", CrsSchema.RECIPIENT_CODE: "Int32"}
    ).assign(
        **{
            CrsSchema.RECIPIENT_NAME: lambda d: d[
                CrsSchema.RECIPIENT_NAME
            ].combine_first(
                d[CrsSchema.RECIPIENT_CODE].map(
                    {
                        434: "Chile",
                        460: "Uruguay",
                        831: "Cook Islands",
                        270: "Seychelles",
                        382: "Saint Kitts and Nevis",
                    }
                )
            )
        }
    )

    return data.pipe(set_crs_data_types)
