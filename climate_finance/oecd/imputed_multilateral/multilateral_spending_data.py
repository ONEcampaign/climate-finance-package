import pandas as pd
from oda_data import read_crs

from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)
from climate_finance.oecd.cleaning_tools.schema import CrsSchema, CRS_MAPPING
from climate_finance.oecd.imputed_multilateral.crs_tools import match_projects_with_crs

UNIQUE_INDEX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.PURPOSE_CODE,
]


CRS_INFO = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.PARTY_NAME,
    CrsSchema.AGENCY_CODE,
    CrsSchema.AGENCY_NAME,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.RECIPIENT_NAME,
    CrsSchema.RECIPIENT_REGION_CODE,
    CrsSchema.RECIPIENT_REGION,
    CrsSchema.RECIPIENT_INCOME,
    CrsSchema.FLOW_CODE,
    CrsSchema.FLOW_NAME,
    CrsSchema.CATEGORY,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.FLOW_MODALITY,
    CrsSchema.USD_COMMITMENT,
    CrsSchema.USD_DISBURSEMENT,
    CrsSchema.USD_RECEIVED,
    CrsSchema.PROJECT_TITLE,
    CrsSchema.PROJECT_DESCRIPTION_SHORT,
    CrsSchema.PROJECT_DESCRIPTION,
    CrsSchema.SECTOR_CODE,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
]

OUTPUT_COLUMNS: list = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.PARTY_NAME,
    CrsSchema.PARTY_DETAILED,
    CrsSchema.PARTY_TYPE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.AGENCY_NAME,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.RECIPIENT_NAME,
    CrsSchema.RECIPIENT_REGION_CODE,
    CrsSchema.RECIPIENT_REGION,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
    CrsSchema.SECTOR_CODE,
    CrsSchema.SECTOR_NAME,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.PURPOSE_NAME,
    CrsSchema.FLOW_MODALITY,
    CrsSchema.FINANCIAL_INSTRUMENT,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.CATEGORY,
    CrsSchema.CONCESSIONALITY,
    CrsSchema.GENDER,
    CrsSchema.PROJECT_TITLE,
    CrsSchema.PROJECT_DESCRIPTION,
    CrsSchema.INDICATOR,
    CrsSchema.FLOW_TYPE,
    CrsSchema.FLOW_CODE,
    CrsSchema.FLOW_NAME,
    CrsSchema.VALUE,
    CrsSchema.TOTAL_VALUE,
    CrsSchema.SHARE,
]

CRS_VALUES: list = [
    CrsSchema.USD_COMMITMENT,
    CrsSchema.USD_DISBURSEMENT,
]


# multilateral providers
MULTI_PROVIDERS = ["Other multilateral", "Multilateral development bank"]


def _set_crs_types_to_strings(data: pd.DataFrame) -> pd.DataFrame:
    """
    Set the types of the CRS data to strings.

    This is done using the UNIQUE_INDEX global variable.

    Args:
        data: The CRS data to set the types for.

    Returns:
        The CRS data with the types set to strings.

    """
    return data.astype({k: str for k in UNIQUE_INDEX})


def _filter_parties(data, party_code: str | list[str] | None) -> pd.DataFrame:
    """
    Filter the CRS data to keep only the parties that are in the party_code list.

    Args:
        data: The CRS data to filter.
        party_code: The party code to filter. This can be a string, a list of strings, or None.
        If None, no filtering is performed.

    Returns:
        The CRS data filtered.

    """

    # Convert party code to list
    if isinstance(party_code, str):
        party_code = [party_code]

    # Filter donor code
    if party_code is not None:
        return data.loc[lambda d: d[CrsSchema.PARTY_CODE].isin(party_code)]

    return data


def _convert_to_flowtypes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape the dataframe so that there is only 1 value column per row. Each
    flow type (commitment, disbursement, net disbursement) will have its own
    row.

    Args:
        data: The dataframe to reshape.

    Returns:
        The reshaped dataframe.

    """

    dfs = []

    for column in [CrsSchema.USD_DISBURSEMENT]:
        dfs.append(
            data.assign(
                flow_type=column,
                value=lambda d: d[column] * d["share"],
            )
        )

    return pd.concat(dfs + [data], ignore_index=True)


def _keep_multilateral_providers(
    df: pd.DataFrame, parties: list[str] | None = None
) -> pd.DataFrame:
    """
    Filter to keep only multilateral providers.

    Args:
        df: The dataframe to filter.

    Returns:
        The filtered dataframe.

    """

    if parties is None:
        parties = MULTI_PROVIDERS

    # filter
    return df.loc[lambda d: d[CrsSchema.PARTY_TYPE].isin(parties)].reset_index(
        drop=True
    )


def _clean_multi_crs_output(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the output of the multilateral CRS data.

    Args:
        data: The dataframe to clean.

    Returns:
        The cleaned dataframe.

    """
    # filter to keep only the columns in the OUTPUT_COLUMNS global variable
    # and the CRS_VALUES global variable. Rename the columns to match the
    # OUTPUT_COLUMNS global variable.
    data = data.astype({k: str for k in UNIQUE_INDEX}).filter(
        OUTPUT_COLUMNS + CRS_VALUES
    )

    for column in CRS_VALUES:
        # convert to USD
        data[column] = data[column] * 1e6

    return data


def _get_unique_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the unique projects from the dataframe.

    Unique projects are defined by the UNIQUE_INDEX global variable.

    Args:
        df: The dataframe to get the unique projects from.

    Returns:
        The dataframe with the unique projects.

    """
    return (
        df.drop_duplicates(subset=UNIQUE_INDEX, keep="first")  # keep first only
        .filter(UNIQUE_INDEX)  # keep only the columns in UNIQUE_INDEX
        .astype({k: str for k in UNIQUE_INDEX})  # convert columns to string
    )


def _get_crs_to_match(
    years: list[int], party_code: str | list[str] | None = None
) -> pd.DataFrame:
    """
    Get the CRS data in order to match it to the multilateral data.
    This means reading the right years, renaming columns to match the multilateral
    naming conventions, and setting the types to strings.

    Args:
        years: The years to read.
        party_code: The donor code to read.

    Returns:
        The CRS data to match.

    """

    # Read the CRS data
    crs_data = read_crs(years=years).rename(columns=CRS_MAPPING)

    # Set the types to strings
    crs_data = _set_crs_types_to_strings(data=crs_data)

    # Filter parties
    crs_data = _filter_parties(data=crs_data, party_code=party_code)

    return crs_data


def add_crs_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function adds columns/details from the CRS to the multilateral data.
    This includes information on the flow type (commitment, disbursement, net disbursement).

    Args:
        df (pd.DataFrame): The multilateral data.

    Returns:
        pd.DataFrame: The multilateral data with the CRS details added.

    """

    # Identify the unique projects contained in the CRS. This is done by keeping only
    # the columns in the UNIQUE_INDEX global variable, dropping duplicates, and
    # converting the columns to strings.
    projects_df = _get_unique_projects(df)

    # Get a version of the CRS data that can be matched with the multilateral data.
    crs_df = _get_crs_to_match(
        years=df.year.unique().tolist(),
        party_code=projects_df.oecd_party_code.unique().tolist(),
    )

    # match projects with crs
    matched = match_projects_with_crs(
        projects=projects_df,
        crs=crs_df,
        unique_index=UNIQUE_INDEX,
        output_cols=CRS_INFO,
    )

    # add back to original df
    data = df.astype({k: str for k in UNIQUE_INDEX}).merge(
        matched, on=UNIQUE_INDEX, how="left", suffixes=("", "_crs")
    )

    # clean and standardise output
    data = _clean_multi_crs_output(data)

    # convert to flow types
    data = _convert_to_flowtypes(data)

    return data.filter(OUTPUT_COLUMNS)


def get_multilateral_data(
    start_year: int,
    end_year: int,
    party: list[str] | None = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Get the multilateral providers data from the recipients' perspective dataset.

    Args:
        start_year: The start year that should be covered in the data
        end_year: The end year that should be covered in the data
        party: Optionally, specify one or more parties. If not specified, all
        parties are included.
        force_update: If True, the data is updated from the source. This can potentially
        overwrite any data that has been downloaded to the 'raw_data' folder.

    Returns:
        The multilateral providers data.

    """
    return get_recipient_perspective(
        start_year=start_year,
        end_year=end_year,
        party=party,
        force_update=force_update,
    ).pipe(_keep_multilateral_providers)
