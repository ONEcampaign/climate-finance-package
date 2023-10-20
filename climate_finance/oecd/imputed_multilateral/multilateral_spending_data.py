import pandas as pd
from oda_data import read_crs

from climate_finance.oecd.cleaning_tools.schema import CrsSchema, CRS_MAPPING
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)
from climate_finance.oecd.imputed_multilateral.crs_tools import match_projects_with_crs

UNIQUE_INDEX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.FINANCE_TYPE,
    # CrsSchema.FLOW_CODE,
    CrsSchema.FLOW_TYPE,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.PURPOSE_CODE,
]

CRS_INFO = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.FLOW_CODE,
    CrsSchema.FLOW_TYPE,
    CrsSchema.FLOW_NAME,
    CrsSchema.CATEGORY,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.FLOW_MODALITY,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
]

OUTPUT_COLUMNS: list = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.AGENCY_CODE,
    CrsSchema.CRS_ID,
    CrsSchema.PROJECT_ID,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.FLOW_MODALITY,
    CrsSchema.FINANCIAL_INSTRUMENT,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.CATEGORY,
    CrsSchema.CONCESSIONALITY,
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

CRDF_VALUES = [
    CrsSchema.ADAPTATION_VALUE,
    CrsSchema.MITIGATION_VALUE,
    CrsSchema.CROSS_CUTTING_VALUE,
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


def _convert_to_indicators(full_data: pd.DataFrame) -> pd.DataFrame:
    dfs = []

    # calculate shares based on data and commitments
    for column in CRDF_VALUES:
        indicator_flow = (
            full_data.assign(
                **{
                    CrsSchema.VALUE: lambda d: d[column],
                    CrsSchema.SHARE: lambda d: d[column] / d[CrsSchema.USD_COMMITMENT],
                    CrsSchema.INDICATOR: column,
                }
            )
            .assign(**{CrsSchema.FLOW_TYPE: CrsSchema.USD_COMMITMENT})
            .drop(columns=CRDF_VALUES + [CrsSchema.USD_COMMITMENT])
        )
        dfs.append(indicator_flow)

    return pd.concat(dfs, ignore_index=True)


def _create_disbursements_df(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(
        **{
            CrsSchema.FLOW_TYPE: CrsSchema.USD_DISBURSEMENT,
            CrsSchema.VALUE: lambda d: d[CrsSchema.USD_DISBURSEMENT]
            * d[CrsSchema.SHARE],
        }
    ).drop(columns=CrsSchema.USD_DISBURSEMENT)


def _create_commitments_df(data: pd.DataFrame) -> pd.DataFrame:
    return data.drop(columns=[CrsSchema.USD_DISBURSEMENT])


def _manual_duplicate_correction(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[lambda d: d.share.round(2) == 2.00, ["share", "value"]] /= 2
    return data


def _deal_with_shares_greater_than_1(data: pd.DataFrame) -> pd.DataFrame:
    # if it can be rounded to 1, round it to 1
    data.loc[lambda d: d.share.between(1, 1.1), ["share"]] = 1

    # drop any projects above 1.01
    data = data.loc[lambda d: ~(d.share > 1.01)]

    return data.reset_index(drop=True)


def convert_to_flowtypes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape the dataframe so that there is only 1 value column per row. Each
    flow type (commitment, disbursement, net disbursement) will have its own
    row.

    Args:
        data: The dataframe to reshape.

    Returns:
        The reshaped dataframe.

    """
    indicators_data = _convert_to_indicators(data)

    # create disbursements version
    disbursements = _create_disbursements_df(data=indicators_data)

    # Create commitments version
    commitments = _create_commitments_df(data=indicators_data)

    # combine commitments and disbursements
    data = pd.concat([commitments, disbursements], ignore_index=True)

    # manual duplicate correction
    data = _manual_duplicate_correction(data)

    # deal with shares that are > 1
    data = _deal_with_shares_greater_than_1(data)

    return data.filter(OUTPUT_COLUMNS)


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


def _convert_crs_values_to_million(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the output of the multilateral CRS data.

    Args:
        data: The dataframe to clean.

    Returns:
        The cleaned dataframe.

    """

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

    return df.drop_duplicates(subset=UNIQUE_INDEX, keep="first").filter(UNIQUE_INDEX)


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
    crs_data = read_crs(years=years)

    crs_data = crs_data.rename(columns=CRS_MAPPING)

    idx = [c for c in CRS_INFO if c not in [CrsSchema.FLOW_TYPE]]

    # Filter parties
    crs_data = _filter_parties(data=crs_data, party_code=party_code)

    # group any clear duplicates

    crs_data = crs_data.filter(idx + CRS_VALUES)

    crs_data = crs_data.pipe(idx_to_str, idx=idx)

    # group by and sum
    crs_data = (
        crs_data.groupby(idx, observed=True, dropna=False)[CRS_VALUES]
        .sum(numeric_only=True)
        .reset_index()
        .pipe(set_crs_data_types)
    )

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

    # Get a version of the CRS data that can be matched with the multilateral data.
    crs_df = _get_crs_to_match(
        years=df.year.unique().tolist(),
        party_code=df.oecd_party_code.unique().tolist(),
    )

    # match projects with crs
    matched = match_projects_with_crs(
        projects=df,
        crs=crs_df,
        unique_index=UNIQUE_INDEX,
        output_cols=OUTPUT_COLUMNS + CRS_VALUES,
    )

    # add back to original df
    df = df.pipe(idx_to_str, idx=UNIQUE_INDEX)
    matched = matched.pipe(idx_to_str, idx=UNIQUE_INDEX)
    data = df.merge(matched, on=UNIQUE_INDEX, how="left", suffixes=("", "_crs")).pipe(
        set_crs_data_types
    )

    # convert figures to millions
    data = _convert_crs_values_to_million(data)

    # Keep only relevant columns
    return data.filter(items=OUTPUT_COLUMNS + CRS_VALUES + CRDF_VALUES)


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
