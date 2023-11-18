import pandas as pd
from oda_data import read_crs

from climate_finance.common.schema import ClimateSchema, CRS_MAPPING
from climate_finance.methodologies.imputed_multilateral.crs_tools import (
    add_crs_data_and_transform,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.crdf.recipient_perspective import (
    get_recipient_perspective,
)
from climate_finance.unfccc.cleaning_tools.channels import clean_string

UNIQUE_INDEX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    # ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.FINANCE_TYPE,
    # ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.PURPOSE_CODE,
]

CRS_INFO = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.FLOW_NAME,
    ClimateSchema.CATEGORY,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
]

OUTPUT_COLUMNS: list = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.FINANCIAL_INSTRUMENT,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.CATEGORY,
    ClimateSchema.CONCESSIONALITY,
    ClimateSchema.INDICATOR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_NAME,
    ClimateSchema.VALUE,
    ClimateSchema.TOTAL_VALUE,
    ClimateSchema.SHARE,
]

CRS_VALUES: list = [
    ClimateSchema.USD_COMMITMENT,
    ClimateSchema.USD_DISBURSEMENT,
]

CRDF_VALUES = [
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
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
        return data.loc[lambda d: d[ClimateSchema.PROVIDER_CODE].isin(party_code)]

    return data


def _convert_to_indicators(full_data: pd.DataFrame) -> pd.DataFrame:
    dfs = []

    # calculate shares based on data and commitments
    for column in CRDF_VALUES:
        indicator_flow = (
            full_data.assign(
                **{
                    ClimateSchema.VALUE: lambda d: d[column],
                    ClimateSchema.SHARE: lambda d: d[column]
                    / d[ClimateSchema.USD_COMMITMENT],
                    ClimateSchema.INDICATOR: column,
                }
            )
            .assign(**{ClimateSchema.FLOW_TYPE: ClimateSchema.USD_COMMITMENT})
            .drop(columns=CRDF_VALUES + [ClimateSchema.USD_COMMITMENT])
        )
        dfs.append(indicator_flow)

    return pd.concat(dfs, ignore_index=True)


def _create_disbursements_df(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(
        **{
            ClimateSchema.FLOW_TYPE: ClimateSchema.USD_DISBURSEMENT,
            ClimateSchema.VALUE: lambda d: d[ClimateSchema.USD_DISBURSEMENT]
            * d[ClimateSchema.SHARE],
        }
    ).drop(columns=ClimateSchema.USD_DISBURSEMENT)


def _create_commitments_df(data: pd.DataFrame) -> pd.DataFrame:
    return data.drop(columns=[ClimateSchema.USD_DISBURSEMENT])


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
    return df.loc[lambda d: d[ClimateSchema.PROVIDER_TYPE].isin(parties)].reset_index(
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

    # Clean project title
    crs_data[ClimateSchema.PROJECT_TITLE] = clean_string(
        crs_data[ClimateSchema.PROJECT_TITLE]
    )

    idx = [
        c
        for c in CRS_INFO + [ClimateSchema.PROJECT_TITLE]
        if c not in [ClimateSchema.FLOW_TYPE]
    ]

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


def add_crs_data(df: pd.DataFrame) -> pd.DataFrame:
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
    data = add_crs_data_and_transform(
        projects=df,
        crs=crs_df,
        unique_index=UNIQUE_INDEX,
        output_cols=OUTPUT_COLUMNS + CRDF_VALUES,
    )

    return data.loc[lambda d: d[ClimateSchema.VALUE] > 0]


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
        provider_code=party,
        force_update=force_update,
    ).pipe(_keep_multilateral_providers)
