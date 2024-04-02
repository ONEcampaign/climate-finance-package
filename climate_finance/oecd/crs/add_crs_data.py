import numpy as np
import pandas as pd
from oda_data.clean_data.channels import clean_string

from climate_finance.common.analysis_tools import check_codes_type
from climate_finance.common.schema import ClimateSchema, MAIN_FLOWS, CLIMATE_VALUES
from climate_finance.oecd.cleaning_tools.tools import idx_to_str
from climate_finance.oecd.crs.get_data import read_clean_crs

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


def _convert_crs_values_to_million(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the output of the multilateral CRS data.

    Args:
        data: The dataframe to clean.

    Returns:
        The cleaned dataframe.

    """

    for column in MAIN_FLOWS:
        # convert to USD
        data[column] = data[column] * 1e6

    return data


def get_matching_crs(
    years: list[int], provider_code: str | list[str] | None = None
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
    crs_data = read_clean_crs(years=years)

    # Clean project title
    crs_data[ClimateSchema.PROJECT_TITLE] = clean_string(
        crs_data[ClimateSchema.PROJECT_TITLE]
    )

    # Create new index to summarise the data
    idx = [
        c
        for c in CRS_INFO + [ClimateSchema.PROJECT_TITLE]
        if c not in [ClimateSchema.FLOW_TYPE]
    ]

    # Filter for the required providers
    if provider_code is not None:
        provider_code = check_codes_type(codes=provider_code)
        crs_data = crs_data.loc[
            lambda d: d[ClimateSchema.PROVIDER_CODE].isin(provider_code)
        ]

    # Convert the index to strings
    crs_data = crs_data.pipe(idx_to_str, idx=idx)

    # group by and sum
    crs_data = (
        crs_data.groupby(idx, observed=True, dropna=False)[MAIN_FLOWS]
        .sum(numeric_only=True)
        .reset_index()
    )

    # Convert values to millions
    crs_data = _convert_crs_values_to_million(crs_data)

    return crs_data


def _create_valid_unique_idx(
    unique_idx: list, projects_df: pd.DataFrame, crs_df: pd.DataFrame
) -> list:
    return [c for c in unique_idx if c in projects_df.columns and c in crs_df.columns]


def _fill_missing_project_id_with_title(projects_df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing project ids with project titles"""

    # if project id and project title are not in the dataframe, return the dataframe
    if not (
        ClimateSchema.PROJECT_ID in projects_df.columns
        and ClimateSchema.PROJECT_TITLE in projects_df.columns
    ):
        return projects_df

    # fill missing project ids with project titles
    projects_df[ClimateSchema.PROJECT_ID] = (
        projects_df[ClimateSchema.PROJECT_ID]
        .replace("nan", np.nan, regex=False)
        .fillna(projects_df[ClimateSchema.PROJECT_TITLE])
    )
    return projects_df


def clean_idx_to_str(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    """Convert idx to string for consistent merge"""

    data = data.copy()

    for col in idx:
        data[col] = (
            data[col]
            .astype("string[pyarrow]")
            .replace("nan", np.nan, regex=False)
            .replace("<NA>", np.nan, regex=False)
        ).fillna("missing")

    return data.pipe(idx_to_str, idx=idx)


def _match_projects_with_crs(
    crs: pd.DataFrame, projects: pd.DataFrame, merge_idx: list[str]
) -> pd.DataFrame:
    """Match projects with CRS"""
    # Fill missing project ids with project titles
    projects = _fill_missing_project_id_with_title(projects_df=projects)

    # Merge the projects and CRS info
    climate_crs = projects.merge(
        crs, on=merge_idx, how="outer", suffixes=("_p", "_crs"), indicator=True
    )

    # Return the data as matched, dropping any columns with _crs suffix
    return climate_crs


def _group_at_unique_index_level_and_sum(
    data: pd.DataFrame, unique_index: list[str], agg_col: str | list[str]
) -> pd.DataFrame:
    # Group the projects and CRS info at the unique index level and sum the values
    return (
        data.groupby(unique_index, observed=True, dropna=False)[agg_col]
        .sum()
        .reset_index()
    )


def _keep_only_matched_projects(
    data: pd.DataFrame, suffix: str = "_crs"
) -> pd.DataFrame:
    """Assumes indicator column is present"""
    return (
        data.loc[lambda d: d["_merge"] == "both"]
        .drop(columns=["_merge"])
        .drop(columns=[c for c in data.columns if c.endswith(suffix)])
        .rename(
            columns={c: c.replace("_p", "") for c in data.columns if c.endswith("_p")}
        )
    )


def _keep_only_unmatched_projects(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.loc[lambda d: d["_merge"] == "left_only"]
        .drop(columns=["_merge"])
        .drop(columns=[c for c in data.columns if c.endswith("_crs")])
        .rename(
            columns={c: c.replace("_p", "") for c in data.columns if c.endswith("_p")}
        )
    )


def _keep_only_unmatched_crs(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.loc[lambda d: d["_merge"] == "right_only"]
        .drop(columns=["_merge"])
        .drop(columns=[c for c in data.columns if c.endswith("_p")])
        .rename(
            columns={
                c: c.replace("_crs", "") for c in data.columns if c.endswith("_crs")
            }
        )
    )


def _merge_unique_projects_with_matched_crs_data(
    unique_projects: pd.DataFrame,
    unique_climate_crs: pd.DataFrame,
    idx: list[str],
) -> pd.DataFrame:
    # Merge the projects and CRS info
    return unique_projects.merge(
        unique_climate_crs,
        on=idx,
        how="outer",
        suffixes=("", "_projects"),
        indicator=True,
    ).drop(columns=[c for c in unique_projects.columns if c.endswith("_projects")])


def _add_climate_total(data: pd.DataFrame) -> pd.DataFrame:
    # Add the climate total
    return data.assign(
        **{ClimateSchema.CLIMATE_UNSPECIFIED: lambda d: d[CLIMATE_VALUES].sum(axis=1)}
    )


def _create_climate_share_columns(data: pd.DataFrame) -> pd.DataFrame:
    # Create the share columns
    for col in CLIMATE_VALUES + [ClimateSchema.CLIMATE_UNSPECIFIED]:
        data[f"{col}_share"] = data[col] / data[ClimateSchema.USD_COMMITMENT]

    return data


def _remove_implausible_shares(
    data: pd.DataFrame,
) -> pd.DataFrame:
    # clean_data keeps the rows with plausible shares
    clean_data = data.loc[
        lambda d: d[f"{ClimateSchema.CLIMATE_UNSPECIFIED}_share"] <= 1.1
    ]

    return clean_data


def _transform_to_flow_type(data: pd.DataFrame, flow_type: str) -> pd.DataFrame:
    data = data.copy(deep=True)

    data[ClimateSchema.FLOW_TYPE] = flow_type

    for column in CLIMATE_VALUES:
        data[column] = data[f"{column}_share"] * data[flow_type]

    return data


def _clean_climate_crs_output(data: pd.DataFrame) -> pd.DataFrame:
    # drop all columns with "share" in the name
    return data.drop(
        columns=[c for c in data.columns if "share" in c]
        + [
            ClimateSchema.USD_DISBURSEMENT,
            ClimateSchema.USD_COMMITMENT,
            ClimateSchema.USD_NET_DISBURSEMENT,
            ClimateSchema.USD_RECEIVED,
            ClimateSchema.USD_GRANT_EQUIV,
            ClimateSchema.CLIMATE_UNSPECIFIED,
        ]
    )


def _combine_not_matched_data(
    df1: pd.DataFrame, df2: pd.DataFrame, output_cols: list
) -> pd.DataFrame:
    return (
        pd.concat(
            [df1, df2],
            ignore_index=True,
        )
        .sort_values(by=CLIMATE_VALUES, ascending=(False, False, False))
        .drop_duplicates(keep="first")
        .filter(output_cols)
    )


def add_crs_info(
    crs: pd.DataFrame, projects: pd.DataFrame, unique_index: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Merge projects and CRS data
    matched_crs = _match_projects_with_crs(
        crs=crs, projects=projects, merge_idx=unique_index
    )

    # Identify matched projects
    climate_crs = _keep_only_matched_projects(data=matched_crs)

    # Identify not matched projects
    not_matched = _keep_only_unmatched_projects(data=matched_crs)

    # Identify unmatched CRS
    unmatched_crs = _keep_only_unmatched_crs(data=matched_crs).filter(crs.columns)

    return climate_crs, not_matched, unmatched_crs


def summarise_matched_data_without_year(
    matched_df: pd.DataFrame, unique_index: list[str]
) -> pd.DataFrame:
    # remove year from unique index
    unique_index = [c for c in unique_index if c != ClimateSchema.YEAR]

    # Group climate CRS at the unique index level and sum the values
    matched_no_year = _group_at_unique_index_level_and_sum(
        data=matched_df,
        unique_index=unique_index,
        agg_col=[ClimateSchema.USD_COMMITMENT] + CLIMATE_VALUES,
    )
    return matched_no_year


def add_climate_totals(merged_climate_data: pd.DataFrame) -> pd.DataFrame:
    # Add the climate total and share columns
    merged_climate_data = merged_climate_data.pipe(_add_climate_total).pipe(
        _create_climate_share_columns
    )

    # Remove implausible shares
    merged_climate_data = _remove_implausible_shares(data=merged_climate_data)

    return merged_climate_data


def merge_crs_and_climate_totals(
    starting_crs: pd.DataFrame,
    merged_climate_data: pd.DataFrame,
    unique_index: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # remove year from unique index
    unique_index = [c for c in unique_index if c != ClimateSchema.YEAR]

    # Merge shares back to climate CRS
    full_climate_crs = starting_crs.merge(
        merged_climate_data,
        on=unique_index,
        how="outer",
        suffixes=("", "_shares"),
        indicator=True,
    )

    # Identify matched projects
    matched_data = _keep_only_matched_projects(data=full_climate_crs, suffix="_shares")

    # The full unmatched CRS
    not_matched = _keep_only_unmatched_projects(data=full_climate_crs)

    return matched_data, not_matched


def transform_to_flow_types(data: pd.DataFrame):
    # transform into flow types
    commitments = _transform_to_flow_type(
        data=data, flow_type=ClimateSchema.USD_COMMITMENT
    )
    disbursements = _transform_to_flow_type(
        data=data, flow_type=ClimateSchema.USD_DISBURSEMENT
    )

    net_disbursements = _transform_to_flow_type(
        data=data, flow_type=ClimateSchema.USD_NET_DISBURSEMENT
    )

    grant_equivalent = _transform_to_flow_type(
        data=data, flow_type=ClimateSchema.USD_GRANT_EQUIV
    )

    # Concatenate the dataframes
    full_climate_crs_by_flow_type = pd.concat(
        [commitments, disbursements, net_disbursements, grant_equivalent],
        ignore_index=True,
    )

    return full_climate_crs_by_flow_type


def add_crs_data_pipeline(
    crs_data: pd.DataFrame, projects_to_match: pd.DataFrame, idx: list[str]
):
    # Create a new index. Excludes year and makes sure that all the columns are
    # in the projects and crs dataframes
    unique_index = _create_valid_unique_idx(
        unique_idx=idx, projects_df=projects_to_match, crs_df=crs_data
    )

    projects_to_match = clean_idx_to_str(data=projects_to_match, idx=unique_index)
    crs_data = clean_idx_to_str(data=crs_data, idx=unique_index)

    # Add CRS info
    matched_projects, not_matched, unmatched_crs = add_crs_info(
        crs=crs_data, projects=projects_to_match, unique_index=unique_index
    )

    # Add matched data back to projects
    matched_unique_projects = summarise_matched_data_without_year(
        matched_df=matched_projects,
        unique_index=unique_index,
    )

    # Add climate totals
    matched_unique_projects_with_climate_totals = add_climate_totals(
        merged_climate_data=matched_unique_projects
    )

    # Merge the matched projects with the unique projects with climate totals
    full_climate_crs, not_matched_climate_totals = merge_crs_and_climate_totals(
        starting_crs=crs_data,
        merged_climate_data=matched_unique_projects_with_climate_totals,
        unique_index=unique_index,
    )

    # Transform to flow types
    full_climate_crs_by_flow_type = transform_to_flow_types(data=full_climate_crs)

    # Clean the output
    clean_data = _clean_climate_crs_output(data=full_climate_crs_by_flow_type)

    # combine not matched data
    # not_matched_combined = _combine_not_matched_data(
    #     df1=not_matched,
    #     df2=not_matched_climate_totals,
    #     output_cols=projects_to_match.columns,
    # )
    not_matched_combined = not_matched.copy().filter(projects_to_match.columns)

    return clean_data, not_matched_combined
