import numpy as np
import pandas as pd

from climate_finance.config import logger
from climate_finance.common.schema import ClimateSchema, CLIMATE_VALUES
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.get_oecd_data import get_oecd_bilateral


def flow_name_mapping() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        19: "Equity Investment",
        14: "Other Official Flows (non Export Credits)",
        30: "Private Development Finance",
        0: "Unspecified",
    }


def map_flow_name_to_code(data: pd.DataFrame, codes_col: str) -> pd.DataFrame:
    replacements = {"nan": np.nan, "": np.nan, "<NA>": np.nan, "Unspecified": np.nan}
    data[codes_col] = (
        data[codes_col]
        .replace(replacements)
        .astype("Int32")
        .map(flow_name_mapping())
        .fillna(data[codes_col])
    )
    return data


def get_yearly_crs_totals(
    start_year: int,
    end_year: int,
    by_index: list[str] | None = None,
    party: str | list[str] | None = None,
    methodology: str = "oecd_bilateral",
) -> pd.DataFrame:
    # get the crs data
    crs_data = get_oecd_bilateral(
        start_year=start_year,
        end_year=end_year,
        methodology=methodology,
        party=party,
    )

    # Make Cross-cutting negative
    crs_data.loc[lambda d: d[ClimateSchema.INDICATOR] == "Cross-cutting", "value"] *= -1

    # Create an index if none is provided
    if by_index is None:
        by_index = [
            c
            for c in crs_data.columns
            if c not in [ClimateSchema.VALUE, ClimateSchema.INDICATOR, ClimateSchema.USD_COMMITMENT]
        ]

    else:
        by_index = [c for c in by_index if c in crs_data.columns]

    # Get the group totals based on the selected index
    return (
        crs_data.groupby(by_index, observed=True)[ClimateSchema.VALUE].sum().reset_index()
    )


def _prepare_crs_and_projects(
    crs: pd.DataFrame, projects: pd.DataFrame, unique_index: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # convert new index to string, and set it
    projects = projects.pipe(idx_to_str, idx=unique_index).set_index(unique_index)
    crs = crs.pipe(idx_to_str, idx=unique_index).set_index(unique_index)

    # Convert CRS commitments and disbursements to millions of USD
    crs[[ClimateSchema.USD_COMMITMENT, ClimateSchema.USD_DISBURSEMENT]] *= 1e6

    return crs, projects


def _match_projects_with_crs(
    crs: pd.DataFrame, projects: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if (
        ClimateSchema.PROJECT_ID in projects.columns
        and ClimateSchema.PROJECT_TITLE in projects.columns
    ):
        projects[ClimateSchema.PROJECT_ID] = (
            projects[ClimateSchema.PROJECT_ID]
            .replace("nan", np.nan, regex=False)
            .fillna(projects[ClimateSchema.PROJECT_TITLE])
        )

    # Identify all the rows in the CRS that match the unique projects
    # Use the fact that both dataframes now have the same MultiIndex structure

    climate_crs = crs.loc[lambda d: d.index.isin(projects.index)]

    # Identify all the rows in the projects that didn't have a CRS match
    not_matched = projects.loc[lambda d: ~d.index.isin(climate_crs.index)]

    return climate_crs.reset_index(), not_matched.reset_index()


def _group_at_unique_index_level_and_sum(
    data: pd.DataFrame, unique_index: list[str], agg_col: str | list[str]
) -> pd.DataFrame:
    # Group the projects and CRS info at the unique index level and sum the values
    return (
        data.groupby(unique_index, observed=True, dropna=False)[agg_col]
        .sum()
        .reset_index()
    )


def _merge_projects_and_crs(
    unique_projects: pd.DataFrame,
    unique_climate_crs: pd.DataFrame,
    idx: list[str],
) -> pd.DataFrame:
    # Merge the projects and CRS info
    return unique_projects.merge(
        unique_climate_crs,
        on=idx,
        how="inner",
        suffixes=("", "_projects"),
    ).filter(idx + CLIMATE_VALUES + [ClimateSchema.USD_COMMITMENT])


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


def _identify_and_remove_implausible_shares(
    data: pd.DataFrame, projects_data: pd.DataFrame, unique_index: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # set the right index
    data = data.set_index(unique_index)
    projects_data = projects_data.set_index(unique_index)

    # Large shares keeps the rows with implausible shares
    large_shares = data.loc[lambda d: d[f"{ClimateSchema.CLIMATE_UNSPECIFIED}_share"] > 1.1]

    # clean_data keeps the rows with plausible shares
    clean_data = data.loc[lambda d: d[f"{ClimateSchema.CLIMATE_UNSPECIFIED}_share"] <= 1.1]

    # Filter the projects data to only keep the rows that are in large_shares
    large_shares_projects = projects_data.loc[
        lambda d: d.index.isin(large_shares.index)
    ]

    return large_shares_projects.reset_index(), clean_data.reset_index()


def _transform_to_flow_type(
    data: pd.DataFrame,
    flow_type: str,
) -> pd.DataFrame:
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
            ClimateSchema.CLIMATE_UNSPECIFIED,
        ]
    ).pipe(set_crs_data_types)


def _add_crs_info_and_transform_to_indicators(
    crs: pd.DataFrame, projects: pd.DataFrame, unique_index: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Create a new index. Excludes year and makes sure that all the columns are
    # in the projects and crs dataframes
    unique_index = [
        c
        for c in unique_index
        if c != "year" and c in projects.columns and c in crs.columns
    ]

    # Prepare CRS and Projects by setting the right index (and values scale for CRS)
    crs, projects = _prepare_crs_and_projects(
        crs=crs, projects=projects, unique_index=unique_index
    )

    # filter CRS to match and create not_matched dataframe
    climate_crs, not_matched = _match_projects_with_crs(crs=crs, projects=projects)

    # Group the unique index level and sum the values
    unique_climate_crs = _group_at_unique_index_level_and_sum(
        data=climate_crs, unique_index=unique_index, agg_col=ClimateSchema.USD_COMMITMENT
    )

    # Group the unique index level and sum the values
    unique_projects = _group_at_unique_index_level_and_sum(
        data=projects, unique_index=unique_index, agg_col=CLIMATE_VALUES
    )

    # Merge the projects and CRS info
    merged_climate_data = _merge_projects_and_crs(
        unique_projects=unique_projects,
        unique_climate_crs=unique_climate_crs,
        idx=unique_index,
    )

    # Add the climate total
    merged_climate_data = _add_climate_total(data=merged_climate_data)

    # Create the share columns
    merged_climate_data = _create_climate_share_columns(data=merged_climate_data)

    # Merge shares back to climate CRS
    full_climate_crs = climate_crs.merge(
        merged_climate_data, on=unique_index, how="left", suffixes=("", "_shares")
    )

    # Identify columns with implausible shares
    implausible_shares, full_climate_crs = _identify_and_remove_implausible_shares(
        data=full_climate_crs, projects_data=unique_projects, unique_index=unique_index
    )

    # Add the implausible shares to the not_matched dataframe
    not_matched = pd.concat([not_matched, implausible_shares], ignore_index=True)

    # transform into flow types

    commitments = _transform_to_flow_type(
        data=full_climate_crs, flow_type=ClimateSchema.USD_COMMITMENT
    )
    disbursements = _transform_to_flow_type(
        data=full_climate_crs, flow_type=ClimateSchema.USD_DISBURSEMENT
    )

    # Concatenate the dataframes
    full_climate_crs_by_flow_type = pd.concat(
        [commitments, disbursements], ignore_index=True
    )

    full_climate_crs_by_flow_type = full_climate_crs_by_flow_type.pipe(
        _clean_climate_crs_output
    )

    return full_climate_crs_by_flow_type, not_matched.pipe(set_crs_data_types)


def _calculate_unmatched_totals(unmatched: pd.DataFrame) -> pd.DataFrame:
    return (
        unmatched.groupby(["year"])[CLIMATE_VALUES].sum().sum(axis=1).div(1e6).round(0)
    )


def _remove_matched_from_crs(
    crs: pd.DataFrame, idx: list[str], matched_data: pd.DataFrame
) -> pd.DataFrame:
    idx = [
        c for c in idx if c != "year" and c in matched_data.columns and c in crs.columns
    ]

    crs = crs.pipe(idx_to_str, idx=idx).set_index(idx)
    matched_data = matched_data.pipe(idx_to_str, idx=idx).set_index(idx)
    crs = crs.loc[lambda d: ~d.index.isin(matched_data.index)]
    return crs.reset_index().pipe(set_crs_data_types)


def add_crs_data_and_transform(
    projects: pd.DataFrame,
    crs: pd.DataFrame,
    unique_index: list[str],
    output_cols: list[str],
) -> pd.DataFrame:
    """
    Match the projects with the CRS data.

    This is done by merging the projects with the CRS data on the columns in the
    UNIQUE_INDEX global variable. If there are projects that were not matched, a second
    attempt is made using a subset of the columns in the UNIQUE_INDEX global variable.

    Args:
        projects: The projects to match. This is a dataframe with the columns in unique_index.
        crs: The CRS data to match. This is a dataframe with the columns in unique_index.
        unique_index: The columns to use to match the projects with the CRS data.
        output_cols: The columns to keep in the output.

    Returns:
        The projects matched with the CRS data.

    """
    # convert index to str
    projects = projects.pipe(idx_to_str, idx=unique_index + [ClimateSchema.PROJECT_TITLE])
    crs = crs.pipe(idx_to_str, idx=unique_index + [ClimateSchema.PROJECT_TITLE])

    # Perform an initial merge. It will be done considering all the columns in the
    # UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    matched, not_matched = _add_crs_info_and_transform_to_indicators(
        crs=crs,
        projects=projects,
        unique_index=[ClimateSchema.PROJECT_TITLE] + unique_index,
    )

    crs = _remove_matched_from_crs(crs, idx=unique_index, matched_data=matched)

    logger.debug(
        f"Didn't match \n{len(not_matched)} projects with CRS data (first pass)"
    )

    # Define the different passes that will be performed to try to merge the data
    # This is done by specifying the merge columns
    unique_index_configurations = [
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.RECIPIENT_CODE,
            ClimateSchema.PROJECT_ID,
            ClimateSchema.PURPOSE_CODE,
        ],
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.RECIPIENT_CODE,
            ClimateSchema.CRS_ID,
            ClimateSchema.PURPOSE_CODE,
        ],
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.RECIPIENT_CODE,
            ClimateSchema.PROJECT_TITLE,
            ClimateSchema.PURPOSE_CODE,
        ],
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.PROJECT_TITLE,
            ClimateSchema.PURPOSE_CODE,
        ],
        unique_index,
        [
            ClimateSchema.PROVIDER_CODE,
            ClimateSchema.PROJECT_ID,
            ClimateSchema.PURPOSE_CODE,
        ],
    ]

    # Loop through each config and try to merge the data
    for pass_number, idx_config in enumerate(unique_index_configurations):
        matched_, not_matched = _add_crs_info_and_transform_to_indicators(
            crs=crs, projects=not_matched, unique_index=idx_config
        )
        logger.debug(
            f"Didn't match \n{len(not_matched)} projects with CRS data"
            f" (attempt {pass_number+2})"
        )
        crs = _remove_matched_from_crs(crs, idx=idx_config, matched_data=matched_)
        matched = pd.concat([matched, matched_], ignore_index=True)

    # Log the usd millions value for projects that were not matched
    logger.debug(
        f"The total unmatched in millions of USD is:"
        f"\n{_calculate_unmatched_totals(unmatched=not_matched)}\n"
    )

    not_matched_text = "Data only reported in the CRDF as commitments"

    not_matched_values = {
        ClimateSchema.PROJECT_TITLE: not_matched_text,
        ClimateSchema.RECIPIENT_CODE: "998",
        ClimateSchema.PURPOSE_CODE: "99810",
        ClimateSchema.PROJECT_ID: "aggregate",
        ClimateSchema.CRS_ID: "aggregate",
        ClimateSchema.CHANNEL_CODE: "0",
        ClimateSchema.FLOW_CODE: "0",
        ClimateSchema.CHANNEL_CODE_DELIVERY: not_matched_text,
        ClimateSchema.CATEGORY: not_matched_text,
        ClimateSchema.FLOW_TYPE: ClimateSchema.USD_COMMITMENT,
    }
    other = [
        ClimateSchema.FINANCE_TYPE,
        ClimateSchema.FLOW_NAME,
        ClimateSchema.FLOW_MODALITY,
    ]
    not_matched = (
        not_matched.assign(**not_matched_values)
        .rename(columns={ClimateSchema.FINANCIAL_INSTRUMENT: ClimateSchema.FLOW_NAME})
        .pipe(map_flow_name_to_code, codes_col=ClimateSchema.FLOW_CODE)
        .groupby(
            [ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE, ClimateSchema.AGENCY_CODE]
            + list(not_matched_values)
            + other,
            observed=True,
            dropna=False,
        )
        .sum(numeric_only=True)
        .reset_index()
        .filter(matched.columns)
        .dropna(subset=[ClimateSchema.YEAR])
    )

    matched = pd.concat([matched, not_matched], ignore_index=True)

    # melt indicators
    data = matched.melt(
        id_vars=[c for c in matched.columns if c not in CLIMATE_VALUES],
        value_vars=CLIMATE_VALUES,
        var_name=ClimateSchema.INDICATOR,
        value_name=ClimateSchema.VALUE,
    )

    return data.filter(output_cols)


def mapping_flow_name_to_code() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        14: "Other Official Flows (non Export Credit)",
        19: "Equity Investment",
        30: "Private Development Finance",
    }
