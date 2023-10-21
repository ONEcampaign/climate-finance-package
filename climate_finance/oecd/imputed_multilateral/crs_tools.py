import pandas as pd

from climate_finance.config import logger
from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending
from climate_finance.oecd.get_oecd_data import get_oecd_bilateral


def get_crs_totals(
    start_year: int,
    end_year: int,
    by_index: list[str] | None = None,
    party_code: str | list[str] | None = None,
) -> pd.DataFrame:
    get_crs_allocable_spending(
        start_year=start_year, end_year=end_year, force_update=update_data
    )


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
    crs_data.loc[lambda d: d[CrsSchema.INDICATOR] == "Cross-cutting", "value"] *= -1

    # Create an index if none is provided
    if by_index is None:
        by_index = [
            c
            for c in crs_data.columns
            if c not in [CrsSchema.VALUE, CrsSchema.INDICATOR, CrsSchema.USD_COMMITMENT]
        ]

    else:
        by_index = [c for c in by_index if c in crs_data.columns]

    # Get the group totals based on the selected index
    return (
        crs_data.groupby(by_index, observed=True)[CrsSchema.VALUE].sum().reset_index()
    )


def merge_projects_with_crs(
    projects: pd.DataFrame, crs: pd.DataFrame, index: list[str]
) -> pd.DataFrame:
    idx = [c for c in index if c in projects.columns and c in crs.columns]
    return projects.merge(
        crs, on=idx, how="left", indicator=True, suffixes=("", "_crs")
    )


def _log_matches(data: pd.DataFrame) -> None:
    # Log the number of projects that were matched
    logger.debug(f"Matched \n{data['_merge'].value_counts()} projects with CRS data")


def _keep_not_matched(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Keep only the projects that were not matched.

    Args:
        data: The dataframe to filter.

    Returns:
        The filtered dataframe.

    """
    return data.loc[lambda d: d["_merge"] == "left_only", columns]


def _concat_matched_dfs(
    data: pd.DataFrame,
    additional_matches1: pd.DataFrame,
    additional_matches2: pd.DataFrame,
) -> pd.DataFrame:
    """
    Concatenate the dataframes of matched projects.

    Args:
        data: The first dataframe of matched projects.
        additional_matches: The second dataframe of matched projects.

    Returns:
        The concatenated dataframe.

    """
    # Concatenate the dataframes
    return pd.concat(
        [
            data.loc[lambda d: d["_merge"] != "left_only"],
            additional_matches1.loc[lambda d: d["_merge"] != "left_only"],
            additional_matches2,
        ],
        ignore_index=True,
    )


def match_projects_with_crs(
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
    # Perform an initial merge. It will be done considering all the columns in the
    # UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    projects = projects.pipe(idx_to_str, idx=unique_index)
    crs = crs.pipe(idx_to_str, idx=unique_index)
    data = merge_projects_with_crs(projects=projects, crs=crs, index=unique_index)

    # Log the number of projects that were matched
    _log_matches(data)

    # If there are projects that were not matched, try to match them using a subset of
    # the columns in the UNIQUE_INDEX global variable.
    not_matched = _keep_not_matched(data, unique_index)

    # Attempt to match the projects that were not matched using a subset of the columns
    # in the UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    additional_matches = merge_projects_with_crs(
        projects=not_matched,
        crs=crs,
        index=[
            CrsSchema.YEAR,
            CrsSchema.PARTY_CODE,
            CrsSchema.CRS_ID,
            CrsSchema.PURPOSE_CODE,
        ],
    )

    # Log the number of projects that were matched
    _log_matches(additional_matches)

    # Another pass
    not_matched = _keep_not_matched(additional_matches, unique_index)

    # Third pass of additional
    additional_matches_second_pass = merge_projects_with_crs(
        projects=not_matched,
        crs=crs,
        index=[
            CrsSchema.YEAR,
            CrsSchema.PARTY_CODE,
            CrsSchema.PROJECT_ID,
            CrsSchema.PURPOSE_CODE,
        ],
    )

    _log_matches(additional_matches_second_pass)

    # Concatenate the dataframes
    data = _concat_matched_dfs(
        data=data,
        additional_matches1=additional_matches,
        additional_matches2=additional_matches_second_pass,
    )

    # Keep only the columns in the CRS_INFO global variable and set the UNIQUE_INDEX
    # columns to strings
    data = data.filter(output_cols).pipe(set_crs_data_types)

    return data


def mapping_flow_name_to_code() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        14: "Other Official Flows (non Export Credit)",
        19: "Equity Investment",
        30: "Private Development Finance",
    }
