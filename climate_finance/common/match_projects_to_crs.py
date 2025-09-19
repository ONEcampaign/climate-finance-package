from collections import OrderedDict

import numpy as np

from climate_finance.common.schema import ClimateSchema, CLIMATE_VALUES, ALL_FLOWS
import pandas as pd
from climate_finance.config import logger


BASE_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.PROJECT_ID,
    ClimateSchema.PROJECT_TITLE,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_MODALITY,
    ClimateSchema.PURPOSE_CODE,
]

MATCHING_STRATEGIES = {
    "full_match": BASE_IDX,
    "no_project_id": [c for c in BASE_IDX if c != ClimateSchema.PROJECT_ID],
    "no_project_title": [c for c in BASE_IDX if c != ClimateSchema.PROJECT_TITLE],
    "no_agency_no_title": [
        c
        for c in BASE_IDX
        if c not in [ClimateSchema.AGENCY_CODE, ClimateSchema.PROJECT_TITLE]
    ],
    "no_agency_no_id": [
        c
        for c in BASE_IDX
        if c not in [ClimateSchema.AGENCY_CODE, ClimateSchema.PROJECT_ID]
    ],
}


IDX_REPLACEMENTS = {
    ClimateSchema.PROJECT_ID: ClimateSchema.CRS_ID,
    ClimateSchema.PROJECT_TITLE: ClimateSchema.PROJECT_ID,
}


def _create_and_validate_idx_col(idx: list[str], df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and creates an index column in the DataFrame.

    Args:
        idx (list): A list of column names to use for the index.
        df (pd.DataFrame): The DataFrame to process.


    Returns:
        pd.DataFrame: The DataFrame with the validated and created index column.
    """
    matched = [c for c in idx if c in df.columns]
    if len(matched) != len(idx):
        not_matched = [c for c in idx if c not in matched]
        logger.debug(f"Columns not matched: {not_matched}")

    if df.empty:
        df["idx"] = pd.Series(dtype="string[pyarrow]")
        return df

    df["idx"] = (
        df[matched]
        .astype("string[pyarrow]")
        .fillna("")
        .astype(str)
        .replace(["<NA>", "nan", "<NAN>"], "", regex=False)
        .agg("_".join, axis=1)
        .str.strip("_")
        .astype("string[pyarrow]")
    )
    return df


def _groupby_idx(df: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    """
    Groups a DataFrame by the columns specified in the 'idx' list and the 'idx' column itself.
    It then sums up the values in the 'CLIMATE_VALUES' column for each group
    and resets the index of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be grouped.
        idx (list): A list of column names to group the DataFrame by.

    Returns:
        pd.DataFrame: The grouped DataFrame with summed 'CLIMATE_VALUES' and reset index.
    """

    id_columns = [c for c in idx if c in df.columns] + ["idx"]
    cols = CLIMATE_VALUES.copy()

    if f"commitment_{ClimateSchema.CLIMATE_SHARE}" in df.columns:

        df["original_commitment"] = df[CLIMATE_VALUES].astype(float).max(axis=1) / df[
            f"commitment_{ClimateSchema.CLIMATE_SHARE}"
        ].astype(float)

    if "original_commitment" in df.columns:
        cols = cols + ["original_commitment"]

    df[cols] = df[cols].astype(float).fillna(0)

    return df.groupby(id_columns, observed=True, dropna=False)[cols].sum().reset_index()


def _add_idx_col(
    unique_idx: list, projects_df: pd.DataFrame, crs_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates the columns specified in 'unique_idx' against the columns in 'projects_df'
    and 'crs_df' DataFrames.
    It then creates an index column in both DataFrames by concatenating the values of
    the validated columns.

    Args:
        unique_idx (list): A list of column names to validate and use for creating the index.
        projects_df (pd.DataFrame): The DataFrame representing the projects data.
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the 'projects_df'
        and 'crs_df' DataFrames with the added index column.
    """
    projects_df = _create_and_validate_idx_col(unique_idx, projects_df)
    crs_df = _create_and_validate_idx_col(unique_idx, crs_df)
    return projects_df, crs_df


def _merge_crs_projects(
    crs_df: pd.DataFrame, projects_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges the 'crs_df' and 'projects_df' DataFrames on the 'idx' column.
    The merge is performed as an outer join, which means that the resulting DataFrame
    will contain all records from both DataFrames.
    An additional column '_merge' is added to the resulting DataFrame, which indicates
    the source of each record.

    Args:
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.
        projects_df (pd.DataFrame): The DataFrame representing the projects data.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    return pd.merge(
        crs_df,
        projects_df,
        on="idx",
        how="outer",
        indicator=True,
        suffixes=("", "_crdf"),
    )


def _extract_matched_data(
    merged_data: pd.DataFrame, side: str = "both"
) -> pd.DataFrame:
    """
    Extracts the matched data from the merged DataFrame based on the '_merge' column value.
    The '_merge' column indicates the source of each record. The function drops the '_merge' column
    and any columns that end with '_crdf' from the resulting DataFrame.

    Args:
        merged_data (pd.DataFrame): The DataFrame resulting from the merge operation.
        side (str, optional): The value in the '_merge' column to filter the DataFrame by.
            It can be 'left_only', 'right_only', or 'both'. Defaults to 'both'.

    Returns:
        pd.DataFrame: The DataFrame containing the matched data.
    """
    data = (
        merged_data.query(f"_merge == '{side}'")
        .drop(columns=["_merge"])
        .drop(columns=merged_data.filter(like="_crdf").columns)
    )

    if data.duplicated(subset=["idx", ClimateSchema.CRS_ID]).sum() > 0:
        logger.warning(
            f"Matched data for {data[ClimateSchema.PROVIDER_CODE].unique().tolist()[0]}"
            f" contains duplicates"
        )

    return data


def _convert_climate_values_to_shares(
    data: pd.DataFrame, commitments_column: str = ClimateSchema.USD_COMMITMENT
) -> pd.DataFrame:
    """
    Converts the climate values in the DataFrame to shares by dividing each climate value
    by the value in the 'commitments_column'. The function modifies the DataFrame in-place.

    Args:
        data (pd.DataFrame): The DataFrame containing the climate values.
        commitments_column (str, optional): The column name of the commitments.
        Defaults to ClimateSchema.USD_COMMITMENT.

    Returns:
        pd.DataFrame: The DataFrame with the converted climate values.
    """
    # For each column in CLIMATE_VALUES, divide by the value in the 'commitments_column'
    data[CLIMATE_VALUES] = data[CLIMATE_VALUES].div(data[commitments_column], axis=0)

    return data


def _extract_matched_climate_shares(
    matched_data: pd.DataFrame, idx: list[str]
) -> pd.DataFrame:
    """
    Extracts the climate shares from the matched data.

    This function first removes the year from the 'idx' values in the matched data.
    It then filters the matched data to include only the rows where the 'idx' value
    is in the 'idx' list.
    Finally, it filters the columns to include only the 'idx' column and the climate values.

    Args:
        matched_data (pd.DataFrame): The DataFrame containing the matched data.
        idx (list[str]): A list of 'idx' values to filter the matched data by.

    Returns:
        pd.DataFrame: The DataFrame containing the climate shares for the specified 'idx' values.
    """
    return matched_data.loc[lambda d: d["idx"].isin(idx)].filter(
        ["idx"] + CLIMATE_VALUES
    )


def _get_crs_to_match(
    original_crs: pd.DataFrame, matched_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Extracts the CRS data that has not been matched yet, from the CRS.

    This function first gets the list of index values ('idx') from the matched data.
    It then returns the rows in the original CRS data where the 'idx' value is not in
    the list of matched 'idx' values.

    Args:
        original_crs (pd.DataFrame): The original DataFrame representing the CRS data.
        matched_data (pd.DataFrame): The DataFrame containing the matched data.

    Returns:
        pd.DataFrame: The DataFrame containing the CRS data that has not been matched yet.
    """
    # Get idx for matches
    idx_match = matched_data["idx"].unique().tolist()

    return original_crs.loc[lambda d: ~d["idx"].isin(idx_match)]


def _get_projects_to_match(
    original_projects: pd.DataFrame, matched_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Extracts the project data that has not been matched yet from the projects dataframe.

    This function first gets the list of index values ('idx') from the unmatched project
    data in the merged DataFrame.
    It then returns the rows in the original project data where the 'idx' value is
    in the list of unmatched 'idx' values.

    Args:
        original_projects (pd.DataFrame): The original DataFrame representing the project data.
        matched_data (pd.DataFrame): The DataFrame resulting from the matching operation.

    Returns:
        pd.DataFrame: The DataFrame containing the project data that has not been matched yet.
    """
    # Get idx for matches
    idx_match = matched_data["idx"].unique().tolist()

    return original_projects.loc[lambda d: ~d["idx"].isin(idx_match)]


def _replace_problematic_column(
    df: pd.DataFrame, column: str, dataset: str
) -> str | None:
    """
    Issues a warning if many values are missing in a column.

    Args:
        df (pd.DataFrame): The DataFrame to check for missing values.
        column (str): The column to check for missing values.
    """

    if column not in df.columns:
        return

    missing_ratio = df[column].isna().mean()

    if missing_ratio > 0.25:
        logger.debug(
            f"More than 25% of values are missing in column '{column}' in {dataset} data"
        )
        return IDX_REPLACEMENTS.get(column)

    return None


def _validate_idx(
    idx: list[str], projects_df: pd.DataFrame, crs_df: pd.DataFrame
) -> list[str]:

    changes = [
        (
            column,
            _replace_problematic_column(projects_df, column, "CRDF")
            or _replace_problematic_column(crs_df, column, "CRS"),
        )
        for column in idx
    ]

    ordered_dict = OrderedDict()

    for column in idx:
        rep = next(
            (rep for col, rep in changes if col == column and rep is not None), None
        )
        if rep is not None:
            ordered_dict[rep] = None
        else:
            ordered_dict[column] = None

    idx = list(ordered_dict.keys())

    return idx


def _dedup_on_commitments(
    matched: pd.DataFrame, original_data: pd.DataFrame
) -> pd.DataFrame:

    # Identified duplicated rows
    duplicated = matched.loc[
        lambda d: d.duplicated(subset=["idx"], keep=False)
    ].sort_values(["idx"])

    no_duplicates = matched.loc[lambda d: ~d.duplicated(subset=["idx"], keep=False)]

    tolerance = 0.01

    duplicated["commitment_match"] = (
        np.abs(
            duplicated[ClimateSchema.USD_COMMITMENT] - duplicated["original_commitment"]
        )
        / duplicated[ClimateSchema.USD_COMMITMENT]
        <= tolerance
    )

    deduplicated = duplicated[duplicated["commitment_match"]]

    if len(deduplicated) > 0:
        duplicated = deduplicated.loc[
            lambda d: d.duplicated(subset=["idx"], keep=False)
        ]
        deduplicated = deduplicated.loc[
            lambda d: ~d.duplicated(subset=["idx"], keep=False)
        ]

    # check if duplicates remain
    if len(duplicated) > 0:
        duplicated["commitment_match"] = duplicated[ClimateSchema.CRS_ID].isin(
            original_data[ClimateSchema.CRS_ID].unique()
        )
        deduplicated_crs = duplicated[duplicated["commitment_match"]]
        deduplicated = pd.concat([deduplicated, deduplicated_crs], ignore_index=True)

    if len(deduplicated) > 0:
        return pd.concat([no_duplicates, deduplicated], ignore_index=True)

    return matched


def _matching_pipeline(
    idx: list[str], projects_df: pd.DataFrame, crs_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Executes the matching pipeline for the given projects and CRS data.

    This function first adds an index column to the projects and CRS dataframes.
    It then groups the projects by the index and merges the grouped projects with the CRS data.
    After that, it extracts the matched data from the merged data and
    converts the climate values to shares.
    Finally, it extracts the CRS and projects data that have not been matched yet.

    Args:
        idx (list[str]): A list of column names to use as the index.
        projects_df (pd.DataFrame): The DataFrame representing the projects data.
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing the
        DataFrame with the matched data, the DataFrame with the projects data that
        has not been matched yet, and the DataFrame with the CRS data
        that has not been matched yet.
    """

    # Valiate for columns where a lot of data is missing
    idx = _validate_idx(idx=idx, projects_df=projects_df, crs_df=crs_df)

    # Add index column to the projects and CRS dataframes
    projects, crs = _add_idx_col(idx, projects_df, crs_df)

    # Group projects by index
    projects_by_idx = projects.pipe(_groupby_idx, idx)

    # Merge grouped projects with CRS data
    merged_data = _merge_crs_projects(crs_df=crs, projects_df=projects_by_idx)

    # Extract matched data from merged data
    matched_data = _extract_matched_data(merged_data=merged_data, side="both")
    unmatched_crdf_idx = _extract_matched_data(
        merged_data=merged_data, side="right_only"
    )["idx"].unique()

    if matched_data.duplicated(subset="idx").sum() > 0:
        matched_data = _dedup_on_commitments(
            matched=matched_data, original_data=projects_df
        )
        logger.debug(
            f"{projects_df[ClimateSchema.PROVIDER_CODE].unique().tolist()[0]}"
            f": One-to-many match possible with {idx}"
        )

    # Convert climate values to shares
    matched_data = _convert_climate_values_to_shares(matched_data)

    # Extract CRS data that has not been matched yet
    crs_to_match = _get_crs_to_match(original_crs=crs, matched_data=matched_data)
    projects_to_match = projects_df.loc[lambda d: d["idx"].isin(unmatched_crdf_idx)]

    return matched_data, projects_to_match, crs_to_match


def _add_matches_without_year(
    idx: list[str],
    matched_data: pd.DataFrame,
    projects_df: pd.DataFrame,
    crs_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Adds matches without considering the year in the index.

    This function first prepares a new index by removing the year from the 'idx' list.
    It then drops the 'idx' column from the 'crs_df' DataFrame if it exists.
    After that, it runs the matching pipeline to get the matched data without considering
    the year and the new CRS data to match.
    It then extracts the climate shares from the matched data and merges it with
    the matched data without the year.
    Finally, it concatenates the original matched data with the new matched data without the year.

    Args:
        idx (list[str]): A list of column names to use as the index.
        matched_data (pd.DataFrame): The DataFrame containing the matched data.
        projects_df (pd.DataFrame): The DataFrame representing the projects data.
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the DataFrame with the
        added matches without the year and the DataFrame with the new CRS data to match.
    """
    # check if commitment year can be used
    if crs_df[ClimateSchema.COMMITMENT_DATE].isna().sum() / len(crs_df) < 0.01:
        crs_df["commitment_year"] = (
            crs_df[ClimateSchema.COMMITMENT_DATE].astype("datetime64[ns]").dt.year
        ).astype("int16[pyarrow]")
        projects_df["commitment_year"] = projects_df[ClimateSchema.YEAR].astype(
            "int16[pyarrow]"
        )
        matched_data["commitment_year"] = (
            matched_data[ClimateSchema.COMMITMENT_DATE]
            .astype("datetime64[ns]")
            .dt.year.astype("int16[pyarrow]")
        )
        idx.append("commitment_year")

    # Prepare new index by removing the year from the 'idx' list
    new_idx = [c for c in idx if c != ClimateSchema.YEAR]

    # Drop 'idx' column from 'crs_df' DataFrame if it exists
    if "idx" in crs_df.columns:
        crs_df = crs_df.drop(columns="idx")

    matched_data = _create_and_validate_idx_col(new_idx, matched_data)

    # Run matching pipeline to get the matched data without considering the
    # year and the new CRS data to match
    matched_no_year, _, crs_to_match = _matching_pipeline(
        idx=new_idx, projects_df=matched_data, crs_df=crs_df
    )

    if len(matched_no_year) > 0:
        logger.debug(
            f"matched additional disbursement: {matched_no_year[ClimateSchema.USD_DISBURSEMENT].sum()/1e6}m"
        )

    # Extract the climate shares from the matched data
    matched_shares = _extract_matched_climate_shares(
        matched_data=matched_data, idx=matched_no_year["idx"].unique()
    )

    # Merge the extracted climate shares with the matched data without the year
    matched_no_year = matched_no_year.drop(columns=CLIMATE_VALUES).merge(
        matched_shares, on="idx", how="left"
    )

    # Concatenate the original matched data with the new matched data without the year
    full_data = pd.concat([matched_data, matched_no_year], ignore_index=True)

    return full_data, crs_to_match


def _match_projects_to_crs(
    idx: list[str], projects_df: pd.DataFrame, crs_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Matches projects to CRS data based on a given index.

    This function first runs the matching pipeline on the projects and CRS data.
    If there are any matches, it then adds matches without considering the year in the index.
    The function returns the matched data, the projects data that has not been matched yet,
    and the CRS data that has not been matched yet.
    The 'idx' column is dropped from all returned DataFrames.

    Args:
        idx (list[str]): A list of column names to use as the index.
        projects_df (pd.DataFrame): The DataFrame representing the projects data.
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing the
        DataFrame with the matched data, the DataFrame with the projects data that
        has not been matched yet, and the DataFrame with the CRS data
        that has not been matched yet.
    """
    matched_data, projects_to_match, crs_to_match = _matching_pipeline(
        idx=idx, projects_df=projects_df, crs_df=crs_df
    )

    if len(matched_data) > 0:
        matched_data, crs_to_match = _add_matches_without_year(
            idx=idx,
            matched_data=matched_data.copy(),
            projects_df=projects_df.copy(),
            crs_df=crs_to_match.copy(),
        )

    return (
        matched_data.drop(columns="idx"),
        projects_to_match.drop(columns="idx"),
        crs_to_match.drop(columns="idx"),
    )


def _flows_to_climate_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the flow values in the DataFrame to climate values (using the climate shares).

    This function first identifies the columns in the DataFrame that are in the 'ALL_FLOWS' list.
    It then reshapes the DataFrame from wide format to long format,
    with one row for each flow value.
    After that, it multiplies each climate value by the corresponding flow value.

    Args:
        df (pd.DataFrame): The DataFrame containing the flow values.

    Returns:
        pd.DataFrame: The DataFrame with the converted climate values.
    """
    # Identify the columns in the DataFrame that are in the 'ALL_FLOWS' list
    value_columns = [c for c in ALL_FLOWS if c in df.columns]

    # Reshape the DataFrame from wide format to long format, with one row for each flow value
    df_melted = df.melt(
        id_vars=[c for c in df.columns if c not in value_columns],
        value_vars=value_columns,
        var_name=ClimateSchema.FLOW_TYPE,
        value_name=ClimateSchema.VALUE,
    )

    # Multiply each climate value by the corresponding flow value
    for climate_column in CLIMATE_VALUES:
        df_melted[climate_column] = (
            df_melted[ClimateSchema.VALUE] * df_melted[climate_column]
        )

    # Drop the 'ClimateSchema.VALUE' column from the DataFrame
    return df_melted.drop(columns=ClimateSchema.VALUE)


def _calculate_matched_amount(matched_data: pd.DataFrame) -> float:
    """
    Calculates the total matched amount from the matched data.

    Args:
        matched_data (pd.DataFrame): The DataFrame containing the matched data.

    Returns:
        float: The total matched amount.
    """
    return (
        matched_data.query(
            f"{ClimateSchema.FLOW_TYPE} == '{ClimateSchema.USD_COMMITMENT}'"
        )
        .assign(
            matched=lambda d: d[ClimateSchema.ADAPTATION_VALUE].fillna(0)
            + d[ClimateSchema.MITIGATION_VALUE].fillna(0)
            - d[ClimateSchema.CROSS_CUTTING_VALUE].fillna(0)
        )["matched"]
        .astype(float)
        .fillna(0)
        .sum()
    )


def get_climate_data_from_crs(projects_df: pd.DataFrame, crs_df: pd.DataFrame):
    """
    Matches projects to CRS data using different strategies, defined as a
    global variable. It returns the 'matched' data from the CRS as climate finance values.

    This function first identifies the unique provider from the projects data.
    It then defines different matching strategies by creating different index lists.
    The function iterates over these index lists and, if there are any projects left to match,
    it runs the matching pipeline for each index list.
    The matched data from each strategy is appended to a list.
    After all strategies have been applied, the function concatenates the matched data,
    converts the flow values to climate values, and calculates the total matched amount.
    Finally, it logs the matched amount and returns it.

    Args:
        projects_df (pd.DataFrame): The DataFrame representing the projects data.
        crs_df (pd.DataFrame): The DataFrame representing the CRS data.

    Returns:
        float: The total matched amount.
    """
    # Identify the unique provider from the projects data
    provider = projects_df[ClimateSchema.PROVIDER_NAME].unique().tolist()[0]
    provider_code = projects_df[ClimateSchema.PROVIDER_CODE].unique().tolist()[0]

    # Calculate the total climate finance value to match
    to_match = projects_df["climate_finance_value"].sum()

    matches = []

    # Iterate over the index lists and run the matching pipeline for each index list
    for strategy, idx in MATCHING_STRATEGIES.items():
        if len(projects_df) > 0:
            matched, projects_df, crs_df = _match_projects_to_crs(
                idx=idx, projects_df=projects_df, crs_df=crs_df
            )
            matches.append(matched)
            logger.debug(f"Matched {len(matched)} projects using strategy '{strategy}'")

    # Concatenate the matched data from all strategies
    matched_data = pd.concat(matches, ignore_index=True)

    # Convert the flow values to climate values
    matched_data = _flows_to_climate_values(matched_data)

    # Calculate the total matched amount
    matched = _calculate_matched_amount(matched_data)

    # Log the matched amount
    logger.info(
        f"Matched {matched/1e6:,.0f}m out of {to_match/1e6:,.0f}m "
        f"({matched/to_match:.1%}) for provider {provider_code} - {provider}"
    )
    return matched_data, projects_df
