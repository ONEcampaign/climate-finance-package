import numpy as np
import pandas as pd
from oda_data import read_crs

from climate_finance.config import logger
from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)
from climate_finance.oecd.cleaning_tools.schema import CrsSchema, CRS_MAPPING
from climate_finance.oecd.get_oecd_data import get_oecd_bilateral

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
    CrsSchema.PURPOSE_CODE,
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
    CrsSchema.VALUE,
    CrsSchema.TOTAL_VALUE,
    CrsSchema.SHARE,
]

CRS_VALUES: list = [
    CrsSchema.USD_COMMITMENT,
    CrsSchema.USD_DISBURSEMENT,
    CrsSchema.USD_NET_DISBURSEMENT,
]


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


def _clean_crs_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the CRS data. This means:
    - Cleaning the year column

    Args:
        data: The CRS data to clean.

    Returns:
        The CRS data cleaned.

    """

    return data.assign(year=lambda d: d["year"].str.replace("\ufeff", "", regex=True))


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

    for column in [CrsSchema.USD_DISBURSEMENT, CrsSchema.USD_NET_DISBURSEMENT]:
        dfs.append(
            data.assign(
                flow_type=column,
                value=lambda d: d[column] * d["share"],
            )
        )

    return pd.concat(dfs + [data], ignore_index=True)


def _merge_projects_with_crs(
    projects: pd.DataFrame, crs: pd.DataFrame, index: list[str]
) -> pd.DataFrame:
    return projects.merge(
        crs, on=index, how="left", indicator=True, suffixes=("", "_crs")
    )


def _log_matches(data: pd.DataFrame) -> None:
    # Log the number of projects that were matched
    logger.debug(f"Matched \n{data['_merge'].value_counts()} projects with CRS data")


def _keep_not_matched(data: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the projects that were not matched.

    Args:
        data: The dataframe to filter.

    Returns:
        The filtered dataframe.

    """
    return data.loc[lambda d: d["_merge"] == "left_only", UNIQUE_INDEX]


def _concat_matched_dfs(
    data: pd.DataFrame, additional_matches: pd.DataFrame
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
        [data.loc[lambda d: d["_merge"] != "left_only"], additional_matches],
        ignore_index=True,
    )


def _match_projects_with_crs(projects: pd.DataFrame, crs: pd.DataFrame) -> pd.DataFrame:
    """
    Match the projects with the CRS data.

    This is done by merging the projects with the CRS data on the columns in the
    UNIQUE_INDEX global variable. If there are projects that were not matched, a second
    attempt is made using a subset of the columns in the UNIQUE_INDEX global variable.

    Args:
        projects: The projects to match. This is a dataframe with the columns in the
        UNIQUE_INDEX global variable.
        crs: The CRS data to match. This is a dataframe with the columns in the
        UNIQUE_INDEX global variable.

    Returns:
        The projects matched with the CRS data.

    """
    # Perform an initial merge. It will be done considering all the columns in the
    # UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    data = _merge_projects_with_crs(projects=projects, crs=crs, index=UNIQUE_INDEX)

    # Log the number of projects that were matched
    _log_matches(data)

    # If there are projects that were not matched, try to match them using a subset of
    # the columns in the UNIQUE_INDEX global variable.
    not_matched = _keep_not_matched(data)

    # Attempt to match the projects that were not matched using a subset of the columns
    # in the UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    additional_matches = _merge_projects_with_crs(
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

    # Concatenate the dataframes
    data = _concat_matched_dfs(data=data, additional_matches=additional_matches)

    # Keep only the columns in the CRS_INFO global variable and set the UNIQUE_INDEX
    # columns to strings
    data = data.filter(CRS_INFO).astype({k: str for k in UNIQUE_INDEX})

    return data


def _keep_multilateral_providers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to keep only multilateral providers.

    Args:
        df: The dataframe to filter.

    Returns:
        The filtered dataframe.

    """
    # multilateral providers
    multi = ["Other multilateral", "Multilateral development bank"]

    # filter
    return df.loc[lambda d: d[CrsSchema.PARTY_TYPE].isin(multi)].reset_index(drop=True)


def _add_net_disbursements_column(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add a net disbursements column to the dataframe.
    This is done by subtracting the disbursements from the received.

    Args:
        data: The dataframe to add the net disbursements column to.

    Returns:
        The dataframe with the net disbursements column added.

    """
    data[CrsSchema.USD_NET_DISBURSEMENT] = data[CrsSchema.USD_DISBURSEMENT].fillna(
        0
    ) - data[CrsSchema.USD_RECEIVED].fillna(0)

    return data


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


def _compute_rolling_sum(group, window: int = 2, values: list[str] = None):
    if values is None:
        values = [CrsSchema.VALUE]
    group[values] = group[values].rolling(window=window).sum().fillna(group[values])
    group["yearly_total"] = (
        group["yearly_total"].rolling(window=window).sum().fillna(group["yearly_total"])
    )
    return group


def _summarise_by_party_idx(
    data: pd.DataFrame, idx: list[str], by_indicator: bool = False
) -> pd.DataFrame:
    grouper = [CrsSchema.PARTY_NAME] + idx

    if by_indicator:
        grouper += [CrsSchema.INDICATOR]

    grouper = list(dict.fromkeys(grouper))

    return data.groupby(grouper, observed=True)[CrsSchema.VALUE].sum().reset_index()


def _merge_total(
    data: pd.DataFrame, totals: pd.DataFrame, idx: list[str]
) -> pd.DataFrame:
    # Make sure index is valid
    idx = [
        c
        for c in idx
        if c in totals.columns
        and c not in [CrsSchema.PARTY_NAME, CrsSchema.RECIPIENT_NAME]
    ]

    # get original datatypes for data
    data_dt = data.dtypes.to_dict()

    # convert index to string
    data = data.astype({k: str for k in idx})
    totals = totals.astype({k: str for k in idx})

    data = (
        data.merge(totals, on=idx, how="left", suffixes=("", "_crs"))
        .replace(0, np.nan)
        .astype(data_dt)
    )
    return data.drop(columns=[c for c in data.columns if c.endswith("_crs")])


def _add_share(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(share=lambda d: d[CrsSchema.VALUE] / d["yearly_total"]).drop(
        columns=["yearly_total", CrsSchema.VALUE]
    )


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
    matched = _match_projects_with_crs(projects=projects_df, crs=crs_df)

    # add back to original df
    data = df.astype({k: str for k in UNIQUE_INDEX}).merge(
        matched, on=UNIQUE_INDEX, how="left", suffixes=("", "_crs")
    )

    # add net disbursements
    data = _add_net_disbursements_column(data)

    # clean and standardise output
    data = _clean_multi_crs_output(data)

    # convert to flow types
    data = _convert_to_flowtypes(data)

    return data.filter(OUTPUT_COLUMNS)


def _highest_marker(df: pd.DataFrame) -> pd.DataFrame:
    # Create a rounded total to identify duplicates
    df = df.assign(
        rounded_total=lambda d: round(d.total_value / 100, 0).astype("Int64")
    )

    # Do a first pass to drop duplicates
    df = df.drop_duplicates(subset=[c for c in df.columns if c not in ["total_value"]])

    # Do a second pass to drop duplicates
    df = df.sort_values(by=["value"]).drop_duplicates(
        subset=[c for c in df.columns if c not in ["share", "value", "total_value"]],
        keep="first",
    )

    # Group by the columns that are not the value, total_value, or share
    df = (
        df.groupby(
            by=[c for c in df.columns if c not in ["value", "share", "total_value"]],
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    # Pivot the dataframe
    df = df.pivot(
        index=[c for c in df.columns if c not in ["indicator", "value"]],
        columns="indicator",
        values="value",
    ).reset_index()

    # Summarise by row
    df = (
        df.groupby(
            by=[
                c
                for c in df.columns
                if c
                not in [
                    "Adaptation",
                    "Mitigation",
                    "Cross-cutting",
                    "share",
                    "total_value",
                    "rounded_total",
                ]
            ],
            observed=True,
        )
        .agg(
            {
                "Adaptation": "sum",
                "Mitigation": "sum",
                "Cross-cutting": "sum",
                "share": "max",
                "total_value": "max",
                "rounded_total": "max",
            }
        )
        .reset_index()
    )

    # Create a mask to check if rounded values of "Adaptation" and "Mitigation" are equal
    mask_adaptation_mitigation_equal = df["Adaptation"].round(0) == df[
        "Mitigation"
    ].round(0)

    # mas to check if cross-cutting is present
    mask_cross_cutting_present = df["Cross-cutting"] > 0

    # mask use cross_cutting
    mask_use_cross_cutting = (
        mask_cross_cutting_present & mask_adaptation_mitigation_equal
    )

    # Find the column with the max value between adaptation and mitigation
    mask_adaptation_higher = df["Adaptation"] > df["Mitigation"]

    # Calculate the new value for each condition
    cross_cutting_values = df["Cross-cutting"]

    adaptation_higher_values = df["Adaptation"] + df["Mitigation"] - df["Cross-cutting"]

    mitigation_higher_values = df["Mitigation"] + df["Adaptation"] - df["Cross-cutting"]

    # Use numpy's where to efficiently create the new column based on the conditions
    df["value"] = np.where(
        mask_use_cross_cutting,
        cross_cutting_values,
        np.where(
            mask_adaptation_higher, adaptation_higher_values, mitigation_higher_values
        ),
    )

    # Adding the indicator column to specify the chosen column
    df["indicator"] = np.where(
        mask_use_cross_cutting,
        "Cross-cutting",
        np.where(mask_adaptation_higher, "Adaptation", "Mitigation"),
    )

    # drop the columns that are not needed
    df = df.drop(
        columns=[
            "Adaptation",
            "Mitigation",
            "Cross-cutting",
            "rounded_total",
        ]
    )

    return df


def oecd_rolling_shares_methodology(
    data: pd.DataFrame, window: int = 2
) -> pd.DataFrame:
    # Define the columns for the level of aggregation
    idx = [CrsSchema.YEAR, CrsSchema.PARTY_CODE, CrsSchema.FLOW_TYPE]

    # Ensure key columns are integers
    data[[CrsSchema.YEAR, CrsSchema.PARTY_CODE]] = data[
        [CrsSchema.YEAR, CrsSchema.PARTY_CODE]
    ].astype("Int32")

    # Make Cross-cutting negative
    data.loc[lambda d: d[CrsSchema.INDICATOR] == "Cross-cutting", CrsSchema.VALUE] *= -1

    # Summarise the data at the right level
    data_by_indicator = _summarise_by_party_idx(data=data, idx=idx, by_indicator=True)

    # Summarise data by yearly totals
    data_yearly = _summarise_by_party_idx(
        data=data, idx=idx, by_indicator=False
    ).assign(**{CrsSchema.INDICATOR: CrsSchema.CLIMATE_UNSPECIFIED})

    # Get the yearly totals for the years present in the data
    yearly_totals = get_yearly_crs_totals(
        start_year=data[CrsSchema.YEAR].min(),
        end_year=data[CrsSchema.YEAR].max(),
        by_index=idx,
    ).rename(columns={CrsSchema.VALUE: "yearly_total"})

    # Merge the yearly totals with the data by indicator
    data_by_indicator = _merge_total(
        data=data_by_indicator, totals=yearly_totals, idx=idx
    )

    data_yearly = _merge_total(data=data_yearly, totals=yearly_totals, idx=idx)

    # Concatenate the dataframes
    data = pd.concat([data_by_indicator, data_yearly], ignore_index=True)

    # Compute the rolling totals
    rolling = (
        data.sort_values([CrsSchema.YEAR, CrsSchema.PARTY_CODE])
        .groupby(
            [
                CrsSchema.PARTY_NAME,
                CrsSchema.PARTY_CODE,
                CrsSchema.FLOW_TYPE,
                CrsSchema.INDICATOR,
            ],
            observed=True,
            group_keys=False,
        )
        .apply(_compute_rolling_sum, window=window)
        .reset_index(drop=True)
    )

    # add shares
    rolling = _add_share(rolling)

    return rolling


def one_rolling_shares_methodology(
    data: pd.DataFrame, window: int = 2, as_shares: bool = True
) -> pd.DataFrame:
    # Drop duplicates
    data = data.drop_duplicates().copy()

    # Define the columns for the level of aggregation
    idx = [
        CrsSchema.YEAR,
        CrsSchema.PARTY_CODE,
        CrsSchema.PARTY_NAME,
        CrsSchema.PARTY_TYPE,
        CrsSchema.RECIPIENT_NAME,
        CrsSchema.RECIPIENT_CODE,
        CrsSchema.SECTOR_CODE,
        CrsSchema.PURPOSE_CODE,
        CrsSchema.FINANCE_TYPE,
        CrsSchema.FLOW_TYPE,
    ]

    # Ensure key columns are integers
    data[[CrsSchema.YEAR, CrsSchema.PARTY_CODE]] = data[
        [CrsSchema.YEAR, CrsSchema.PARTY_CODE]
    ].astype("Int32")

    # Summarise the data at the right level
    data_by_indicator = (
        _summarise_by_party_idx(data=data, idx=idx, by_indicator=True)
        .pivot(index=idx, columns=CrsSchema.INDICATOR, values=CrsSchema.VALUE)
        .reset_index()
    )

    # Get the yearly totals for the years present in the data
    yearly_totals = get_yearly_crs_totals(
        start_year=data[CrsSchema.YEAR].min(),
        end_year=data[CrsSchema.YEAR].max(),
        by_index=idx,
        party=None,
    ).rename(columns={CrsSchema.VALUE: "yearly_total"})

    # Merge the yearly totals with the data by indicator
    data_by_indicator = _merge_total(
        data=data_by_indicator, totals=yearly_totals, idx=idx
    )

    # drop rows for which all the totals are missing
    climate_cols = ["Adaptation", "Mitigation", "Cross-cutting"]

    # check if any of the climate columns are missing
    missing_climate = [c for c in climate_cols if c not in data_by_indicator.columns]

    if len(missing_climate) > 0:
        for c in missing_climate:
            data_by_indicator[c] = np.nan

    data_by_indicator = data_by_indicator.dropna(
        subset=climate_cols + ["yearly_total"], how="all"
    )

    # Add climate total column
    data_by_indicator[CrsSchema.CLIMATE_UNSPECIFIED] = (
        data_by_indicator["Adaptation"].fillna(0)
        + data_by_indicator["Mitigation"].fillna(0)
        + data_by_indicator["Cross-cutting"].fillna(0)
    )

    # fill yearly_total gaps with climate total
    data_by_indicator["yearly_total"] = data_by_indicator["yearly_total"].fillna(
        data_by_indicator[CrsSchema.CLIMATE_UNSPECIFIED]
    )

    # Compute the rolling totals
    rolling = (
        data_by_indicator.sort_values([CrsSchema.YEAR, CrsSchema.PARTY_CODE])
        .groupby(
            idx,
            observed=True,
            group_keys=False,
        )
        .apply(
            _compute_rolling_sum,
            window=window,
            values=climate_cols + ["climate_total", "yearly_total"],
        )
        .reset_index(drop=True)
    )

    if as_shares:
        for col in climate_cols + ["climate_total"]:
            rolling[col] = (rolling[col].fillna(0) / rolling["yearly_total"]).fillna(0)

    return rolling.drop(columns=["yearly_total"])


def get_oecd_imputed_shares_calculated(
    start_year: int, end_year: int, rolling_window: int = 2
) -> pd.DataFrame:
    return (
        get_recipient_perspective(start_year=start_year, end_year=end_year)
        .pipe(_keep_multilateral_providers)
        .pipe(add_crs_details)
        .pipe(oecd_rolling_shares_methodology, window=rolling_window)
    )


def get_one_imputed_shares_calculated(
    start_year: int, end_year: int, rolling_window: int = 2
) -> pd.DataFrame:
    return (
        get_recipient_perspective(start_year=start_year, end_year=end_year)
        .pipe(_keep_multilateral_providers)
        .pipe(_highest_marker)
        .pipe(add_crs_details)
        .pipe(one_rolling_shares_methodology, window=rolling_window, as_shares=True)
    )


if __name__ == "__main__":
    df = get_one_imputed_shares_calculated(2018, 2021, rolling_window=1)
