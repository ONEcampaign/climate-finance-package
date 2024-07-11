import numpy as np
import pandas as pd

from climate_finance.common.schema import ClimateSchema, CLIMATE_VALUES
from climate_finance.config import logger
from climate_finance.oecd.crs.add_crs_data import add_crs_data_pipeline



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
    data: pd.DataFrame,
    projects_data: pd.DataFrame,
    unique_index: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # set the right index
    data = data.set_index(unique_index)

    # Large shares keeps the rows with implausible shares
    large_shares = data.loc[
        lambda d: d[f"{ClimateSchema.CLIMATE_UNSPECIFIED}_share"] > 1.1
    ]

    # clean_data keeps the rows with plausible shares
    clean_data = data.loc[
        lambda d: d[f"{ClimateSchema.CLIMATE_UNSPECIFIED}_share"] <= 1.1
    ]

    # Filter the projects data to only keep the rows that are in large_shares
    large_shares_projects = projects_data.loc[
        lambda d: (d.index.isin(large_shares.index))
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




def _remove_matched_from_crs(
    crs: pd.DataFrame, idx: list[str], matched_data: pd.DataFrame
) -> pd.DataFrame:
    idx = [
        c for c in idx if c != "year" and c in matched_data.columns and c in crs.columns
    ]

    crs = crs.set_index(idx)
    matched_data = matched_data.set_index(idx)
    crs = crs.loc[lambda d: ~d.index.isin(matched_data.index)]
    return crs.reset_index()


def _clean_not_matched(
    not_matched: pd.DataFrame, output_idx: list[str]
) -> pd.DataFrame:
    # Define text to use for not matched values
    not_matched_text = "Data only reported in the CRDF as commitments"

    # Define mapping of values for not matched data
    not_matched_values = {
        ClimateSchema.PROJECT_TITLE: not_matched_text,
        ClimateSchema.PROJECT_ID: "aggregate",
        ClimateSchema.CRS_ID: "aggregate",
        ClimateSchema.CHANNEL_CODE: "0",
        ClimateSchema.CHANNEL_CODE_DELIVERY: not_matched_text,
        ClimateSchema.FLOW_TYPE: ClimateSchema.USD_COMMITMENT,
    }

    # Fill missing recipient and purpose codes
    not_matched = not_matched.fillna(
        {ClimateSchema.RECIPIENT_CODE: "998", ClimateSchema.PURPOSE_CODE: "99810"}
    )

    # Replace "Debt instrument" with ODA Loans when concessional
    not_matched.loc[
        lambda d: (
            (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Debt instrument")
            & (d["concessionality"] == "Concessional and developmental")
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "ODA Loans"

    # Replace "Debt instrument" with OOFs when not concessional
    not_matched.loc[
        lambda d: (
            (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Debt instrument")
            & (
                d["concessionality"]
                == "Not concessional or not primarily developmental"
            )
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "Other Official Flows (non Export Credits)"

    # Replace "Grant" with ODA Grants
    not_matched.loc[
        lambda d: (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Grant"),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "ODA Grants"

    # Replace "Equity and shares in collective investment vehicles" with Equity Investment
    not_matched.loc[
        lambda d: (
            d[ClimateSchema.FINANCIAL_INSTRUMENT]
            == "Equity and shares in collective investment vehicles"
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "Equity Investment"

    # Add categories
    not_matched[ClimateSchema.CATEGORY] = not_matched[
        ClimateSchema.FINANCIAL_INSTRUMENT
    ].map(
        {
            "ODA Grants": "10",
            "ODA Loans": "10",
            "Other Official Flows (non Export Credit)": "21",
            "Equity Investment": "10",
        }
    )

    # Fill missing financial instruments
    not_matched[ClimateSchema.FINANCIAL_INSTRUMENT] = (
        not_matched[ClimateSchema.FINANCIAL_INSTRUMENT]
        .replace("nan", np.nan, regex=False)
        .fillna("Unspecified")
    )

    # Assign remaining values and rename
    not_matched = not_matched.assign(**not_matched_values)

    not_matched = (
        not_matched.groupby(
            by=[c for c in output_idx if c not in CLIMATE_VALUES],
            observed=True,
            dropna=False,
        )
        .sum(numeric_only=True)
        .reset_index()
        .rename(columns={ClimateSchema.FINANCIAL_INSTRUMENT: ClimateSchema.FLOW_NAME})
    )

    not_matched = not_matched.dropna(subset=[ClimateSchema.YEAR])

    return not_matched


def _compute_total_to_match(projects_df: pd.DataFrame) -> float:
    return round(
        projects_df.groupby([ClimateSchema.YEAR], observed=True, dropna=False)[
            CLIMATE_VALUES
        ]
        .sum()
        .sum()
        .sum()
        / 1e9,
        1,
    )


def mapping_flow_name_to_code() -> dict:
    return {
        11: "ODA Grants",
        13: "ODA Loans",
        14: "Other Official Flows (non Export Credit)",
        19: "Equity Investment",
        30: "Private Development Finance",
    }
