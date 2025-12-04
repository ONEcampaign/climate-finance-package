from pathlib import Path

import pandas as pd

from climate_finance.common.match_projects_to_crs import get_climate_data_from_crs
from climate_finance.common.schema import ClimateSchema, CLIMATE_VALUES
from climate_finance.config import logger
from climate_finance.core.dtypes import set_default_types
from climate_finance.methodologies.spending.crdf import (
    drop_names,
    clean_string_cols,
    group_and_summarize,
    split_into_markers_and_components,
    transform_marker_columns_to_value_column,
    process_climate_components,
)
from climate_finance.methodologies.spending.crs import transform_markers_into_indicators


def _compute_total_to_match(projects_df: pd.DataFrame) -> float:
    return round(
        projects_df[CLIMATE_VALUES].astype(float).sum().sum() / 1e9,
        1,
    )


def _replace_concessional_instruments(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[
        lambda d: (
            (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Debt instrument")
            & (d["concessionality"] == "Concessional and developmental")
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "ODA Loans"

    return data


def _replace_non_concessional_instruments(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[
        lambda d: (
            (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Debt instrument")
            & (
                d["concessionality"]
                == "Not concessional or not primarily developmental"
            )
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "Other Official Flows (non Export Credits)"

    return data


def _replace_grant_instruments(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[
        lambda d: (d[ClimateSchema.FINANCIAL_INSTRUMENT] == "Grant"),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "ODA Grants"

    return data


def _replace_equity_instruments(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[
        lambda d: (
            d[ClimateSchema.FINANCIAL_INSTRUMENT]
            == "Equity and shares in collective investment vehicles"
        ),
        ClimateSchema.FINANCIAL_INSTRUMENT,
    ] = "Equity Investment"

    return data


def add_categories_mapping_from_instruments(data: pd.DataFrame) -> pd.DataFrame:
    # Add categories
    data[ClimateSchema.CATEGORY] = data[ClimateSchema.FINANCIAL_INSTRUMENT].map(
        {
            "ODA Grants": "10",
            "ODA Loans": "10",
            "Other Official Flows (non Export Credit)": "21",
            "Equity Investment": "10",
        }
    )

    return data


def _clean_not_matched(not_matched: pd.DataFrame) -> pd.DataFrame:
    to_str = [
        ClimateSchema.GENDER,
        ClimateSchema.COAL_FINANCING,
        ClimateSchema.FINANCIAL_INSTRUMENT,
        ClimateSchema.CHANNEL_CODE_DELIVERY,
        ClimateSchema.CHANNEL_NAME_DELIVERY,
    ]

    not_matched[to_str] = not_matched[to_str].astype("string[pyarrow]")

    # Make replacements
    not_matched = (
        not_matched.pipe(_replace_concessional_instruments)
        .pipe(_replace_non_concessional_instruments)
        .pipe(_replace_grant_instruments)
        .pipe(_replace_equity_instruments)
        .pipe(add_categories_mapping_from_instruments)
    )

    return not_matched


def _get_list_of_providers(df: pd.DataFrame) -> str:
    return ", ".join(df[ClimateSchema.PROVIDER_CODE].astype("string[pyarrow]").unique())


def log_matching_stats(projects_df: pd.DataFrame) -> None:
    # List of providers
    providers = _get_list_of_providers(df=projects_df)

    # Totals to match
    to_match = _compute_total_to_match(projects_df=projects_df)

    # Log matching stats
    logger.info(f"Total to match for providers {providers}:\n${to_match}bn")


def log_final_matching_stats(matched: pd.DataFrame) -> None:
    providers = _get_list_of_providers(df=matched)

    total_matched = _compute_total_to_match(
        projects_df=matched.loc[
            lambda d: d[ClimateSchema.FLOW_TYPE] == ClimateSchema.USD_COMMITMENT
        ]
    )
    logger.info(f"Total matched for providers {providers}:\n${total_matched}bn")


def restrict_918_3_data(crs: pd.DataFrame) -> pd.DataFrame:
    if 918 in crs[ClimateSchema.PROVIDER_CODE].unique():
        # drop agencies 1 and 2 for provider 918
        crs = crs.loc[
            lambda d: ~(
                d[ClimateSchema.AGENCY_CODE].isin([1, 2])
                & (d[ClimateSchema.PROVIDER_CODE] == 918)
            )
        ]

    return crs


def combine_matched_and_unmatched_data(
    matched: pd.DataFrame, not_matched: pd.DataFrame
) -> pd.DataFrame:
    # Clean the not matched data
    not_matched = _clean_not_matched(not_matched=not_matched)

    # Combine the matched and not matched data
    data = pd.concat(
        [
            c.dropna(how="all", axis=1)
            for c in [matched.assign(matched=True), not_matched.assign(matched=False)]
        ],
        ignore_index=True,
    ).filter(matched.columns.tolist() + ["matched"], axis=1)

    return data


def add_crs_data_and_transform(
    crdf: pd.DataFrame,
    crs: pd.DataFrame,
    save_not_matched: Path | None = None,
) -> pd.DataFrame:
    """
    Match the projects with the CRS data.

    This is done by merging the projects with the CRS data on the columns in the
    UNIQUE_INDEX global variable. If there are projects that were not matched, a second
    attempt is made using a subset of the columns in the UNIQUE_INDEX global variable.

    Args:
        crdf: The projects to match. This is a dataframe with the columns in unique_index.
        crs: The CRS data to match. This is a dataframe with the columns in unique_index.
        unique_index: The columns to use to match the projects with the CRS data.
        save_not_matched: The path to save the not matched data to. If None, the data is
        not saved.

    Returns:
        The projects matched with the CRS data.

    """

    # Restrict 918(3) data if needed
    crs = restrict_918_3_data(crs=crs)

    matched_dfs, unmatched_dfs = [], []

    # Match the data, provider by provider
    for provider in crdf[ClimateSchema.PROVIDER_CODE].unique():
        m_, un_ = get_climate_data_from_crs(
            projects_df=crdf.query(f"{ClimateSchema.PROVIDER_CODE}=={provider}").copy(),
            crs_df=crs.query(f"{ClimateSchema.PROVIDER_CODE}=={provider}").copy(),
        )
        matched_dfs.append(m_)
        unmatched_dfs.append(un_)

    # Combine the matched and unmatched data
    matched = pd.concat(matched_dfs, ignore_index=True)
    not_matched = pd.concat(unmatched_dfs, ignore_index=True)

    # if a path is passed to save the not matched data, save it
    if save_not_matched is not None:
        not_matched.to_csv(save_not_matched, index=False)

    # Combine the matched and not matched data
    data = combine_matched_and_unmatched_data(matched=matched, not_matched=not_matched)

    # Set right data types
    data = set_default_types(data)

    if "commitment_match" in data.columns:
        data = data.drop(columns=["commitment_match"])

    return data


def climate_components_providers(crdf: pd.DataFrame) -> list[str]:
    return (
        crdf.loc[
            lambda d: d[ClimateSchema.CLIMATE_OBJECTIVE] == "Climate components",
            ClimateSchema.PROVIDER_CODE,
        ]
        .unique()
        .tolist()
    )


def transform_crs_crdf_into_indicators(
    crs: pd.DataFrame,
    crdf: pd.DataFrame,
    percentage_significant: float | int = 1,
    percentage_principal: float | int = 1,
    highest_marker: bool = True,
) -> pd.DataFrame:
    """Transforms the CRDF data into climate indicators

    The marker data is treated just as the CRS data. The climate components data is
    processed differently, since there are no levels of marking.

    Args:
        crs: The CRS data.
        crdf: The CRDF data.
        percentage_significant: The percentage of the activity that is considered
            climate relevant when the marker is 1. The default is 1.0.
        percentage_principal: The percentage of the activity that is considered
            climate relevant when the marker is 2. The default is 1.0.
        highest_marker: Whether to use the highest marker value.

    Returns:
        A DataFrame containing the transformed indicators data.
    """
    # get climate components providers
    providers = climate_components_providers(crdf=crdf)

    # Clean all string columns
    crdf = crdf.pipe(
        clean_string_cols,
        cols=[ClimateSchema.PROJECT_TITLE, ClimateSchema.PROJECT_DESCRIPTION],
    )

    data = add_crs_data_and_transform(crs=crs, crdf=crdf)

    # Flag climate components
    data.loc[
        lambda d: d[ClimateSchema.PROVIDER_CODE].isin(providers),
        [ClimateSchema.ADAPTATION, ClimateSchema.MITIGATION],
    ] = 100

    data = data.pipe(drop_names).pipe(group_and_summarize)

    markers, components = split_into_markers_and_components(data)

    # Process the markers data
    markers = markers.pipe(transform_marker_columns_to_value_column).pipe(
        transform_markers_into_indicators,
        percentage_significant=percentage_significant,
        percentage_principal=percentage_principal,
        highest_marker=highest_marker,
    )

    # Process the climate components data
    components = components.pipe(
        process_climate_components, highest_marker=highest_marker
    )

    # Combine the data
    data = pd.concat([markers, components], ignore_index=True)

    # set the right data types
    data = set_default_types(data)

    return data
