from pathlib import Path

import pandas as pd

from climate_finance.common.schema import ClimateSchema, CLIMATE_VALUES
from climate_finance.config import logger
from climate_finance.core.dtypes import set_default_types
from climate_finance.core.tools import string_to_missing
from climate_finance.methodologies.spending.crdf import (
    drop_names,
    clean_string_cols,
    group_and_summarize,
    split_into_markers_and_components,
    clean_crdf_markers,
    process_climate_components,
)
from climate_finance.methodologies.spending.crs import transform_markers_into_indicators
from climate_finance.oecd.cleaning_tools.settings import all_flow_columns
from climate_finance.oecd.crs.add_crs_data import add_crs_data_pipeline


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


def config_indices() -> list[list[str]]:
    common = [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.PURPOSE_CODE,
        ClimateSchema.RECIPIENT_CODE,
    ]
    unique_index_configurations = [
        common
        + [  # 2. No CRS ID
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.PROJECT_ID,
            ClimateSchema.FINANCE_TYPE,
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_TITLE,
        ],
        common
        + [  # 3.No CRS ID,  No title
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.PROJECT_ID,
            ClimateSchema.FINANCE_TYPE,
            ClimateSchema.FLOW_MODALITY,
        ],
        common
        + [  # 4. No CRS ID. No project ID
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.FINANCE_TYPE,
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_TITLE,
        ],
        common
        + [  # 5. No CRS ID. No project ID. No Finance type
            ClimateSchema.AGENCY_CODE,
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_TITLE,
        ],
        common
        + [  # 6. No Agency. No CRS ID. No project ID. No Finance type
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_TITLE,
        ],
        common
        + [  # 7. No Agency. No CRS ID. No project title. No Finance type
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_ID,
        ],
        common
        + [  # 8. No agency. No CRS ID. No Modality. No finance type.
            ClimateSchema.PROJECT_TITLE,
            ClimateSchema.PROJECT_ID,
        ],
        common
        + [  # 9. No agency. No CRS ID. No project ID. No Modality. No finance type.
            ClimateSchema.PROJECT_TITLE,
        ],
        common
        + [  # 12. No agency. No CRS ID. No project ID. No Modality. No finance type.
            ClimateSchema.CRS_ID,
            ClimateSchema.FLOW_MODALITY,
        ],
        common
        + [  # 13. No CRS ID. No project title. No Modality. No finance type.
            ClimateSchema.FLOW_MODALITY,
            ClimateSchema.PROJECT_DESCRIPTION,
        ],
        common,
    ]

    return unique_index_configurations


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


def loop_through_matching_strategies(
    crs: pd.DataFrame, matched: pd.DataFrame, not_matched: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Define the different passes that will be performed to try to merge the data
    # This is done by specifying the merge columns
    configurations = config_indices()

    # Loop through each config and try to merge the data
    for pass_number, idx_config in enumerate(configurations):
        matched_, not_matched = add_crs_data_pipeline(
            crs_data=crs,
            projects_to_match=not_matched,
            idx=idx_config,
        )
        matched = pd.concat(
            [c.dropna(axis=1, how="all") for c in [matched, matched_]],
            ignore_index=True,
        )

    return matched, not_matched


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
    unique_index: list[str],
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
    # Log matching stats
    log_matching_stats(projects_df=crdf)

    # Restrict 918(3) data if needed
    crs = restrict_918_3_data(crs=crs)

    # Perform an initial merge. It will be done considering all the columns in the
    # UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    matched, not_matched = add_crs_data_pipeline(
        crs_data=crs,
        projects_to_match=crdf,
        idx=unique_index,
    )

    # Try to match the remaining projects using a series of subsets of the columns in the
    # dataframe
    matched, not_matched = loop_through_matching_strategies(
        crs=crs, matched=matched, not_matched=not_matched
    )

    # Log final matching stats
    log_final_matching_stats(matched=matched)

    # if a path is passed to save the not matched data, save it
    if save_not_matched is not None:
        not_matched.to_csv(save_not_matched, index=False)

    # Combine the matched and not matched data
    data = combine_matched_and_unmatched_data(matched=matched, not_matched=not_matched)

    # 'missing' to <NA>
    data = string_to_missing(data, missing="missing")

    # Set right data types
    data = set_default_types(data)

    return data


def prepare_crs(crs: pd.DataFrame) -> pd.DataFrame:
    # Pivot flow_type
    crs = crs.pivot(
        index=[
            c
            for c in crs.columns
            if c not in [ClimateSchema.VALUE, ClimateSchema.FLOW_TYPE]
        ],
        columns=ClimateSchema.FLOW_TYPE,
        values=ClimateSchema.VALUE,
    ).reset_index()


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

    crdf = crdf.pipe(
        clean_string_cols,
        cols=[ClimateSchema.PROJECT_TITLE, ClimateSchema.PROJECT_DESCRIPTION],
    )

    flow_cols = all_flow_columns()

    data = add_crs_data_and_transform(
        crs=crs,
        crdf=crdf,
        unique_index=[c for c in crs.columns if c not in flow_cols],
    )

    # Flag climate components
    data.loc[
        lambda d: d[ClimateSchema.PROVIDER_CODE].isin(providers),
        [ClimateSchema.ADAPTATION, ClimateSchema.MITIGATION],
    ] = 100

    data = data.pipe(drop_names).pipe(group_and_summarize)

    markers, components = split_into_markers_and_components(data)

    # Process the markers data
    markers = markers.pipe(clean_crdf_markers).pipe(
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
