import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    VALUE_COLUMNS,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str

ONE_IMPUTATIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.FLOW_TYPE,
]

ONE_CONTRIBUTIONS_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.CHANNEL_CODE,
]

ONE_IMPUTATIONS_SPENDING_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.AGENCY_NAME,
    ClimateSchema.FLOW_TYPE,
]

ONE_IMPUTATIONS_OUTPUT_COLUMNS = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.PROVIDER_NAME,
    ClimateSchema.CHANNEL_CODE,
    ClimateSchema.CHANNEL_NAME,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.RECIPIENT_NAME,
    ClimateSchema.SECTOR_CODE,
    ClimateSchema.SECTOR_NAME,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.PURPOSE_NAME,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_TYPE,
    *VALUE_COLUMNS,
]


def _filter_merge_transform_provider_data(
    spending_data: pd.DataFrame,
    contributions_data: pd.DataFrame,
    provider: str | int,
    idx: list[str],
) -> pd.DataFrame:
    # filter for the specific provider
    filtered_multi = contributions_data.loc[
        lambda d: d[ClimateSchema.PROVIDER_CODE] == provider
    ]

    # Merge the spending data
    combined = (
        spending_data.merge(
            filtered_multi, on=idx, how="left", suffixes=("_spending", "")
        )
        .dropna(subset=[f"{ClimateSchema.PROVIDER_CODE}"])
        .reset_index(drop=True)
    )

    # Define columns to transform to amounts
    amounts = [
        ClimateSchema.MITIGATION_VALUE,
        ClimateSchema.ADAPTATION_VALUE,
        ClimateSchema.CROSS_CUTTING_VALUE,
        ClimateSchema.CLIMATE_UNSPECIFIED,
    ]

    # Compute the amounts, based on shares
    for col in amounts:
        combined[col] = combined[col] * combined[ClimateSchema.VALUE]

    return combined.loc[lambda d: d["climate_total"] > 0]


def convert_to_imputations(
    multi_spending: pd.DataFrame,
    multi_contributions: pd.DataFrame,
    groupby: list[str] = None,
) -> pd.DataFrame:
    # Get allocable ratios

    if groupby is None:
        groupby = ONE_IMPUTATIONS_IDX

    idx = list(
        set(multi_spending.columns) & set(multi_contributions.columns) & set(groupby)
    )
    multi_spending = multi_spending.pipe(idx_to_str, idx=idx)
    multi_contributions = multi_contributions.pipe(idx_to_str, idx=idx)

    # List to store individual provider data
    dfs = []

    # Loop through each provider
    for provider in multi_contributions[ClimateSchema.PROVIDER_CODE].unique():
        dfs.append(
            _filter_merge_transform_provider_data(
                spending_data=multi_spending,
                contributions_data=multi_contributions,
                provider=provider,
                idx=idx,
            )
        )

    imputations_data = pd.concat(dfs, ignore_index=True)

    # Clean
    imputations_data = imputations_data.rename(
        columns={
            ClimateSchema.PROVIDER_TYPE: "channel_type",
            ClimateSchema.VALUE: "total_contribution",
        }
    ).filter(ONE_IMPUTATIONS_OUTPUT_COLUMNS)

    return imputations_data
