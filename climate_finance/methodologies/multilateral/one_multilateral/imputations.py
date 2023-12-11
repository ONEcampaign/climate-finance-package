import pandas as pd

from climate_finance.common.schema import (
    ClimateSchema,
    VALUE_COLUMNS,
)
from climate_finance.methodologies.multilateral.one_multilateral.shares import (
    multilateral_shares_pipeline,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str
from climate_finance.oecd.multisystem.get_data import (
    get_multilateral_contributions,
    remap_select_channels_at_spending_level,
)

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
    f"imputed_{ClimateSchema.VALUE}",
    f"imputed_{ClimateSchema.VALUE}_rolling",
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

    # Calculate value
    combined[f"imputed_{ClimateSchema.VALUE}"] = (
        combined[ClimateSchema.CLIMATE_SHARE] * combined[ClimateSchema.VALUE]
    )

    # calculate rolling value
    combined[f"imputed_{ClimateSchema.VALUE}_rolling"] = (
        combined[ClimateSchema.CLIMATE_SHARE_ROLLING] * combined[ClimateSchema.VALUE]
    )

    return combined


def convert_to_imputations(
    multi_spending: pd.DataFrame,
    multi_contributions: pd.DataFrame,
    groupby: list[str] = None,
) -> pd.DataFrame:
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


if __name__ == "__main__":
    contributions = (
        get_multilateral_contributions(start_year=2013, end_year=2021)
        .pipe(remap_select_channels_at_spending_level)
        .reset_index(drop=True)
    )

    contributions.to_feather("contributions_temp.feather")
    spending_shares = multilateral_shares_pipeline(
        start_year=2013,
        end_year=2021,
        groupby=[
            "year",
            "oecd_channel_code",
            "flow_type",
            # "indicator",
            # "oecd_recipient_code",
            # "purpose_code",
        ],
    ).reset_index(drop=True)

    spending_shares.to_feather("spending_temp.feather")

    # contributions = pd.read_feather("contributions_temp.feather")
    # spending_shares = pd.read_feather("spending_temp.feather")

    imp = convert_to_imputations(
        multi_spending=spending_shares,
        multi_contributions=contributions,
    )

    france = (
        imp.query("provider == 'Italy' and year == '2021'")
        .groupby(
            ["year", "flow_type", "oecd_channel_code", "oecd_channel_name"],
            dropna=False,
            observed=True,
        )[["imputed_value", "imputed_value_rolling"]]
        .sum(numeric_only=True)
    )

    france.to_clipboard(index=True)
