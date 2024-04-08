import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.methodologies.multilateral.crs_tools import get_yearly_crs_totals
from climate_finance.methodologies.multilateral.tools import (
    summarise_by_party_idx,
    compute_rolling_sum,
    merge_total,
)


def _add_share(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(share=lambda d: d[ClimateSchema.VALUE] / d["yearly_total"]).drop(
        columns=["yearly_total", ClimateSchema.VALUE]
    )


def oecd_rolling_shares_methodology(
    data: pd.DataFrame, window: int = 2
) -> pd.DataFrame:
    # Define the columns for the level of aggregation
    idx = [ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE, ClimateSchema.FLOW_TYPE]

    # Ensure key columns are integers
    data[[ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE]] = data[
        [ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE]
    ].astype("Int32")

    # Make Cross-cutting negative
    data.loc[
        lambda d: d[ClimateSchema.INDICATOR] == "Cross-cutting", ClimateSchema.VALUE
    ] *= -1

    # Summarise the data at the right level
    data_by_indicator = summarise_by_party_idx(data=data, idx=idx, by_indicator=True)

    # Summarise data by yearly totals
    data_yearly = summarise_by_party_idx(data=data, idx=idx, by_indicator=False).assign(
        **{ClimateSchema.INDICATOR: ClimateSchema.CLIMATE_UNSPECIFIED}
    )

    # Get the yearly totals for the years present in the data
    yearly_totals = get_yearly_crs_totals(
        start_year=data[ClimateSchema.YEAR].min(),
        end_year=data[ClimateSchema.YEAR].max(),
        by_index=idx,
    ).rename(columns={ClimateSchema.VALUE: "yearly_total"})

    # Merge the yearly totals with the data by indicator
    data_by_indicator = merge_total(
        data=data_by_indicator, totals=yearly_totals, idx=idx
    )

    data_yearly = merge_total(data=data_yearly, totals=yearly_totals, idx=idx)

    # Concatenate the dataframes
    data = pd.concat([data_by_indicator, data_yearly], ignore_index=True)

    # Compute the rolling totals
    rolling = (
        data.sort_values([ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE])
        .groupby(
            [
                ClimateSchema.PROVIDER_NAME,
                ClimateSchema.PROVIDER_CODE,
                ClimateSchema.FLOW_TYPE,
                ClimateSchema.INDICATOR,
            ],
            observed=True,
            group_keys=False,
        )
        .apply(compute_rolling_sum, window=window)
        .reset_index(drop=True)
    )

    # add shares
    rolling = _add_share(rolling)

    return rolling
