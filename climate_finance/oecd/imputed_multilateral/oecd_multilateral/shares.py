import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.imputed_multilateral.crs_tools import get_yearly_crs_totals
from climate_finance.oecd.imputed_multilateral.multilateral_spending_data import (
    get_multilateral_data,
    add_crs_details,
)
from climate_finance.oecd.imputed_multilateral.tools import (
    summarise_by_party_idx,
    compute_rolling_sum,
    merge_total,
)


def _add_share(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(share=lambda d: d[CrsSchema.VALUE] / d["yearly_total"]).drop(
        columns=["yearly_total", CrsSchema.VALUE]
    )


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
    data_by_indicator = summarise_by_party_idx(data=data, idx=idx, by_indicator=True)

    # Summarise data by yearly totals
    data_yearly = summarise_by_party_idx(data=data, idx=idx, by_indicator=False).assign(
        **{CrsSchema.INDICATOR: CrsSchema.CLIMATE_UNSPECIFIED}
    )

    # Get the yearly totals for the years present in the data
    yearly_totals = get_yearly_crs_totals(
        start_year=data[CrsSchema.YEAR].min(),
        end_year=data[CrsSchema.YEAR].max(),
        by_index=idx,
    ).rename(columns={CrsSchema.VALUE: "yearly_total"})

    # Merge the yearly totals with the data by indicator
    data_by_indicator = merge_total(
        data=data_by_indicator, totals=yearly_totals, idx=idx
    )

    data_yearly = merge_total(data=data_yearly, totals=yearly_totals, idx=idx)

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
        .apply(compute_rolling_sum, window=window)
        .reset_index(drop=True)
    )

    # add shares
    rolling = _add_share(rolling)

    return rolling


def get_oecd_imputed_shares_calculated(
    start_year: int, end_year: int, rolling_window: int = 2
) -> pd.DataFrame:
    return (
        get_multilateral_data(start_year=start_year, end_year=end_year)
        .pipe(add_crs_details)
        .pipe(oecd_rolling_shares_methodology, window=rolling_window)
    )
