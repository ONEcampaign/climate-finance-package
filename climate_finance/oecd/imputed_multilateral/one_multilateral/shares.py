import numpy as np
import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.imputed_multilateral.multilateral_spending_data import (
    get_multilateral_data,
    add_crs_details,
)
from climate_finance.oecd.imputed_multilateral.one_multilateral.highest_marker import (
    clean_marker,
)
from climate_finance.oecd.imputed_multilateral.tools import (
    summarise_by_party_idx,
    compute_rolling_sum,
    merge_total,
)
from climate_finance.oecd.imputed_multilateral.crs_tools import get_yearly_crs_totals


def one_rolling_shares_methodology(
    data: pd.DataFrame,
    window: int = 2,
    as_shares: bool = True,
    use_year_total: bool = True,
) -> pd.DataFrame:
    """
    Compute the rolling totals or shares of climate finance for the
    multilateral spending data.

    Args:
        data: A dataframe containing the multilateral spending data.
        window: The window size for the rolling totals or shares (in years).
        as_shares: Whether to compute the rolling shares or totals.
        use_year_total: Whether to compute the rolling shares of total spending.

    Returns:
        pd.DataFrame: The rolling totals or shares of climate finance for the multilateral
        spending data.

    """

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
        CrsSchema.SECTOR_NAME,
        CrsSchema.PURPOSE_CODE,
        CrsSchema.PURPOSE_NAME,
        CrsSchema.FINANCE_TYPE,
        CrsSchema.FLOW_TYPE,
        CrsSchema.FLOW_CODE,
        CrsSchema.FLOW_NAME,
    ]

    # Ensure key columns are integers
    data[[CrsSchema.YEAR, CrsSchema.PARTY_CODE]] = data[
        [CrsSchema.YEAR, CrsSchema.PARTY_CODE]
    ].astype("Int32")

    # Summarise the data at the right level
    data_by_indicator = (
        summarise_by_party_idx(data=data, idx=idx, by_indicator=True)
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

    # Get yearly totals for each party by flow type
    yearly_totals_by_flow_type = (
        yearly_totals.groupby(
            [
                CrsSchema.YEAR,
                CrsSchema.PARTY_CODE,
                CrsSchema.PARTY_NAME,
                CrsSchema.FLOW_NAME,
                CrsSchema.FLOW_TYPE,
            ]
        )["yearly_total"]
        .sum()
        .reset_index()
    )

    # Merge the yearly totals with the data by indicator
    data_by_indicator = merge_total(
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

    # Add total spending
    if use_year_total:
        data_by_indicator = data_by_indicator.drop(columns=["yearly_total"]).merge(
            yearly_totals_by_flow_type,
            on=[
                CrsSchema.YEAR,
                CrsSchema.PARTY_CODE,
                CrsSchema.PARTY_NAME,
                CrsSchema.FLOW_NAME,
                CrsSchema.FLOW_TYPE,
            ],
            how="left",
        )

    # Compute the rolling totals
    rolling = (
        data_by_indicator.sort_values([CrsSchema.YEAR, CrsSchema.PARTY_CODE])
        .groupby(
            [c for c in idx if c not in ["year"]],
            observed=True,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            values=climate_cols + ["climate_total", "yearly_total"],
        )
        .reset_index(drop=True)
    )

    if as_shares:
        for col in climate_cols + ["climate_total"]:
            rolling[col] = (rolling[col].fillna(0) / rolling["yearly_total"]).fillna(0)

        rolling = rolling.drop(columns=["yearly_total"])

    return rolling


def one_multilateral_spending(
    start_year: int,
    end_year: int,
    rolling_window: int = 2,
    as_shares: bool = True,
    party: list[str] = None,
    force_update: bool = False,
) -> pd.DataFrame:
    """
    Compute the rolling totals or shares of climate finance for the multilateral spending data.

    This is done using ONE's methodology. This methodology gets the spending data (aggregated
    by purpose, country, flow_type), applies the highest marker methodology, and then computes
    the rolling totals or shares (based on the window size).

    Args:
        start_year: The start year of the data.
        end_year: The end year of the data.
        rolling_window: The window size for the rolling totals or shares (in years).
        as_shares: Whether to compute the rolling shares or totals.
        party: The list of parties to filter the data by.
        force_update: Whether to force update the data.

    Returns:
        pd.DataFrame: The rolling totals or shares of climate finance for the multilateral
        spending data.

    """
    return (
        get_multilateral_data(
            start_year=start_year,
            end_year=end_year,
            party=party,
            force_update=force_update,
        )
        .pipe(clean_marker)
        .pipe(add_crs_details)
        .pipe(
            one_rolling_shares_methodology, window=rolling_window, as_shares=as_shares
        )
    )
