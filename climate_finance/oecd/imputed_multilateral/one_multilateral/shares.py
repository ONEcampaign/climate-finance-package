import numpy as np
import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.imputed_multilateral.crs_tools import get_yearly_crs_totals
from climate_finance.oecd.imputed_multilateral.multilateral_spending_data import (
    get_multilateral_data,
    add_crs_details,
)
from climate_finance.oecd.imputed_multilateral.one_multilateral.climate_components import (
    clean_component,
)
from climate_finance.oecd.imputed_multilateral.tools import (
    summarise_by_party_idx,
    compute_rolling_sum,
    merge_total,
)

SHARES_IDX = [
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

CLIMATE_COLS = [
    CrsSchema.MITIGATION_VALUE,
    CrsSchema.ADAPTATION_VALUE,
    CrsSchema.CROSS_CUTTING_VALUE,
]

SIMPLE_IDX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.PARTY_NAME,
    CrsSchema.FLOW_NAME,
    CrsSchema.FLOW_TYPE,
]


def _pivot_indicators_as_columns(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    return data.pivot(
        index=idx, columns=CrsSchema.INDICATOR, values=CrsSchema.VALUE
    ).reset_index()


def _get_spending_summary_from_crs(
    data: pd.DataFrame,
) -> pd.DataFrame:
    # Get yearly totals for each party by flow type
    return data.groupby(SIMPLE_IDX)["yearly_total"].sum().reset_index()


def _validate_missing_climate_cols(data: pd.DataFrame) -> pd.DataFrame:
    missing_climate = [c for c in CLIMATE_COLS if c not in data.columns]

    if len(missing_climate) > 0:
        for c in missing_climate:
            data[c] = np.nan

    return data


def _drop_rows_missing_climate(data: pd.DataFrame) -> pd.DataFrame:
    return data.dropna(subset=CLIMATE_COLS + ["yearly_total"], how="all")


def _add_climate_total_column(data: pd.DataFrame) -> pd.DataFrame:
    data[CrsSchema.CLIMATE_UNSPECIFIED] = (
        data[CrsSchema.ADAPTATION_VALUE].fillna(0)
        + data[CrsSchema.MITIGATION_VALUE].fillna(0)
        + data[CrsSchema.CROSS_CUTTING_VALUE].fillna(0)
    )

    return data


def _fill_total_spending_gaps(data: pd.DataFrame) -> pd.DataFrame:
    data["yearly_total"] = data["yearly_total"].fillna(
        data[CrsSchema.CLIMATE_UNSPECIFIED]
    )

    return data


def _add_total_spending_by_year(
    data: pd.DataFrame, totals_by_flow: pd.DataFrame
) -> pd.DataFrame:
    return data.drop(columns=["yearly_total"]).merge(
        totals_by_flow, on=SIMPLE_IDX, how="left"
    )


def _compute_rolling_total_multi_spending(
    data: pd.DataFrame, window: int, agg: str = "sum", include_yearly_total: bool = True
) -> pd.DataFrame:
    return (
        data.sort_values([CrsSchema.YEAR, CrsSchema.PARTY_CODE])
        .groupby(
            [c for c in SHARES_IDX if c not in [CrsSchema.YEAR]],
            observed=True,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            values=CLIMATE_COLS + ["climate_total", "yearly_total"],
            agg=agg,
            include_yearly_total=include_yearly_total,
        )
        .reset_index(drop=True)
    )


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
    data = data.drop_duplicates()

    # Summarise the data at the right level
    data_by_indicator = data.pipe(
        summarise_by_party_idx, idx=SHARES_IDX, by_indicator=True
    ).pipe(_pivot_indicators_as_columns, idx=SHARES_IDX)

    # Get yearly totals for years present in the data
    yearly_totals = get_yearly_crs_totals(
        start_year=data[CrsSchema.YEAR].min(),
        end_year=data[CrsSchema.YEAR].max(),
        by_index=SHARES_IDX,
    ).rename(columns={CrsSchema.VALUE: "yearly_total"})

    # Get the yearly totals for the years present in the data
    yearly_totals_by_flow_type = _get_spending_summary_from_crs(data=yearly_totals)

    data_by_indicator = (
        data_by_indicator.pipe(merge_total, totals=yearly_totals, idx=SHARES_IDX)
        .pipe(_validate_missing_climate_cols)
        .pipe(_drop_rows_missing_climate)
        .pipe(_add_climate_total_column)
        .pipe(_fill_total_spending_gaps)
    )

    # Add total spending
    if use_year_total:
        data_by_indicator = _add_total_spending_by_year(
            data=data_by_indicator, totals_by_flow=yearly_totals_by_flow_type
        )

    # Compute the rolling totals
    rolling = _compute_rolling_total_multi_spending(
        data_by_indicator, window=window, agg="sum"
    )

    if as_shares:
        for col in CLIMATE_COLS + ["climate_total"]:
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
        .pipe(clean_component)
        .pipe(add_crs_details)
        .pipe(
            one_rolling_shares_methodology,
            window=rolling_window,
            as_shares=as_shares,
        )
    )
