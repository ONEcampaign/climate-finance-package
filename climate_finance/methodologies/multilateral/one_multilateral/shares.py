import numpy as np
import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.oecd.cleaning_tools.tools import (
    idx_to_str,
    keep_only_allocable_aid,
)
from climate_finance.oecd.crs.get_data import get_crs
from climate_finance.methodologies.multilateral.multilateral_spending_data import (
    get_multilateral_spending_data,
    add_crs_data,
)
from climate_finance.methodologies.multilateral.one_multilateral.climate_components import (
    clean_component,
)
from climate_finance.methodologies.multilateral.tools import (
    summarise_by_party_idx,
    compute_rolling_sum,
    merge_total,
)

SHARES_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.RECIPIENT_CODE,
    ClimateSchema.PURPOSE_CODE,
    ClimateSchema.FINANCE_TYPE,
    ClimateSchema.FLOW_TYPE,
    ClimateSchema.FLOW_CODE,
    ClimateSchema.FLOW_NAME,
]

CLIMATE_COLS = [
    ClimateSchema.MITIGATION_VALUE,
    ClimateSchema.ADAPTATION_VALUE,
    ClimateSchema.CROSS_CUTTING_VALUE,
]

SIMPLE_IDX = [
    ClimateSchema.YEAR,
    ClimateSchema.PROVIDER_CODE,
    ClimateSchema.AGENCY_CODE,
    ClimateSchema.FLOW_TYPE,
]


def _pivot_indicators_as_columns(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    return data.pivot(
        index=idx, columns=ClimateSchema.INDICATOR, values=ClimateSchema.VALUE
    ).reset_index()


def _summarise_spending_data(
    data: pd.DataFrame,
) -> pd.DataFrame:
    # Get yearly totals for each party by flow type
    return (
        data.pipe(idx_to_str, idx=SIMPLE_IDX)
        .groupby(SIMPLE_IDX, observed=True, dropna=False)["yearly_total"]
        .sum()
        .reset_index()
    )


def _validate_missing_climate_cols(data: pd.DataFrame) -> pd.DataFrame:
    missing_climate = [c for c in CLIMATE_COLS if c not in data.columns]

    if len(missing_climate) > 0:
        for c in missing_climate:
            data[c] = np.nan

    return data


def _drop_rows_missing_climate(data: pd.DataFrame) -> pd.DataFrame:
    return data.dropna(subset=CLIMATE_COLS + ["yearly_total"], how="all")


def add_climate_total_column(data: pd.DataFrame) -> pd.DataFrame:
    data[ClimateSchema.CLIMATE_UNSPECIFIED] = (
        data[ClimateSchema.ADAPTATION_VALUE].fillna(0)
        + data[ClimateSchema.MITIGATION_VALUE].fillna(0)
        + data[ClimateSchema.CROSS_CUTTING_VALUE].fillna(0)
    )

    return data


def _fill_total_spending_gaps(data: pd.DataFrame) -> pd.DataFrame:
    data["yearly_total"] = data["yearly_total"].fillna(
        data[ClimateSchema.CLIMATE_UNSPECIFIED]
    )

    return data


def _add_total_spending_by_year(
    data: pd.DataFrame, totals_by_flow: pd.DataFrame
) -> pd.DataFrame:
    return (
        data.drop(columns=["yearly_total"])
        .pipe(idx_to_str, idx=SIMPLE_IDX)
        .merge(
            totals_by_flow.pipe(idx_to_str, idx=SIMPLE_IDX),
            on=SIMPLE_IDX,
            how="left",
        )
    )


def _compute_rolling_total_multi_spending(
    data: pd.DataFrame,
    start_year: int,
    end_year: int,
    window: int,
    agg: str = "sum",
    include_yearly_total: bool = True,
) -> pd.DataFrame:
    return (
        data.astype({ClimateSchema.YEAR: "Int32"})
        .sort_values([ClimateSchema.YEAR, ClimateSchema.PROVIDER_CODE])
        .groupby(
            [
                c
                for c in SHARES_IDX
                if c not in [ClimateSchema.YEAR] and c in data.columns
            ],
            observed=True,
            dropna=False,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            start_year=start_year,
            end_year=end_year,
            values=CLIMATE_COLS + ["climate_total", "yearly_total"],
            agg=agg,
            include_yearly_total=include_yearly_total,
        )
        .reset_index(drop=True)
    )


def _filter_flow_types(data: pd.DataFrame, flow_types=None) -> pd.DataFrame:
    if flow_types is None:
        flow_type = [ClimateSchema.USD_COMMITMENT, ClimateSchema.USD_DISBURSEMENT]

    return data.loc[lambda d: d[ClimateSchema.FLOW_TYPE].isin(flow_type)]


def _keep_non_zero_climate_total(data: pd.DataFrame) -> pd.DataFrame:
    return data.loc[lambda d: d["climate_total"] != 0]


def one_rolling_shares_methodology(
    data: pd.DataFrame,
    start_year: int,
    end_year: int,
    output_groupby: list[str] = None,
    window: int = 2,
    agg: str = "sum",
    as_shares: bool = True,
    use_year_total: bool = True,
) -> pd.DataFrame:
    """
    Compute the rolling totals or shares of climate finance for the
    multilateral spending data.

    Args:
        data: A dataframe containing the multilateral spending data.
        start_year: The start year of the data.
        end_year: The end year of the data.
        window: The window size for the rolling totals or shares (in years).
        agg: The aggregation method to use for the rolling totals.
        as_shares: Whether to compute the rolling shares or totals.
        use_year_total: Whether to compute the rolling shares of total spending.

    Returns:
        pd.DataFrame: The rolling totals or shares of climate finance for the multilateral
        spending data.

    """
    shares_idx = [c for c in SHARES_IDX if c in data.columns]

    if output_groupby is None:
        output_groupby = shares_idx

    # check if all columns are present
    # Drop duplicates
    data = data.drop_duplicates()

    # Summarise the data at the right level
    data_by_indicator = data.pipe(
        summarise_by_party_idx,
        idx=shares_idx,
        by_indicator=True,
    ).pipe(_pivot_indicators_as_columns, idx=shares_idx)

    # Get yearly totals for years present in the data, for ALLOCABLE
    yearly_totals = (
        get_crs(
            start_year=start_year,
            end_year=end_year,
            groupby=shares_idx + [ClimateSchema.FLOW_MODALITY],
            provider_code=data_by_indicator[ClimateSchema.PROVIDER_CODE]
            .unique()
            .tolist(),
        )
        .pipe(_filter_flow_types)
        .pipe(keep_only_allocable_aid)
        .pipe(summarise_by_party_idx, idx=shares_idx)
        .rename(columns={ClimateSchema.VALUE: "yearly_total"})
    )

    data_by_indicator = (
        data_by_indicator.pipe(merge_total, totals=yearly_totals, idx=shares_idx)
        .pipe(_validate_missing_climate_cols)
        .pipe(_drop_rows_missing_climate)
        .pipe(add_climate_total_column)
        .pipe(_keep_non_zero_climate_total)
        .pipe(_fill_total_spending_gaps)
    )

    # Add total spending
    if use_year_total:
        # Get the yearly totals for the years present in the data
        yearly_totals_by_flow_type = _summarise_spending_data(data=yearly_totals)

        data_by_indicator = _add_total_spending_by_year(
            data=data_by_indicator, totals_by_flow=yearly_totals_by_flow_type
        )

    # group by requested level
    value_cols = [
        c
        for c in CLIMATE_COLS + ["climate_total", "yearly_total"]
        if c in data_by_indicator.columns
    ]

    data_by_indicator = (
        data_by_indicator.groupby(output_groupby, observed=True, dropna=False)[
            value_cols
        ]
        .sum()
        .reset_index()
    )

    # Compute the rolling totals
    rolling = _compute_rolling_total_multi_spending(
        data_by_indicator,
        window=window,
        agg=agg,
        start_year=start_year,
        end_year=end_year,
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
    agg: str = "mean",
    groupby: list[str] | None = None,
    as_shares: bool = True,
    provider_code: list[str] | str | int | None = None,
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
        agg: The aggregation method to use for the rolling totals.
        groupby: The level of detail to provide for the output data.
        as_shares: Whether to compute the rolling shares or totals.
        provider_code: The list of parties to filter the data by.
        force_update: Whether to force update the data.

    Returns:
        pd.DataFrame: The rolling totals or shares of climate finance for the multilateral
        spending data.

    """
    data = (
        get_multilateral_spending_data(
            start_year=start_year,
            end_year=end_year,
            provider_code=provider_code,
            force_update=force_update,
        )
        .loc[lambda d: d[ClimateSchema.PROVIDER_TYPE] != "DAC member"]
        .pipe(clean_component)
        .pipe(add_crs_data)
    )

    if rolling_window > 1:
        data = data.pipe(
            one_rolling_shares_methodology,
            window=rolling_window,
            agg=agg,
            as_shares=as_shares,
            output_groupby=groupby,
            start_year=start_year,
            end_year=end_year,
        )
    else:
        data = (
            data.pivot(
                index=[
                    c
                    for c in data.columns
                    if c not in [ClimateSchema.INDICATOR, ClimateSchema.VALUE]
                ],
                columns=ClimateSchema.INDICATOR,
                values=ClimateSchema.VALUE,
            )
            .reset_index()
            .pipe(_validate_missing_climate_cols)
            .pipe(add_climate_total_column)
        )

    return data
