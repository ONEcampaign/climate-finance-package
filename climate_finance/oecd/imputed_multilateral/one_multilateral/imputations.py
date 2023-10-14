import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import (
    CrsSchema,
    OECD_CLIMATE_INDICATORS,
)
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending
from climate_finance.oecd.imputed_multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)
from climate_finance.oecd.imputed_multilateral.tools import compute_rolling_sum
from climate_finance.oecd.multisystem.get_data import get_multilateral_contributions
from climate_finance.unfccc.download.pre_process import map_channel_names_to_oecd_codes

ONE_IMPUTATIONS_IDX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.FLOW_TYPE,
]

ONE_CONTRIBUTIONS_IDX = [
    CrsSchema.YEAR,
    CrsSchema.FLOW_TYPE,
    CrsSchema.CHANNEL_CODE,
]

ONE_IMPUTATIONS_SPENDING_IDX = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.PARTY_NAME,
    CrsSchema.AGENCY_NAME,
    CrsSchema.FLOW_TYPE,
]


def _filter_merge_transform_provider_data(
    spending_data: pd.DataFrame, contributions_data: pd.DataFrame, provider: str | int
) -> pd.DataFrame:
    # Check that index is present in both dataframes
    idx = list(
        set(spending_data.columns)
        & set(contributions_data.columns)
        & set(ONE_IMPUTATIONS_IDX)
    )

    # filter for the specific provider
    filtered_multi = contributions_data.loc[
        lambda d: d[CrsSchema.PARTY_CODE] == provider
    ]

    # Merge the spending data
    combined = (
        spending_data.merge(filtered_multi, on=idx, how="left")
        .dropna(subset=[CrsSchema.PARTY_CODE])
        .reset_index(drop=True)
    )

    # Define columns to transform to amounts
    amounts = [
        c
        for c in OECD_CLIMATE_INDICATORS.values()
        if c not in OECD_CLIMATE_INDICATORS[CrsSchema.NOT_CLIMATE]
    ]

    # Compute the amounts, based on shares
    for col in amounts:
        combined[col] = combined[col] * combined[CrsSchema.VALUE]

    return combined.loc[lambda d: d["climate_total"] > 0]


def convert_to_imputations(
    multi_spending: pd.DataFrame, multi_contributions: pd.DataFrame
) -> pd.DataFrame:
    # List to store individual provider data
    dfs = []

    # Loop through each provider
    for provider in multi_contributions[CrsSchema.PARTY_CODE].unique():
        dfs.append(
            _filter_merge_transform_provider_data(
                spending_data=multi_spending,
                contributions_data=multi_contributions,
                provider=provider,
            )
        )

    return pd.concat(dfs, ignore_index=True)


def _get_unique_agencies_df(data: pd.DataFrame) -> pd.DataFrame:
    idx = [CrsSchema.PARTY_CODE, CrsSchema.PARTY_NAME, CrsSchema.AGENCY_NAME]
    return data.drop_duplicates(idx).filter(idx).dropna(subset=[CrsSchema.PARTY_CODE])


def _map_agency_then_party_to_channel_code(spending_data: pd.DataFrame) -> pd.DataFrame:
    # Get unique agencies as a dataframe
    unique_agencies = _get_unique_agencies_df(spending_data)

    # Try a first map through agencies
    first_pass = unique_agencies.pipe(
        map_channel_names_to_oecd_codes, channel_names_column=CrsSchema.AGENCY_NAME
    ).rename(columns={CrsSchema.CHANNEL_CODE: f"{CrsSchema.CHANNEL_CODE}_agency"})

    # Try a second map through parties
    second_pass = first_pass.pipe(
        map_channel_names_to_oecd_codes, channel_names_column=CrsSchema.PARTY_NAME
    )

    # Assign the channel code to the original dataframe, filling gaps
    return second_pass.assign(
        **{
            CrsSchema.CHANNEL_CODE: lambda d: d[
                f"{CrsSchema.CHANNEL_CODE}_agency"
            ].fillna(d[CrsSchema.CHANNEL_CODE])
        }
    ).filter([CrsSchema.PARTY_CODE, CrsSchema.AGENCY_NAME, CrsSchema.CHANNEL_CODE])


def _summarise_spending_by_idx(data: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    return (
        data.groupby(idx, observed=True, dropna=False)[CrsSchema.VALUE]
        .sum()
        .reset_index()
        .loc[
            lambda d: d.flow_type.isin(
                [CrsSchema.USD_DISBURSEMENT, CrsSchema.USD_COMMITMENT]
            )
        ]
    )


def get_multilateral_spending_totals(
    start_year: int, end_year: int, party_code: list[str] = None
) -> pd.DataFrame:
    # Get total yearly spend
    total_spend = get_crs_allocable_spending(
        start_year=start_year, end_year=end_year
    ).pipe(_summarise_spending_by_idx, idx=ONE_IMPUTATIONS_SPENDING_IDX)

    # Filter for specific party code(s)
    if party_code is not None:
        total_spend = total_spend.loc[
            lambda d: d[CrsSchema.PARTY_CODE].isin(party_code)
        ]

    # Get unique agencies and codes
    codes = _map_agency_then_party_to_channel_code(total_spend)

    # Merge spending and agencies data
    total_spend = total_spend.merge(
        codes, on=[CrsSchema.PARTY_CODE, CrsSchema.AGENCY_NAME], how="left"
    )

    return total_spend


def calculate_rolling_contributions(
    contributions_data: pd.DataFrame, window: int = 2, agg: str = "mean"
) -> pd.DataFrame:
    return (
        contributions_data.sort_values([CrsSchema.YEAR])
        .groupby(
            [c for c in contributions_data.columns if c not in [CrsSchema.YEAR]],
            observed=True,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            values=[CrsSchema.VALUE],
            include_yearly_total=False,
            agg=agg,
        )
        .reset_index(drop=True)
    )


def calculate_attribution_ratio(
    contributions_data: pd.DataFrame,
    spending_data: pd.DataFrame,
    contributions_window: int = 2,
) -> pd.DataFrame:
    # set contributions data to yearly totals

    contributions_data = (
        contributions_data.groupby(ONE_CONTRIBUTIONS_IDX, observed=True, dropna=False)[
            CrsSchema.VALUE
        ]
        .sum()
        .reset_index()
        .pipe(calculate_rolling_contributions, window=contributions_window)
    )

    merged = (
        contributions_data.merge(
            spending_data,
            on=ONE_CONTRIBUTIONS_IDX,
            how="left",
            suffixes=("_contribution", "_spending"),
        )
        .groupby(
            ONE_CONTRIBUTIONS_IDX + [CrsSchema.PARTY_NAME],
            observed=True,
            as_index=False,
            dropna=False,
        )
        .agg(
            {
                f"{CrsSchema.VALUE}_contribution": "max",
                f"{CrsSchema.VALUE}_spending": "sum",
            }
        )
        .loc[lambda d: d[f"{CrsSchema.VALUE}_spending"] > 0]
        .assign(
            contribution2spending=lambda d: d[f"{CrsSchema.VALUE}_contribution"]
            / d[f"{CrsSchema.VALUE}_spending"]
        )
    )
    return merged


if __name__ == "__main__":
    df = one_multilateral_spending(2019, 2021, rolling_window=2)
    df = df.pipe(map_channel_names_to_oecd_codes, channel_names_column="party")

    contributions = get_multilateral_contributions(start_year=2019, end_year=2021)
    data = convert_to_imputations(multi_spending=df, multi_contributions=contributions)

    total_spending = get_multilateral_spending_totals(
        start_year=2021, end_year=2021, party_code=df.oecd_party_code.unique()
    )

    ratio = calculate_attribution_ratio(
        contributions_data=contributions, spending_data=total_spending
    )
