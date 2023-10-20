import pandas as pd

from climate_finance import config
from climate_finance.oecd.cleaning_tools.schema import (
    CrsSchema,
    OECD_CLIMATE_INDICATORS,
    VALUE_COLUMNS,
)
from climate_finance.oecd.cleaning_tools.tools import idx_to_str, set_crs_data_types
from climate_finance.oecd.crs.get_data import (
    get_crs_allocable_spending,
    get_crs_allocable_to_total_ratio,
)
from climate_finance.oecd.imputed_multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)
from climate_finance.oecd.imputed_multilateral.tools import compute_rolling_sum
from climate_finance.oecd.multisystem.get_data import get_multilateral_contributions
from climate_finance.unfccc.download.pre_process import map_channel_names_to_oecd_codes

ONE_IMPUTATIONS_IDX = [
    CrsSchema.YEAR,
    CrsSchema.CHANNEL_CODE,
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

ONE_IMPUTATIONS_OUTPUT_COLUMNS = [
    CrsSchema.YEAR,
    CrsSchema.PARTY_CODE,
    CrsSchema.PARTY_NAME,
    CrsSchema.CHANNEL_CODE,
    CrsSchema.CHANNEL_NAME,
    CrsSchema.RECIPIENT_CODE,
    CrsSchema.RECIPIENT_NAME,
    CrsSchema.SECTOR_CODE,
    CrsSchema.SECTOR_NAME,
    CrsSchema.PURPOSE_CODE,
    CrsSchema.PURPOSE_NAME,
    CrsSchema.FINANCE_TYPE,
    CrsSchema.FLOW_CODE,
    CrsSchema.FLOW_TYPE,
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
        lambda d: d[CrsSchema.PARTY_CODE] == provider
    ]

    # Merge the spending data
    combined = (
        spending_data.merge(
            filtered_multi, on=idx, how="left", suffixes=("_spending", "")
        )
        .dropna(subset=[f"{CrsSchema.PARTY_CODE}"])
        .reset_index(drop=True)
    )

    # Define columns to transform to amounts
    amounts = [
        CrsSchema.MITIGATION_VALUE,
        CrsSchema.ADAPTATION_VALUE,
        CrsSchema.CROSS_CUTTING_VALUE,
        CrsSchema.CLIMATE_UNSPECIFIED,
    ]

    # Compute the amounts, based on shares
    for col in amounts:
        combined[col] = combined[col] * combined[CrsSchema.VALUE]

    return combined.loc[lambda d: d["climate_total"] > 0]


def _correct_for_allocable_share(imputations_data: pd.DataFrame) -> pd.DataFrame:
    # Allocable index
    allocable_idx = [CrsSchema.YEAR, CrsSchema.PARTY_CODE, CrsSchema.FLOW_TYPE]

    # Imputations index
    imputations_idx = [
        CrsSchema.YEAR,
        f"{CrsSchema.PARTY_CODE}_spending",
        CrsSchema.FLOW_TYPE,
    ]

    # Get allocable ratios
    allocable_ratios = get_crs_allocable_to_total_ratio(
        start_year=int(imputations_data[CrsSchema.YEAR].min()),
        end_year=int(imputations_data[CrsSchema.YEAR].max()),
    ).filter(allocable_idx + ["allocable_share"])

    # Set index to str
    allocable_ratios = allocable_ratios.pipe(idx_to_str, idx=allocable_idx)
    imputations_data = imputations_data.pipe(idx_to_str, idx=imputations_idx)

    # Merge the datasets
    merged_data = imputations_data.merge(
        allocable_ratios,
        left_on=imputations_idx,
        right_on=allocable_idx,
        how="left",
        suffixes=("", "_allocable"),
    )

    for column in [
        CrsSchema.ADAPTATION_VALUE,
        CrsSchema.MITIGATION_VALUE,
        CrsSchema.CROSS_CUTTING_VALUE,
        CrsSchema.CLIMATE_UNSPECIFIED,
    ]:
        merged_data[column] = merged_data[column] * merged_data["allocable_share"]

    return merged_data


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
    for provider in multi_contributions[CrsSchema.PARTY_CODE].unique():
        dfs.append(
            _filter_merge_transform_provider_data(
                spending_data=multi_spending,
                contributions_data=multi_contributions,
                provider=provider,
                idx=idx,
            )
        )

    imputations_data = pd.concat(dfs, ignore_index=True).pipe(
        _correct_for_allocable_share
    )

    # Clean
    imputations_data = (
        imputations_data.rename(
            columns={
                CrsSchema.PARTY_TYPE: "channel_type",
                CrsSchema.VALUE: "total_contribution",
            }
        )
        .pipe(set_crs_data_types)
        .filter(ONE_IMPUTATIONS_OUTPUT_COLUMNS)
    )

    return imputations_data


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
        data.pipe(idx_to_str, idx=idx)
        .groupby(idx, observed=True, dropna=False)[CrsSchema.VALUE]
        .sum()
        .reset_index()
        .loc[
            lambda d: d.flow_type.isin(
                [CrsSchema.USD_DISBURSEMENT, CrsSchema.USD_COMMITMENT]
            )
        ]
        .pipe(set_crs_data_types)
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

    # idx
    idx = [CrsSchema.PARTY_CODE, CrsSchema.AGENCY_NAME]

    # Set index to str
    codes = codes.pipe(idx_to_str, idx=idx)
    total_spend = total_spend.pipe(idx_to_str, idx=idx)

    # Merge spending and agencies data
    total_spend = total_spend.merge(codes, on=idx, how="left")

    return total_spend.pipe(set_crs_data_types)


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


def get_imputations_by_party_channel(
    start_year: int,
    end_year: int,
    rolling_window_spending: int = 2,
    rolling_agg: str = "mean",
) -> pd.DataFrame:
    # Yearly multilateral spending
    spending = one_multilateral_spending(
        start_year=start_year,
        end_year=end_year,
        rolling_window=rolling_window_spending,
        agg=rolling_agg,
    ).pipe(
        map_channel_names_to_oecd_codes,
        channel_names_column="party",
    )

    contributions = get_multilateral_contributions(
        start_year=start_year, end_year=end_year
    )

    imputed_data = convert_to_imputations(
        multi_spending=spending, multi_contributions=contributions
    )

    return imputed_data.reset_index(drop=True)


if __name__ == "__main__":
    imputed = get_imputations_by_party_channel(
        start_year=2019, end_year=2021, rolling_window_spending=2
    )

    #
    # imputedr = get_imputations_by_party_channel(
    #     start_year=2013, end_year=2021, rolling_contributions=True
    # )
    #
    # imputed.to_feather(
    #     config.ClimateDataPath.output
    #     / "imputed_one_multilateral_single_year_contributions.feather"
    # )
    #
    # imputedr.to_feather(
    #     config.ClimateDataPath.output
    #     / "imputed_one_multilateral_rolling_contributions.feather"
    # )
    #
    # imputed_r = get_imputations_by_party_channel(
    #     start_year=2013, end_year=2021, rolling_contributions=True
    # )
    #
    # imputedR = pd.read_feather(
    #     config.ClimateDataPath.output
    #     / "imputed_one_multilateral_rolling_contributions.feather"
    # ).assign(methodology="rolling")
    #
    # imputed = pd.read_feather(
    #     config.ClimateDataPath.output
    #     / "imputed_one_multilateral_single_year_contributions.feather"
    # ).assign(methodology="single_year")
    #
    # imputed = pd.concat([imputed, imputedR], ignore_index=True)
    #
    # imputed.to_csv(
    #     config.ClimateDataPath.output
    #     / "imputed_one_multilateral_rolling_contributions.csv",
    #     index=False,
    # )

    banks = imputed.oecd_channel_name.unique().tolist()
