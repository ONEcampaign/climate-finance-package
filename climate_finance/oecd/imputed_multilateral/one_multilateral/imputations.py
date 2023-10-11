import pandas as pd

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.crs.get_data import get_crs_allocable_spending
from climate_finance.oecd.imputed_multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)
from climate_finance.oecd.imputed_multilateral.tools import compute_rolling_sum
from climate_finance.oecd.multisystem.get_data import get_multilateral_contributions
from climate_finance.unfccc.download.pre_process import map_channel_names_to_oecd_codes


def convert_to_imputations(
    multi_spending: pd.DataFrame, multi_contributions: pd.DataFrame
) -> pd.DataFrame:
    dfs = []

    for donor in multi_contributions.oecd_donor_code.unique():
        m_ = multi_contributions.loc[lambda d: d.oecd_donor_code == donor]
        d_ = (
            multi_spending.merge(
                m_, on=["year", "oecd_channel_code", "flow_type"], how="left"
            )
            .dropna(subset=["oecd_donor_code"])
            .reset_index(drop=True)
        )
        for col in ["Adaptation", "Mitigation", "Cross-cutting", "climate_total"]:
            d_[col] = d_[col] * d_["value"]

        d_ = d_.loc[lambda d: d.climate_total > 0]

        dfs.append(d_)

    return pd.concat(dfs, ignore_index=True)


def _map_agency_then_party_to_channel_code(spending_data: pd.DataFrame) -> pd.DataFrame:
    unique_agencies = (
        spending_data.drop_duplicates(["oecd_party_code", "party", "agency"])
        .filter(["oecd_party_code", "party", "agency"])
        .dropna(subset=["oecd_party_code"])
    )

    first_pass = unique_agencies.pipe(
        map_channel_names_to_oecd_codes, channel_names_column="agency"
    ).rename(columns={"oecd_channel_code": "oecd_channel_code_agency"})

    second_pass = first_pass.pipe(
        map_channel_names_to_oecd_codes, channel_names_column="party"
    )

    return second_pass.assign(
        oecd_channel_code=lambda d: d.oecd_channel_code_agency.fillna(
            d.oecd_channel_code
        )
    ).filter(["oecd_party_code", "agency", "oecd_channel_code"])


def get_multilateral_spending_totals(
    start_year: int, end_year: int, party_code: list[str] = None
) -> pd.DataFrame:
    # Get total yearly spend
    total_spend = get_crs_allocable_spending(start_year=start_year, end_year=end_year)

    total_spend = (
        total_spend.groupby(
            ["year", "oecd_party_code", "party", "agency", "flow_type"],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
        .loc[lambda d: d.flow_type.isin(["usd_disbursement", "usd_commitment"])]
    )

    if party_code is not None:
        total_spend = total_spend.loc[lambda d: d.oecd_party_code.isin(party_code)]

    agencies_with_code = _map_agency_then_party_to_channel_code(total_spend)

    total_spend = total_spend.merge(
        agencies_with_code, on=["oecd_party_code", "agency"], how="left"
    )

    return total_spend


def calculate_rolling_contributions(
    contributions_data: pd.DataFrame, window: int = 2
) -> pd.DataFrame:
    return (
        contributions_data.sort_values([CrsSchema.YEAR])
        .groupby(
            [c for c in contributions_data.columns if c not in ["year"]],
            observed=True,
            group_keys=False,
        )
        .apply(
            compute_rolling_sum,
            window=window,
            values=["value"],
            include_yearly_total=False,
            agg="mean",
        )
        .reset_index(drop=True)
    )


def calculate_attribution_ratio(
    contributions_data: pd.DataFrame, spending_data: pd.DataFrame
) -> pd.DataFrame:
    # set contributions data to yearly totals

    contributions_data = (
        contributions_data.groupby(
            ["year", "flow_type", "oecd_channel_code"],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
        .pipe(calculate_rolling_contributions, window=2)
    )

    merged = (
        contributions_data.merge(
            spending_data,
            on=["year", "oecd_channel_code", "flow_type"],
            how="left",
            suffixes=("_contribution", "_spending"),
        )
        .groupby(
            ["year", "flow_type", "oecd_channel_code", "party"],
            observed=True,
            as_index=False,
            dropna=False,
        )
        .agg({"value_contribution": "max", "value_spending": "sum"})
        .loc[lambda d: d.value_spending > 0]
        .assign(contribution2spending=lambda d: d.value_contribution / d.value_spending)
    )
    return merged


def calculate_contribution_share(contributions_data: pd.DataFrame) -> pd.DataFrame:
    # calculate the oecd_donor share of oecd_channel_code total value

    return contributions_data.assign(
        share_of_total=lambda d: d.value
        / d.groupby(["year", "flow_type", "oecd_channel_code"], group_keys=False)[
            "value"
        ].transform("sum")
    ).drop(columns=["value"])


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
