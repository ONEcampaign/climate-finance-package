import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.oecd.cleaning_tools.tools import keep_only_allocable_aid


def pivot_by_modality(data: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the data by flow modality.

    Args:
        data: A dataframe with a modality column.

    Returns:
        A dataframe with the data pivoted by modality.

    """
    return data.pivot(
        index=[
            c
            for c in data.columns
            if c not in [ClimateSchema.VALUE, ClimateSchema.FLOW_MODALITY]
        ],
        columns=ClimateSchema.FLOW_MODALITY,
        values=ClimateSchema.VALUE,
    ).reset_index()


def add_allocable_share(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with the allocable share of the total.
    Args:
        data: A dataframe with a bilateral_allocable and a total column.

    Returns:
        A dataframe with a allocable_share column added.

    """
    return data.assign(
        **{
            ClimateSchema.ALLOCABLE_SHARE: lambda d: (
                d.bilateral_allocable / d.total
            ).fillna(0)
        }
    )


def check_provider_codes_type(
    provider_codes: list[str | int] | str | int | None,
) -> list[str] | None:
    """
    Checks that the provider codes are of the right type.

    Args:
        provider_codes (list[str] | str | None): The provider codes to check.

    Returns:
        list[str] | None: The provider codes if they are of the right type.

    """
    if provider_codes is None:
        return None
    if isinstance(provider_codes, float):
        raise TypeError(f"Provider codes must be integers")
    if isinstance(provider_codes, str):
        provider_codes = [str(int(provider_codes))]
    if isinstance(provider_codes, int):
        provider_codes = [str(provider_codes)]
    if not all(isinstance(code, str) for code in provider_codes):
        try:
            provider_codes = [str(int(code)) for code in provider_codes]
        except ValueError:
            raise TypeError(f"Provider codes must all be integers")
    return provider_codes


def add_net_disbursement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with net disbursement values.
    Args:
        df: A dataframe with a usd_disbursement and a usd_received column.

    Returns:
        A dataframe with a usd_net_disbursement column added.

    """
    return df.assign(
        **{
            ClimateSchema.USD_NET_DISBURSEMENT: lambda d: d[
                ClimateSchema.USD_DISBURSEMENT
            ].fillna(0)
            - d[ClimateSchema.USD_RECEIVED].fillna(0)
        }
    )


def get_crs_allocable_to_total_ratio(full_crs: pd.DataFrame) -> pd.DataFrame:
    """
    Fetches bilateral spending data for a given flow type and time period.

    Args: full_crs (pd.DataFrame): The full clean CRS data, not filtered for allocable.

    Returns:
        pd.DataFrame: A dataframe containing bilateral spending data for
        the specified flow type and time period.
    """

    simpler_columns = [
        ClimateSchema.YEAR,
        ClimateSchema.PROVIDER_CODE,
        ClimateSchema.AGENCY_CODE,
        ClimateSchema.FLOW_MODALITY,
        ClimateSchema.FLOW_TYPE,
    ]

    # Calculate the total
    total = (
        full_crs.copy(deep=True)
        .assign(**{ClimateSchema.FLOW_MODALITY: "total"})
        .groupby(simpler_columns, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Calculate the allocable
    allocable = (
        full_crs.pipe(keep_only_allocable_aid)
        .assign(**{ClimateSchema.FLOW_MODALITY: "bilateral_allocable"})
        .groupby(simpler_columns, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Combine the data
    data = pd.concat([allocable, total], ignore_index=True)

    data = data.pipe(pivot_by_modality).pipe(add_allocable_share)

    return data
