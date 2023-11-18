import pandas as pd

from climate_finance.common.schema import ClimateSchema


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
