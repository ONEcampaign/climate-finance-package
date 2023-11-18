import pandas as pd

from climate_finance.common.schema import ClimateSchema


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
