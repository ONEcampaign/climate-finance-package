import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    check_totals,
    check_countries,
    check_iso,
)


def cabei_2013_2021() -> pd.DataFrame:
    """
    The structure of CABEI's tables change from 2022 and therefore the steps required from 2013-2021 differ to
    2022. This function reads in the tabs 2013-2021 from the CABEI excel file and drops
    the irrelevant columns. It then standardises the column names and removes the irrelevant rows.

    Returns: DataFrame in long format with standardised column names (provider, iso_3, value and year).
    """
    df = _read_mdb_xlsx_long_format(mdb="CABEI", first_year=2013, last_year=2021).drop(
        labels=[
            "Callable subscribed",
            "Subscribed payable in cash",
            "Paid-in",
            "Subscribed payable in cash recievable",
        ],
        axis=1,
    )

    # Standardise column names
    df = standardise_column_names(
        df=df, provider_col="Countries", value_col="Subscribed/Unsubscribed Capital"
    )

    # create a list of rows to remove. Need to do this as one of the 'regional' columns needs to stay.
    rows = [
        "Subtotal founding countries",
        "Non-regional countries",
        "Subtotal non-regional countries",
        "Regional non-founding countries",
        "Subtotal regional non-founding countries",
        "Subtotal subscribed capital and paid-in capital",
        "Unsubscribed capital",
        # 'Non-regional countries and regional non-founding countries' --> commented out as this appears to count towards the overall total, with this country group not being dissagregated. We cannot get this data, but they should be included in totals otherwise other countries' shares will be inflated.
        "Total",
        "Non-founding regional countries",
        "Subtotal non-founding regional countries",
        "Non-regional countries ",
        "Non-regional countrie",
        "Unsubscribed Capital",
        "Authorized capital",
        "Subtotal non-founding regional countries ",
    ]

    # Keep all rows that don't match the condition
    df = df.loc[lambda d: ~d.provider.isin(rows)]

    return df


def cabei_2022() -> pd.DataFrame:
    """
    The structure of CABEI's tables change from 2022 and therefore the steps required for 2022 differ from
    2013-2021. This function reads in the 2022 tab from the CABEI excel file and standardises the column names.
    It also multiplies the values by 1000 to match 2013-2021 units.

    Returns: DataFrame in long format with standardised column names (provider, iso_3, value and year).
    """
    df = _read_mdb_xlsx_long_format(mdb="CABEI", first_year=2022, last_year=2022)

    df = standardise_column_names(
        df=df, provider_col="Countries", value_col="Subscribed/Unsubscribed Capital"
    )

    # Multiply by 1000 to match other years incase this data is used for something outside of shares.
    df["value"] = df["value"] * 1000

    return df


def scrape_cabei_data() -> pd.DataFrame:
    """
    This pipeline function concatenates the cleaned and standardised Data Frames from cabei_2013_2021() and cabei_2022()
    and calculates the share of total capital subscriptions held by member countries by year.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.

    """
    df_2013_2021 = cabei_2013_2021()
    df_2022 = cabei_2022()

    df = pd.concat([df_2013_2021, df_2022], ignore_index=True).sort_values(
        by="year", ascending=False
    )

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    cabei_data = scrape_cabei_data()

    checks = check_totals(cabei_data)
    count = check_countries(cabei_data)
    count_iso = check_iso(cabei_data)
