import pandas as pd
from scripts.analysis.other.capital_subscriptions.common import (
    _read_mdb_xlsx_long_format,
    calculate_share_of_annual_total,
    standardise_column_names,
    check_totals,
    remove_irrelevant_rows,
    check_countries,
    check_iso,
)


def scrape_ebrd_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean EBRD data. It reads the excel file into
    a DataFrame and drops all irrelevant columns. It then standardises the column names to match all the other MDB data
    outputs. It removes the totals rows before calculating the share of total capital subscriptions held by member
    countries by year.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and drop irrelevant columns
    df = _read_mdb_xlsx_long_format(mdb="EBRD", first_year=2013, last_year=2022).drop(
        labels=[
            "Total shares (number)",
            "Resulting votes85 (number)",
            "Callable capital C million",
            "Paid-in capital C million",
            "Check",
            "Unnamed: 8",
        ],
        axis=1,
    )

    # standardise column names
    df = standardise_column_names(
        df=df,
        provider_col="Member",
        iso_col="iso_3",
        value_col="Total capital C million",
        year_col="year",
    )

    df = remove_irrelevant_rows(df=df, key_word="Capital")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    ebrd_data = scrape_ebrd_tables()

    checks = check_totals(ebrd_data)
    count = check_countries(ebrd_data)
    count_iso = check_iso(ebrd_data)
