import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
)


def scrape_caf_tables() -> pd.DataFrame:
    # read excel and standardise column names
    df = _read_mdb_xlsx_long_format(mdb="CAF", first_year=2013, last_year=2022)

    df = standardise_column_names(
        df=df,
        provider_col="Stockholder",
        iso_col="iso_3",
        value_col="Total Subscribed + Paid-in capital (Serie A, B and C)",
        year_col="year",
    )
    #
    # # remove total row to prevent double counting
    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    caf_data = scrape_caf_tables()

    checks = check_totals(caf_data)
    count = check_countries(caf_data)
