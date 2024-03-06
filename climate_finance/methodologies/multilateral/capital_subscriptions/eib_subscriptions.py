import pandas as pd
from scripts.analysis.other.capital_subscriptions.common import (
    _read_mdb_xlsx_long_format,
    calculate_share_of_annual_total,
    standardise_column_names,
    check_totals,
    remove_irrelevant_rows,
    check_countries,
)


def scrape_eib_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean EIB data. It reads the excel file into
    a DataFrame and standardises the column names to match all the other MDB data outputs. It removes the totals rows
    before calculating the share of total capital subscriptions held by member countries by year.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and standardise column names
    df = _read_mdb_xlsx_long_format(mdb="EIB", first_year=2013, last_year=2022)

    df = standardise_column_names(
        df=df,
        provider_col="Member States",
        iso_col="iso_code",
        value_col="Subscribed capital",
        year_col="year",
    )

    # remove total row to prevent double counting
    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    eib_data = scrape_eib_tables()

    checks = check_totals(eib_data)
    count = check_countries(eib_data)
