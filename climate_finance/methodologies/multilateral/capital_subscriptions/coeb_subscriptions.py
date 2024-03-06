import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
)


def scrape_coeb_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean CoEB data. It reads the excel file into
    a DataFrame and drops all irrelevant columns. It then standardises the column names to match all the other MDB data
    outputs. It removes the totals rows before calculating the share of total capital subscriptions held by member
    countries by year.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and standardise column names
    df = _read_mdb_xlsx_long_format(mdb="CoEB", first_year=2013, last_year=2022).drop(
        labels=[
            "Uncalled Capital",
            "Called Capital",
            "Percentage of subscribed capital",
        ],
        axis=1,
    )

    df = standardise_column_names(
        df=df,
        provider_col="Members",
        iso_col="iso_3",
        value_col="Subscribed Capital",
        year_col="year",
    )
    #
    # # remove total row to prevent double counting
    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    coeb_data = scrape_coeb_tables()

    checks = check_totals(coeb_data)
    count = check_countries(coeb_data)
