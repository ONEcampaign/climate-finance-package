import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    add_value_column_from_annual_total_columns,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
    check_iso,
)


def scrape_iadb_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean IADB data. It reads the excel file into
    a DataFrame. This MDB reports data for both the current and previous year, each year (i.e. 2015 data is reported in
    column 'Total (2015)' in both tabs 2015 and 2016). This functions puts all the data for the year in question into a
    new 'value' column (i.e. 2015 would hold the value for 'total (2015) from the tab  2015, not 2016). It then drops
    all irrelevant columns, before standardising column names to match all the other MDB data outputs, removing all total
    rows, and calculating the share of total capital subscriptions held by member countries by year.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    df = _read_mdb_xlsx_long_format(
        mdb="Inter-american Development Bank", first_year=2013, last_year=2022
    )

    # Remove the Total (year) columns and move into single 'value' column.
    df = add_value_column_from_annual_total_columns(
        df=df, first_year=2013, last_year=2022
    ).drop(
        columns=[
            "Subscribed Voting share",
            "Paid-in portion of subscribed capital",
            "Callable portion of subscribed capital",
            "Total (2013)",
            "Total (2012)",
            "Total (2014)",
            "Total (2015)",
            "Total (2016)",
            "Additional paid-in capital",
            "Total (2017)",
            "Total (2018)",
            "Total (2019)",
            "Total (2020)",
            "Total (2021)",
            "Total (2022)",
        ],
        axis=1,
    )

    # standardise column names
    df = standardise_column_names(df=df, provider_col="Member")

    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    iadb_data = scrape_iadb_tables()

    checks = check_totals(iadb_data)
    count = check_countries(iadb_data)
    count_iso = check_iso(iadb_data)
