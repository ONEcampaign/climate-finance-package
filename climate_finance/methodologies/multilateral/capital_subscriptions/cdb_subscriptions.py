import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
    check_iso,
)


def scrape_cdb_data() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean CBD data. It reads the excel files into
    a DataFrame and drops all irrelevant columns. It then standardises the column names to match all the other MDB data
    outputs. It removes the totals rows and totals the DataFrame by member country and year such that each country
    only has one line per year (often the tables have multiple lines depending on the timing of capital subscription
    purchases or a change to a member's status within the MDB). It then calculates the share of total capital
    subscriptions held by member countries by year, before reordering the column names to match all other outputs.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and drop irrelevant columns
    df = _read_mdb_xlsx_long_format(mdb="CDB", first_year=2013, last_year=2022).drop(
        labels=[
            "No. of shares",
            "% of total",
            "Callable capital",
            "Paid-up capital",
            "subscriptions matured",
            "No. of votes",
            "% of total votes",
            "Recievable from members. Non-negotiable Demand notes.",
            "Unnamed: 11",
        ],
        axis=1,
    )

    # Standardise column names
    df = standardise_column_names(
        df, provider_col="Member", value_col="Total subscribed Capital"
    )

    # Remove total columns
    df = remove_irrelevant_rows(df=df, key_word="Total").pipe(
        remove_irrelevant_rows, key_word="regional"
    )

    # grouby as often providers have 'additional subscriptions' which need to be counted towards their overall total
    df = df.groupby(by=["provider", "iso_3", "year"]).sum("value").reset_index()

    # add shares column
    df = calculate_share_of_annual_total(df)

    # reorder the columns because groupby changes it out of sync with other mdbs
    df = df[["provider", "iso_3", "value", "year", "share"]]

    return df


if __name__ == "__main__":
    cdb_data = scrape_cdb_data()

    checks = check_totals(cdb_data)
    count = check_countries(cdb_data)
    count_iso = check_iso(cdb_data)
