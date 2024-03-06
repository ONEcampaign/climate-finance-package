import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
    check_iso,
    add_short_names_column,
)


def scrape_idb_invest_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean IDB Invest data. It reads the excel file
    into a DataFrame and drops all irrelevant columns. It then standardises the column names to match all the other MDB
    data outputs. It removes the totals rows before calculating the share of total capital subscriptions held by member
    countries by year. This excel file comes with 'messy' provider names which are cleaned using the iso_code converter
    in add_short_names_column.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and drop irrelevant columns
    df = _read_mdb_xlsx_long_format(
        mdb="IDB_invest", first_year=2013, last_year=2022
    ).drop(
        labels=[
            "Total capital stock subscribed (shares)",
            "Subscriptions receivable from members",
            "Additional paid-in capital",
            "Recievable from members",
            "Shares",
            "recievable from members",
            "Paid in capital",
            "Capital Stock (shares)",
            "Capital Stock (additional paid-in capital)",
            "Capital Stock (recievable from members)",
            "Capital Stock (total paid in capital)",
            "Capital Stock (percent of paid in capital)",
            "Number of votes",
            "Percent of total votes",
            "Unnamed: 10",
        ],
        axis=1,
    )

    # Standardise column names
    df = standardise_column_names(
        df,
        provider_col="Member",
        value_col="Capital Stock (amount subscribed par value)",
    )

    # Remove total columns
    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    # fix country names
    df = add_short_names_column(df=df, id_column="iso_3", target_column="provider")

    return df


if __name__ == "__main__":
    idb_invest_data = scrape_idb_invest_tables()

    checks = check_totals(idb_invest_data)
    count = check_countries(idb_invest_data)
    count_iso = check_iso(idb_invest_data)
