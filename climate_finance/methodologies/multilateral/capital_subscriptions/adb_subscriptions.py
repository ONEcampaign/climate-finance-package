import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
)


def load_adb_2013_2016() -> pd.DataFrame:
    """
    The structure of the ADB's tables change from 2017 and therefore the steps required from 2013-2016 differ to
    2017-2022. This function reads in the tabs 2013-2016 from the ADB excel file and drops
    the irrelevant columns. The arguments are specified in the _read_mdb_xlsx_long_format() function (see common.py).

    Returns: DataFrame in long format with additional column for year.
    """
    return _read_mdb_xlsx_long_format(mdb="ADB", first_year=2013, last_year=2016).drop(
        labels=[
            "Number of shares",
            "Percent of total",
            "Callable Capital",
            "Paid-in",
            "Number of Votes",
            "Percent of total.1",
        ],
        axis=1,
    )


def clean_adb_2013_2016(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the data read from ADB's tables for 2013-2016. It standardises the column names, removes rows
    constaining totals to prevent double counting (when calculating shares) and calculates the share of annual total.
    To conform with the 2017-2022 years which have no value column, this function also removes the 'value' column.
    Args:
        df (pd.DataFrame): raw data from ADB Excel file, years 2013-2016.

    Returns: Clean ADB tables
    """

    df = (
        standardise_column_names(
            df=df, provider_col="Member", value_col="Capital subscriptions"
        )
        .pipe(remove_irrelevant_rows, key_word="total")
        .pipe(calculate_share_of_annual_total)
        .drop(labels="value", axis=1)
    )
    return df


def load_adb_2017_2022() -> pd.DataFrame:
    """
    Structure of the tables changes from 2017 and therefore the steps required from 2013-2016 differ to
    2017-2022. This function reads in the tabs 2017 to 2022 from the adb excel file and drops
    the irrelevant columns.

    Returns: DataFrame in long format with additional column for year.
    """
    return _read_mdb_xlsx_long_format(mdb="ADB", first_year=2017, last_year=2021).drop(
        labels=["Year of Membership", "Voting Power Share"],
        axis=1,
    )


def clean_adb_2017_2022(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the data read from ADB's tables for 2017-2022. It standardises the column names, removes rows
    containing totals and divides the share column by 100 to present in decimal terms (same as 2013-2016). Finally, it
    reorders the columns to match 2013-2016 for a concatenate in scrape_adb_tables().

    Args:
        df (pd.DataFrame): raw data from ADB Excel file, years 2017-2022.

    Returns: Clean ADB tables
    """
    df = df.rename(
        columns={"Member": "provider", "Subscribed Capital Share": "share"}
    ).pipe(remove_irrelevant_rows, key_word="total")

    # divide shares by 100 to match earlier year calculations
    df["share"] = df["share"] / 100

    # reorder columns to match earlier years
    df = df[["provider", "iso_3", "year", "share"]]

    return df


def scrape_adb_tables() -> pd.DataFrame:
    """
    This function concatenates the two clean dataframes produced for 2013-2016 and 2017-2022 above.

    Returns: final DataFrame with columns for provider, iso_3, year and share.

    """
    df_2013_2016 = load_adb_2013_2016().pipe(clean_adb_2013_2016)
    df_2017_2022 = load_adb_2017_2022().pipe(clean_adb_2017_2022)

    df = pd.concat(
        [
            df_2013_2016,
            df_2017_2022,
        ],
        ignore_index=True,
    ).sort_values(by="year", ascending=False)

    return df


if __name__ == "__main__":
    adb_data = scrape_adb_tables()

    checks = adb_data.groupby("year")[["share"]].sum()
