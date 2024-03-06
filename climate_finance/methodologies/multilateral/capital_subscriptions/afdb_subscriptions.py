import pandas as pd
from bblocks import clean_numeric_series

from scripts.analysis.other.capital_subscriptions.common import (
    read_mdb_table,
    concatenate_tables,
    set_column_names,
    filter_rows_columns,
    clean_table,
    add_iso_short_country_name,
    standardise_column_names,
    calculate_share_of_annual_total,
    remove_irrelevant_rows,
    check_totals,
)


def clean_afdb_table(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    This is the custom cleaning function for AfDB tables that do not conform to the clean_tables() function in common.py.
    You can specify this cleaning function in page_settings. This function merges columns [0] and [1] into column [1],
    as the country names were split across both columns. It removes all digits from the country names. It then filters
    for the relevant rows, keeping the rows and columns specified in first_row and keep_cols, before setting the
    column names specific for this mdb (see set_column_names() in common.py).

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data).
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question. In this case, AfDB.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.
    """

    # Merge columns which may contain names (and remove digits)
    df[1] = (df[0] + df[1]).str.replace("\d+", "", regex=True)

    return df.pipe(filter_rows_columns, first_row=first_row, keep_cols=keep_cols).pipe(
        set_column_names, mdb=mdb
    )


afdb_settings = {
    2013: (
        "168-169",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 4, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2014: (
        "185-186",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 4, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2015: (
        "212-213",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 4, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2016: (
        "75-76",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 4, "keep_cols": [1, 4, 5]},
        },
        clean_afdb_table,
    ),
    2017: (
        "73-74",  # pages
        {  # page settings
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 4, "keep_cols": [1, 4, 5]},
        },
        clean_afdb_table,  # custom cleaning
    ),
    2018: (
        "80-81",
        {
            0: {"first_row": 5, "keep_cols": [1, 4, 5]},
            1: {"first_row": 3, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2019: (
        "88-89",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 3, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2020: (
        "86-87",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 3, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2021: (
        "91-92",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 3, "keep_cols": [1, 4, 5]},
        },
        clean_table,
    ),
    2022: (
        "94-95",
        {
            0: {"first_row": 4, "keep_cols": [1, 4, 5]},
            1: {"first_row": 3, "keep_cols": [1, 4, 5]},
        },
        clean_afdb_table,
    ),
}


def afdb_calculate_subscribed_capital(df: pd.DataFrame) -> pd.DataFrame:
    """
    creates a new column which totals capital paid in and capital callable to get subscribed capital. Value column is
    also converted from string to float.
    Args:
        df (pd.DataFrame): DataFrame with columns for 'amount_paid' and 'callable_capital' by year and provider.

    Returns (pd.DataFrame): DataFrame with column for 'capital_subscriptions'by year and provider.
    """

    # convert columns to numeric
    df = clean_numeric_series(
        data=df, series_columns=["amount_paid", "callable_capital"], to=float
    )

    # calculate subscribed capital by totalling paid in capital and called in capital
    df["capital_subscriptions"] = df["amount_paid"] + df["callable_capital"]

    # drop irrelevant rows
    df = df.drop(labels=["amount_paid", "callable_capital"], axis=1)

    return df


def _afdb_scrape(
    year: int, pages: str, page_settings: dict, custom_cleaning: callable
) -> pd.DataFrame:
    """
    This function reads the pdf file that holds AfDB data. The function is used in a for loop in scrape_afdb_tables,
    which loops through each years' pdf, locates the table on the page(s) specified, and reads the data into a pd.DataFrame.
    This helper function takes the pdf by year (i.e. afdb_2015), uses the page number to specify the location of the table
    within the pdf, and uses the page_settings and callable cleaning function to output the required data from the
    tables. It reads each table, adds an additional column for the year, and concatenates each table into one DataFrame.
    Args:
        year (int): year of the pdf in question.
        pages (str): page range of the relevant tables in the pdf, formatted as "45-57".
        page_settings (dict): for each year, this dictionary holds data for page range, keep_cols/first_row, and the
        required cleaning function.
        custom_cleaning (callable): the cleaning function specified for each pdf's set of tables.
        mdb: the mdb in question. In this case, AfDB.

    Returns: Complete DataFrame of AfDB data.
    """

    # Read the raw data (which is a table)
    raw_data = read_mdb_table(mdb="afdb", year=year, pages=pages, flavor="stream")

    # Create a dataframe by putting together all the tables
    df = concatenate_tables(
        raw_data=raw_data,
        page_settings=page_settings,
        mdb="afdb",
        cleaning_function=custom_cleaning,
    )

    # Add year
    df["year"] = year

    return df


def scrape_afdb_tables(settings: dict) -> pd.DataFrame:
    """
    This pipeline function scrapes all AfDB capital subscriptions data from the PDF tables, cleans it, and stores it in
    a standardised DataFrame.
    This function first scrapes all the data from all pdf tables using the helper function _afdb_scrape(). It then
    calculates subscribed capital using the function afdb_calculate_subscribed_capital (see function for explanation),
    before removing all 'total' rows (in this function, these are identified by the keyword 'Total') and dropping all
    NaN values. We add iso_3 codes and clean the provider names that are misread or change from one year to another,
    and standardise the column names to match the other MDB outputs. Finally, we calculate each countries' share of total
    capital subscriptions by year.
    Args:
        settings (dict): page_settings containing the pdf specifications for year, pages, first_row, keep_cols, and
        cleaning_function. See _afdb_scrape() above.

    Returns: standardised DataFrame with all afdb capital subscriptions data, with columns for provider, iso_3, value,
    year, and share.
    """
    # Create an empty dataframe to hold the data for each year
    afdb = pd.DataFrame()

    for year, (pages, page_settings, custom_cleaning_fn) in settings.items():
        df = _afdb_scrape(
            year=year,
            pages=pages,
            page_settings=page_settings,
            custom_cleaning=custom_cleaning_fn,
        )
        afdb = pd.concat([afdb, df], ignore_index=True)

    # Calculate subscribed capital and remove irrelevant rows
    afdb = (
        afdb_calculate_subscribed_capital(df=afdb)
        .pipe(remove_irrelevant_rows, key_word="Total")
        .dropna(subset=["capital_subscriptions"])
    )

    # finalise dataset with iso code, clean provider names, column names, and shares.
    afdb = (
        add_iso_short_country_name(afdb)
        .pipe(standardise_column_names, value_col="capital_subscriptions")
        .pipe(calculate_share_of_annual_total)
    )

    return afdb


if __name__ == "__main__":
    afdb_data = scrape_afdb_tables(settings=afdb_settings)

    checks = check_totals(afdb_data)
