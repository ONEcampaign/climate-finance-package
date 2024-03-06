import pandas as pd
from bblocks import clean_numeric_series

from scripts.analysis.other.capital_subscriptions.common import (
    read_mdb_table,
    concatenate_tables,
    calculate_share_of_annual_total,
    standardise_column_names,
    add_iso_short_country_name,
    check_totals,
    remove_irrelevant_rows,
    check_countries,
)


ida_settings = {
    2013: (
        "52-54",
        {
            0: {"first_row": 3, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 3]},
            2: {"first_row": 4, "keep_cols": [0, 3]},
        },
    ),
    2014: (
        "50-52",
        {
            0: {"first_row": 3, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 3]},
            2: {"first_row": 4, "keep_cols": [0, 3]},
        },
    ),
    2015: (
        "55-57",
        {
            0: {"first_row": 4, "keep_cols": [0, 4]},
            1: {"first_row": 6, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 4]},
        },
    ),
    2016: (
        "63-65",
        {
            0: {"first_row": 4, "keep_cols": [0, 4]},
            1: {"first_row": 6, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 4]},
        },
    ),
    2017: (
        "63-66",
        {
            0: {"first_row": 6, "keep_cols": [0, 4]},
            1: {"first_row": 8, "keep_cols": [0, 4]},
            2: {"first_row": 8, "keep_cols": [0, 4]},
            3: {"first_row": 8, "keep_cols": [0, 4]},
        },
    ),
    2018: (
        "65-68",
        {
            0: {"first_row": 7, "keep_cols": [0, 4]},
            1: {"first_row": 6, "keep_cols": [0, 4]},
            2: {"first_row": 6, "keep_cols": [0, 3]},
            3: {"first_row": 6, "keep_cols": [0, 4]},
        },
    ),
    2019: (
        "69-72",
        {
            0: {"first_row": 4, "keep_cols": [0, 5]},
            1: {"first_row": 6, "keep_cols": [0, 4]},
            2: {"first_row": 6, "keep_cols": [0, 3]},
            3: {"first_row": 6, "keep_cols": [0, 4]},
        },
    ),
    2020: (
        "71-74",
        {
            0: {"first_row": 4, "keep_cols": [0, 5]},
            1: {"first_row": 6, "keep_cols": [0, 5]},
            2: {"first_row": 6, "keep_cols": [0, 4]},
            3: {"first_row": 6, "keep_cols": [0, 4]},
        },
    ),
    2021: (
        "71-74",
        {
            0: {"first_row": 7, "keep_cols": [0, 5]},
            1: {"first_row": 6, "keep_cols": [0, 5]},
            2: {"first_row": 6, "keep_cols": [0, 4]},
            3: {"first_row": 6, "keep_cols": [0, 4]},
        },
    ),
    2022: (
        "77-80",
        {
            0: {"first_row": 7, "keep_cols": [0, 6]},
            1: {"first_row": 6, "keep_cols": [0, 5]},
            2: {"first_row": 6, "keep_cols": [0, 5]},
            3: {"first_row": 6, "keep_cols": [0, 5]},
        },
    ),
}


def ida_manual_corrections(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function runs manual correction to country names that were not scraped correctly from the pdf.

    Args:
        df (pd.DataFrame): Data Frame with unclean provider names.

    Returns: DataFrame with clean provider names.
    """
    df.loc[(df["year"] == 2021) & (df["provider"] == ""), "provider"] = (
        "Total--June 30, 2021b"
    )

    return df


def _ida_scrape(year: int, pages: str, page_settings: dict) -> pd.DataFrame:
    """
    This function reads the pdf file that holds IDA data. The function is used in a for loop in scrape_ida_tables,
    which loops through each years' pdf, locates the table on the page(s) specified, and reads the data into a pd.DataFrame.
    This helper function takes the pdf by year (i.e. ida_2015), uses the page number to specify the location of the table
    within the pdf, and uses the page_settings and callable cleaning function to output the required data from the
    tables. It reads each table, adds an additional column for the year, and concatenates each table into one DataFrame.
    Args:
        year (int): year of the pdf in question.
        pages (str): page range of the relevant tables in the pdf, formatted as "45-57".
        page_settings (dict): for each year, this dictionary holds data for page range, keep_cols/first_row, and the
        required cleaning function.
        custom_cleaning (callable): the cleaning function specified for each pdf's set of tables.
        mdb: the mdb in question. In this case, IDA.

    Returns: Complete DataFrame of IDA data.
    """

    # Read the raw data (which is a table)
    raw_data = read_mdb_table(mdb="ida", year=year, pages=pages, flavor="stream")

    # Create a dataframe by putting together all the tables
    df = concatenate_tables(raw_data=raw_data, page_settings=page_settings, mdb="ida")

    df["year"] = year

    return df


def scrape_ida_tables(settings: dict) -> pd.DataFrame:
    """
    This pipeline function scrapes all IDA capital subscriptions data from the PDF tables, cleans it, and stores it in a
    standardised DataFrame.
    This function first scrapes all the data from all pdf tables using the helper function _ida_scrape(). It then
    manually corrects misread country names, before converting the numeric column into a float as the pdf scraper reads
    text as strings and often include '$' signs in the table.  To prevent double counting, we remove all 'total' rows
    (in this function, these are identified by the keyword 'Total'). We add iso_3 codes and clean the provider names
    that are misread or change from one year to another, before standardising the column names to match the other MDB
    outputs. Finally, we calculate each countries' share of total capital subscriptions by year.
    Args:
        settings (dict): page_settings containing the pdf specifications for year, pages, first_row, keep_cols, and
        cleaning_function. See _ida_scrape() above.

    Returns: standardised DataFrame with all IDA capital subscriptions data, with columns for provider, iso_3, value,
    year, and share.
    """
    # Create an empty dataframe to hold the data for each year
    ida = pd.DataFrame()

    for year, (pages, page_settings) in settings.items():
        df = _ida_scrape(year=year, pages=pages, page_settings=page_settings)
        ida = pd.concat([ida, df], ignore_index=True)

    # Manual corrections from scraping
    ida = ida_manual_corrections(ida)

    # Clean capital subscription column to remove commas and $
    ida["capital_subscriptions"] = clean_numeric_series(ida["capital_subscriptions"])

    # remove irrelevant rows
    ida = remove_irrelevant_rows(ida, key_word="Total").dropna(
        subset=["capital_subscriptions"]
    )

    # finalise dataset with iso code, clean provider names, column names, and shares.
    ida = (
        add_iso_short_country_name(ida)
        .pipe(standardise_column_names, value_col="capital_subscriptions")
        .pipe(calculate_share_of_annual_total)
    )

    return ida


if __name__ == "__main__":
    ida_data = scrape_ida_tables(settings=ida_settings)

    checks = check_totals(ida_data)
    count = check_countries(ida_data)
