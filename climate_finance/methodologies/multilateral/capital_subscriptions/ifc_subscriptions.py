import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    read_mdb_table,
    concatenate_tables,
    set_column_names,
    filter_rows_columns,
    numeric_column_str_to_float,
    remove_irrelevant_rows,
    add_iso_short_country_name,
    standardise_column_names,
    check_totals,
    check_countries,
    check_iso,
    calculate_share_of_annual_total,
)


def clean_ifc_table_columns(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    This is one of two custom cleaning function for IFC tables that do not conform to the clean_tables() function in
    common.py. You can specify this cleaning function in page_settings.
    This function cleans the data for tables with two provider and value columns per page.
    This function filters for the relevant rows/columns, keeping the rows/columns specified in first_row and keep_cols,
    and sets the column names specific for this mdb (see set_column_names() in common.py). It stacks the columns from
    the same page, as the IFC reports their tables with two provider and value columns per page. Finally, it cleans the
    strings in the provider column to remove all non-letter and non-number characters.

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question. In this case, IFC.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.
    """
    df = filter_rows_columns(df=df, first_row=first_row, keep_cols=keep_cols).pipe(
        _set_cleaning_column_names
    )

    # Stack columns from same page
    df = _stack_ifc_columns(df=df)

    # clean text
    df["provider"] = (
        df["provider"]
        .str.replace(r" ", "_", regex=True)
        .str.replace(r"\W", "", regex=True)
        .str.replace("_", " ", regex=True)
        .str.replace("cid1", "", regex=False)
        .str.strip()
    )

    return df


def _set_cleaning_column_names(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "provider_left",
        "amount_paid_value_left",
        "provider_right",
        "amount_paid_value_right",
    ]

    df.columns = cols

    return df


def clean_ifc_table_pages(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    This is one of two custom cleaning function for IFC tables that do not conform to the clean_tables() function in
    common.py. You can specify this cleaning function in page_settings.
    This function cleans the data for tables with only one provider and value column per page. This function
    filters for the relevant rows/columns, keeping the rows and columns specified in first_row and keep_cols. It cleans
    the strings in the provider column to remove all non-letter and non-number characters, drops column [0] and sets
    the column names for IFC.

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question. In this case, IFC.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.
    """
    df = filter_rows_columns(df=df, first_row=first_row, keep_cols=keep_cols)

    df[1] = (
        (df[0] + df[1])
        .str.replace(r" ", "_", regex=True)
        .str.replace(r"\W", "", regex=True)
        .str.replace("_", " ", regex=True)
        .str.replace("cid1", "", regex=False)
        .str.strip()
    )

    # clean up columns
    df = df.drop(df.columns[0], axis=1).pipe(set_column_names, mdb="ifc")

    return df


ifc_settings = {
    2013: (
        "44",
        {
            0: {"first_row": 4, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2014: (
        "43",
        {
            0: {"first_row": 3, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2015: (
        "46",
        {
            0: {"first_row": 4, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2016: (
        "70-74",
        {
            0: {"first_row": 5, "keep_cols": [0, 1, 44]},
            1: {"first_row": 4, "keep_cols": [0, 1, 48]},
            2: {"first_row": 4, "keep_cols": [0, 1, 46]},
            3: {"first_row": 4, "keep_cols": [0, 1, 46]},
            4: {"first_row": 4, "keep_cols": [0, 1, 45]},
        },
        clean_ifc_table_pages,
    ),
    2017: (
        "73-77",
        {
            0: {"first_row": 4, "keep_cols": [0, 1, 44]},
            1: {"first_row": 4, "keep_cols": [0, 1, 48]},
            2: {"first_row": 4, "keep_cols": [0, 1, 46]},
            3: {"first_row": 4, "keep_cols": [0, 1, 46]},
            4: {"first_row": 4, "keep_cols": [0, 1, 45]},
        },
        clean_ifc_table_pages,
    ),
    2018: (
        "75-79",
        {
            0: {"first_row": 4, "keep_cols": [0, 1, 44]},
            1: {"first_row": 4, "keep_cols": [0, 1, 48]},
            2: {"first_row": 4, "keep_cols": [0, 1, 46]},
            3: {"first_row": 4, "keep_cols": [0, 1, 46]},
            4: {"first_row": 4, "keep_cols": [0, 1, 45]},
        },
        clean_ifc_table_pages,
    ),
    2019: (
        "65-66",
        {
            0: {"first_row": 3, "keep_cols": [0, 1, 5, 6]},
            1: {"first_row": 3, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2020: (
        "65-66",
        {
            0: {"first_row": 3, "keep_cols": [0, 1, 5, 6]},
            1: {"first_row": 3, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2021: (
        "67-68",
        {
            0: {"first_row": 5, "keep_cols": [0, 1, 5, 6]},
            1: {"first_row": 5, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
    2022: (
        "75-76",
        {
            0: {"first_row": 5, "keep_cols": [0, 1, 5, 6]},
            1: {"first_row": 5, "keep_cols": [0, 1, 5, 6]},
        },
        clean_ifc_table_columns,
    ),
}


def _stack_ifc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    This helper function is used in clean_ifc_table_columns(). Some tables for the IFC are presented as two seperate
    tables on the same page, with identical column name. This function stacks the two tables, left on top of the right.
    Args:
        df (pd.DataFrame): DataFrame with two seperate sets of data to be stacked: 'provider_left',
        'amount_paid_left_value', 'provider_right', 'amount_paid_value_right' .

    Returns: DataFrame with one set of columns for 'provider' and 'amount_paid_value'.
    """

    column_names = ["provider", "amount_paid_value"]

    df_left = df[["provider_left", "amount_paid_value_left"]]
    df_left.columns = column_names

    df_right = df[["provider_right", "amount_paid_value_right"]]
    df_right.columns = column_names

    return pd.concat([df_left, df_right], ignore_index=True)


def manual_corrections_to_country_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function runs manual correction to country names that were not scraped correctly from the pdf.

    Args:
        df (pd.DataFrame): Data Frame with unclean provider names.

    Returns: DataFrame with clean provider names.
    """
    conditions_antigua = [
        (df["year"] == 2015) & (df["provider"] == "Barbuda"),
    ]

    for condition in conditions_antigua:
        df.loc[condition, "provider"] = "Antigua and Barbuda"

    conditions_venezuela = [
        (df["year"] == 2020) & (df["provider"] == ""),
        (df["year"] == 2021) & (df["provider"] == "Bolivariana de"),
        (df["year"] == 2022) & (df["provider"] == "Bolivariana de"),
    ]

    for condition in conditions_venezuela:
        df.loc[condition, "provider"] = "Venezuela"

    conditions_laos = [
        (df["year"] == 2020) & (df["provider"] == "Republic"),
        (df["year"] == 2021) & (df["provider"] == "Republic"),
    ]

    for condition in conditions_laos:
        df.loc[condition, "provider"] = "Lao, Peoples Democratic Republic"

    return df


def _ifc_scrape(
    year: int, pages: str, page_settings: dict, custom_cleaning: callable, mdb: str
) -> pd.DataFrame:
    """
    This function reads the pdf file that holds IFC data. The function is used in a for loop in scrape_ifc_tables,
    which loops through each years' pdf, locates the table on the page(s) specified, and reads the data into a pd.DataFrame.
    This helper function takes the pdf by year (i.e. ifc_2015), uses the page number to specify the location of the table
    within the pdf, and uses the page_settings and callable cleaning function to output the required data from the
    tables. It reads each table, adds an additional column for the year, and concatenates each table into one DataFrame.
    Args:
        year (int): year of the pdf in question.
        pages (str): page range of the relevant tables in the pdf, formatted as "45-57".
        page_settings (dict): for each year, this dictionary holds data for page range, keep_cols/first_row, and the
        required cleaning function.
        custom_cleaning (callable): the cleaning function specified for each pdf's set of tables.
        mdb: the mdb in question. In this case, IFC.

    Returns: Complete DataFrame of IFC data.
    """

    # Read the raw data (which is a table)
    raw_data = read_mdb_table(mdb=mdb, year=year, pages=pages, flavor="stream")

    # Create a dataframe by putting together all the tables
    df = concatenate_tables(
        raw_data=raw_data,
        page_settings=page_settings,
        mdb=mdb,
        cleaning_function=custom_cleaning,
    )

    # Add year
    df["year"] = year

    return df


def scrape_ifc_tables(settings: dict) -> pd.DataFrame:
    """
    This pipeline function scrapes all IFC capital subscriptions data from the PDF tables, cleans it, and stores it in a
    standardised DataFrame.
    This function first scrapes all the data from all pdf tables using the helper function _ifc_scrape(). It then
    converts the numeric column into a float as the pdf scraper reads text as strings. It then manually corrects
    misread country names, renaming all columns with recorded capital subscription values but no provider names, before
    dropping all additional rows recorded by the PDF scraper but without any value. To prevent double counting, we remove
    all 'total' rows (in this function, these are identified by the keyword 'June'). We add iso_3 codes and clean the
    provider names that are misread or change from one year to another, before standardising the column names to match
    the other MDB outputs. Finally, we calculate each countries' share of total capital subscriptions by year.
    Args:
        settings (dict): page_settings containing the pdf specifications for year, pages, first_row, keep_cols, and
        cleaning_function. See _ifc_scrape() above.

    Returns: standardised DataFrame with all IFC capital subscriptions data, with columns for provider, iso_3, value,
    year, and share.
    """
    # Create an empty dataframe to hold the data for each year
    ifc = pd.DataFrame()

    for year, (pages, page_settings, custom_cleaning_fn) in settings.items():
        df = _ifc_scrape(
            year=year,
            pages=pages,
            page_settings=page_settings,
            custom_cleaning=custom_cleaning_fn,
            mdb="ifc",
        )
        ifc = pd.concat([ifc, df], ignore_index=True)

    # Fix column types
    ifc = numeric_column_str_to_float(df=ifc, col="amount_paid_value")

    # implement manual corrections from double rows in PDF
    ifc = manual_corrections_to_country_names(df=ifc).dropna(
        subset=["amount_paid_value"]
    )

    # remove irrelevant rows
    ifc = remove_irrelevant_rows(df=ifc, key_word="June")

    # Add iso_3 column and fix misread country names
    ifc = add_iso_short_country_name(df=ifc)

    # standardise column names to match other outputs
    ifc = standardise_column_names(df=ifc, value_col="amount_paid_value")

    # calculate shares
    ifc = calculate_share_of_annual_total(df=ifc)

    return ifc


if __name__ == "__main__":
    ifc_data = scrape_ifc_tables(settings=ifc_settings)

    checks = check_totals(ifc_data)
    count = check_countries(ifc_data)
    check_iso = check_iso(ifc_data)
