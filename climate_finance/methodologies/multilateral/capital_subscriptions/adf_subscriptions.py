import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    read_mdb_table,
    concatenate_tables,
    set_column_names,
    filter_rows_columns,
    clean_table,
    numeric_column_str_to_float,
    remove_irrelevant_rows,
    add_iso_short_country_name,
    standardise_column_names,
    check_totals,
    check_countries,
    check_iso,
    calculate_share_of_annual_total,
)


def clean_adf_table(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    This is the custom cleaning function for ADF tables that do not conform to the clean_tables() function in common.py.
    You can specify this cleaning function in page_settings. This function merges columns [0] and [1] into column [1],
    as the country names were split across both columns. It then filters for the relevant rows, keeping the rows and
    columns specified in first_row and keep_cols.

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question. In this case, ADF.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.
    """

    df[1] = df[0] + df[1]

    return df.pipe(filter_rows_columns, first_row=first_row, keep_cols=keep_cols).pipe(
        set_column_names, mdb=mdb
    )


adf_settings = {
    2013: (
        "283",
        {
            0: {"first_row": 1, "keep_cols": [0, 1]},
        },
        clean_table,
    ),
    2014: (
        "238",
        {
            0: {"first_row": 5, "keep_cols": [1, 7]},
        },
        clean_table,
    ),
    2015: (
        "267",
        {
            0: {"first_row": 6, "keep_cols": [1, 7]},
        },
        clean_table,
    ),
    2016: (
        "124",
        {
            0: {"first_row": 6, "keep_cols": [0, 6]},
        },
        clean_table,
    ),
    2017: (
        "124",
        {
            0: {"first_row": 5, "keep_cols": [0, 6]},
        },
        clean_table,
    ),
    2018: (
        "134",
        {
            0: {"first_row": 7, "keep_cols": [1, 7]},
        },
        clean_adf_table,
    ),
    2019: (
        "145",
        {
            0: {"first_row": 6, "keep_cols": [1, 7]},
        },
        clean_adf_table,
    ),
    2020: (
        "144",
        {
            0: {"first_row": 6, "keep_cols": [1, 8]},
        },
        clean_adf_table,
    ),
    2021: (
        "151",
        {
            0: {"first_row": 6, "keep_cols": [1, 8]},
        },
        clean_adf_table,
    ),
    2022: (
        "155",
        {
            0: {"first_row": 5, "keep_cols": [0, 7]},
        },
        clean_table,
    ),
}


def manually_add_missed_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function manually adds rows that not scraped from the pdf but needed for later calculations

    Args:
        df (pd.DataFrame): Data Frame with missing rows.

    Returns: Complete DataFrame.
    """
    rows_to_add = pd.DataFrame(
        {
            "provider": [
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2013.1
                "Supplementary contributions through cash to reduce the gap",  # 2013.2
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2014
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2015
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2016
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2017
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2018
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2019
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2020
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2021
                "Supplementary contributions through accelerated encashment to reduce the gap",  # 2022
            ],
            "capital_subscriptions": [
                "103892879",  # 2013.1
                "50818264",  # 2013.2
                "65321",  # 2014
                "65321",  # 2015
                "65321",  # 2016
                "72238",  # 2017
                "78899",  # 2018
                "78899",  # 2019
                "78899",  # 2020
                "65321",  # 2021
                "65321",  # 2022
            ],
            "year": [
                2013,  # 2013.1
                2013,  # 2013.2
                2014,  # 2014
                2015,  # 2015
                2016,  # 2016
                2017,  # 2017
                2018,  # 2018
                2019,  # 2019
                2020,  # 2020
                2021,  # 2021
                2022,  # 2022
            ],
        }
    )

    return pd.concat([df, rows_to_add], ignore_index=False)


def manual_corrections_to_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function runs manual correction to values that were not scraped correctly from the pdf.

    Args:
        df (pd.DataFrame): Data Frame with incorrect values.

    Returns: DataFrame with correct values.
    """
    df.loc[
        (df["year"] == 2021) & (df["provider"] == "Total"), "capital_subscriptions"
    ] = "33956736"

    df.loc[
        (df["year"] == 2022) & (df["provider"] == "Total"), "capital_subscriptions"
    ] = "33956941"

    return df


def manual_corrections_to_providers(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function runs manual correction to country names that were not scraped correctly from the pdf.

    Args:
        df (pd.DataFrame): Data Frame with unclean provider names.

    Returns: DataFrame with clean provider names.
    """
    # standardise ADB
    df.loc[
        (df["year"] == 2013) & (df["capital_subscriptions"] == "111,740,678"),
        "provider",
    ] = "ADB"

    # United States corrections
    conditions_usa = [
        (df["year"] == 2022) & (df["provider"] == "32"),
        (df["year"] == 2014) & (df["capital_subscriptions"] == "2,624,512"),
        (df["provider"] == "America"),
        # (df["year"] == 2016) & (df["provider"] == "America"),
        # (df["year"] == 2017) & (df["provider"] == "America"),
        (df["year"] == 2018) & (df["provider"] == "31"),
        (df["year"] == 2019) & (df["provider"] == "31"),
        (df["year"] == 2020) & (df["provider"] == "32"),
        (df["year"] == 2021) & (df["provider"] == "32"),
    ]

    for condition in conditions_usa:
        df.loc[condition, "provider"] = "United States of America"

    # UAE corrections
    conditions_uae = [
        (df["year"] == 2022) & (df["provider"] == "30"),
        (df["year"] == 2018) & (df["provider"] == "29"),
        (df["year"] == 2019) & (df["provider"] == "29"),
        (df["year"] == 2020) & (df["provider"] == "30"),
        (df["year"] == 2021) & (df["provider"] == "30"),
    ]

    for condition in conditions_uae:
        df.loc[condition, "provider"] = "United Arab Emirates"

    # Contributions corrections
    conditions_supplementary_contributions = [
        (df["provider"] == "contributions"),
        (df["provider"] == "voluntary contributions"),
        (df["provider"] == "voluntarycontributions"),
        (df["year"] == 2014) & (df["capital_subscriptions"] == "94,709"),
        # (df["year"] == 2015) & (df["provider"] == "voluntary contributions"),
        # (df["year"] == 2016) & (df["provider"] == "contributions"),
        # (df["year"] == 2017) & (df["provider"] == "contributions"),
        # (df["year"] == 2018) & (df["provider"] == "contributions"),
        # (df["year"] == 2019) & (df["provider"] == "contributions"),
        # (df["year"] == 2020) & (df["provider"] == "contributions"),
        # (df["year"] == 2021) & (df["provider"] == "contributions"),
    ]

    for condition in conditions_supplementary_contributions:
        df.loc[condition, "provider"] = "Supplementary or Voluntary contributions"

    return df


def clean_provider_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the provider column by removing all non-letter and number characters.

    Args:
        df (pd.DataFrame): Data Frame with unclean provider names.

    Returns: DataFrame with clean provider names.
    """
    df["provider"] = (
        df["provider"]
        .str.replace(r"\d+", "", regex=True)
        .str.replace(r" ", "_", regex=True)
        .str.replace(r"\W", "", regex=True)
        .str.replace("_", " ", regex=True)
        .str.strip()
    )

    return df


def _adf_scrape(
    year: int, pages: str, page_settings: dict, custom_cleaning: callable, mdb: str
) -> pd.DataFrame:
    """
    This function reads the pdf file that holds ADF data. The function is used in a for loop in scrape_adf_tables, which
    loops through each years' pdf, locates the table on the page(s) specified, and reads the data into a pd.DataFrame.
    This helper function takes the pdf by year (i.e. adf_2015), uses the page number to specify the location of the table
    within the pdf, and uses the page_settings and callable cleaning function to output the required data from the
    tables. It reads each table, adds an additional column for the year, and concatenates each table into one DataFrame.
    Args:
        year (int): year of the pdf in question.
        pages (str): page range of the relevant tables in the pdf, formatted as "45-57".
        page_settings (dict): for each year, this dictionary holds data for page range, keep_cols/first_row, and the
        required cleaning function.
        custom_cleaning (callable): the cleaning function required for each pdf's set of tables.
        mdb: the mdb in question. In this case, ADF.

    Returns: Complete DataFrame of ADF data.

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


def scrape_adf_tables(settings: dict) -> pd.DataFrame:
    """
    This pipeline function scrapes all ADF capital subscriptions data from the PDF tables, cleans it, and stores it in a
    standardised DataFrame.
    This function first scrapes all the data from all pdf tables using the helper function _adf_scrape(). It then
    runs a series of manual correction functions to (1) add missing rows which were not scraped by the pdf scraper, (2)
    correct missing values, and (3) correct misread country names. The function then cleans the provider names, before
    converting the capital_subscriptions column from a string to a float, dropping all NaN values, and removing all total
    rows to prevent double counting. Finally, we add iso_codes, standardise column names to match the other MDB
    outputs, and calculate each countries' share of total capital subscriptions by year.
    Args:
        settings (dict): page_settings containing the pdf specifications for year, pages, first_row, keep_cols, and
        cleaning_function. See _adb_scrape() above.

    Returns: standardised DataFrame with all IDA capital subscriptions data, with columns for provider, iso_3, value,
    year, and share.
    """
    # Create an empty dataframe to hold the data for each year
    adf = pd.DataFrame()

    for year, (pages, page_settings, custom_cleaning_fn) in settings.items():
        df = _adf_scrape(
            year=year,
            pages=pages,
            page_settings=page_settings,
            custom_cleaning=custom_cleaning_fn,
            mdb="adf",
        )
        adf = pd.concat([adf, df], ignore_index=True)

    # implement manual corrections from scraping from PDF
    adf = (
        manually_add_missed_rows(df=adf)
        .pipe(manual_corrections_to_values)
        .pipe(manual_corrections_to_providers)
    )

    # clean provider name column (needs to be after manual corrections).
    adf = clean_provider_column(df=adf)

    # Fix column types
    adf = numeric_column_str_to_float(df=adf, col="capital_subscriptions").dropna(
        subset=["capital_subscriptions"]
    )

    # remove irrelevant rows
    adf = remove_irrelevant_rows(df=adf, key_word="Total")

    # Add iso_3 column and fix misread country names
    adf = add_iso_short_country_name(df=adf)

    # standardise column names to match other outputs
    adf = standardise_column_names(df=adf, value_col="capital_subscriptions")

    # calculate shares
    adf = calculate_share_of_annual_total(df=adf)

    return adf


if __name__ == "__main__":
    adf_data = scrape_adf_tables(settings=adf_settings)

    checks = check_totals(adf_data)
    count = check_countries(adf_data)
    check_iso = check_iso(adf_data)
