import pandas as pd

from scripts.analysis.other.capital_subscriptions.common import (
    read_mdb_table,
    concatenate_tables,
    set_column_names,
    filter_rows_columns,
    calculate_share_of_annual_total,
    standardise_column_names,
    add_iso_short_country_name,
    remove_irrelevant_rows,
    check_totals,
    numeric_column_str_to_float,
)


def clean_ibrd_table(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    This is the custom cleaning function for ADF tables that do not conform to the clean_tables() function in common.py.
    You can specify this cleaning function in page_settings. This function merges columns [0] and [1] into column [1],
    as the country names were split across both columns. It then removes all non-letter and digit characters from the
    country strings, as the pdf scraper outputted multiple errors. It then filters for the relevant rows, keeping the rows and
    columns specified in first_row and keep_cols, before setting the column names specific for this mdb (see
    set_column_names() in common.py).

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question. In this case, IBRD.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.
    """

    # Merge columns which may contain names (and remove non letter characters)
    df[1] = df[0] + df[1]

    df[1] = (
        df[1]
        .str.replace(r" ", "_", regex=True)
        .str.replace(r"\W", "", regex=True)
        .str.replace("_", " ", regex=True)
        .str.strip()
    )

    return df.pipe(filter_rows_columns, first_row=first_row, keep_cols=keep_cols).pipe(
        set_column_names, mdb=mdb
    )


ibrd_settings = {
    2013: (
        "71-74",
        {
            0: {"first_row": 7, "keep_cols": [0, 3]},
            1: {"first_row": 7, "keep_cols": [0, 3]},
            2: {"first_row": 7, "keep_cols": [0, 3]},
            3: {"first_row": 7, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2014: (
        "72-75",
        {
            0: {"first_row": 5, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 3]},
            3: {"first_row": 5, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2015: (
        "76-79",
        {
            0: {"first_row": 5, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 3]},
            3: {"first_row": 5, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2016: (
        "82-85",
        {
            0: {"first_row": 4, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 3]},
            2: {"first_row": 5, "keep_cols": [0, 3]},
            3: {"first_row": 3, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2017: (
        "88-91",
        {
            0: {"first_row": 4, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 3]},
            2: {"first_row": 5, "keep_cols": [0, 3]},
            3: {"first_row": 4, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2018: (
        "93-96",
        {
            0: {"first_row": 4, "keep_cols": [0, 3]},
            1: {"first_row": 6, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 4]},
            3: {"first_row": 3, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2019: (
        "100-104",
        {
            0: {"first_row": 5, "keep_cols": [1, 21]},
            1: {"first_row": 4, "keep_cols": [1, 19]},
            2: {"first_row": 5, "keep_cols": [1, 22]},
            3: {"first_row": 5, "keep_cols": [1, 21]},
            4: {"first_row": 5, "keep_cols": [1, 22]},
        },
        clean_ibrd_table,
    ),
    2020: (
        "88-91",
        {
            0: {"first_row": 4, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 4]},
            3: {"first_row": 3, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2021: (
        "88-91",
        {
            0: {"first_row": 3, "keep_cols": [0, 3]},
            1: {"first_row": 4, "keep_cols": [0, 4]},
            2: {"first_row": 4, "keep_cols": [0, 4]},
            3: {"first_row": 5, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
    2022: (
        "90-93",
        {
            0: {"first_row": 5, "keep_cols": [0, 4]},
            1: {"first_row": 6, "keep_cols": [0, 3]},
            2: {"first_row": 5, "keep_cols": [0, 3]},
            3: {"first_row": 4, "keep_cols": [0, 3]},
        },
        clean_ibrd_table,
    ),
}


def _ibrd_scrape(
    year: int, pages: str, page_settings: dict, custom_cleaning: callable
) -> pd.DataFrame:
    """
    This function reads the pdf file that holds IBRD data. The function is used in a for loop in scrape_ibrd_tables,
    which loops through each years' pdf, locates the table on the page(s) specified, and reads the data into a pd.DataFrame.
    This helper function takes the pdf by year (i.e. ibrd_2015), uses the page number to specify the location of the table
    within the pdf, and uses the page_settings and callable cleaning function to output the required data from the
    tables. It reads each table, adds an additional column for the year, and concatenates each table into one DataFrame.
    Args:
        year (int): year of the pdf in question.
        pages (str): page range of the relevant tables in the pdf, formatted as "45-57".
        page_settings (dict): for each year, this dictionary holds data for page range, keep_cols/first_row, and the
        required cleaning function.
        custom_cleaning (callable): the cleaning function specified for each pdf's set of tables.
        mdb: the mdb in question. In this case, IBRD.

    Returns: Complete DataFrame of IBRD data.
    """

    # Read the raw data (which is a table)
    raw_data = read_mdb_table(mdb="ibrd", year=year, pages=pages, flavor="stream")

    # Create a dataframe by putting together all the tables
    df = concatenate_tables(
        raw_data=raw_data,
        page_settings=page_settings,
        mdb="ibrd",
        cleaning_function=custom_cleaning,
    )

    # Add year
    df["year"] = year

    return df


def _split_number_columns(df: pd.DataFrame, keep: int) -> pd.DataFrame:
    """
    helper function for clean_2022_ibrd. Some of the capital_subscription data in the IBRD pdfs is read into the same
    cell as other, irrelevant data. Hence, we need to seperate this data from the rest. This function allows you to split
    the data into two seperate columns, keeping only the relevant side holding the capital_subscriptions data.
    Args:
        - 'keep': allows you to select which side of the split result you want to keep.
                0: keeps left side.
                1:keeps right side.
    """

    df = df.copy()

    df["capital_subscriptions"] = (
        df["capital_subscriptions"]
        .str.replace(r" ", "_", regex=True)
        .str.replace(r"[^\w.]", "", regex=True)
        .str.split("_", expand=True)[keep]
        .str.strip()
    )

    return df


def clean_2022_ibrd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some of the capital_subscription data in the IBRD pdfs is read into the same cell as other, irrelevant data. This
    function uses the helper function _split_number_columns (see function for explanation) to seperate the relevant and
    irrelevant data. This function focuses on the portion of 2022 data (from El Salvador down to Madagascar) that requires
    splitting, before add this data back to the original dataframe where the data is read correctly.
    Args:
        df: Full DataFrame for all years, where a sizeable portion of 2022 data requires cleaning to remove irrelevant
        values from the capital_subscription column.

    Returns: clean IBRD data from 2013-2022, containing only relevant capital_subscriptions data in the
    capital_subscriptions column.

    """

    df_not_2022 = df.loc[lambda d: d.year != 2022]
    df_2022 = df.loc[lambda d: d.year == 2022]

    # Filter for page 1
    df_2022_page_1 = df_2022.loc[
        lambda d: (d.provider <= "Egypt, Arab Republic of")
        & (d.provider != "Bolivariana de")
    ]

    # Split number column, keeping number to the right of the space.
    df_2022_page_1 = _split_number_columns(df_2022_page_1, keep=1)

    # Filter for page 2
    df_2022_page_2 = df_2022.loc[
        lambda d: (d.provider >= "El Salvador") & (d.provider <= "Madagascar")
    ]

    # Split number column, keeping number to the left of the space.
    df_2022_page_2 = _split_number_columns(df_2022_page_2, keep=0)

    # Create DataFrame of providers in pages 1 and 2 (so we can later concatenate clean data with
    # the remaining 2022 data).
    providers_page_1_2 = pd.concat(
        [df_2022_page_1["provider"], df_2022_page_2["provider"]]
    )

    remaining_2022 = df_2022.loc[lambda d: ~d["provider"].isin(providers_page_1_2)]

    final_2022 = pd.concat(
        [df_2022_page_1, df_2022_page_2, remaining_2022], ignore_index=True
    )

    # El Salvador and China data has been removed. Manually add these values back in.
    # Using strings to conform with other objects in columns for clean_numeric_series function
    final_2022.loc[df["provider"] == "China", "capital_subscriptions"] = "17239.6"
    final_2022.loc[df["provider"] == "El Salvador", "capital_subscriptions"] = "48.6"

    return pd.concat([df_not_2022, final_2022], ignore_index=True)


def _correct_multiple_row_providers(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function runs manual correction to country names that were not scraped correctly from the pdf.

    Args:
        df (pd.DataFrame): Data Frame with unclean provider names.

    Returns: DataFrame with clean provider names.
    """
    conditions_venezuela = [
        (df["year"] == 2018) & (df["provider"] == "Bolivariana de"),
        (df["year"] == 2019) & (df["provider"] == "de"),
        (df["year"] == 2020) & (df["provider"] == "Bolivariana de"),
        (df["year"] == 2022) & (df["provider"] == "Bolivariana de"),
    ]

    for condition in conditions_venezuela:
        df.loc[condition, "provider"] = "Venezuela, Republica Bolivariana de"

    conditions_laos = [
        (df["year"] == 2018) & (df["provider"] == "Republic"),
        (df["year"] == 2019) & (df["provider"] == "Republic"),
        (df["year"] == 2020) & (df["provider"] == "Republic"),
        (df["year"] == 2021) & (df["provider"] == ""),
    ]

    for condition in conditions_laos:
        df.loc[condition, "provider"] = "Lao, Peoples Democratic Republic"

    return df


def scrape_ibrd_tables(settings: dict) -> pd.DataFrame:
    """
    This pipeline function scrapes all IBRD capital subscriptions data from the PDF tables, cleans it, and stores it in
    a standardised DataFrame.
    This function first scrapes all the data from all pdf tables using the helper function _ibrd_scrape(). The data for
    2022 requires some manual fixes, completed in clean_2022_data (see function for explanation). It then
    converts the numeric column into a float and runs manual corrections for providers whose names span over two rows in
    the pdf table, by inputting the full name into the row with the recorded value and dropping the additional, non-value
    row. To prevent double counting, we remove all 'total' rows (in this function, these are identified by the keyword
    'Total'). We add iso_3 codes and clean the provider names that are misread or change from one year to another,
    before standardising the column names to match the other MDB outputs. Finally, we calculate each countries' share of
    total capital subscriptions by year.
    Args:
        settings (dict): page_settings containing the pdf specifications for year, pages, first_row, keep_cols, and
        cleaning_function. See _ibrd_scrape() above.

    Returns: standardised DataFrame with all IDA capital subscriptions data, with columns for provider, iso_3, value,
    year, and share.
    """
    # Create an empty dataframe to hold the data for each year
    ibrd = pd.DataFrame()

    for year, (pages, page_settings, custom_cleaning_fn) in settings.items():
        df = _ibrd_scrape(
            year=year,
            pages=pages,
            page_settings=page_settings,
            custom_cleaning=custom_cleaning_fn,
        )
        ibrd = pd.concat([ibrd, df], ignore_index=True)

    # Clean 2022 data which is read incorrectly. It merges total subscriptions and amount paid columns
    ibrd = clean_2022_ibrd(df=ibrd)

    ibrd = (
        numeric_column_str_to_float(
            df=ibrd, col="capital_subscriptions"
        )  # clean numeric series
        .pipe(
            _correct_multiple_row_providers
        )  # Clean providers that go over multiple rows (primarily "Venezuela, Republica Bolivariana de")
        .pipe(remove_irrelevant_rows, key_word="June 30")  # Remove totals
        .dropna(subset=["capital_subscriptions"])
        .pipe(add_iso_short_country_name)  # clean provider names and add iso_codes
        .pipe(standardise_column_names, value_col="capital_subscriptions")
        .pipe(calculate_share_of_annual_total)
    )

    return ibrd


if __name__ == "__main__":
    ibrd_data = scrape_ibrd_tables(settings=ibrd_settings)

    checks = check_totals(ibrd_data)
