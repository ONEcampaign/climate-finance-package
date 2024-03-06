import camelot
import pandas as pd
from bblocks import add_iso_codes_column, add_short_names_column, clean_numeric_series
from camelot.core import TableList

from scripts.config import Paths


def filter_rows_columns(df: pd.DataFrame, first_row: int, keep_cols: list[int]):
    """
    Takes the pdf tables in DataFrame form and removes (1) the top columns down to (but excluding)
    the specified first row, and keeps only the specified columns.
    Args:
        df (pd.DataFrame): DataFrame containing the PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.

    Returns: pd.DataFrame of relevant rows and columns

    """
    return df.iloc[first_row:, keep_cols]


def set_column_names(df: pd.DataFrame, mdb: str) -> pd.DataFrame:
    """
    Sets column names depending on the mdb string passed. Each mdb has different ways of recording
    capital subscriptions. This function sets the relevant column name to the cleaned DataFrame
    (see filter_rows_columns). It is important to standardise the column names for the same MDB across
    years, as it varies from PDF to PDF.

    Args:
        df (pd.DataFrame): the input DataFrame with varied column names by year.
        mdb: the Multilateral Development Bank in question.

    Returns:

    """
    columns = {
        "ida": ["provider", "capital_subscriptions"],
        "ibrd": ["provider", "capital_subscriptions"],
        "afdb": ["provider", "amount_paid", "callable_capital"],
        "ifc": ["provider", "amount_paid_value"],
        "adf": ["provider", "capital_subscriptions"],
    }

    df.columns = columns[mdb]

    return df


def clean_table(
    df: pd.DataFrame, first_row: int, keep_cols: list[int], mdb: str
) -> pd.DataFrame:
    """
    Default function to clean tables. Some MDBs require more complex cleaning, depending
    on the standard of the DataFrame outputted by Camelot. This function offers cleaning
    for the most basic tables. It first filters for the relevant rows and columns (filter_rows_columns),
    then sets standardised column names (set_column_names).

    Args:
        df (pd.DataFrame): input DataFrame from PDF tables.
        first_row (int): Row index of the first relevant row (i.e. row with the first country's data.
        keep_cols (int): Column indices to keep.
        mdb (str): the Multilateral Development Bank in question.

    Returns: pd.DataFrame: cleaned DataFrame of a single table from the list of tables specified in page_settings.

    """
    return df.pipe(filter_rows_columns, first_row=first_row, keep_cols=keep_cols).pipe(
        set_column_names, mdb=mdb
    )


def concatenate_tables(
    raw_data: TableList,
    page_settings: dict,
    mdb: str,
    cleaning_function: callable = clean_table,
) -> pd.DataFrame:
    """stacks tables from multiple pages of a pdf into a single dataframe. For each page, the tables are read, cleaned,
    and stacked under the page from the previous year's pdf. The cleaning of tables differs by multilateral and year,
    hence the callable cleaning_function, specified in the page_settings.

    Args:
        - raw_data (TableList): the list of tables scraped using the read_mdb_table function
        - page_settings (dict): a dictionary with the required pdf characteristics to determine how to clean the raw_data.
        - mdb (str): name of the relevant mdb
        - cleaning function: determines the cleaning function run on raw data (primarily when there is messy provider columns).

    Returns: pd.DataFrame with each years' data stacked on top of the other.
    """

    # Create an empty dataframe to store all the tables
    final_df = pd.DataFrame()

    for page, settings in page_settings.items():
        df = raw_data[page].df
        df = cleaning_function(
            df=df,
            first_row=settings["first_row"],
            keep_cols=settings["keep_cols"],
            mdb=mdb,
        )

        final_df = pd.concat([final_df, df], ignore_index=False)

    return final_df


def read_mdb_table(mdb: str, year: int, pages: str, flavor: str):
    """
    Reads PDF tables and stores them as a list of tables.

    Args:
        mdb (str): the Multilateral Development Bank in question,
        year (str): the year in question.
        pages (str): the page range of capital subscrpition/voting power tables. Written as "xx-xx".
        flavor: used in camelot's 'read_pdf' method. See documentation for explanation.

    Returns: List of tables: containing all tables in the specified page range, for the specified mdb and year.

    """
    return camelot.read_pdf(
        f"{Paths.raw_data}/mdb_pdfs/{mdb}_{year}.pdf", pages=pages, flavor=flavor
    )


def add_value_column_from_annual_total_columns(
    df: pd.DataFrame, first_year: int, last_year: int
) -> pd.DataFrame:
    """
    Some mdbs record annual data in multiple columns in addition to multiple pdfs. This means
    the tables have multiple values for each year. This function keeps the column for the year of the pdf
    i.e. if using ida_2015, it will keep the 2015 column and remove the 2014 column.

    Args:
        df (pd.DataFrame): DataFrame with multiple value columns, labelled Total (year).
        first_year (int): first year in the pdfs which follow this structure.
        last_year (int): last year in the pdfs which follow this structure.

    Returns: DataFrame with single year and value columns.

    """

    years = range(first_year, last_year + 1)

    # change year type to integer incase it comes as a string/object, otherwise will output an error in loc method.
    df["year"] = df["year"].astype(int)

    # create empty DataFrame
    final_df = pd.DataFrame()

    # Make single 'value' column taking the value from the relevant "total (year)" column
    for year in years:
        dff = df.copy()
        dff = dff.loc[lambda d: d.year == year]
        dff["value"] = dff.loc[:, f"Total ({year})"]

        final_df = pd.concat([final_df, dff], ignore_index=False)

    return final_df


def calculate_share_of_annual_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates each countries' share of total capital subscriptions by year.

    Args:
        df (pd.DataFrame): DataFrame with absolute value and year columns.

    Returns: pd.DataFrame with additional share column

    """
    df["year_total"] = df.groupby("year")["value"].transform(sum)

    df["share"] = df["value"] / df["year_total"]

    return df.drop("year_total", axis=1)


def standardise_column_names(
    df: pd.DataFrame,
    provider_col: str = "provider",
    iso_col: str = "iso_3",
    value_col: str = "value",
    year_col: str = "year",
) -> pd.DataFrame:
    """
    Standardises column names such that every mdb output has the same columns (provider, iso_3, value, year).
    Required for "calculate_share_of_annual_total" function above.

    Args:
        df (pd.DataFrame): DataFrame with mdb specific column names.
        provider_col (str, 'provider': provider column name for the specified mdb.
        iso_col (str, 'iso_3'): iso code column name for the specified mdb.
        value_col (str, 'value'): capital subscription (absolute value)  column name for the specified mdb.
        year_col (str, 'year'): year column name for the specified mdb.

    Returns: pd.DataFrame with standardised column names and column order.

    """
    # Rename columns to match standardisation
    df = df.rename(
        columns={
            provider_col: "provider",
            iso_col: "iso_3",
            value_col: "value",
            year_col: "year",
        }
    )

    # reorder columns
    df = df[["provider", "iso_3", "value", "year"]]

    return df


def _remove_specified_total_rows(
    df: pd.DataFrame, total_name: str, first_year: int, last_year: int
) -> pd.DataFrame:
    """removes the rows for totals in each MDB table based on how they label totals.
    Only relevant if totals column is in the format of "text ... year"
    Since been refactored with "_remove_irrelevant_rows" function.

    Args:
        df: the required dataframe. Must have been standardised through _standardise_column_names().
        total_name: the string of text leading up to the year.
        first_year: the first year of the total rows that need removing.
        last_year: the final year of the total rows that need removing.
    """

    years = [f"{total_name}{year}" for year in range(first_year, last_year + 1)]

    # Keep all rows that don't match the condition
    df = df.loc[lambda d: ~d.provider.isin(years)]

    return df


def add_iso_short_country_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses bblocks functions to add iso_codes to DataFrames based on the provider column. These functions
    use regex to identify the relevant country. Function then uses the iso_codes applied to clean
    provider column.

    Args:
        df (pd.DataFrame): DataFrame without an iso_3 column.

    Returns: DataFrame with an iso_3 column and cleaned provider column.

    """
    # Use regex within country_converter to find iso_3 codes (cleans most of the country names that are broken)
    df = add_iso_codes_column(df=df, id_column="provider", target_column="iso_3")

    # replace the country provider names with country name from country_converter
    df = add_short_names_column(df=df, id_column="iso_3", target_column="provider")

    return df


def remove_irrelevant_rows(df: pd.DataFrame, key_word: str = "Total") -> pd.DataFrame:
    """
    Removes rows from DataFrame that contain the specified keyword (default of total). Totals
    need to be removed to prevent double counting when calculating the shares in
    calculate_share_of_annual_total() function.

    Args:
        df (pd.DataFrame): DataFrame with totals and other irrelevant rows.
        key_word (str, 'Total'): Key word present within provider column entries to be removed.

    Returns:

    """
    # remove total and text columns

    df = df[~df["provider"].str.contains(key_word, case=False, na=False)]

    return df


def check_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check function: totals each year to ensure that (1) the totals included are equal to the totals
    reported in the pdf, and (2) the shares total to 1.
    Args:
        df (pd.DataFrame): Final DataFrame.

    Returns: Check DataFrame with totals for value and share by year.

    """
    return df.groupby("year")[["value", "share"]].sum()


def check_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check function: Counts the number of rows for each provider across all years. Should equal 10 for
    each provider, unless they were new members after 2013. Manually checked all non-10 providers.
    Args:
        df (pd.DataFrame): Final DataFrame.

    Returns: Check DataFrame with number of entries for each unique provider.
    """

    return df["provider"].value_counts()


def check_iso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check function: Counts the number of rows for each iso_3 across all years. Should equal 10 for
    each iso_code, unless they were new members after 2013. Manually checked all non-10 iso_codes.
    Also checked all iso_codes which did not match their corresponding provider value (check_countries
    function).
    Args:
        df (pd.DataFrame): Final DataFrame.

    Returns: Check DataFrame with number of entries for each unique iso_code.
    """
    return df["iso_3"].value_counts()


def _read_mdb_xlsx_long_format(
    mdb: str, first_year: int, last_year: int
) -> pd.DataFrame:
    """
    Reads all pre-standardised excel files into a DataFrame. Concatenates each year below the previous year,
    adding an additional column for 'year' based on the year of the data.

    Args:
        mdb (str): the Multilateral Development Bank in question,
        first_year (str): the first year available for the mdb.
        last_year (str): the last year available for the mdb

    Returns: long-format DataFrame with an additional column for year.
    """

    years = [str(year) for year in range(first_year, last_year + 1)]

    full_df = pd.DataFrame()

    for year in years:
        df = pd.read_excel(f"{Paths.raw_data}/mdb_xlsx/{mdb}.xlsx", sheet_name=year)
        df["year"] = year

        full_df = pd.concat([full_df, df], ignore_index=False)

    return full_df


def numeric_column_str_to_float(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    cleans str column and converts into float. Does this by replacing all non-number or letter characters with empty strings
    and stripping all additional spaces. Then passes this output to bblock's clean_numeric_series function.
    Args:
        df (pd.DataFrame): DataFrame with string column.
        col (str): The column that requires converting from string to float.

    Returns:

    """
    # Remove all non-number/letter characters from strings (keeping '.')
    df[col] = df[col].str.replace(r"[^\w.]", "", regex=True).str.strip()

    # Convert strings to floats
    df = clean_numeric_series(data=df, series_columns=col, to=float)

    return df
