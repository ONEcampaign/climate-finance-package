# UNFCCC data

This module contains scripts for downloading and pre-processing data from UNFCCC.

There are two key ways to get data from UNFCCC:

- Use an [automated tool](#climatefinanceunfcccdownload) to download the data from the UNFCCC data interface.
    - [download_data](#climatefinanceunfcccdownloaddownloaddata)
        - [get_unfccc_export()](#climatefinanceunfcccdownloaddownloaddatagetunfcccexport)
    - [pre_process](#climatefinanceunfcccdownloadpreprocess)
        - [clean_unfccc()](#climatefinanceunfcccdownloadpreprocesscleanunfccc)
    - [get_data](#climatefinanceunfcccdownloadgetdata)
        - [get_unfccc_summary()](#climatefinanceunfcccdownloadgetdatagetunfcccsummary)
        - [get_unfccc_bilateral()](#climatefinanceunfcccdownloadgetdatagetunfcccbilateral)
        - [get_unfccc_multilateral()](#climatefinanceunfcccdownloadgetdatagetunfcccmultilateral)
- [Manually download](#climatefinanceunfcccmanual) the BR data files (one excel per party)
    - [read_data]
    - [pre_process]
    - [get_files]



## climate_finance.unfccc.download

The `download` module contains scripts for automatically downloading and pre-processing data from the UNFCCC data
interface.

### climate_finance.unfccc.download.download_data

`download_data` deals with getting the data from the Data Interface website. It uses the `selenium` package to interact
with
the website and download the data. The downloaded data is saved as Excel files.

**Settings**

Some settings are needed in order for the script to work. They must be defined inside a dictionary, and they are
specific to each version of the data that is downloaded. The settings are:

- url: The url of the data interface website, including the parameter that specifies the version of the data to
  download.
- br_dropdown: The Xpath to the BR dropdown menu.
- br_select: the Xpath to each of the BR dropdown options. The key is a lambda function that takes the BR version
  as input and returns the Xpath to the corresponding dropdown option.
- search_button: The id to the search button.
- export_button: The id to the export button.
- folder_name: The name of the folder where the downloaded data will be saved.
- file_name: The name used for saving the file
- wait_time: how long to wait for the page to load before trying to interact with it.

#### climate_finance.unfccc_download.download_data.get_unfccc_export()

`get_unfccc_export` is the main function for downloading data from the UNFCCC data interface.
It takes:

- a `settings` dictionary as input and downloads the data according to the specified settings.
  Three settings are predefined: `SUMMARY_SETTINGS`, `MULTILATERAL_SETTINGS`, and `BILATERAL_SETTINGS`.
- a `br` argument that specifies which BR versions to download. If `br` is `None`,
  it downloads the default BR versions (4 and 5).
- a `party` argument that specifies which donors to download. If `party` is `None`,
  it downloads all parties. It also accepts a single party or a list of parties.

Here's an example of how to use the `get_unfccc_export()` function to download data:

```python
# Import get_data
from climate_finance.unfccc.download import download_data

# Define the settings. We'll use bilateral data as an example
settings = download_data.BILATERAL_SETTINGS

# Define the BR versions to download
br = [5]

# Define the parties to download
parties = ["France", "Germany"]

# Download the data
download_data.get_unfccc_export(settings=settings, br=br, party=parties)
```

This will download 2 files: one for France, containing bilateral BR5 data, and one for Germany,
containing bilateral BR5 data. They will be saved in the raw_data folder, inside the `unfccc_data_interface` folder.

### climate_finance.unfccc.download.pre_process

`pre_process` deals with basic pre-processing of the downloaded data. The data from the UNFCCC interface is cleaner
than the equivalent data directly from the BR files, which has a lot of formatting and other issues. However, it still
needs some pre-processing before it can be used.

#### climate_finance.unfccc.download.pre_process.clean_unfccc()

`clean_unfccc(df: pd.DataFrame)` is the main function for pre-processing the data. It takes a dataframe as input and
returns a cleaned dataframe. It does the following:

- Renames the columns to more usable and
  readable names ([rename_columns](#climatefinanceunfccccleaningtoolstoolsrenamecolumns))
- Clean the currency data (extracting the currency code from the string)
  ([clean_currency](#climatefinanceunfccccleaningtoolstoolscleancurrency))
- Ensure that the year and value columns are numeric
- Dropping rows with empty values
- Filling gaps in the _type of support_ column (filled with 'Cross-cutting')
  ([fill_type_of_support_gaps](#climatefinanceunfccccleaningtoolstoolsfilltypeofsupportgaps)
- Harmonising the _type of support_ column (e.g. strings containing 'adaptation' are replaced with 'Adaptation')
  ([harmonise_type_of_support](#climatefinanceunfccccleaningtoolstoolsharmonisetypeofsupport))
- Cleaning the _status_ column (if applicable) (e.g harmonising to 'disbursed', 'committed', 'unknown', etc.)
  ([clean_status](#climatefinanceunfccccleaningtoolstoolscleanstatus))

Here is an example of how to use the `clean_unfccc()` function:

```python
# Import clean_unfccc
from climate_finance.unfccc.download.pre_process import clean_unfccc

# Assuming that the data is already loaded into a dataframe called df

# Clean the data
df = clean_unfccc(df)
```

#### climate_finance.unfccc.download.pre_process.map_channel_names_to_oecd_codes()

`map_channel_names_to_oecd_codes(df: pd.DataFrame)` maps the channel names to the OECD CRS channel codes. It takes a
dataframe as input and returns a dataframe with an additional column containing the channel codes. It also includes
a column with clean channel names.

This process follows a 3-part strategy to match the 'dirty' channel names as found in the UNFCCC data to the OECD
CRS channel codes. It is described in more detail in the `generate_channel_mapping_dictionary` TODO: add link.

Here is an example of how to use the `map_channel_names_to_oecd_codes()` function:

```python
# Import map_channel_names_to_oecd_codes
from climate_finance.unfccc.download.pre_process import map_channel_names_to_oecd_codes

# Assuming that the data is already loaded into a dataframe called df

# Map the channel names to the OECD CRS channel codes
df = map_channel_names_to_oecd_codes(df)
```

### climate_finance.unfccc.download.get_data

The get_data module contains functions for reading and cleaning the data downloaded from the UNFCCC data interface.
It uses the [clean_unfccc](#climatefinanceunfcccdownloadpreprocesscleanunfccc) and
[map_channel_names_to_oecd_codes](#climatefinanceunfcccdownloadpreprocessmapchannelnamestooecdcodes) functions from
the [pre_process](#climatefinanceunfcccdownloadpreprocess) module for cleaning and pre-processing.

### climate_finance.unfccc.download.get_data.get_unfccc_summary()

`get_unfccc_summary(start_year: int, end_year: int)` reads and cleans the UNFCCC summary data.

The function takes two arguments:

- `start_year`: The start year of the data.
- `end_year`: The end year of the data.

It returns a pandas DataFrame containing the cleaned UNFCCC summary data for the specified years.

Here is an example of how to use the get_unfccc_summary() function:

```python
# Import get_unfccc_summary
from climate_finance.unfccc.download.get_data import get_unfccc_summary

# Define the start and end years
start_year = 2015
end_year = 2020

# Get the UNFCCC summary data
df = get_unfccc_summary(start_year, end_year)
```

#### climate_finance.unfccc.download.get_data.get_unfccc_multilateral()

`get_unfccc_multilateral(start_year: int, end_year: int)` Reads, cleans and processes the UNFCCC multilateral data.

The function takes two arguments:

- `start_year`: The start year of the data.
- `end_year`: The end year of the data.

It returns a pandas DataFrame containing the cleaned and processed UNFCCC multilateral data for the specified years.

Here is an example of how to use the get_unfccc_multilateral() function:

```python
# Import get_unfccc_multilateral
from climate_finance.unfccc.download.get_data import get_unfccc_multilateral

# Define the start and end years
start_year = 2015
end_year = 2020

# Get the UNFCCC multilateral data
df = get_unfccc_multilateral(start_year, end_year)
```

#### climate_finance.unfccc.download.get_data.get_unfccc_bilateral()

`get_unfccc_bilateral(start_year: int, end_year: int)` reads and cleans the UNFCCC bilateral data.

The function takes two arguments:

- `start_year`: The start year of the data.
- `end_year`: The end year of the data.

It returns a pandas DataFrame containing the cleaned UNFCCC bilateral data for the specified years.

Here is an example of how to use the get_unfccc_bilateral() function:

```python
# Import get_unfccc_bilateral
from climate_finance.unfccc.download.get_data import get_unfccc_bilateral

# Define the start and end years
- start_year = 2015
- end_year = 2020

# Get the UNFCCC bilateral data
df = get_unfccc_bilateral(start_year, end_year)
```


---


## climate_finance.unfccc.manual
The `manual` module contains functions to manually clean the UNFCCC data. It is assumed that you have
downloaded the BR data (CTF Tables) from the UNFCCC website and saved as individual Excel files for each party in a specified folder path.

Users should save all BR files as the specific party name in the `..raw_data` folder (`../climate_finance/.raw_data`) before using the manual module. Files should be saved as the respective party names, outlined here (TO DO: add link to the file that specifies how to name parties/file names).

### climate_finance.unfccc.manual.read_files

The `read_files` sub-module deals with reading in the raw data from the UNFCCC biennial reports saved to the `folder_path`. 

#### climate_finance.unfccc.manual.read_files.load_br_files_tables7()

`load_br_files_tables7(folder_path: str | pathlib.Path)` loads all Table 7s (7, 7a and 7b) from the biennial reports saved in the folder path into a dictionary of DataFrames. 

The arguement `folder_path` can either be inputted as a string (str) or a pathlib.Path object.

The function passes the `folder_path` and `table_pattern="Table 7"` to the helper function `_load_br_files()`. 
The helper function:
- Creates a dictionary of all the parties available in the `folder_path` (using the csv file names. See (ADD SAME LINK AS ABOVE) for standardised party names).
- Loops through all of the sheets within these BRs, and reads all of the "Table 7" tabs into the dictionary as DataFrames. 

The result is a dictionary with all the parties saved as `str` and their corresponding table 7s saved as pandas DataFrames. 

Here is an example of how to use `load_br_files_tables7()`

```python
# import load_br_files_tables7()
from climate_finance.unfccc.manual.read_data import load_br_files_tables7

# create an empty dictionary
br_files = {}

# populate dictionary with load_br_files_tables7
br_files = load_br_files_tables7(folder_path)
```

### climate_finance.unfccc.manual.get_data

The `get_data` sub-module contains the pipeline functions that read, process and output clean Table 7 data from the Biennial Reports. There are three pipeline functions, one for each table 7: `table7_pipeline`, `table7a_pipeline` and `table7b_pipeline`.

#### climate_finance.unfccc.manual.get_data._table7_pipeline()

`table7_pipeline(folder_path: str | pathlib.Path)` creates a single DataFrame of table 7 data.

The arguement `folder_path` can either be inputted as a string (str) or a pathlib.Path object.

The function passes the relevant arguements to the helper function `_load_br_files()` to 'get' Table 7 data.

`_load_br_files(folder_path: str | pathlib.Path, table_name: str, clean_func: callable)` has the following three arguements:
- `folder_path`: the location of the pre-downloaded Biennial Reports. The path can be inputted as either a string (str) or a pathlib.Path object.
- `table_name`: the name of the required table. In this case: `Table 7`.
- `clean_func`: the cleaning pipeline function in the `climate_finance.unfccc.manual.pre_process` sub-module. In this case: [clean_table7](#climatefinanceunfcccmanualpreprocesscleantable7).

It returns a single DataFrame of table7 data. It does this by:
- Loading the BR files using [load_br_files_tables7](#climatefinanceunfcccmanualreadfilesloadbrfilestables7)
- Cleaning the data using the specified `clean_func` (cleaning each Table 7 by party and year). In this case: [clean_table7](#climatefinanceunfcccmanualpreprocesscleantable7)
- Merging the cleaned DataFrames together. 

For example, if Table 7 data is required: 

```python
# import table7_pipeline
from climate_finance.unfccc.manual.get_data import table7_pipeline

# run table7_pipeline
df =  table7_pipeline(folder_path: str | pathlib.Path)
```

#### climate_finance.unfccc.manual.get_data._table7a_pipeline()

`table7a_pipeline(folder_path: str | pathlib.Path)` creates a single DataFrame of table 7a data.

The arguement `folder_path` can either be inputted as a string (str) or a pathlib.Path object.

The function passes the relevant arguements to the helper function `_load_br_files()` to 'get' Table 7a data.

`_load_br_files(folder_path: str | pathlib.Path, table_name: str, clean_func: callable)` has the following three arguements:
- `folder_path`: the location of the pre-downloaded Biennial Reports. The path can be inputted as either a string (str) or a pathlib.Path object.
- `table_name`: the name of the required table. In this case: `Table 7a`.
- `clean_func`: the cleaning pipeline function in the `climate_finance.unfccc.manual.pre_process` sub-module. In this case: [clean_table7a](#climatefinanceunfcccmanualpreprocesscleantable7a).

It returns a single DataFrame of table7a data. It does this by:
- Loading the BR files using [load_br_files_tables7a](#climatefinanceunfcccmanualreadfilesloadbrfilestables7a)
- Cleaning the data using the specified `clean_func` (cleaning each Table 7a by party and year). In this case: [clean_table7a](#climatefinanceunfcccmanualpreprocesscleantable7a)
- Merging the cleaned DataFrames together. 

For example, if Table 7a data is required: 

```python
# import table7a_pipeline
from climate_finance.unfccc.manual.get_data import table7a_pipeline

# run table7a_pipeline
df =  table7a_pipeline(folder_path: str | pathlib.Path)
```

#### climate_finance.unfccc.manual.get_data._table7b_pipeline()

`table7b_pipeline(folder_path: str | pathlib.Path)` creates a single DataFrame of table 7b data.

The arguement `folder_path` can either be inputted as a string (str) or a pathlib.Path object.

The function passes the relevant arguements to the helper function `_load_br_files()` to 'get' Table 7b data.

`_load_br_files(folder_path: str | pathlib.Path, table_name: str, clean_func: callable)` has the following three arguements:
- `folder_path`: the location of the pre-downloaded Biennial Reports. The path can be inputted as either a string (str) or a pathlib.Path object.
- `table_name`: the name of the required table. In this case: `Table 7b`.
- `clean_func`: the cleaning pipeline function in the `climate_finance.unfccc.manual.pre_process` sub-module. In this case: [clean_table7b](#climatefinanceunfcccmanualpreprocesscleantable7b).

It returns a single DataFrame of table7b data. It does this by:
- Loading the BR files using [load_br_files_tables7b](#climatefinanceunfcccmanualreadfilesloadbrfilestables7b)
- Cleaning the data using the specified `clean_func` (cleaning each Table 7b by party and year). In this case: [clean_table7b](#climatefinanceunfcccmanualpreprocesscleantable7b)
- Merging the cleaned DataFrames together. 

For example, if Table 7b data is required: 

```python
# import table7b_pipeline
from climate_finance.unfccc.manual.get_data import table7b_pipeline

# run table7b_pipeline
df =  table7b_pipeline(folder_path: str | pathlib.Path)
```

### climate_finance.unfccc.manual.pre_process

The `pre-process` sub-module deals with the basic pre-processing of the downloaded data. The data available in the BR files requires significant cleaning before it can be used. There are three pipeline functions to do this: `clean_table7`, `clean_table7a`, and `clean_table7b`. 

#### climate_finance.unfccc.manual.pre_process.clean_table7()

`clean_table7(df: pd.DataFrame, country: str, year: int)` processes and cleans Table 7 data. 

The function takes two arguements:
- `country`: The country associated with the data.
- `year`: The year associated with the data.

It returns a cleaned DataFrame with clean table 7 data for the specified years and countries. 

It does the following:
- Identifies the first (domestic) and second (USD) currencies.
- Cleans column names [clean_table_7_columns](#climatefinanceunfcccmanualpreprocesscleantable7columns).
- Cleans channel names [clean_column_string](#climatefinanceunfcccmanualpreprocesscleancolumnstring).
- Reshapes table7 data into long format [reshape_table_7](#climatefinanceunfcccmanualpreprocessreshapetable7), with columns for `channel`, `currency`, `indicator`, and `value`. 
- Converts `value` into a float [clean_numeric_series](TODO: Add link to clean script from bblocks).
- Drops all rows with no value. 
- Adds columns for the specified `country` (party) and `year`.

Here is an example of how to use `clean_table7()`

```python
# import clean_table7
from climate_finance.unfccc.manual.pre_process import clean_table7

# Assuming that the data is already loaded into a dataframe called df
df =  clean_table7(df, “France”, 2020)
```

#### climate_finance.unfccc.manual.pre_process.clean_table7a()

`clean_table7a(df: pd.DataFrame, country: str, year: int)` processes and cleans Table 7a data.

The function takes two arguements:
- `country`: The country associated with the data.
- `year`: The year associated with the data.

It returns a cleaned DataFrame with clean table 7a data for the specified years and countries. 

The steps followed are very similar to `clean_table7()`, as follows: 
- Identifies the first (domestic) and second (USD) currencies.
- Cleans the column names using [rename_table_7a_columns](#climatefinanceunfcccmanualpreprocessrenametable7acolumns), using the `first_currency` and `second_currency` identified in the earlier step as arguments. 
- Reshapes table7a data into long format using [reshape_table_7a](#climatefinanceunfcccmanualpreprocessreshapetable7a). This specificly excludes `recipient` and `additional_information` columns, leaving `status`, `funding_source`, `financial_instrument`, `type_of_support`, `channel`, `sector`, `currency`, `indicator`, and `value`. 
- Converts `value` to a float (clean_numeric_series) (TODO: Add link to clean script)
- Drops all rows with no value.
- Maps multilaterals to correct category (e.g. "Multilateral climate change funds") using `table7a_heading_mapping`
- Adds columns for the specified `country` (party) and `year`.

Here is an example of how to use `clean_table7a()`

```python
# import clean_table7a
from climate_finance.unfccc.manual.pre_process import clean_table7a

# Assuming that the data is already loaded into a dataframe called df
df =  clean_table7a(df, “France”, 2020)
```

#### climate_finance.unfccc.manual.pre_process.clean_table7b()

`clean_table7b(df: pd.DataFrame, country: str, year: int)` processes and cleans Table 7b data. 

The function takes two arguements:
- `country`: The country associated with the data.
- `year`: The year associated with the data.

It returns a cleaned DataFrame with clean table 7b data for the specified years and countries. 

The steps followed are very similar to `clean_table7()`, as follows: 
- Identifies the first (domestic) and second (USD) currencies.
- Cleans the column names using [rename_table_7b_columns](#climatefinanceunfcccmanualpreprocessrenametable7b) using the `first_currency` and `second_currency` identified in the earlier step as arguments.
- Cleans the recipient names (clean_recipient_names) (TODO: Add link to tools script)
- Reshapes table7b data into long format using [reshape_table_7b](#climatefinanceunfcccmanualpreprocessreshapetable7a). This specifically excludes the `channel` column, leaving `status`, `funding_source`, `financial_instrument`, `type_of_support`, `channel`, `sector`, `currency`, `recipient`, `additional_information`, `indicator`, and `value`. 
- Converts `value` to a float (clean_numeric_series) (TODO: Add link to clean script)
- Drops all rows with no value.
- Adds columns for the specified `country` (party) and `year`.

Here is an example of how to use `clean_table7b()`

```python
# import clean_table7b
from climate_finance.unfccc.manual.pre_process import clean_table7b

# Assuming that the data is already loaded into a dataframe called df
df =  clean_table7b(df, “France”, 2020)
```

Below lists the functions implemented within the pipeline functions:

#### climate_finance.unfccc.manual.pre_process.clean_column_string()

`clean_column_string(string: str):` makes a series of replacements to clean up the strings of column names. It also removes any digits from column names using regex.

#### climate_finance.unfccc.manual.pre_process.clean_table_7_columns()

`clean_table_7_columns(df: pd.DataFrame, first_currency: str, second_currency: str)` cleans the column names of Table 7. 

The function takes two arguements:
- `first_currency`: The first currency. This is extracted from the data in a previous step.
- `second_currency`: The second currency. This is extracted from the data in a previous step.

It returns a DataFrame with cleaned column names for Table 7. 

Column names are made for both the `first_currency` and `second_currency` for all indicators in the format `currency_indicator`. As such, the final DataFrame consists of column names for `channel`, `first_currency_core_contributions`, ...,`first_currency_mitigation`,..., `usd_core_contributions`, ... ,`usd_mitigation`. The indicator channel names are cleaned through `clean_column_string`. 

#### climate_finance.unfccc.manual.pre_process.reshape_table7()

`reshape_table_7(df: pd.DataFrame)` reshapes table 7 DataFrames into a long format. 

It melts the table channel, pivotting on the 'currency_indicator' columns from `clean_table_7_columns`, before splitting into two columns: `currency` and `indicator`. 
The result is a DataFrame with columns for `channel`, `currency`, `indicator` and `value`.

#### climate_finance.unfccc.manual.pre_process.rename_table_7a_columns()

`rename_table_7a_columns(df: pd.DataFrame, first_currency: str, second_currency: str)` cleans the column names for table 7a. 

The function takes two arguements:
- `first_currency`: The first currency. This is extracted from the data in a previous step.
- `second_currency`: The second currency. This is extracted from the data in a previous step.

It returns a DataFrame with cleaned column names for Table 7a. 

#### climate_finance.unfccc.manual.pre_process.table7a_heading_mapping()

`table7a_heading_mapping(df: pd.DataFrame)` maps rows in a DataFrame to the correct category. It takes a DataFrame containing a channel column and returns a DataFrame with mapped channel types. 

It firstly cleans the channel names, before mapping them using the unfccc_channel_mapping.json file (TODO: add link to file). 

#### climate_finance.unfccc.manual.pre_process.rename_table_7b_columns()

`rename_table_7b_columns(df: pd.DataFrame, first_currency: str, second_currency: str)` cleans the column names for Table 7b. 

The function takes two arguements:
- `first_currency`: The first currency. This is extracted from the data in a previous step.
- `second_currency`: The second currency. This is extracted from the data in a previous step.

It returns a DataFrame of Table 7b with clean column names. 

#### climate_finance.unfccc.manual.pre_process.reshape_table_7x()

#### climate_finance.unfccc.manual.pre_process.reshape_table_7a()

MODIFY FOR RESHAPE_TABLE_7A 
`reshape_table_7x(df: pd.DataFrame, excluded_cols: list[str])` reshapes the dataframes into a long format. 
The arguement `excluded_cols` is a list of columns to exclude from id_vars in the melt operation.

`reshape_table_7x` is used as a partial function for variables `reshape_table_7a` and `reshape_table_7b`, which exclude different id_vars from the melt operation (and therefore have different arguements for `excluded_cols`). 



Here is an example of these partial functions:

```python
# import `reshape_table_7x
from climate_finance.unfccc.manual.pre_process import reshape_table_7x

# Assuming that the data is already loaded into a dataframe called df
reshape_table_7a = partial(reshape_table_7x, excluded_cols=["recipient", "additional_information"])
```

#### climate_finance.unfccc.manual.pre_process.reshape_table_7b()




---



## climate_finance.unfccc.cleaning_tools
The cleaning_tools module contains functions to clean the UNFCCC data.

TODO: TOC and additional info

### climate_finance.unfccc.cleaning_tools.tools

The tools module in `cleaning_tools` provides functions to process and clean the downloaded UNFCCC data.
It includes functions to clean specific columns, fill missing values, and rename columns.

#### climate_finance.unfccc.cleaning_tools.tools.clean_currency()

`clean_currency` is a function that cleans the 'currency' column of the dataframe.
It uses regular expressions to extract the currency code from the string.

Example usage:

```python
# Import clean_currency
from climate_finance.unfccc.cleaning_tools.tools import clean_currency

# Assuming that the data is already loaded into a dataframe called df
# Clean the 'currency' column
df = clean_currency(df, currency_column='currency')
```

#### climate_finance.unfccc.cleaning_tools.tools.fill_type_of_support_gaps()

`fill_type_of_support_gaps(df:pd.DataFrame, support_type_column:str)` is a function that fills missing values
in the 'type_of_support' column. By default, it fills the missing values with 'Cross-cutting'.

This behaviour can be changed by setting the `CROSS_CUTTING` variable in
the `climate_finance.unfccc.cleaning_tools.tools`
module to a different value, before calling on the `fill_type_of_support_gaps` function.

```python
global CROSS_CUTTING
CROSS_CUTTING = 'Some other value'
```

The function can be used like this:

```python
from climate_finance.unfccc.cleaning_tools.tools import fill_type_of_support_gaps

# Assuming that the data is already loaded into a dataframe called df
# Fill gaps in 'type_of_support' column
df = fill_type_of_support_gaps(df, support_type_column='type_of_support')
```

#### climate_finance.unfccc.cleaning_tools.tools.harmonise_type_of_support()

`harmonise_type_of_support(df: pd.DataFrame)` is a function that harmonises the values in the 'type_of_support'
column. It ensures that the support types are one of 'Cross-cutting', 'Adaptation', 'Mitigation', or 'Other'.

'Other' is used when the support type strings don't contain the words 'cross-cutting', 'adaptation', or 'mitigation'.

Example usage:

```python
# Import harmonise_type_of_support
from climate_finance.unfccc.cleaning_tools.tools import harmonise_type_of_support

# Assuming that the data is already loaded into a dataframe called df
# Harmonise the 'type_of_support' column
df = harmonise_type_of_support(df, type_of_support_column='type_of_support')
```

#### climate_finance.unfccc.cleaning_tools.tools.fill_financial_instrument_gaps()

`fill_financial_instrument_gaps(df: pd.DataFrame, financial_instrument_column: str = "financial_instrument") `is a
function that fills missing values in the 'financial_instrument' column.
By default, it fills the missing values with 'other'. This behaviour can be changed using the
"default_value" argument.

Example usage:

```python
# Import fill_financial_instrument_gaps
from climate_finance.unfccc.cleaning_tools.tools import fill_financial_instrument_gaps

# Assuming that the data is already loaded into a dataframe called df
# Fill gaps in 'financial_instrument' column

df = fill_financial_instrument_gaps(
    df=df,
    financial_instrument_column='financial_instrument',
    default_value="other"
)
```

#### climate_finance.unfccc.cleaning_tools.tools.clean_status()

`clean_status(df: pd.DataFrame, status_column: str = "status")` is a function that cleans the 'status' column
of the dataframe. It maps the status values to one of 'disbursed', 'committed', or 'unknown'
based on a predefined mapping.

```python
STATUS_MAPPING: dict = {
    "provided": "disbursed",
    "disbursed": "disbursed",
    "pledged": "committed",
    "committed": "committed",
}
```

Example usage:

```python
# Import clean_status
from climate_finance.unfccc.cleaning_tools.tools import clean_status

# Assuming that the data is already loaded into a dataframe called df
# Clean the 'status' column

df = clean_status(df=df, status_column='status')
```

#### climate_finance.unfccc.cleaning_tools.tools.rename_columns()

`rename_columns(df: pd.DataFrame)` is a function that renames the dataframe columns based on a predefined mapping.

You can view the mapping inside the `tools.py` file.

Example usage:

```python
# Import rename_columns
from climate_finance.unfccc.cleaning_tools.tools import rename_columns

# Assuming that the data is already loaded into a dataframe called df
# Rename the columns
df = rename_columns(df=df)
```

### climate_finance.unfccc.cleaning_tools.channels

The `channels` module in `cleaning_tools` provides functions to handle and clean channel names from the
downloaded UNFCCC data. These functions are used to match channel names to OECD CRS channel codes in the data
cleaning and pre-processing phase.

Cleaning the UNFCCC channel names isn't that straightforward because the channel names are not standardised.
The functions in this module try to automate the process as much as possible by following a 3-part strategy.

1. The first part of the strategy is to use the official CRS mapping to match channel names to channel codes.
   In order to do that, we clean the UNFCCC channel names and the official CRS channel names, removing punctuation,
   lowercasing, etc. Then we try to match the cleaned UNFCCC channel names to the cleaned CRS channel names directly.
2. The second part of the strategy is to do a fuzzy match between the clean UNFCCC channel names and the clean CRS
   channel names. This allows for some flexibility for minor differences in spelling or for smaller words to be missing.
3. The third part of the strategy is to use regular expressions to match the UNFCCC channel names to the CRS channel
   codes. This is the most flexible part of the strategy. Generating the regular expressions is potentially a very
   time-consuming process, so we generate most of them automatically.

_Generating the regular expressions_:
We create the regular expressions by taking the individual words in a channel name (other
than very common words) and generating regular expressions that look for all (or a majority) the words in the UNFCCC
channel
name, in any order.

We also have a manual list of additional regular expressions for channel codes which have a UNFCCC
name that does not match its CRS name.

You can read more about each part of the process in the documentation for each function, below.

#### climate_finance.unfccc.cleaning_tools.channels.get_crs_official_mapping()

`get_crs_official_mapping()` fetches the official CRS mapping file which maps different channel names to their
corresponding CRS channel codes. The file also contains english and french acronyms for the channel names.

The function returns a DataFrame.

Usage:

```python
# Import get_crs_official_mapping
from climate_finance.oecd.cleaning_tools.tools import get_crs_official_mapping

# Get the CRS official mapping file
mapping_df = get_crs_official_mapping()
```

#### climate_finance.unfccc.cleaning_tools.channels.clean_string()

`clean_string(text: str)` is a function that takes a string as input and performs the following cleaning operations:

- Converts the text to lower case
- Replaces punctuation with spaces
- Removes any leading or trailing spaces
- Replaces multiple spaces with a single space

It returns the cleaned string.

Usage:

```python
# Import clean_string
from climate_finance.unfccc.cleaning_tools.channels import clean_string

# Example string to clean
text = "Example TEXT for Cleaning!"

# Clean the string
cleaned_text = clean_string(text)
```

Running the code above would return the following string:

```python
"example text for cleaning"
```

#### climate_finance.unfccc.cleaning_tools.channels.raw_data_to_unique_channels()

`raw_data_to_unique_channels(raw_data: pd.DataFrame, channel_names_column: str)` is a function that takes
a dataframe and a column name as inputs. It returns a dataframe with unique channel names from the specified column
in the input dataframe.

Usage:

```python
# Import raw_data_to_unique_channels
from climate_finance.unfccc.cleaning_tools.channels import raw_data_to_unique_channels

# Assuming that the data is already loaded into a dataframe called df and 'channel' is the column containing channel names
# Get a dataframe with unique channel names
unique_channels_df = raw_data_to_unique_channels(df, channel_names_column='channel')
```

The function will clean the channel names and drop any duplicates. It will only return the channel
names column (as a dataframe).

#### climate_finance.unfccc.cleaning_tools.channels.channel_to_code()

/
`channel_to_code(map_to: str)` is a function that generates a dictionary mapping channel names or
acronyms to their corresponding channel codes. The argument `map_to` specifies whether to map to
'channel_name', 'en_acronym', or 'fr_acronym'.

Usage:

```python
# Import channel_to_code
from climate_finance.unfccc.cleaning_tools.channels import channel_to_code

# Generate a dictionary mapping channel names to channel codes
channel_name_to_code_dict = channel_to_code('channel_name')

# Generate a dictionary mapping english acronyms to channel codes
en_acronym_to_code_dict = channel_to_code('en_acronym')

# Generate a dictionary mapping french acronyms to channel codes
fr_acronym_to_code_dict = channel_to_code('fr_acronym')
```

#### climate_finance.unfccc.cleaning_tools.channels.match_names_direct_and_fuzzy()

`match_names_direct_and_fuzzy(channels: pd.DataFrame)` is a function that matches channel names to
channel codes using a direct match and a fuzzy match. It takes as input a dataframe of unique channel
names and returns a dataframe with channel codes added.

This function uses a 2-part strategy:

- First it will try to match clean versions of the raw channel names to their official CRS names
- If a direct match is not found, it will try a fuzzy match, first on the full names, and then on the acronyms

The resulting dataframe will contain two additional columns:

- `channel_code`: The CRS channel code
- `mapped_name`: The official CRS channel name

Usage:

```python
# Import match_names_direct_and_fuzzy
from climate_finance.unfccc.cleaning_tools.channels import match_names_direct_and_fuzzy

# Assuming that the data is already loaded into a dataframe called df
# Match channel names to channel codes
df_with_codes = match_names_direct_and_fuzzy(df)
```

#### climate_finance.unfccc.cleaning_tools.channels.regex_to_code_dictionary()

`regex_to_code_dictionary(channels: pd.DataFrame, names_column: str)` is a function that generates a dictionary
mapping channel names to channel codes using regular expressions. It takes as input a dataframe containing
the (official) channel names and codes and the column name containing channel names.

This function brings together the logic from other functions in this module in order to generate a 'master'
dictionary of regular expressions matching channel names to channel codes.

It has different strategies for matching channel names to channel codes:

- It contains regular expressions that look for all (non-common) words in the channel name, in any order
- It contains regular expressions that look for all (non-common) words in the channel name, in any order,
  but only check that at least half of the words in the original channel name are present
- It contains regular expressions that look for the presence of the official english acronym
- It contains regular expressions that are manually added/maintained under `ADDITIONAL_PATTERNS`

The regular expressions are ordered from the longest to the shortest length. This is to ensure that the
the longest (and therefore most specific) match is found first.

Usage:

```python
# Import regex_to_code_dictionary
from climate_finance.unfccc.cleaning_tools.channels import regex_to_code_dictionary

# Generate a dictionary mapping channel names to channel codes using regular expressions
regex_dict = regex_to_code_dictionary(channels=df, names_column='channel')
```

#### climate_finance.unfccc.cleaning_tools.channels.generate_channel_mapping_dictionary()

`generate_channel_mapping_dictionary(raw_data: pd.DataFrame, channel_names_column: str,
export_missing_path: str | None)` is a function that generates a dictionary of channel names to channel codes
using the raw data.

If the `export_missing_path` is provided, it will export a csv of the missing channels to the specified path.

This function basically executes the full logic to generate the right mapping between the 'raw' channel names
and the official channel codes.

You can read more about the logic here. TODO: Add link to documentation

Usage:

```python
# Import generate_channel_mapping_dictionary
from climate_finance.unfccc.cleaning_tools.channels import generate_channel_mapping_dictionary

# Generate a dictionary mapping channel names to channel codes
channel_mapping_dict = generate_channel_mapping_dictionary(
    raw_data=df, channel_names_column='channel',
    export_missing_path='missing_channels.csv'
)
```

---
