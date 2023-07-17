# UNFCCC data

This module contains scripts for downloading and pre-processing data from UNFCCC.

There are two key ways to get data from UNFCCC:

- Manually download the BR data files (one excel per party)
- Use an automated tool to download the data from the UNFCCC data interface.

## climate_finance.unfccc.download

The `download` module contains scripts for automatically downloading and pre-processing data from the UNFCCC data
interface.

### climate_finance.unfccc.download.download_data

`get_data` deals with getting the data from the Data Interface website. It uses the `selenium` package to interact with
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

### climate_finance.unfccc.download.get_data.get_unfccc_summary

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

#### climate_finance.unfccc.download.get_data.get_unfccc_multilateral

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

#### climate_finance.unfccc.download.get_data.get_unfccc_bilateral

`get_unfccc_bilateral(start_year: int, end_year: int)` Reads and cleans the UNFCCC bilateral data.

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

### climate_finance.unfccc.cleaning_tools.tools

The tools module in `cleaning_tools` provides functions to process and clean the downloaded UNFCCC data.
It includes functions to clean specific columns, fill missing values, and rename columns.

#### climate_finance.unfccc.cleaning_tools.tools.clean_currency

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

#### climate_finance.unfccc.cleaning_tools.tools.fill_type_of_support_gaps

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

#### climate_finance.unfccc.cleaning_tools.tools.harmonise_type_of_support

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

#### climate_finance.unfccc.cleaning_tools.tools.fill_financial_instrument_gaps

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

#### climate_finance.unfccc.cleaning_tools.tools.clean_status
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

#### climate_finance.unfccc.cleaning_tools.tools.rename_columns
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