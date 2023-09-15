# UNFCCC data

This module contains scripts for downloading and pre-processing data from UNFCCC.

There are two key ways to get data from UNFCCC:

- [Manually download](#climatefinanceunfcccmanual) the BR data files (one excel per party)
    - [read_data]
    - [pre_process]
    - [get_data]
- Use an [automated tool](#climatefinanceunfcccdownload) to download the data from the UNFCCC data interface.
    - [download_data](#climatefinanceunfcccdownloaddownloaddata)
        - [get_unfccc_export()](#climatefinanceunfcccdownloaddownloaddatagetunfcccexport)
    - [pre_process](#climatefinanceunfcccdownloadpreprocess)
        - [clean_unfccc()](#climatefinanceunfcccdownloadpreprocesscleanunfccc)
    - [get_data](#climatefinanceunfcccdownloadgetdata)
        - [get_unfccc_summary()](#climatefinanceunfcccdownloadgetdatagetunfcccsummary)
        - [get_unfccc_bilateral()](#climatefinanceunfcccdownloadgetdatagetunfcccbilateral)
        - [get_unfccc_multilateral()](#climatefinanceunfcccdownloadgetdatagetunfcccmultilateral)

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


---


## climate_finance.unfccc.manual
The `manual` module contains functions to manually clean the UNFCCC data. It is assumed that you have
downloaded the BR data from the UNFCCC data and saved it as individual Excel files for each party.

The files should be stored in a folder which only contains data from the same BR. In other words,
the scripts assume one folder per BR.






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