# UNFCCC data

This module contains scripts for downloading and pre-processing data from UNFCCC.

There are two key ways to get data from UNFCCC:
- Manually download the BR data files (one excel per party)
- Use an automated tool to download the data from the UNFCCC data interface.

## climate_finance.unfccc.download

The `download` module contains scripts for automatically downloading and pre-processing data from the UNFCCC data interface.

### climate_finance.unfccc.download.get_data
`get_data` deals with getting the data from the Data Interface website. It uses the `selenium` package to interact with
the website and download the data. The downloaded data is saved as Excel files.

**Settings**

Some settings are needed in order for the script to work. They must be defined inside a dictionary, and they are 
specific to each version of the data that is downloaded. The settings are:
- url: The url of the data interface website, including the parameter that specifies the version of the data to download.
- br_dropdown: The Xpath to the BR dropdown menu.
- br_select: the Xpath to each of the BR dropdown options. The key is a lambda function that takes the BR version 
as input and returns the Xpath to the corresponding dropdown option.
- search_button: The id to the search button.
- export_button: The id to the export button.
- folder_name: The name of the folder where the downloaded data will be saved.
- file_name: The name used for saving the file
- wait_time: how long to wait for the page to load before trying to interact with it.

#### climate_finance.unfccc_download.get_data.get_unfccc_export()
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
settings = get_data.BILATERAL_SETTINGS

# Define the BR versions to download
br = [5]

# Define the parties to download
parties = ["France", "Germany"]

# Download the data
get_data.get_unfccc_export(settings=settings, br=br, party=parties)
```

This will download 2 files: one for France, containing bilateral BR5 data, and one for Germany, 
containing bilateral BR5 data. They will be saved in the raw_data folder, inside the `unfccc_data_interface` folder.
