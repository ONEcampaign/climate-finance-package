[![pypi](https://img.shields.io/pypi/v/climate_finance.svg)](https://pypi.org/project/climate_finance/)
[![python](https://img.shields.io/pypi/pyversions/climate_finance.svg)](https://pypi.org/project/climate_finance/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# The climate finance package

_**climate-finance** is the python package to get, clean, and work with international public climate finance._ 

You can use this package to get, rebuild, remix, and create using our tools and methodologies — all with only a few lines of code.

Climate finance data is notoriously difficult to work with. It's messy - really messy - and comes in all sorts of shapes and sizes, scattered across multiple websites.

It took us months to understand which climate finance data to use, and even longer to clean the data ready for the Climate Finance Files. 

We don't think you should have to do this too. 

We have built **climate-finance** to lower the barriers to access that many organisations face when seeking to conduct research or advocacy on these topics. For too long, bad data has restricted climate accoutability. 
And for too long, global leaders have capitalised on bad data to dictate the narrative on climate finance. 

We hope these tools equip everyone with the data to hold global leaders accountable in the fight against climate change. As currently, they are not doing enough. 

## Getting started 
This package provides a set of tools to help you work with climate finance data.
It can be used to:

- download data from the OECD databases (The Creditor Reporting System (CRS)
  and the Climate-related Development Finance database (CRDF))
- download data from the UNFCCC data portal
- clean and harmonise data from the different sources
- convert climate finance data to different currencies and prices

### Set up

The Climate Finance package is composed of many different tools to download, process
and analyse data.

To get started, you will need to install the package. You can do this from pipy using pip:

```bash

pip install climate_finance --upgrade

```

or directly from the source code:

```bash

pip install git+https://github.com/ONEcampaign/climate-finance-package.git

```

### Basic Usage

The first step when using the Climate Finance package should be to set a working directory
where the data will be stored. This can be done importing using the
`set_climate_finance_data_path` function:

```python
from climate_finance import set_climate_finance_data_path

set_climate_finance_data_path('path/to/your/data')

```

The easiest way to interact with the data is through the `ClimateData` class.

**For a detailed overview of how to use the ClimateData class,[ please see its documentation.](./climate_finance/README.md)**

## Questions? Would like to collaborate?
We want this package to help others analyse climate finance data. If you want to collaborate, or have any questions, please reach out.

