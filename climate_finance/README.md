# Using the Climate Finance package

This package provides a set of tools to help you work with climate finance data.
It can be used to:

- download data from the OECD databases (The Creditor Reporting System (CRS)
  and the Climate-related Development Finance database (CRDF))
- download data from the UNFCCC data portal
- clean and harmonise data from the different sources
- convert climate finance data to different currencies and prices

## Set up

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

## Usage

The first step when using the Climate Finance package should be to set a working directory
where the data will be stored. This can be done importing using the
`set_climate_finance_data_path` function:

```python

from climate_finance import set_climate_finance_data_path

set_climate_finance_data_path('path/to/your/data')

```

The easiest way to interact with the data is through the `ClimateData` class.

### The `ClimateData` class

To use the `ClimateData` class, you will need to import it:

```python

from climate_finance import ClimateData, set_climate_finance_data_path

# As always, start by setting the data path
set_climate_finance_data_path('path/to/your/data')

```

#### Creating an instance of `ClimateData`

The first step when using the `ClimateData` class is to create an instance of it.
You can define a number of parameters when creating the instance:

- _years_: a list of years (or a range) to get data for.
- _providers_: a list of provider codes (or a single one) for whom to get data.
  For now, only 'OECD' codes are supported. If `None` are provided, all available providers
  are included.
- _recipients_: a list of recipient codes (or a single one) for whom to get data.
  For now, only 'OECD' codes are supported. If `None` are provided, all available recipients
  are included.
- _currency_: the currency used for the data. The default is 'USD'. Available options
  include 'USD', 'EUR', 'GBP','CAD'. Others can be added by request.
- _prices_: the price type to use. The default is 'current'. The data can also be
  converted to constant prices. In that case DAC deflators are used by default, though
  we will support using IMF and WB deflators in the future.
- _base_year_: the base year to use for constant prices. It must be specified if
  prices are set to 'constant'. Otherwise, it must remain `None` (its default value).

```python

from climate_finance import ClimateData, set_climate_finance_data_path

# As always, start by setting the data path
set_climate_finance_data_path('path/to/your/data')

# Create an instance of the ClimateData class
# In this example, it will be set for the years 2018, 2019, 2020 and 2021, for
# providers 4 and 12 (France and the UK), for all recipients, in constant 2022 Euros.
climate_data = ClimateData(
        years=range(2018, 2022),                  
        providers=[4, 12],
        recipients=None, # which means getting all available
        currency='EUR',
        prices='constant',
        base_year=2022
    )
```

No data is downloaded or processed at this stage. These options just define the behaviour
of the object when a specific indicator is requested.

#### Loading data
For now the `ClimateData` supports loading 'spending' data from the OECD CRS and CRDF databases.

To load the data, you can use the `load_spending_data` method. A few parameters should
be defined:
- _methodology_: a string defining the methodology to use. The default is 'ONE' but 'OECD'
and 'custom' are also available. The 'custom' methodology allows you to define a specific
methodology to discount 'principal' and 'significant' climate activities. You can call
`.available_methodologies` for a full list of available methodologies.
- _flows_': a single string or a list of strings defining the flows to get data for.
The default is 'grant_equivalents' or 'net_disbursements'. You can call
`.available_flows` for a full list of available flows.
- _source_: a string defining the source of the data. The default is 'OECD_CRS'. Other
options include 'OECD_CRS_ALLOCABLE', 'OECD_CRDF', 'OECD_CRDF_DONOR', 'OECD_CRDF_CRS'.
You can call `.available_sources` for a full list of available sources.


The combination of methodology and source are key in determining the data that is returned.

For example, to view the data as the OECD presents it in the CRS, you would use:
```python
methodology = 'OECD'
source = 'OECD_CRS'
```

This would identify data marked as climate finance, but also include data that is
not eligible for marking (i.e data that isn't considered 'bilateral allocable'). To get
just data data that is eligible for marking as climate finance, you would use:
```python
methodology = 'OECD'
source = 'OECD_CRS_ALLOCABLE'
```

The OECD publishes a separate database for climate finance, the CRDF. There are two
'perspectives', one from the donor and one from the recipient. 

The 'recipient' perspective
should, for bilateral providers who follow the Rio Markers methodology, be the same as 
the 'OECD_CRS_ALLOCABLE' source (though it isn't always). But it also includes data
from multilateral providers who use the 'Climate Components' methodology.

To get this data, as presented by the OECD, you would use:
```python
methodology = 'OECD'
source = 'OECD_CRDF'
```

The 'donor' perspective includes only bilateral providers, and it includes imputations
for the climate finance that is provided through the multilateral system. To get this
data, as presented by the OECD, you would use:
```python
methodology = 'OECD'
source = 'OECD_CRDF_DONOR'
```

The `ClimateData` class supports ONE's methodologies out of the box. 
Generally, applying one's methodology means:
- discounting 'significant' climate activities to count only 40% of their value.
- applying the 'highest marker' rule to assign the value of the activity to "adaptation"
or "mitigation" depending on the highest marker that is used, or as "cross-cutting" if
both markers are used at the same level.

This methodology can be applied to the different sources. For example, for the CRS data
(i.e. using the Rio Markers), you would use:
```python
methodology = 'ONE'
source = 'OECD_CRS'
```

For the CRDF data, from the recipient perspective, you would use:
```python
methodology = 'ONE'
source = 'OECD_CRDF'
```

Since the CRDF only includes data as "commitments", the CRDF data can be combined with 
the CRS data to get a complete picture of the climate finance that is actually disbursed.
This is done following ONE's approach which:
- uses CRS data for any provider (bilateral or multilateral) that uses the Rio Markers
- uses CRDF data for any provider (bilateral or multilateral) that uses the Climate
Components methodology to *identify* climate activities, but uses CRS data to get the
*value* of disbursements. This process is not 100% perfect, and you can read more
about it in our methodology note.

To get disbursements data using ONE's methodology only for Rio Markers providers, you
would use:
```python
methodology = 'ONE'
source = 'OECD_CRS_ALLOCABLE' # or 'OECD_CRS' if you want to include non-allocable data
flows = 'gross_disbursements'
```

To get disbursements data using ONE's methodology for all providers, you would use:
```python
methodology = 'ONE'
source = 'OECD_CRDF_CRS'
flows = 'gross_disbursements'
```

Putting it all together, you can load the data using the `load_spending_data` method:

```python
from climate_finance import ClimateData, set_climate_finance_data_path

# As always, start by setting the data path
set_climate_finance_data_path('path/to/your/data')

# Create an instance of the ClimateData class
# In this example, it will be set for the years 2018, 2019, 2020 and 2021, for
# providers 4 and 12 (France and the UK), for all recipients, in constant 2022 Euros.
climate_data = ClimateData(
        years=range(2018, 2022),                  
        providers=[4, 12],
        recipients=None, # which means getting all available
        currency='EUR',
        prices='constant',
        base_year=2022
    )

# Load disbursements spending data using the 'ONE' methodology
climate_data.load_spending_data(
  methodology='ONE', 
  source='OECD_CRDF_CRS', 
  flows='gross_disbursements'
)

```

#### Getting the data as a DataFrame

Once the data is loaded, you can get it as a DataFrame using the `get_data` method:

```python
from climate_finance import ClimateData, set_climate_finance_data_path

# As always, start by setting the data path
set_climate_finance_data_path('path/to/your/data')

# Create an instance of the ClimateData class
# In this example, it will be set for the years 2018, 2019, 2020 and 2021, for
# providers 4 and 12 (France and the UK), for all recipients, in constant 2022 Euros.
climate_data = ClimateData(
  years=range(2018, 2022),
  providers=[4, 12],
  recipients=None, # which means getting all available
  currency='EUR',
  prices='constant',
  base_year=2022
)

# Load disbursements spending data using the 'ONE' methodology
climate_data.load_spending_data(
  methodology='ONE',
  source='OECD_CRDF_CRS',
  flows='gross_disbursements'
)

# Get the data as a DataFrame

df = climate_data.get_data()
```

In this example, the data will be returned for providers 4 and 12, for all recipients,
for the years 2018, 2019, 2020 and 2021, in constant 2022 Euros.


#### Other functionality

##### Specifying a custom methodology
The `ClimateData` class also allows the user to specify a custom methodology.

This can be done using the `set_custom_spending_methodology` method. 

This method takes a
tuple of two coefficients (as integers or floats) as the 'coefficient' parameter. They
are for (significant, principal) activities. For reference, the OECD uses (1, 1) and 
ONE uses (0.4, 1).

This method also takes a `highest_marker` parameter, which is a boolean set to `True` if
the highest marker rule should be applied, and `False` otherwise.

```python

from climate_finance import ClimateData, set_climate_finance_data_path

# As always, start by setting the data path
set_climate_finance_data_path('path/to/your/data')

# Create an instance of the ClimateData class
# In this example, it will be set for the years 2018, 2019, 2020 and 2022, for 
# all providers in current USD
climate_data = ClimateData(
  years=range(2018, 2022),
  providers=[4, 12]
)

# Set a custom methodology. 
# In this example, principal activities are counted at 30% of their value, significant
# at 80%, and the highest marker rule is applied.

climate_data.set_custom_spending_methodology(
  coefficients=(0.3, 0.8),
  highest_marker=True
)

# Load disbursements spending data using the custom methodology and
# focusing on CRS allocable data
climate_data.load_spending_data(
  methodology='custom',
  source='OECD_CRS_ALLOCABLE',
  flows='commitments'
)

# Get the data as a DataFrame
df = climate_data.get_data()

```




