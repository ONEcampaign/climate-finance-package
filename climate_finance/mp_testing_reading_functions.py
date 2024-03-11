from climate_finance import ClimateData, set_climate_finance_data_path

from climate_finance.config import ClimateDataPath

set_climate_finance_data_path(ClimateDataPath.raw_data)

climate_data = ClimateData(
    years=range(2020, 2023),
    providers=12,
    currency="GBP",
    prices="constant",
    base_year=2021
)

climate_data.set_custom_spending_methodology(
    coefficients=(0.5, 0.85),
    highest_marker=True
)

climate_data.load_spending_data(
    methodology='custom',
    source='OECD_CRS',
    flows='gross_disbursements'
)

df = climate_data.get_data()




"""
Thoughts on usage:
- It's not immediately clear when you're going down the documentation how you can check
to see more available providers. It would be good to list all of these, as you have
`.available_methodologies` and `.available_flows`. I now see those are not listed options. 
Not hugely important, but would be good to add later on (as I think very few people know
the donor codes off by heart....) 

is OECD_CRDF OECD_CRDF_RECIPIENT? Seeing as there is a OECD_CRDF_DONOR, it may make sense 
to also include RECIPIENT in that source name for consistency? 

I'm not sure it works with deflators currently. Error suggests its because its currently
not downloading the DAC1 feather file. 

Love the explanation of the different combinations of methodology and source!!!
Really like the INFO [read_clean_crs] etc prints in the console. Makes it really clear 
what has happened so far!

Is "once the data is loaded" common coding language? May be better to say "downloaded". 

"""

#%%
