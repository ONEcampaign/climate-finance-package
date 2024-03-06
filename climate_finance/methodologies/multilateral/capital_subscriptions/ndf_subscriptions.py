import pandas as pd

from bblocks import add_iso_codes_column
from scripts.analysis.other.capital_subscriptions.common import (
    calculate_share_of_annual_total,
    standardise_column_names,
    _read_mdb_xlsx_long_format,
    remove_irrelevant_rows,
    check_totals,
    check_countries,
    check_iso,
)

"""
This is a more challenging institution to calculate attributed shares.

- Firstly, there is no publicly available 2015 report. However, total subscriptions do not differ between 2014 and 2016,
so we can assume 2015 is equal to these two years. 

- Each provider holds capital subscriptions in both SDRs and Euros. 
- Capital subscriptions here include the amount of 'paid-in' capital. Hence, the final figure required is:
***'Subscribed Capital (SDR)' + 'Subscribed Capital (EUR)'.***
- However, SDR totals are not provided in Euro-terms, and thus that sum underweights SDR contributions (which have an 
exchange rate to Euros exceeding 1). 
- We need to input SDRs as Euros for this calculation. 
- SDRs are provided in Euros in 'Paid-in Subscribed Capital (SDR in Euros)'. From 2016, the 'paid-in' amount of SDRs are 
equal to the subscribed amount, meaning the Euros exchange rate conversion is fair and can be used as total SDR 
subscriptions in Euros. 
- The subscribed amount of capital as SDRs in 2013, 2014 and 2015 is equal to 2016, so we can use the Euro value for 2016. 
"""


def scrape_ndf_tables() -> pd.DataFrame:
    """
    This pipeline function completes all the data manipulation to produce clean NDF data. It reads the excel file into
    a DataFrame and drops all irrelevant columns. It then sums subscribed capital (EUR) and paid-in capital (SDR) in Euro
    terms to calculate total capital subscriptions. It then standardises the column names (dropping the now irrelevant
    EUR and SDR columns) to match all the other MDB data outputs, adds in iso_3 codes, removes total rows, and
    calculates the share of total capital subscriptions held by member countries by year. Finally it takes the data for
    2016 and inputs this for the years 2013-2015.

    Returns: final DataFrame with columns for provider, iso_3, value, year and share.
    """
    # read excel and standardise column names
    df = _read_mdb_xlsx_long_format(
        mdb="Nordic Development Fund", first_year=2016, last_year=2022
    ).drop(
        labels=[
            # "Subscribed Capital",
            "Subscribed Capital (SDR)",
            "Subscribed Capital (SDR) (%)",
            # "Subscribed Capital (EUR)",
            "Subscribed Capital (EUR) (%)",
            "Unnamed: 5",
            "Paid in subscribed capital",
            "Paid-in Subscribed Capital (SDR)",
            # "Paid-in Subscribed Capital (SDR in Euros)",
            "Paid-in Subscribed Capital (EUR)",
            "Total Paid-in Capital",
            # "year",
            "Total (%)",
        ],
        axis=1,
    )

    # calculate total capital subscriptions
    df["capital_subscriptions_euros"] = (
        df["Subscribed Capital (EUR)"] + df["Paid-in Subscribed Capital (SDR in Euros)"]
    )

    df = (
        df.drop(
            labels=[
                "Subscribed Capital (EUR)",
                "Paid-in Subscribed Capital (SDR in Euros)",
            ],
            axis=1,
        )
        .pipe(
            add_iso_codes_column, id_column="Subscribed Capital", target_column="iso_3"
        )
        .pipe(
            standardise_column_names,
            provider_col="Subscribed Capital",
            iso_col="iso_3",
            value_col="capital_subscriptions_euros",
            year_col="year",
        )
        .pipe(remove_irrelevant_rows, key_word="Subscribed")
        .pipe(calculate_share_of_annual_total)
    )

    # add in 2013-2015
    years = ["2013", "2014", "2015"]
    df_2016 = df.loc[lambda d: d.year == "2016"]

    for year in years:
        dff = df_2016.copy()
        dff["year"] = year
        df = pd.concat([df, dff], ignore_index=False)

    return df.sort_values(by="year")


if __name__ == "__main__":
    ndf_data = scrape_ndf_tables()

    checks = check_totals(ndf_data)
    count = check_countries(ndf_data)
