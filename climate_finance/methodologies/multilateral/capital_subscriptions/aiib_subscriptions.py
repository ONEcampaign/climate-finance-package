import pandas as pd
from scripts.analysis.other.capital_subscriptions.common import (
    _read_mdb_xlsx_long_format,
    calculate_share_of_annual_total,
    standardise_column_names,
    check_totals,
    remove_irrelevant_rows,
    check_countries,
    check_iso,
)


def aiib_2016_2019() -> pd.DataFrame:
    df = _read_mdb_xlsx_long_format(mdb="AIIB", first_year=2016, last_year=2019).drop(
        labels=[
            "Total shares",
            "Callable capital",
            "Paid-in capital",
            "Paid-in capital received",
            "Paid-in capital not yet received",
        ],
        axis=1,
    )

    return df


def aiib_2020_2022() -> pd.DataFrame:
    df = _read_mdb_xlsx_long_format(mdb="AIIB", first_year=2020, last_year=2022).drop(
        labels=[
            "Total shares",
            "Callable capital",
            "Paid-in capital",
        ],
        axis=1,
    )

    return df


def scrape_aiib_tables() -> pd.DataFrame:
    df_2016_2019 = aiib_2016_2019()
    df_2020_2022 = aiib_2020_2022()

    # concat two datasets as they have the same structure
    df = pd.concat(
        [
            df_2016_2019,
            df_2020_2022,
        ],
        ignore_index=True,
    ).sort_values(by="year", ascending=False)

    # standardise column names
    df = standardise_column_names(
        df=df, provider_col="Members", value_col="Subscribed capital"
    )

    # remove irrelevant rows
    df = remove_irrelevant_rows(df=df, key_word="Total")

    # add shares column
    df = calculate_share_of_annual_total(df)

    return df


if __name__ == "__main__":
    aiib_data = scrape_aiib_tables()

    checks = check_totals(aiib_data)
    count = check_countries(aiib_data)
    count_iso = check_iso(aiib_data)
