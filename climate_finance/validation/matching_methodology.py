from pathlib import Path

import pandas as pd

from climate_finance import ClimateData, set_climate_finance_data_path, config

set_climate_finance_data_path(config.ClimateDataPath.raw_data)


def compare_crdf_crs_disbursements(
    start_year,
    end_year,
    providers: list[int],
    flow: str = "gross_disbursements",
    save_to: Path | None = None,
) -> pd.DataFrame:
    """"""
    years = range(start_year, end_year + 1)
    grouper = [
        "year",
        "oecd_provider_code",
        "provider",
        "flow_name",
        "flow_type",
        "source",
    ]

    crdf_crs = ClimateData(years=years, providers=providers)
    crs = ClimateData(years=years, providers=providers)

    crdf_crs_df = (
        crdf_crs.load_spending_data(
            methodology="OECD",
            flows=[flow],
            source="OECD_CRDF_CRS",
        )
        .get_data()
        .groupby(grouper + ["matched", "indicator"], dropna=False, observed=True)[
            ["value"]
        ]
        .sum()
        .reset_index()
        .pivot(index=grouper + ["matched"], columns="indicator", values="value")
        .reset_index()
    )

    crs_df = (
        crs.load_spending_data(
            methodology="OECD",
            flows=[flow],
            source="OECD_CRS_ALLOCABLE",
        )
        .get_data()
        .loc[lambda d: d.indicator != "not_climate_relevant"]
        .groupby(grouper + ["indicator"], dropna=False, observed=True)[["value"]]
        .sum()
        .reset_index()
        .pivot(index=grouper, columns="indicator", values="value")
        .reset_index()
    )

    data = pd.concat([crdf_crs_df, crs_df], ignore_index=True)

    return data


def compare_crdf_crdf_crs(
    start_year,
    end_year,
    providers: list[int],
    flow: str = "gross_disbursements",
    save_to: Path | None = None,
) -> pd.DataFrame:
    """"""
    years = range(start_year, end_year + 1)
    grouper = [
        "year",
        "oecd_provider_code",
        "provider",
        # "flow_name",
        "flow_type",
        "source",
    ]

    crdf_crs = ClimateData(years=years, providers=providers)
    crdf = ClimateData(years=years, providers=providers)

    crdf_crs_df = (
        crdf_crs.load_spending_data(
            methodology="OECD",
            flows=[flow],
            source="OECD_CRDF_CRS",
        )
        .get_data()
        .groupby(grouper + ["matched", "indicator"], dropna=False, observed=True)[
            ["value"]
        ]
        .sum()
        .reset_index()
        .pivot(index=grouper + ["matched"], columns="indicator", values="value")
        .reset_index()
    )

    crdf_crdf_df = (
        crdf.load_spending_data(
            methodology="OECD",
            flows=[flow],
            source="OECD_CRDF",
        )
        .get_data()
        .groupby(grouper + ["indicator"], dropna=False, observed=True)[["value"]]
        .sum()
        .reset_index()
        .pivot(index=grouper, columns="indicator", values="value")
        .reset_index()
    )

    data = pd.concat([crdf_crs_df, crdf_crdf_df], ignore_index=True)

    return data


if __name__ == "__main__":
    result = compare_crdf_crdf_crs(2021, 2021, [12], flow="commitments")
    result.to_clipboard(index=False)
