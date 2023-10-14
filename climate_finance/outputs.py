import pandas as pd

from climate_finance import config
from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.get_oecd_data import get_oecd_bilateral
from oda_data import set_data_path, donor_groupings

from climate_finance.oecd.imputed_multilateral.one_multilateral.shares import (
    one_multilateral_spending,
)

set_data_path(config.ClimateDataPath.raw_data)

date = "131023"


def bilateral():
    one_version_bilateral = get_oecd_bilateral(
        start_year=2013,
        end_year=2021,
        methodology="one_bilateral",
    )

    rio_multi = [
        k for k, v in donor_groupings()["multilateral"].items() if v in CRS_MULTI
    ]
    rio_bilat = list(donor_groupings()["all_bilateral"])

    rio_parties = rio_multi + rio_bilat

    one_version_bilateral = one_version_bilateral.loc[
        lambda d: (d.flow_name != "Private Development Finance")
        & (d.oecd_party_code.isin(rio_parties))
        & (d.indicator != "Not climate relevant")
        & (d.value > 0)
    ]

    one_version_multilateral = one_multilateral_spending(
        start_year=2013, end_year=2021, rolling_window=1, as_shares=False
    )

    one_version_multilateral = one_version_multilateral.loc[
        lambda d: ~d.oecd_party_code.isin(rio_multi)
    ].dropna(subset=["oecd_party_code", "party"], how="all")

    one_version_multilateral = one_version_multilateral.rename(columns={
        CrsSchema.MITIGATION_VALUE: "Mitigation",
        CrsSchema.ADAPTATION_VALUE: "Adaptation",
        CrsSchema.CROSS_CUTTING_VALUE: "Cross-cutting",
    })

    one_version_multilateral = one_version_multilateral.melt(
        id_vars=[
            c
            for c in one_version_multilateral
            if c
            not in [
                "Adaptation",
                "Mitigation",
                "Cross-cutting",
                "climate_total",
                "yearly_total",
            ]
        ],
        value_vars=[
            "Adaptation",
            "Mitigation",
            "Cross-cutting",
            "climate_total",
            "yearly_total",
        ],
        var_name="indicator",
    )

    bi_types = {
        k: v
        for k, v in one_version_bilateral.dtypes.to_dict().items()
        if k in one_version_multilateral.columns
    }

    one_version_multilateral = one_version_multilateral.loc[
        lambda d: (d.indicator != "climate_total")
        & (d.indicator != "yearly_total")
        & (d.value > 0)
    ].astype(bi_types)

    bilateral_data = pd.concat(
        [one_version_bilateral, one_version_multilateral], ignore_index=True
    ).drop(columns=["party_type"])

    bilateral_data.reset_index(drop=True).to_feather(
        config.ClimateDataPath.output / f"one_bilateral_2013_2021_{date}.feather"
    )


def multilateral():
    one_version_multilateral = one_multilateral_spending(
        start_year=2013, end_year=2021, rolling_window=1, as_shares=False
    )


CRS_MULTI = [
    "Adaptation Fund",
    "Council of Europe Development Bank",
    "Food and Agriculture Organisation",
    "Global Environment Facility",
    "Nordic Development Fund",
    "European Bank for Reconstruction and Development",
    "EU Institutions",
]
bilateral()
bd = pd.read_feather(
    config.ClimateDataPath.output / f"one_bilateral_2013_2021_{date}.feather"
)
