import pandas as pd
from oda_data import read_crs

from climate_finance.oecd.cleaning_tools.schema import CrsSchema
from climate_finance.oecd.cleaning_tools.tools import rename_crs_columns, idx_to_str
from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)


def add_provider_agency_names(data: pd.DataFrame, crs_year: int = 2021) -> pd.DataFrame:
    idx = [
        CrsSchema.PARTY_CODE,
        CrsSchema.AGENCY_CODE,
        CrsSchema.PARTY_NAME,
        CrsSchema.AGENCY_NAME,
    ]
    crs = (
        read_crs([crs_year])
        .pipe(rename_crs_columns)
        .drop_duplicates(subset=idx)
        .filter(items=idx)
        .pipe(idx_to_str, idx=idx)
    )

    crdf = (
        get_recipient_perspective(start_year=crs_year, end_year=crs_year)
        .filter(items=idx)
        .drop_duplicates(subset=idx)
        .pipe(idx_to_str, idx=[CrsSchema.PARTY_CODE, CrsSchema.AGENCY_CODE])
    )

    crs = pd.concat([crs, crdf], ignore_index=True).drop_duplicates(
        subset=[CrsSchema.PARTY_CODE, CrsSchema.AGENCY_CODE]
    )

    data = data.merge(
        crs,
        on=[CrsSchema.PARTY_CODE, CrsSchema.AGENCY_CODE],
        how="left",
        suffixes=("", "_crs_names"),
    )

    # drop any columns which contain the string "_crs_names"
    data = data.drop(columns=[c for c in data.columns if "_crs_names" in c])

    return data
