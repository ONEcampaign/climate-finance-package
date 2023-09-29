import pandas as pd
from oda_data import read_crs

from climate_finance.config import logger
from climate_finance.oecd.climate_related_activities.recipient_perspective import (
    get_recipient_perspective,
)

UNIQUE_INDEX = [
    "year",
    "oecd_party_code",
    "agency_code",
    "crs_identification_n",
    "donor_project_n",
    "recipient_code",
    "purpose_code",
]

VALUES = [
    "climate_adaptation_value",
    "climate_mitigation_value",
    "overlap_commitment_current",
    "climate_related_development_finance_commitment_current",
    "share_of_the_underlying_commitment_when_available",
]


def unique_projects(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.drop_duplicates(subset=UNIQUE_INDEX, keep="first")
        .filter(UNIQUE_INDEX)
        .astype({k: str for k in UNIQUE_INDEX})
    )


def get_crs_to_match(
    years: list[int], donor_code: str | list[str] | None = None
) -> pd.DataFrame:
    df_crs = (
        read_crs(years)
        .rename(
            columns={
                "donor_code": "oecd_party_code",
                "crs_id": "crs_identification_n",
                "project_number": "donor_project_n",
            }
        )
        .astype({k: str for k in UNIQUE_INDEX})
        .assign(year=lambda d: d.year.str.replace("\ufeff", "", regex=True))
    )
    if donor_code is not None:
        if isinstance(donor_code, str):
            donor_code = [donor_code]
        df_crs = df_crs.loc[lambda d: d.oecd_party_code.isin(donor_code)]

    return df_crs


def match_projects_with_crs(projects: pd.DataFrame, crs: pd.DataFrame) -> pd.DataFrame:
    # Perform an initial merge. It will be done considering all the columns in the
    # UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    data = projects.merge(
        crs, on=UNIQUE_INDEX, how="left", indicator=True, suffixes=("", "_crs")
    )

    # Log the number of projects that were matched
    logger.debug(f"Matched \n{data['_merge'].value_counts()} projects with CRS data")

    # If there are projects that were not matched, try to match them using a subset of
    # the columns in the UNIQUE_INDEX global variable.
    not_matched = data.loc[lambda d: d["_merge"] == "left_only", [UNIQUE_INDEX]]

    # Attempt to match the projects that were not matched using a subset of the columns
    # in the UNIQUE_INDEX global variable. A left join is attempted. The indicator column
    # is shown to see how many projects were matched.
    additional_matches = not_matched.merge(
        crs,
        on=["year", "oecd_party_code", "crs_identification_n", "purpose_code"],
        how="left",
        indicator=True,
        suffixes=("", "_crs"),
    )

    # Log the number of projects that were matched
    logger.debug(
        f"Matched \n{additional_matches['_merge'].value_counts()} projects with CRS data"
    )

    # Concatenate the dataframes
    data = pd.concat(
        [
            data.loc[lambda d: d["_merge"] != "left_only"],
            additional_matches,
        ]
    )

    return data


def filter_multilateral_providers(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[
        lambda d: d.provider_type.isin(
            ["Other multilateral", "Multilateral development bank"]
        )
    ].reset_index(drop=True)


def add_crs_data(df: pd.DataFrame) -> pd.DataFrame:
    projects_df = unique_projects(df)
    crs_df = get_crs_to_match(
        years=df.year.unique().tolist(),
        donor_code=projects_df.oecd_party_code.unique().tolist(),
    )

    matched = match_projects_with_crs(projects=projects_df, crs=crs_df)

    return matched


if __name__ == "__main__":
    df = get_recipient_perspective(start_year=2021, end_year=2021).pipe(
        filter_multilateral_providers
    )

    dfcrs = add_crs_data(df)
