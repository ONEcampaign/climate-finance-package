from typing import Protocol, Any

import pandas as pd

from climate_finance.config import logger
from climate_finance.oecd.crs.get_data import get_crs


class SourceLoader(Protocol):
    """A protocol for loading data from a source."""

    def get_data(self, settings: dict[str, Any]) -> pd.DataFrame:
        ...


class CrsLoader:
    def get_data(self, settings: dict[str, Any]) -> pd.DataFrame:
        # define the years to load (from start to end). Additional filtering for years
        # is done after loading the data
        year_start = min(settings["years"])
        year_end = max(settings["years"])

        years = [str(year) for year in settings["years"]]

        # load the data. Apply other filters like provider_code and recipient_code
        data = get_crs(
            start_year=year_start,
            end_year=year_end,
            provider_code=settings.get("providers"),
            recipient_code=settings.get("recipients"),
        )

        # filter the data to only include the years specified in the settings
        return data.loc[lambda d: d.year.isin(years)].reset_index(drop=True)


class CrdfRecipientLoader:
    def get_data(self, settings: dict[str, Any]) -> pd.DataFrame:
        ...


class CrdfProviderLoader:
    def get_data(self, settings: dict[str, Any]) -> pd.DataFrame:
        ...


class UnfcccLoader:
    def get_data(self, settings: dict[str, Any]) -> pd.DataFrame:
        raise NotImplementedError


AVAILABLE_LOADERS = {
    "OECD_CRS": CrsLoader,
    "OECD_CRDF": CrdfRecipientLoader,
    "OECD_CRDF_DONOR": CrdfProviderLoader,
    "UNFCCC": UnfcccLoader,
}


def get_data(source_name: str, settings: dict[str, Any]) -> pd.DataFrame:
    loader = AVAILABLE_LOADERS.get(source_name)

    if loader is None:
        raise ValueError(f"Invalid source: {source_name}")

    try:
        data = loader().get_data(settings)
        logger.info(f"Loaded raw {source_name} data")
        return data
    except NotImplementedError:
        raise NotImplementedError(f"Source {source_name} is yet not implemented")
