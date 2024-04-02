from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from climate_finance.common.schema import ClimateSchema
from climate_finance.config import logger
from climate_finance.oecd.crdf.provider_perspective import get_provider_perspective
from climate_finance.oecd.crdf.recipient_perspective import get_recipient_perspective
from climate_finance.oecd.crs.get_data import (
    get_crs,
    get_crs_allocable_spending,
    get_raw_allocable_crs,
    get_raw_crs,
)


class SourceLoader(ABC):
    """A base class for loading data from a source."""

    def __init__(self, settings):
        self.settings = settings

    @property
    @abstractmethod
    def get_data_retrieval_function(self, **kwargs) -> callable:
        """Abstract method to return the specific data retrieval function."""

    def _years_from_setting(self) -> tuple[int, int, list[int]]:
        """Define the years to load from settings."""
        years = self.settings.get("years", [])
        year_start, year_end = min(years), max(years)
        return year_start, year_end, years

    def get_data(self) -> pd.DataFrame:
        """Common logic for getting data."""
        year_start, year_end, years = self._years_from_setting()
        data_retrieval_func = self.get_data_retrieval_function

        data = data_retrieval_func(
            start_year=year_start,
            end_year=year_end,
            provider_code=self.settings.get("providers"),
            recipient_code=self.settings.get("recipients"),
            force_update=self.settings.get("update"),
        )

        return data.loc[lambda d: d[ClimateSchema.YEAR].isin(years)].reset_index(
            drop=True
        )


class CrsLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_crs


class CrsAllocableLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_crs_allocable_spending


class CrsRawAllocableLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_raw_allocable_crs


class CrsRawLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_raw_crs


class CrdfRecipientLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_recipient_perspective


class CrdfProviderLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        return get_provider_perspective


class UnfcccLoader(SourceLoader):
    @property
    def get_data_retrieval_function(self):
        raise NotImplementedError


AVAILABLE_LOADERS = {
    "OECD_CRS": CrsLoader,
    "OECD_CRS_ALLOCABLE": CrsAllocableLoader,
    "OECD_CRS_RAW_ALLOCABLE": CrsRawAllocableLoader,
    "OECD_CRS_RAW": CrsRawLoader,
    "OECD_CRDF": CrdfRecipientLoader,
    "OECD_CRDF_DONOR": CrdfProviderLoader,
    "UNFCCC": UnfcccLoader,
}


def get_data(source_name: str, settings: dict[str, Any]) -> pd.DataFrame:
    loader = AVAILABLE_LOADERS.get(source_name)

    if loader is None:
        raise ValueError(f"Invalid source: {source_name}")

    try:
        data = loader(settings=settings).get_data()
        logger.info(f"Loaded raw {source_name} data")
        return data
    except NotImplementedError:
        raise NotImplementedError(f"Source {source_name} is yet not implemented")
