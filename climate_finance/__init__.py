__version__ = "1.1.0"

# Easy access to importers

from climate_finance.unfccc import get_unfccc_data
from climate_finance.core.data import ClimateData


def set_climate_finance_data_path(path):
    from pathlib import Path
    from climate_finance.config import ClimateDataPath

    """Set the path to the _data folder."""
    global ClimateDataPath

    ClimateDataPath.raw_data = Path(path).resolve()


__all__ = [
    "set_climate_finance_data_path",
    "get_unfccc_data",
    "ClimateData",
]
