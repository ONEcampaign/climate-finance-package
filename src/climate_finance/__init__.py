__version__ = "1.2.2"

# Easy access to importers

from climate_finance.unfccc import get_unfccc_data
from climate_finance.core.data import ClimateData


def set_climate_finance_data_path(path):
    from pathlib import Path
    from climate_finance.config import ClimateDataPath
    from oda_data import set_data_path
    from pydeflate import set_pydeflate_path

    """Set the path to the _data folder."""
    ClimateDataPath.raw_data = Path(path).resolve()
    set_data_path(Path(path).resolve())
    set_pydeflate_path(Path(path).resolve())


__all__ = [
    "set_climate_finance_data_path",
    "get_unfccc_data",
    "ClimateData",
]
