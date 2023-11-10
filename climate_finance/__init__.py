__version__ = "0.1.19"

# Easy access to importers
from climate_finance.oecd import get_oecd_data
from climate_finance.unfccc import get_unfccc_data


def set_climate_finance_data_path(path):
    from pathlib import Path
    from climate_finance.config import ClimateDataPath

    """Set the path to the _data folder."""
    global ClimateDataPath

    ClimateDataPath.raw_data = Path(path).resolve()
