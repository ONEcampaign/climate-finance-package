import logging
from pathlib import Path


class ClimateDataPath:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "climate_finance"
    raw_data = scripts / ".raw_data"
    unfccc_cleaning_tools = scripts / "unfccc" / "cleaning_tools"
    oecd_cleaning_tools = scripts / "oecd" / "cleaning_tools"
    crs_channel_mapping = scripts / "oecd" / "multisystem" / "crs_channel_mapping.csv"


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

logger = logging.getLogger("pydeflate")
