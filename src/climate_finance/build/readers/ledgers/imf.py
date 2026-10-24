"""The IMF's US dollar rate of the SDR, as `sources.imf` saves it."""

import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl


def read_sdr_rates(path: Path) -> pl.DataFrame:
    """US dollars per SDR by year, the annual average the IMF publishes.

    Args:
        path: The SDMX XML `download_sdr_rates` saved.

    Returns:
        `year` and `usd_per_xdr`, one row per year with a value.
    """
    rows = [
        {"year": int(obs.attrib["TIME_PERIOD"]), "usd_per_xdr": float(value)}
        for obs in ET.parse(path).iter()
        if obs.tag.rpartition("}")[2] == "Obs"
        and (value := obs.attrib.get("OBS_VALUE"))
    ]
    return pl.DataFrame(rows, schema={"year": pl.Int16, "usd_per_xdr": pl.Float64})
