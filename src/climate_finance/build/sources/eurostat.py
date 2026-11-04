"""Eurostat's climate finance of the EU member states, the Commission and the EIB."""

from pathlib import Path

import httpx

from climate_finance.build.readers.eurostat import FILE, read_eurostat
from climate_finance.build.sources.files import RawFile, get, http_client, save
from climate_finance.errors import SourceUnavailableError

SOURCE = "eurostat_sdg_13_50"
# The dataset as SDMX-CSV: one row per geo and year, with its flag and last update.
URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_50"
_PARAMS = {"format": "SDMX-CSV"}


def download_climate_finance(
    raw_dir: Path,
    *,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save Eurostat's `sdg_13_50` as SDMX-CSV at `<raw_dir>/eurostat/sdg_13_50.csv`,
    the file `read_eurostat` takes. The manifest `version` is the dataset's last
    update date, as the endpoint sends no ETag.

    Args:
        raw_dir: The raw directory.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause before a retry.

    Raises:
        SourceUnavailableError: when the API answers with anything but the dataset.
        BuildError: when the dataset holds a geo code the release has no provider
            for (`read_eurostat`).
    """
    with http_client(client) as session:
        response = get(session, URL, params=_PARAMS, pause_seconds=pause_seconds)
    if not response.content.startswith(b"DATAFLOW,"):
        raise SourceUnavailableError(f"{URL} returned no SDMX-CSV observations.")
    record = save(
        response.content, raw_dir=raw_dir, relative=FILE, source=SOURCE, url=URL
    )
    table = read_eurostat(raw_dir / FILE)
    record["version"] = str(table["last_update"].max())
    return [record]
