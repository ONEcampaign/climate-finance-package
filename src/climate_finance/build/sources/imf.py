"""The IMF's US dollar rate of the SDR, which converts the AfDB's XDR ledger."""

from pathlib import Path

import httpx

from climate_finance.build.readers.ledgers.imf import SDR_FILE
from climate_finance.build.sources.files import (
    RawFile,
    get,
    http_client,
    import_archive,
    save,
)
from climate_finance.errors import SourceUnavailableError

SOURCE = "ledger_imf_sdr"
# US dollars per SDR, annual period average, from the IMF's exchange rates dataset.
URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,ER/USA.XDC_XDR.PA_RT.A"


def download_sdr_rates(
    raw_dir: Path,
    *,
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save the IMF's annual average US dollar rate of the SDR as SDMX XML.

    The file lands at `<raw_dir>/ledgers/imf/sdr_usd.xml`, the file
    `read_sdr_rates` takes.

    Args:
        raw_dir: The raw directory.
        archive: A directory holding `sdr_usd.xml`, which is placed (`files.place`).
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause before a retry.

    Raises:
        SourceUnavailableError: when the API answers with anything but SDMX data.
    """
    if archive is not None:
        return import_archive(
            {SDR_FILE: archive / SDR_FILE.name}, raw_dir=raw_dir, source=SOURCE
        )
    with http_client(client) as session:
        response = get(session, URL, pause_seconds=pause_seconds)
    if b"<Obs " not in response.content:
        raise SourceUnavailableError(f"{URL} returned no observations.")
    return [
        save(
            response.content,
            raw_dir=raw_dir,
            relative=SDR_FILE,
            source=SOURCE,
            url=URL,
            etag=response.headers.get("etag"),
        )
    ]
