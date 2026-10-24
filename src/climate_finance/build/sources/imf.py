"""The IMF's US dollar rate of the SDR, which converts the AfDB's XDR ledger."""

from pathlib import Path

import httpx

from climate_finance.build.sources.files import (
    LEDGER_DIR,
    RawFile,
    get,
    http_client,
    import_archive,
    require_online,
    save,
)
from climate_finance.errors import SourceUnavailableError

SOURCE = "ledger_imf_sdr"
# US dollars per SDR, annual period average, from the IMF's exchange rates dataset.
URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,ER/USA.XDC_XDR.PA_RT.A"
FOLDER = Path(LEDGER_DIR) / "imf"
SDR_FILE = FOLDER / "sdr_usd.xml"
# The ledgers published in SDR: the AfDB and the African Development Fund.
XDR_PUBLISHERS = frozenset({"afdb"})


def download_sdr_rates(
    raw_dir: Path,
    *,
    offline: bool,
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save the IMF's annual average US dollar rate of the SDR as SDMX XML.

    The file lands at `<raw_dir>/ledgers/imf/sdr_usd.xml`, the file
    `read_sdr_rates` takes.

    Args:
        raw_dir: The raw directory.
        offline: Refuse to call the API. Ignored when `archive` is given.
        archive: A directory holding `sdr_usd.xml`, which is copied.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause before a retry.

    Raises:
        SourceUnavailableError: when offline without an archive, or when the API
            answers with anything but SDMX data.
    """
    if archive is not None:
        return import_archive(
            {SDR_FILE: archive / SDR_FILE.name}, raw_dir=raw_dir, source=SOURCE
        )
    require_online(SOURCE, offline=offline)
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
