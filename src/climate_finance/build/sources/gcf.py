"""The Green Climate Fund's projects API and its readiness list."""

import json
import re
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

PROJECTS_SOURCE = "gcf_projects"
READINESS_SOURCE = "ledger_gcf_readiness"
PROJECTS_URL = "https://api.gcfund.org/v1/projects"
READINESS_URL = "https://data.greenclimate.fund/public/data/readiness"
# The projects file feeds the external GCF reader and the GCF ledger, so it sits with
# the external sources and every fetch writes it.
PROJECTS_FILE = Path("external") / "gcf" / "projects.json"
READINESS_FILE = Path(LEDGER_DIR) / "gcf" / "readiness.json"
# The readiness page is server-rendered with its full table in this script tag.
_PAGE_DATA = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(?P<json>.*?)</script>', re.DOTALL
)


def download_gcf_projects(
    raw_dir: Path,
    *,
    offline: bool,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save the projects API response, which carries each project's disbursements.

    The file lands at `<raw_dir>/external/gcf/projects.json`, the file
    `read_gcf_projects` and `read_gcf_disbursements` take.

    Args:
        raw_dir: The raw directory.
        offline: Refuse to call the API.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause before a retry.

    Raises:
        SourceUnavailableError: when offline, or when the API answers with anything
            but a JSON list of projects.
    """
    require_online(PROJECTS_SOURCE, offline=offline)
    with http_client(client) as session:
        response = get(session, PROJECTS_URL, pause_seconds=pause_seconds)
    try:
        projects = response.json()
    except ValueError as error:
        raise SourceUnavailableError(f"{PROJECTS_URL} did not return JSON.") from error
    if not isinstance(projects, list):
        raise SourceUnavailableError(f"{PROJECTS_URL} did not return a list.")
    return [
        save(
            response.content,
            raw_dir=raw_dir,
            relative=PROJECTS_FILE,
            source=PROJECTS_SOURCE,
            url=PROJECTS_URL,
            etag=response.headers.get("etag"),
        )
    ]


def download_gcf_readiness(
    raw_dir: Path,
    *,
    offline: bool,
    archive: Path | None = None,
    client: httpx.Client | None = None,
    pause_seconds: float = 1.0,
) -> list[RawFile]:
    """Save the readiness grants table of the GCF open data library.

    The library has no API; its readiness page carries the table as JSON in the page
    data. The array of grants is saved at `<raw_dir>/ledgers/gcf/readiness.json`, the
    file `read_gcf_readiness` takes.

    Args:
        raw_dir: The raw directory.
        offline: Refuse to load the page. Ignored when `archive` is given.
        archive: A directory holding `readiness.json`, which is copied.
        client: An HTTP client to use in place of the default.
        pause_seconds: The pause before a retry.

    Raises:
        SourceUnavailableError: when offline without an archive, or when the page no
            longer holds the table where this expects it.
    """
    if archive is not None:
        return import_archive(
            {READINESS_FILE: archive / READINESS_FILE.name},
            raw_dir=raw_dir,
            source=READINESS_SOURCE,
        )
    require_online(READINESS_SOURCE, offline=offline)
    with http_client(client) as session:
        page = get(session, READINESS_URL, pause_seconds=pause_seconds)
    grants = _grants(page.text)
    return [
        save(
            json.dumps(grants).encode(),
            raw_dir=raw_dir,
            relative=READINESS_FILE,
            source=READINESS_SOURCE,
            url=READINESS_URL,
            etag=page.headers.get("etag"),
        )
    ]


def _grants(html: str) -> list[dict[str, object]]:
    found = _PAGE_DATA.search(html)
    try:
        grants = json.loads(found["json"] if found else "")["props"]["pageProps"][
            "data"
        ]["data"]
    except (TypeError, KeyError, json.JSONDecodeError) as error:
        raise SourceUnavailableError(
            f"{READINESS_URL} no longer carries the readiness table in its page data."
        ) from error
    if not isinstance(grants, list) or not grants:
        raise SourceUnavailableError(f"{READINESS_URL} lists no readiness grants.")
    return grants
