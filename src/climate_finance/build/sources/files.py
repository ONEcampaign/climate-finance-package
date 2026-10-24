"""What the source downloads share: the offline refusal, retries and manifest rows."""

import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import httpx

from climate_finance.errors import SourceUnavailableError
from climate_finance.schema import sha256_file

LEDGER_DIR = "ledgers"
_TIMEOUT = httpx.Timeout(60.0, read=300.0)
_ATTEMPTS = 5
# Rate limits and transient server faults; the World Bank catalogue answers 429.
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class RawFile(TypedDict):
    """One downloaded or imported file, with the fields of `schema.RAW_MANIFEST`."""

    source: str
    url: str | None
    path: str
    sha256: str
    etag: str | None
    version: str | None
    fetched_at: datetime
    method: str
    first_year: int | None
    last_year: int | None
    held_first_year: int | None
    held_last_year: int | None


def require_online(source: str, *, offline: bool) -> None:
    """Refuse a download when the build runs offline."""
    if offline:
        raise SourceUnavailableError(
            f"{source} needs a download and the build is offline; "
            "fetch it online once, or point `archive` at a local copy."
        )


@contextmanager
def http_client(client: httpx.Client | None) -> Generator[httpx.Client]:
    """Yield `client`, or a client with explicit timeouts that is closed afterwards."""
    if client is not None:
        yield client
        return
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as own:
        yield own


def get(
    client: httpx.Client,
    url: str,
    *,
    params: Mapping[str, str | int] | None = None,
    pause_seconds: float,
) -> httpx.Response:
    """GET `url`, retrying rate limits and server faults with a growing pause.

    Raises:
        SourceUnavailableError: when the request fails or is still being refused
            after the last attempt.
    """
    failure = ""
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            response = client.get(url, params=params)
        except httpx.TransportError as error:
            failure = str(error) or type(error).__name__
        else:
            if response.is_success:
                return response
            failure = f"HTTP {response.status_code}"
            if response.status_code not in _RETRY_STATUS:
                break
        if attempt < _ATTEMPTS:
            time.sleep(pause_seconds * attempt)
    raise SourceUnavailableError(f"{url} could not be fetched: {failure}.")


def save(
    content: bytes,
    *,
    raw_dir: Path,
    relative: Path,
    source: str,
    url: str,
    etag: str | None = None,
) -> RawFile:
    """Write a downloaded file under `raw_dir` and describe it for the manifest."""
    return _write(content, raw_dir, relative, source, url, etag, method="http")


def import_archive(
    files: Mapping[Path, Path], *, raw_dir: Path, source: str
) -> list[RawFile]:
    """Copy local archive files into `raw_dir`.

    Args:
        files: Each destination, relative to `raw_dir`, and the archive file to copy.
        raw_dir: The raw directory.
        source: The source name recorded in the manifest.

    Raises:
        SourceUnavailableError: when `files` is empty or an archive file is missing.
    """
    missing = [str(origin) for origin in files.values() if not origin.is_file()]
    if missing or not files:
        raise SourceUnavailableError(
            f"The {source} archive lacks {missing or 'any file to import'}."
        )
    return [
        _write(origin.read_bytes(), raw_dir, relative, source, None, None, "manual")
        for relative, origin in files.items()
    ]


def _write(
    content: bytes,
    raw_dir: Path,
    relative: Path,
    source: str,
    url: str | None,
    etag: str | None,
    method: str,
) -> RawFile:
    path = raw_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return RawFile(
        source=source,
        url=url,
        path=relative.as_posix(),
        sha256=sha256_file(path),
        etag=etag,
        version=None,
        fetched_at=datetime.now(UTC),
        method=method,
        first_year=None,
        last_year=None,
        held_first_year=None,
        held_last_year=None,
    )
