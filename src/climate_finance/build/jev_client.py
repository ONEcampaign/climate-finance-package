"""The Jev client: canonical request hashing, cache replay, cost cap, retries.

A request is the body of TypeSafe's `POST /v1/systemone` without its model:
`{"state": ..., "questions": {...}}`. The client adds the pinned model, so the
model is part of every hash, and sends the canonical text it hashed.

The cache holds one JSONL file per model, `<cache_dir>/<model>.jsonl`, with one
`{"request", "response"}` line per answered request. Each line keeps its
request, so the cache re-keys itself on load if canonicalisation changes.
"""

import hashlib
import json
import math
import os
import random
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import IO

import httpx

from climate_finance.build.errors import BuildError, CostCapExceededError

API_URL = "https://api.typesafe.ai/v1/systemone"
API_KEY_ENV = "TYPESAFE_API_KEY"
API_KEY_PATH = Path.home() / ".config" / "typesafe" / "api_key"

# Input tokens are billed. Output tokens are free (docs.typesafe.ai/models).
USD_PER_INPUT_TOKEN = {"jev-1.13.0": 0.042e-6, "jev-1.14.0": 0.042e-6}
# Upper bound on input tokens per character of the canonical request. Over the
# 58,144 imported prototype responses the ratio ran from 0.241 to 0.497, median
# 0.364, so the estimate overstates a typical batch by about a third.
TOKENS_PER_CHAR = 0.5

# The TypeSafe SDK's default retry policy: 408, 429 and every 5xx (529 included).
RETRY_STATUSES = frozenset({408, 429, *range(500, 600)})
MAX_RETRIES = 8
BACKOFF_INITIAL_S = 1.0
BACKOFF_MAX_S = 60.0
# Each computed backoff loses up to this share of itself, so parallel workers
# spread out. A delay the server asked for is a floor and is waited in full.
JITTER_SHARE = 0.25


class JevError(BuildError):
    """A Jev call failed or answered with a model other than the pinned one."""


class JevCacheMissError(JevError):
    """An offline build asked Jev a question its cache cannot answer."""

    def __init__(self, *, misses: int, model: str) -> None:
        self.misses = misses
        self.model = model
        super().__init__(
            f"{misses} {model} requests are not in the Jev cache and the build "
            "is offline; run once online or import the responses."
        )


type Json = bool | int | float | str | list[Json] | dict[str, Json] | None


def _canonical(value: object) -> Json:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Jev requests take finite numbers only, not {value}")
        return int(value) if value.is_integer() else value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError(f"Jev request keys must be strings: {list(value)}")
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list | tuple):
        return [_canonical(item) for item in value]
    raise TypeError(f"{type(value).__name__} is not a JSON value: {value!r}")


def canonical_request(request: Mapping[str, object]) -> str:
    """The canonical JSON text of `request`.

    Keys are sorted at every level, separators are compact, and an integral float
    is written as an integer, so dict order and `1.0` against `1` leave it
    unchanged. List order is kept, since a Score's levels are ordered.

    Raises:
        TypeError: when a value is not JSON or a key is not a string.
        ValueError: when a number is NaN or infinite, which the API rejects.
    """
    return json.dumps(
        _canonical(request), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def canonical_hash(request: Mapping[str, object]) -> str:
    """Hash of the canonicalised request, the cache key."""
    return _sha256(canonical_request(request))


def _read_cache(path: Path) -> dict[str, dict[str, object]]:
    cache: dict[str, dict[str, object]] = {}
    if not path.exists():
        return cache
    with path.open(encoding="utf-8") as lines:
        for line in lines:
            entry = json.loads(line)
            cache.setdefault(canonical_hash(entry["request"]), entry["response"])
    return cache


def _append(stream: IO[str], body: str, response: Mapping[str, object]) -> None:
    entry = {"request": json.loads(body), "response": response}
    stream.write(json.dumps(entry, ensure_ascii=False) + "\n")
    stream.flush()


def _api_key() -> str:
    if key := os.environ.get(API_KEY_ENV):
        return key
    try:
        return API_KEY_PATH.read_text().strip()
    except FileNotFoundError as error:
        raise JevError(f"No Jev API key in ${API_KEY_ENV} or {API_KEY_PATH}") from error


def _backoff(attempt: int) -> float:
    return min(BACKOFF_MAX_S, BACKOFF_INITIAL_S * 2**attempt)


def _server_delay(response: httpx.Response) -> float | None:
    """Seconds the server asked for in `retry-after-ms` or `retry-after`, if any."""
    try:
        return float(response.headers["retry-after-ms"]) / 1000
    except (KeyError, ValueError):
        pass
    try:
        return float(response.headers["retry-after"])
    except (KeyError, ValueError):
        return None


class JevClient:
    """Jev calls for one build, replayed from cache first, capped in cost.

    One client serves a whole build, so the cap counts every live call its
    stages make. `spent_usd` and `live_calls` report them.
    """

    def __init__(
        self,
        *,
        model: str,
        cost_cap_usd: float,
        cache_dir: Path,
        offline: bool,
        transport: httpx.BaseTransport | None = None,
        max_workers: int = 6,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        if model not in USD_PER_INPUT_TOKEN:
            raise JevError(f"No price for Jev model {model!r}")
        self.model = model
        self.cost_cap_usd = cost_cap_usd
        self.cache_dir = cache_dir
        self.offline = offline
        self.spent_usd = 0.0
        self.live_calls = 0
        self._transport = transport
        self._max_workers = max_workers
        self._sleep = sleep
        self._jitter = jitter
        self._path = cache_dir / f"{model}.jsonl"
        self._cache = _read_cache(self._path)
        self._lock = threading.Lock()

    def score(
        self, requests: Sequence[Mapping[str, object]]
    ) -> list[dict[str, object]]:
        """Responses for `requests`, live only for cache misses within the cap.

        Each request is a System One body without `model`. Each response is the
        API's body: `model`, `answers` keyed by question name, and `usage`.

        Raises:
            ValueError: when a request names its own model.
            JevCacheMissError: when offline and a request is not cached.
            CostCapExceededError: when the batch's estimated cost exceeds the cap.
            JevError: when Jev answers with another model.
            httpx.HTTPStatusError: when Jev refuses a request, or still answers
                a `RETRY_STATUSES` code after `MAX_RETRIES` retries.
            httpx.TransportError: when a timeout or connection failure persists
                through `MAX_RETRIES` retries. Responses received before either
                failure are cached, so a rerun resumes from them.
        """
        misses: dict[str, str] = {}
        digests = []
        for request in requests:
            if "model" in request:
                raise ValueError("Jev requests take the client's pinned model")
            body = canonical_request({**request, "model": self.model})
            digest = _sha256(body)
            digests.append(digest)
            if digest not in self._cache:
                misses[digest] = body
        if misses:
            self._call_live(misses)
        return [self._cache[digest] for digest in digests]

    def _call_live(self, bodies: Mapping[str, str]) -> None:
        if self.offline:
            raise JevCacheMissError(misses=len(bodies), model=self.model)
        tokens = sum(len(body) for body in bodies.values()) * TOKENS_PER_CHAR
        estimate = self.spent_usd + tokens * USD_PER_INPUT_TOKEN[self.model]
        if estimate > self.cost_cap_usd:
            raise CostCapExceededError(estimate_usd=estimate, cap_usd=self.cost_cap_usd)

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        headers = {
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
        }
        with (
            httpx.Client(
                transport=self._transport, headers=headers, timeout=60
            ) as http,
            self._path.open("a", encoding="utf-8") as stream,
            ThreadPoolExecutor(max_workers=self._max_workers) as pool,
        ):
            futures = [
                pool.submit(self._call, http, stream, digest, body)
                for digest, body in bodies.items()
            ]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                pool.shutdown(cancel_futures=True)
                raise

    def _jittered_backoff(self, attempt: int) -> float:
        return _backoff(attempt) * (1 - JITTER_SHARE * self._jitter())

    def _call(
        self, http: httpx.Client, stream: IO[str], digest: str, body: str
    ) -> None:
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = http.post(API_URL, content=body.encode())
            except httpx.TransportError:
                if attempt == MAX_RETRIES:
                    raise
                delay = self._jittered_backoff(attempt)
            else:
                if response.status_code not in RETRY_STATUSES or attempt == MAX_RETRIES:
                    break
                server_delay = _server_delay(response)
                delay = (
                    self._jittered_backoff(attempt)
                    if server_delay is None
                    else server_delay
                )
            self._sleep(delay)
        response.raise_for_status()
        answer = response.json()
        if answer.get("model") != self.model:
            raise JevError(f"Asked {self.model}, answered by {answer.get('model')!r}")
        with self._lock:
            _append(stream, body, answer)
            self._cache[digest] = answer
            tokens = answer["usage"]["input_tokens"]
            self.spent_usd += tokens * USD_PER_INPUT_TOKEN[self.model]
            self.live_calls += 1
