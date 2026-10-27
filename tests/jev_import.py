"""The prototype's cached Jev responses, re-keyed into a build's Jev cache, so the
archive tests replay them offline."""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from climate_finance.build.jev_client import (
    _append,
    _read_cache,
    _sha256,
    canonical_request,
)


@dataclass(frozen=True)
class PrototypeCache:
    """A prototype's JSONL of Jev responses and the requests behind them.

    The prototype files hold responses only. Each record names its state by the
    field `key` (such as `state_hash` or `rid`), `states` maps that value to the
    state sent, and every record in the file answered `questions` (wire form).
    """

    responses: Path
    key: str
    states: Mapping[object, object]
    questions: Mapping[str, object]


@dataclass(frozen=True)
class ImportCounts:
    """Records read, entries written, and records not written, by reason."""

    read: int
    written: int
    collapsed: int
    unmatched: int


def import_prototype_cache(
    caches: Iterable[PrototypeCache], *, cache_dir: Path
) -> ImportCounts:
    """Re-key prototype Jev caches under `canonical_hash` into `cache_dir`.

    The model is each record's `model_requested`. A record whose request hashes
    to an entry already cached, or written earlier in the import, is collapsed
    and the first response kept. A record whose state is missing from `states`
    is unmatched.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    known: dict[str, set[str]] = {}
    streams: dict[str, IO[str]] = {}
    read = written = collapsed = unmatched = 0
    try:
        for cache in caches:
            with cache.responses.open(encoding="utf-8") as lines:
                for line in lines:
                    record = json.loads(line)
                    read += 1
                    if (state := cache.states.get(record[cache.key])) is None:
                        unmatched += 1
                        continue
                    model = record["model_requested"]
                    if model not in streams:
                        path = cache_dir / f"{model}.jsonl"
                        known[model] = set(_read_cache(path))
                        streams[model] = path.open("a", encoding="utf-8")
                    request = {
                        "model": model,
                        "state": state,
                        "questions": cache.questions,
                    }
                    body = canonical_request(request)
                    digest = _sha256(body)
                    if digest in known[model]:
                        collapsed += 1
                        continue
                    known[model].add(digest)
                    _append(streams[model], body, record["response"])
                    written += 1
    finally:
        for stream in streams.values():
            stream.close()
    return ImportCounts(
        read=read, written=written, collapsed=collapsed, unmatched=unmatched
    )
