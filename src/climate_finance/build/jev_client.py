"""The Jev client: canonical request hashing, cache replay, cost cap, backoff."""

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path


def canonical_hash(request: Mapping[str, object]) -> str:
    """Hash of the canonicalised request, the cache key."""
    raise NotImplementedError("slice S18a")


class JevClient:
    """Jev calls for one build, replayed from cache first, capped in cost."""

    def __init__(
        self, *, model: str, cost_cap_usd: float, cache_dir: Path, offline: bool
    ) -> None:
        self.model = model
        self.cost_cap_usd = cost_cap_usd
        self.cache_dir = cache_dir
        self.offline = offline

    def score(
        self, requests: Sequence[Mapping[str, object]]
    ) -> list[dict[str, object]]:
        """Responses for `requests`, live only for cache misses within the cap.

        Raises:
            CostCapExceededError: when the batch's estimated cost exceeds the cap.
        """
        raise NotImplementedError("slice S18a")


def import_prototype_cache(paths: Iterable[Path], *, cache_dir: Path) -> int:
    """Re-key prototype Jev caches under `canonical_hash`; return entries written."""
    raise NotImplementedError("slice S18a")
