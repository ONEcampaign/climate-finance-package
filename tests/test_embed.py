"""Embedding texts and the vector cache behind the blocking's embedding neighbours."""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from climate_finance.build.link import embed
from climate_finance.build.link.embed import (
    DIMENSIONS,
    SHARD_TEXTS,
    EmbeddingCacheMissError,
    TextEmbedder,
    record_text,
    text_key,
)

MODEL = "intfloat/multilingual-e5-small"
REVISION = "614241f622f53c4eeff9890bdc4f31cfecc418b3"


@pytest.mark.parametrize(
    ("title", "short", "long", "text"),
    [
        ("Solar", "Mini-grids", "Kenya", "Solar. Mini-grids. Kenya"),
        # A description repeating an earlier field, in any case, is left out.
        (" Solar ", "SOLAR", "solar", "Solar"),
        ("Solar", "Kenya", "kenya ", "Solar. Kenya"),
        (None, "", "Kenya", "Kenya"),
        (None, " ", None, None),
    ],
)
def test_record_text(
    title: str | None, short: str | None, long: str | None, text: str | None
) -> None:
    record = pl.DataFrame(
        {"title": [title], "short_description": [short], "long_description": [long]},
        schema=dict.fromkeys(
            ("title", "short_description", "long_description"), pl.String
        ),
    )
    assert record.select(record_text()).item() == text


class _Encoder:
    """Gives text i of all it has seen the vector with a one at i, and records each
    call's texts."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, texts: list[str]) -> np.ndarray:
        seen = sum(len(call) for call in self.calls)
        self.calls.append(texts)
        return np.eye(len(texts), DIMENSIONS, k=seen, dtype=np.float32)


def _embedder(
    cache_dir: Path,
    encoder: _Encoder,
    *,
    offline: bool = False,
    model: str = MODEL,
    revision: str = REVISION,
) -> TextEmbedder:
    return TextEmbedder(
        model=model,
        revision=revision,
        cache_dir=cache_dir,
        offline=offline,
        encoder=encoder,
    )


def test_a_build_embeds_only_the_texts_the_cache_lacks(tmp_path: Path) -> None:
    encoder = _Encoder()
    first = _embedder(tmp_path, encoder)(["solar", "wind", "solar"])
    second = _embedder(tmp_path, encoder)(["water", "wind", "solar"])

    assert [sorted(call) for call in encoder.calls] == [["solar", "wind"], ["water"]]
    np.testing.assert_array_equal(first[0], first[2])
    np.testing.assert_array_equal(second[1:], first[[1, 0]])


def test_an_offline_build_reads_the_cache_and_refuses_a_miss(tmp_path: Path) -> None:
    online = _embedder(tmp_path, _Encoder())(["solar", "wind"])
    encoder = _Encoder()

    offline = _embedder(tmp_path, encoder, offline=True)
    np.testing.assert_array_equal(offline(["wind", "solar"]), online[[1, 0]])
    with pytest.raises(EmbeddingCacheMissError) as error:
        offline(["solar", "water"])
    assert error.value.misses == 1
    with pytest.raises(EmbeddingCacheMissError):
        _embedder(tmp_path, encoder, offline=True, revision="other")(["solar"])
    assert encoder.calls == []


def test_vectors_reach_the_cache_one_shard_at_a_time(tmp_path: Path) -> None:
    encoder = _Encoder()
    texts = [f"text {i}" for i in range(SHARD_TEXTS + 1)]
    _embedder(tmp_path, encoder)(texts)

    assert [len(call) for call in encoder.calls] == [SHARD_TEXTS, 1]
    assert len(list(tmp_path.glob("*.parquet"))) == 2


def test_another_model_does_not_read_the_cached_vectors(tmp_path: Path) -> None:
    small, base = _Encoder(), _Encoder()
    _embedder(tmp_path, small)(["solar"])
    _embedder(tmp_path, base, model="intfloat/multilingual-e5-base")(["solar"])

    assert small.calls == base.calls == [["solar"]]
    with pytest.raises(EmbeddingCacheMissError):
        _embedder(tmp_path, _Encoder(), model="other", offline=True)(["solar"])


def test_a_new_preprocessing_version_misses_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = text_key("solar", model=MODEL, revision=REVISION)
    _embedder(tmp_path, _Encoder())(["solar"])
    monkeypatch.setattr(embed, "PREPROCESSING_VERSION", embed.PREPROCESSING_VERSION + 1)

    assert text_key("solar", model=MODEL, revision=REVISION) != key
    with pytest.raises(EmbeddingCacheMissError):
        _embedder(tmp_path, _Encoder(), offline=True)(["solar"])
