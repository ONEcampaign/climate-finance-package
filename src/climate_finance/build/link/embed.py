"""Embedding neighbours for blocking, and the vector cache behind them.

A unit's text is its largest record's title, then its short description where that
differs from the title, then its long description where that differs from both,
joined by ". " (`record_text`). The model is `intfloat/multilingual-e5-small` at a
pinned revision: each text takes the `PREFIX`, is cut at `MAX_TOKENS` tokens, and
its vector is L2-normalised, so a dot product is a cosine.

Within a donor, a disbursement and a commitment of an earlier record year are
neighbours when either ranks the other among its `NEIGHBOURS` nearest and their
cosine is at least `MIN_COSINE`. Search is exact. Cosines are computed once per pair
of distinct texts, so units with equal texts tie exactly, and ties go to the
smaller unit id.

`TextEmbedder` reads vectors from a cache keyed by the sha256 of (model revision,
text) and embeds only the texts the cache lacks. An offline embedder reads the cache
only and refuses a miss, so two offline builds give identical candidates.
"""

import hashlib
import json
import logging
from collections.abc import Callable, Sequence
from pathlib import Path

import numpy as np
import polars as pl

from climate_finance.build.errors import BuildError

logger = logging.getLogger(__name__)

PREFIX = "query: "
MAX_TOKENS = 128
DIMENSIONS = 384
NEIGHBOURS = 3
MIN_COSINE = 0.90
# Texts embedded between cache writes, so an interrupted build keeps its vectors.
SHARD_TEXTS = 20_000
# Cosines per matrix product: 40 million float32 values take 160 MB.
MAX_PRODUCT = 40_000_000
_VECTOR = pl.Array(pl.Float32, DIMENSIONS)

# Texts to an (n, `DIMENSIONS`) float32 array of L2-normalised vectors.
type Encoder = Callable[[list[str]], np.ndarray]


class EmbeddingError(BuildError):
    """An encoder returned vectors of the wrong shape."""


class EmbeddingCacheMissError(BuildError):
    """An offline build needs vectors its embedding cache does not hold."""

    def __init__(self, *, misses: int, revision: str) -> None:
        self.misses = misses
        self.revision = revision
        super().__init__(
            f"{misses} texts have no vector at revision {revision} in the embedding "
            "cache and the build is offline; run once online to embed them."
        )


def text_key(text: str, *, revision: str) -> str:
    """The cache key of `text` under the model `revision`."""
    body = json.dumps([revision, text], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def sentence_transformer(*, model: str, revision: str) -> Encoder:
    """An `Encoder` running `model` at `revision` on the CPU.

    Needs the `embed` extra, which installs sentence-transformers and torch.
    """
    # Imported here, as only the embed extra installs it.
    from sentence_transformers import SentenceTransformer

    encoder = SentenceTransformer(model, revision=revision, device="cpu")
    encoder.max_seq_length = MAX_TOKENS

    def encode(texts: list[str]) -> np.ndarray:
        return encoder.encode(
            [PREFIX + text for text in texts],
            batch_size=128,
            normalize_embeddings=True,
            convert_to_numpy=True,
        ).astype(np.float32)

    return encode


class TextEmbedder:
    """Vectors of texts, read from the cache first and embedded on a miss.

    The cache is a directory of parquet shards of (key, vector). Each batch of
    `SHARD_TEXTS` new vectors is written as its own shard, so a rerun resumes from
    the vectors an interrupted build wrote. The encoder is made on the first miss,
    so a build the cache answers loads no model.
    """

    def __init__(
        self,
        *,
        model: str,
        revision: str,
        cache_dir: Path,
        offline: bool,
        encoder: Encoder | None = None,
    ) -> None:
        self.model = model
        self.revision = revision
        self.cache_dir = cache_dir
        self.offline = offline
        self.embedded = 0
        self._encoder = encoder

    def __call__(self, texts: Sequence[str]) -> np.ndarray:
        """An (n, `DIMENSIONS`) float32 array, one vector per text in order.

        Raises:
            EmbeddingCacheMissError: when offline and a text is not cached.
            EmbeddingError: when the encoder returns vectors of the wrong shape.
        """
        wanted = pl.DataFrame(
            {
                "text": texts,
                "key": [text_key(text, revision=self.revision) for text in texts],
            },
            schema={"text": pl.String, "key": pl.String},
        )
        cached = self._read(wanted)
        missing = wanted.join(cached, on="key", how="anti").unique("key").sort("key")
        if missing.height:
            if self.offline:
                raise EmbeddingCacheMissError(
                    misses=missing.height, revision=self.revision
                )
            cached = pl.concat([cached, self._embed(missing)])
        vectors = wanted.join(cached, on="key", how="left", maintain_order="left")
        return vectors["vector"].to_numpy().reshape(len(texts), DIMENSIONS)

    def _read(self, wanted: pl.DataFrame) -> pl.DataFrame:
        """The cached (key, vector) rows of `wanted`'s keys."""
        if not any(self.cache_dir.glob("*.parquet")):
            return pl.DataFrame(schema={"key": pl.String, "vector": _VECTOR})
        return (
            pl.scan_parquet(self.cache_dir / "*.parquet")
            .join(wanted.lazy().select("key"), on="key", how="semi")
            .unique("key")
            .collect()
        )

    def _embed(self, missing: pl.DataFrame) -> pl.DataFrame:
        """Vectors of `missing`'s texts, each batch written to the cache as a shard."""
        if self._encoder is None:
            self._encoder = sentence_transformer(
                model=self.model, revision=self.revision
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        shards = []
        for batch in missing.iter_slices(SHARD_TEXTS):
            vectors = self._encoder(batch["text"].to_list())
            if vectors.shape != (batch.height, DIMENSIONS):
                raise EmbeddingError(
                    f"Expected {batch.height} vectors of {DIMENSIONS} values, "
                    f"got an array of shape {vectors.shape}"
                )
            shard = pl.DataFrame(
                {
                    "key": batch["key"],
                    "vector": pl.Series(vectors.astype(np.float32), dtype=_VECTOR),
                }
            )
            _write_shard(shard, cache_dir=self.cache_dir)
            shards.append(shard)
            self.embedded += batch.height
            logger.info("Embedded %d of %d texts", self.embedded, missing.height)
        return pl.concat(shards)


def _write_shard(shard: pl.DataFrame, *, cache_dir: Path) -> None:
    """Write `shard` under the digest of its keys, through a temporary file so a
    reader never sees part of it."""
    name = hashlib.sha256("\n".join(shard["key"]).encode()).hexdigest()
    temporary = cache_dir / f"{name}.parquet.tmp"
    shard.write_parquet(temporary)
    temporary.replace(cache_dir / f"{name}.parquet")


def record_text() -> pl.Expr:
    """A CRS record's embedding text from `title`, `short_description` and
    `long_description`; null when all three are empty.

    Each field is trimmed, and a description joins only when it differs from the
    fields before it, ignoring case.
    """
    fields = {
        name: pl.col(name).str.strip_chars()
        for name in ("title", "short_description", "long_description")
    }
    present = {name: pl.when(field != "").then(field) for name, field in fields.items()}
    lower = {
        name: field.str.to_lowercase().fill_null("") for name, field in fields.items()
    }
    text = pl.concat_str(
        present["title"],
        pl.when(lower["short_description"] != lower["title"]).then(
            present["short_description"]
        ),
        pl.when(
            (lower["long_description"] != lower["title"])
            & (lower["long_description"] != lower["short_description"])
        ).then(present["long_description"]),
        separator=". ",
        ignore_nulls=True,
    )
    return pl.when(text != "").then(text)


def neighbour_pairs(
    commitments: pl.DataFrame, disbursements: pl.DataFrame, *, vectors: np.ndarray
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs that are embedding neighbours.

    Both frames hold `unit`, `unit_id`, `donor_code`, `year` and `vector`, the row
    of the unit's text in `vectors`, null for a unit with no text.
    """
    columns = ("unit", "unit_id", "donor_code", "year", "vector")
    commitments = commitments.drop_nulls("vector").select(columns)
    disbursements = disbursements.drop_nulls("vector").select(columns)
    pairs = [pl.DataFrame(schema={"left": pl.UInt32, "right": pl.UInt32})]
    for (donor,), donor_commitments in commitments.group_by("donor_code"):
        donor_disbursements = disbursements.filter(donor_code=donor)
        if donor_disbursements.is_empty():
            continue
        close = _close_texts(
            donor_commitments["vector"].unique().sort().to_numpy(),
            donor_disbursements["vector"].unique().sort().to_numpy(),
            vectors=vectors,
        )
        pairs.append(_nearest(close, donor_commitments, donor_disbursements))
    return pl.concat(pairs).unique()


def _close_texts(
    commitment_rows: np.ndarray, disbursement_rows: np.ndarray, *, vectors: np.ndarray
) -> pl.DataFrame:
    """Pairs of distinct texts, as rows of `vectors`, whose cosine is at least
    `MIN_COSINE`: vector, vector_disbursement and cosine."""
    right = vectors[disbursement_rows].T
    step = max(1, MAX_PRODUCT // len(disbursement_rows))
    close = [
        pl.DataFrame(
            schema={
                "vector": pl.UInt32,
                "vector_disbursement": pl.UInt32,
                "cosine": pl.Float32,
            }
        )
    ]
    for start in range(0, len(commitment_rows), step):
        rows = commitment_rows[start : start + step]
        cosine = vectors[rows] @ right
        i, j = np.nonzero(cosine >= MIN_COSINE)
        close.append(
            pl.DataFrame(
                {
                    "vector": rows[i],
                    "vector_disbursement": disbursement_rows[j],
                    "cosine": cosine[i, j],
                },
                schema=close[0].schema,
            )
        )
    return pl.concat(close)


def _nearest(
    close: pl.DataFrame, commitments: pl.DataFrame, disbursements: pl.DataFrame
) -> pl.DataFrame:
    """The unit pairs of `close` texts, the disbursement of a later year, that rank
    among the `NEIGHBOURS` nearest of either unit."""
    rank = pl.int_range(pl.len())
    return (
        close.join(
            commitments.select("vector", "year", "unit_id", right="unit"), on="vector"
        )
        .join(
            disbursements.select(
                "unit_id",
                vector_disbursement="vector",
                year_disbursement="year",
                left="unit",
            ),
            on="vector_disbursement",
            suffix="_disbursement",
        )
        .filter(pl.col("year_disbursement") > pl.col("year"))
        .sort(
            "cosine",
            "unit_id_disbursement",
            "unit_id",
            descending=[True, False, False],
        )
        .filter((rank.over("right") < NEIGHBOURS) | (rank.over("left") < NEIGHBOURS))
        .select("left", "right")
    )
