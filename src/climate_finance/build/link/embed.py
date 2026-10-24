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
from climate_finance.build.link.keys import partner_recipient

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

    Both frames hold `unit`, `unit_id`, `donor_code`, `recipient_code`, `year` and
    `vector`, the row of the unit's text in `vectors`, null for a unit with no text.
    A unit ranks only the units whose recipients partner its own
    (`keys.partner_recipient`).

    Units of one donor with the same text, year and recipient form a group, whose
    units have the same cosine to every other unit and the same partners. A unit's
    nearest units lie in the `NEIGHBOURS` groups nearest its own, ranked by cosine
    and then by their smallest unit id, and are the first units by id of those
    groups. So the search ranks groups and expands only those, and a text that
    thousands of units share costs one group per year and recipient.
    """
    columns = ("unit", "unit_id", "donor_code", "recipient_code", "year", "vector")
    commitments = commitments.drop_nulls("vector").select(columns)
    disbursements = disbursements.drop_nulls("vector").select(columns)
    # Ties go to the smaller unit id, as an integer order over both sides.
    order = (
        pl.concat([commitments.select("unit_id"), disbursements.select("unit_id")])
        .unique()
        .sort("unit_id")
        .with_row_index("order")
    )
    commitments = commitments.join(order, on="unit_id")
    disbursements = disbursements.join(order, on="unit_id")
    pairs = [pl.DataFrame(schema={"left": pl.UInt32, "right": pl.UInt32})]
    for (donor,), donor_commitments in commitments.group_by("donor_code"):
        donor_disbursements = disbursements.filter(donor_code=donor)
        if donor_disbursements.is_empty():
            continue
        com_units, com_groups = _groups(donor_commitments)
        dis_units, dis_groups = _groups(donor_disbursements)
        partner = _partners(
            donor_commitments["recipient_code"],
            donor_disbursements["recipient_code"],
        )
        later = _nearest_groups(
            com_groups, dis_groups, vectors=vectors, later=True, partner=partner
        )
        earlier = _nearest_groups(
            dis_groups, com_groups, vectors=vectors, later=False, partner=partner.T
        )
        pairs.append(
            _nearest_units(later, row_units=com_units, column_units=dis_units).select(
                left="column_unit", right="unit"
            )
        )
        pairs.append(
            _nearest_units(earlier, row_units=dis_units, column_units=com_units).select(
                left="unit", right="column_unit"
            )
        )
    return pl.concat(pairs).unique()


def _groups(units: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """`units` with their `group`, and the groups: group, vector, year,
    `recipient`, the recipient's place among the sorted distinct recipients of
    `units`, and `first`, the smallest order of its units."""
    key = ["vector", "year", "recipient_code"]
    groups = (
        units.group_by(key)
        .agg(first=pl.col("order").min())
        .with_columns(recipient=pl.col("recipient_code").rank("dense") - 1)
        .sort("first")
        .with_row_index("group")
    )
    return units.join(groups.select(*key, "group"), on=key), groups


def _partners(commitments: pl.Series, disbursements: pl.Series) -> np.ndarray:
    """partner[i, j]: whether a line to the j-th distinct recipient of
    `disbursements` can continue a commitment to the i-th of `commitments`, both
    sorted (`keys.partner_recipient`)."""
    left = commitments.unique().sort()
    right = disbursements.unique().sort()
    partner = (
        pl.DataFrame({"commitment": left})
        .join(pl.DataFrame({"disbursement": right}), how="cross")
        .select(
            partner_recipient(
                commitment=pl.col("commitment"),
                disbursement=pl.col("disbursement"),
            )
        )
        .to_series()
    )
    return partner.to_numpy().reshape(left.len(), right.len())


def _nearest_groups(
    rows: pl.DataFrame,
    columns: pl.DataFrame,
    *,
    vectors: np.ndarray,
    later: bool,
    partner: np.ndarray,
) -> pl.DataFrame:
    """Each row group's `NEIGHBOURS` nearest column groups of a later year (`later`)
    or an earlier one, whose recipient `partner` allows (`_partners`, indexed by
    the row's and the column's `recipient`), at a cosine of at least `MIN_COSINE`,
    ties to the smaller `first`: row, column and cosine.

    Cosines are computed once per pair of distinct texts and then spread over the
    groups, so groups of equal texts tie exactly.
    """
    row_texts, row_text = np.unique(rows["vector"].to_numpy(), return_inverse=True)
    column_texts, column_text = np.unique(
        columns["vector"].to_numpy(), return_inverse=True
    )
    right = vectors[column_texts].T
    row_year = rows["year"].to_numpy()
    column_year = columns["year"].to_numpy()
    row_recipient = rows["recipient"].to_numpy()
    column_recipient = columns["recipient"].to_numpy()
    step = max(1, MAX_PRODUCT // len(columns))
    found = []
    for start in range(0, len(rows), step):
        stop = min(start + step, len(rows))
        texts, text = np.unique(row_text[start:stop], return_inverse=True)
        cosine = (vectors[row_texts[texts]] @ right)[text][:, column_text]
        years = row_year[start:stop, None]
        allowed = column_year[None, :] > years if later else column_year < years
        allowed &= partner[row_recipient[start:stop]][:, column_recipient]
        cosine[~allowed | (cosine < MIN_COSINE)] = -np.inf
        # Every column at or above each row's NEIGHBOURS-th largest cosine, so ties
        # at the boundary are all kept and resolved by `first` below.
        if cosine.shape[1] > NEIGHBOURS:
            kth = np.partition(cosine, -NEIGHBOURS, axis=1)[:, -NEIGHBOURS]
            keep = (cosine >= kth[:, None]) & np.isfinite(cosine)
        else:
            keep = np.isfinite(cosine)
        i, j = np.nonzero(keep)
        found.append(
            pl.DataFrame(
                {"row": i + start, "column": j, "cosine": cosine[i, j]},
                schema={"row": pl.UInt32, "column": pl.UInt32, "cosine": pl.Float32},
            )
        )
    return (
        pl.concat(found)
        .join(columns.select(column="group", first="first"), on="column")
        .sort("cosine", "first", descending=[True, False])
        .filter(pl.int_range(pl.len()).over("row") < NEIGHBOURS)
        .select("row", "column", "cosine")
    )


def _nearest_units(
    nearest: pl.DataFrame, *, row_units: pl.DataFrame, column_units: pl.DataFrame
) -> pl.DataFrame:
    """Each row unit with its `NEIGHBOURS` nearest column units, from the nearest
    groups of its group: unit and column_unit."""
    rank = pl.int_range(pl.len())
    firsts = (
        column_units.sort("order")
        .filter(rank.over("group") < NEIGHBOURS)
        .select(column="group", column_unit="unit", column_order="order")
    )
    chosen = (
        nearest.join(firsts, on="column")
        .sort("cosine", "column_order", descending=[True, False])
        .filter(rank.over("row") < NEIGHBOURS)
        .select("row", "column_unit")
    )
    return row_units.select("unit", row="group").join(chosen, on="row").drop("row")
