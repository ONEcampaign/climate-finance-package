"""A unit's title words and the word share of two word lists.

The comparison levels (`levels`), the scoring rules (`score`), the review packet and
allocate read titles the same way. This module imports neither Splink nor DuckDB.
"""

import polars as pl

from climate_finance.build.link.candidates import STOPWORDS

# Texts that name nothing once lower-cased and stripped.
NO_TEXT = ("not applicable", "na", "none", "n a", "nan", "null")


def title_words(units: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Each unit's title words (`title_toks`), as `levels.unit_attributes` reads
    them: unit_id and title_toks, null where the unit has no title."""
    fields = ("title", "short_description")
    return (
        representative(units, crs=crs, fields=fields)
        .select("unit_id", title_toks=words(title_norm()))
        .collect()
    )


def word_share(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """The Jaccard similarity of two word lists, null where either is null or both
    are empty."""
    union = left.list.set_union(right).list.len()
    return pl.when(union > 0).then(left.list.set_intersection(right).list.len() / union)


def representative(
    units: pl.DataFrame, *, crs: pl.LazyFrame, fields: tuple[str, ...]
) -> pl.LazyFrame:
    """`fields` of each unit's largest record by commitment plus disbursement,
    ties to the smaller record id, as `rep_` columns."""
    size = pl.col("commitment_usd").abs().fill_null(0) + pl.col(
        "disbursement_usd"
    ).abs().fill_null(0)
    return (
        units.lazy()
        .select("unit_id", record_id="record_ids")
        .explode("record_id")
        .join(
            crs.select("record_id", "commitment_usd", "disbursement_usd", *fields),
            on="record_id",
        )
        .sort(size, "record_id", descending=[True, False])
        .unique("unit_id", keep="first")
        .select("unit_id", *(pl.col(name).alias(f"rep_{name}") for name in fields))
    )


def title_norm() -> pl.Expr:
    """The representative record's normalised title, else short description."""
    title = normalise_text(pl.coalesce("rep_title", "rep_short_description"))
    return pl.when(~title.is_in(NO_TEXT)).then(title)


def normalise_text(text: pl.Expr) -> pl.Expr:
    """`text` lower-cased, each run of characters other than a to z and digits one
    space, trimmed; null when nothing is left."""
    cleaned = (
        text.str.to_lowercase().str.replace_all("[^a-z0-9]+", " ").str.strip_chars()
    )
    return pl.when(cleaned != "").then(cleaned)


def words(text: pl.Expr, *, first: int | None = None) -> pl.Expr:
    """The distinct words of normalised `text`, or of its first `first` words, that
    have three letters or more and are not in `STOPWORDS`."""
    split = text.str.split(" ")
    if first is not None:
        split = split.list.head(first)
    return split.list.eval(
        pl.element().filter(
            (pl.element().str.len_chars() >= 3) & ~pl.element().is_in(STOPWORDS)
        )
    ).list.unique(maintain_order=True)
