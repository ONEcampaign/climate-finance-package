"""How the linker reads text: cleaned titles, their words, and a unit's largest record.

Candidate blocking (`candidates`), the comparison levels (`levels`), the scoring
rules (`score`), the BTR link (`btr`), the review packet and allocate read titles
the same way. This module imports no other part of the package.
"""

import polars as pl

# Words that say nothing about which project a title names.
STOPWORDS = (
    "the",
    "and",
    "for",
    "des",
    "les",
    "del",
    "von",
    "und",
    "with",
    "from",
    "that",
    "this",
    "dans",
    "pour",
    "por",
    "para",
    "zur",
    "der",
    "die",
    "das",
    "project",
    "programme",
    "program",
    "projet",
    "proyecto",
    "support",
)
# Titles that name no project once lower-cased and stripped.
NO_TITLE = ("not applicable", "na", "none", "n a", "nan", "null")


def clean_text(text: pl.Expr) -> pl.Expr:
    """`text` lower-cased, every run of characters other than a to z and digits a
    space, trimmed."""
    return text.str.to_lowercase().str.replace_all("[^a-z0-9]+", " ").str.strip_chars()


def text_words(text: pl.Expr, *, first: int | None = None) -> pl.Expr:
    """The distinct words of `text`, or of its first `first` words, that can tell
    projects apart, in order: three letters or more and not in `STOPWORDS`."""
    words = clean_text(text).str.split(" ")
    if first is not None:
        words = words.list.head(first)
    return words.list.eval(
        pl.element().filter(
            (pl.element().str.len_chars() >= 3) & ~pl.element().is_in(STOPWORDS)
        )
    ).list.unique(maintain_order=True)


def word_share(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Shared words over all words of two word lists; null when either is null or
    both are empty."""
    union = left.list.set_union(right).list.len()
    return pl.when(union > 0).then(left.list.set_intersection(right).list.len() / union)


def largest_record(*columns: str) -> pl.Expr:
    """`columns` of the group's largest record by commitment plus disbursement,
    ties to the smaller record id."""
    size = pl.col("commitment_usd").abs().fill_null(0) + pl.col(
        "disbursement_usd"
    ).abs().fill_null(0)
    return pl.col(*columns).sort_by(size, "record_id", descending=[True, False]).first()


def representative(
    units: pl.DataFrame, *, crs: pl.LazyFrame, fields: tuple[str, ...]
) -> pl.LazyFrame:
    """`fields` of each unit's largest record (`largest_record`), as `rep_`
    columns."""
    return (
        units.lazy()
        .select("unit_id", record_id="record_ids")
        .explode("record_id")
        .join(
            crs.select("record_id", "commitment_usd", "disbursement_usd", *fields),
            on="record_id",
        )
        .group_by("unit_id")
        .agg(largest_record(*fields).name.prefix("rep_"))
    )


def text_norm(text: pl.Expr) -> pl.Expr:
    """`text` cleaned (`clean_text`); null when nothing is left or it names nothing
    (`NO_TITLE`)."""
    cleaned = clean_text(text)
    return pl.when((cleaned != "") & ~cleaned.is_in(NO_TITLE)).then(cleaned)


def title_norm() -> pl.Expr:
    """The representative record's normalised title, else short description."""
    return text_norm(pl.coalesce("rep_title", "rep_short_description"))


def title_words(units: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """Each unit's title words (`title_toks`), as `levels.unit_attributes` reads
    them: unit_id and title_toks, null where the unit has no title."""
    fields = ("title", "short_description")
    return (
        representative(units, crs=crs, fields=fields)
        .select("unit_id", title_toks=text_words(title_norm()))
        .collect()
    )
