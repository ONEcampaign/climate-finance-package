"""Comparison levels and pair features for the scoring models.

A pair joins a disbursement unit (left, as in `candidates` and `trace` links) and a
commitment unit (right). Each unit reads its fields from its representative record,
the largest by absolute commitment plus disbursement, ties to the smaller record id.
The ten levels follow Splink's comparisons, with -1 where either side lacks the field:

- project number: 3 the same number, 2 the same once lower-cased with every
  character other than a to z and digits and leading zeros dropped, 1 Jaro-Winkler at
  least 0.92 on that form, else 0. Numbers that name no project, or shorter than
  three characters in that form, count as missing.
- text, over the title (the project title, else the short description) and the
  first `DESCRIPTION_WORDS` words of the long description: 5 the same title and
  descriptions that agree (word Jaccard at least 0.7, or either missing), 4 the same
  title, 3 and 2 close titles or descriptions, 1 descriptions sharing a fifth of
  their words, else 0.
- purpose: 2 the same code, 1 the same three-digit category, else 0.
- recipient, channel, flow and finance type: 1 the same, else 0.
- amount, the disbursement over the commitment: 4 from half to all of it, 3 from a
  tenth to half, 2 under a tenth, 1 up to 1.1 times, else 0; -1 without a commitment.
- later commitment: 2 when the disbursement unit commits nothing, 1 when its
  commitment equals its disbursement within 1%, else 0.
- year gap: 2 one year, 1 two years, else 0.

The features beside the levels are the year gap (a `candidates` column), log10 of
the disbursement over the commitment, and whether the disbursement unit also commits.
"""

import numpy as np
import polars as pl
from rapidfuzz.distance import JaroWinkler
from rapidfuzz.process import cpdist

from climate_finance.build.link.candidates import STOPWORDS
from climate_finance.schema import COMPARISON_LEVELS

LEVELS = tuple(COMPARISON_LEVELS.to_schema())
# The long description's leading words the text level compares.
DESCRIPTION_WORDS = 80
# Pairs compared at once. Each batch joins both units' word lists to its pairs, so
# the batch size bounds memory.
BATCH_PAIRS = 500_000
MISSING = -1
# Project numbers and texts that name nothing once cleaned.
_NO_PN = ("notapplicable", "na", "none", "nan", "null", "0", "")
_NO_TEXT = ("not applicable", "na", "none", "n a", "nan", "null")


def unit_attributes(units: pl.DataFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """The fields the levels compare, one row per unit of `units`.

    Args:
        units: the units to describe, in the `units` schema.
        crs: the CRS records the units sum.
    """
    size = pl.col("commitment_usd").abs().fill_null(0) + pl.col(
        "disbursement_usd"
    ).abs().fill_null(0)
    representative = (
        units.select("unit_id", record_id="record_ids")
        .explode("record_id")
        .lazy()
        .join(
            crs.select(
                "record_id",
                "title",
                "short_description",
                "long_description",
                "channel_code",
                "flow_code",
                "finance_t",
                "commitment_usd",
                "disbursement_usd",
            ),
            on="record_id",
        )
        .group_by("unit_id")
        .agg(
            pl.col(
                "title",
                "short_description",
                "long_description",
                "channel_code",
                "flow_code",
                "finance_t",
            )
            .sort_by(size, "record_id", descending=[True, False])
            .first()
        )
        .select(
            "unit_id",
            "channel_code",
            "flow_code",
            "finance_t",
            title=_text(pl.coalesce("title", "short_description")),
            description=_text(pl.col("long_description")),
        )
    )
    return (
        units.lazy()
        .select(
            "unit_id",
            "project_number",
            "recipient_code",
            "purpose_code",
            "year",
            "commitment_usd",
            "disbursement_usd",
            pn_norm=_pn_norm(pl.col("project_number")),
        )
        .join(representative, on="unit_id", how="left")
        .with_columns(
            title_words=_words(pl.col("title")),
            description_words=_words(pl.col("description"), first=DESCRIPTION_WORDS),
        )
        .drop("description")
        .rename({"title": "title_norm"})
        .collect()
    )


def comparison_levels(pairs: pl.DataFrame, *, attributes: pl.DataFrame) -> pl.DataFrame:
    """`pairs` with the ten Int8 levels, `log_ratio` and `later_commit`.

    The year gap, a feature too, is a `candidates` column.

    Args:
        pairs: rows with `left_id` (a disbursement unit) and `right_id` (a
            commitment unit), in any order; the output keeps it.
        attributes: `unit_attributes` of every unit the pairs name.
    """
    levels = [
        _levels(batch, attributes=attributes)
        for batch in pairs.select("left_id", "right_id").iter_slices(BATCH_PAIRS)
    ]
    if not levels:
        levels = [_levels(pairs.select("left_id", "right_id"), attributes=attributes)]
    return pl.concat([pairs, pl.concat(levels)], how="horizontal")


def _levels(pairs: pl.DataFrame, *, attributes: pl.DataFrame) -> pl.DataFrame:
    """The levels and features of `pairs` (left_id, right_id), in their order."""
    sides = pairs.join(
        attributes,
        left_on="left_id",
        right_on="unit_id",
        how="left",
        maintain_order="left",
    ).join(
        attributes,
        left_on="right_id",
        right_on="unit_id",
        how="left",
        suffix="_c",
        maintain_order="left",
    )
    pn_similarity = _jaro_winkler(sides["pn_norm"], sides["pn_norm_c"])
    title_similarity = _jaro_winkler(sides["title_norm"], sides["title_norm_c"])
    levels = sides.with_columns(
        pn_similarity=pn_similarity,
        title_similarity=title_similarity,
        title_jaccard=_jaccard("title_words"),
        description_jaccard=_jaccard("description_words"),
    ).select(
        gamma_project_number=_project_number_level(),
        gamma_text=_text_level(),
        gamma_purpose=_purpose_level(),
        gamma_recipient_code=_equal_level("recipient_code"),
        gamma_channel_code=_equal_level("channel_code"),
        gamma_flow_code=_equal_level("flow_code"),
        gamma_finance_type_code=_equal_level("finance_t"),
        gamma_amount=_amount_level(),
        gamma_later_commitment=_later_commitment_level(),
        gamma_year_gap=_year_gap_level(),
        log_ratio=pl.when(
            (pl.col("commitment_usd_c") > 0) & (pl.col("disbursement_usd") > 0)
        ).then((pl.col("disbursement_usd") / pl.col("commitment_usd_c")).log10()),
        later_commit=(pl.col("commitment_usd") > 0).cast(pl.Int8),
    )
    return levels.with_columns(pl.col(LEVELS).cast(pl.Int8))


def levels_struct() -> pl.Expr:
    """The ten level columns packed into `schema.COMPARISON_LEVELS`."""
    return pl.struct(LEVELS).cast(COMPARISON_LEVELS)


def _jaro_winkler(left: pl.Series, right: pl.Series) -> pl.Series:
    """Jaro-Winkler similarity of each pair of strings; null where either is null.

    Each distinct pair is scored once.
    """
    frame = pl.DataFrame({"a": left, "b": right})
    distinct = frame.drop_nulls().unique(maintain_order=True)
    scores = cpdist(
        distinct["a"].to_list(),
        distinct["b"].to_list(),
        scorer=JaroWinkler.normalized_similarity,
        dtype=np.float64,
        workers=-1,
    )
    scored = distinct.with_columns(similarity=pl.Series(scores, dtype=pl.Float64))
    return frame.join(scored, on=["a", "b"], how="left", maintain_order="left")[
        "similarity"
    ]


def _pn_norm(pn: pl.Expr) -> pl.Expr:
    """`pn` lower-cased with every character other than a to z and digits and the
    leading zeros dropped; null when it names no project or is under three long."""
    norm = (
        pn.str.to_lowercase()
        .str.replace_all("[^a-z0-9]", "")
        .str.strip_chars_start("0")
    )
    return pl.when(~norm.is_in(_NO_PN) & (norm.str.len_chars() >= 3)).then(norm)


def _text(text: pl.Expr) -> pl.Expr:
    """`text` lower-cased, each run of characters other than a to z and digits one
    space, trimmed; null when empty or a placeholder."""
    clean = text.str.to_lowercase().str.replace_all("[^a-z0-9]+", " ").str.strip_chars()
    return pl.when((clean != "") & ~clean.is_in(_NO_TEXT)).then(clean)


def _words(text: pl.Expr, *, first: int | None = None) -> pl.Expr:
    """The distinct words of a cleaned `text` of three letters or more that are not
    stopwords, among its first `first` words when given."""
    words = text.str.split(" ")
    if first is not None:
        words = words.list.head(first)
    return words.list.eval(
        pl.element().filter(
            (pl.element().str.len_chars() >= 3) & ~pl.element().is_in(STOPWORDS)
        )
    ).list.unique(maintain_order=True)


def _jaccard(name: str) -> pl.Expr:
    """Shared words over all words of the two sides' `name` lists; null when either
    is null or both are empty."""
    left, right = pl.col(name), pl.col(f"{name}_c")
    union = left.list.set_union(right).list.len()
    return pl.when(union > 0).then(left.list.set_intersection(right).list.len() / union)


def _either_missing(name: str) -> pl.Expr:
    return pl.col(name).is_null() | pl.col(f"{name}_c").is_null()


def _project_number_level() -> pl.Expr:
    same = pl.col("project_number") == pl.col("project_number_c")
    return (
        pl.when(_either_missing("pn_norm"))
        .then(MISSING)
        .when(same.fill_null(value=False))
        .then(3)
        .when(pl.col("pn_norm") == pl.col("pn_norm_c"))
        .then(2)
        .when(pl.col("pn_similarity") >= 0.92)
        .then(1)
        .otherwise(0)
    )


def _text_level() -> pl.Expr:
    title = pl.col("title_norm") == pl.col("title_norm_c")
    titles, descriptions = pl.col("title_jaccard"), pl.col("description_jaccard")
    similarity = pl.col("title_similarity")
    no_text = pl.col("title_norm").is_null() & pl.col("description_words").is_null()
    no_text_c = (
        pl.col("title_norm_c").is_null() & pl.col("description_words_c").is_null()
    )
    return (
        pl.when(no_text | no_text_c)
        .then(MISSING)
        .when(
            title
            & (_either_missing("description_words") | (descriptions >= 0.7)).fill_null(
                value=False
            )
        )
        .then(5)
        .when(title)
        .then(4)
        .when((similarity >= 0.95) | (titles >= 0.6) | (descriptions >= 0.7))
        .then(3)
        .when((similarity >= 0.85) | (titles >= 0.3) | (descriptions >= 0.4))
        .then(2)
        .when(descriptions >= 0.2)
        .then(1)
        .otherwise(0)
    )


def _purpose_level() -> pl.Expr:
    return (
        pl.when(_either_missing("purpose_code"))
        .then(MISSING)
        .when(pl.col("purpose_code") == pl.col("purpose_code_c"))
        .then(2)
        .when(pl.col("purpose_code") // 100 == pl.col("purpose_code_c") // 100)
        .then(1)
        .otherwise(0)
    )


def _equal_level(name: str) -> pl.Expr:
    return (
        pl.when(_either_missing(name))
        .then(MISSING)
        .when(pl.col(name) == pl.col(f"{name}_c"))
        .then(1)
        .otherwise(0)
    )


def _amount_level() -> pl.Expr:
    commitment = pl.col("commitment_usd_c")
    ratio = pl.col("disbursement_usd") / commitment
    return (
        pl.when(commitment.is_null() | (commitment <= 0))
        .then(MISSING)
        .when(ratio.is_between(0.5, 1.0))
        .then(4)
        .when((ratio >= 0.1) & (ratio < 0.5))
        .then(3)
        .when(ratio < 0.1)
        .then(2)
        .when(ratio <= 1.1)
        .then(1)
        .otherwise(0)
    )


def _later_commitment_level() -> pl.Expr:
    commitment, disbursement = pl.col("commitment_usd"), pl.col("disbursement_usd")
    return (
        pl.when(commitment.is_null() | (commitment <= 0))
        .then(2)
        .when((commitment - disbursement).abs() <= 0.01 * disbursement)
        .then(1)
        .otherwise(0)
    )


def _year_gap_level() -> pl.Expr:
    gap = pl.col("year") - pl.col("year_c")
    return pl.when(gap == 1).then(2).when(gap == 2).then(1).otherwise(0)
