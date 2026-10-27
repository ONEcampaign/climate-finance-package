"""Comparison levels and pair features for the scoring models.

`comparisons()` defines the ten levels once, as Splink comparisons. The Splink models
fit and score with them, and `comparison_levels` evaluates the same SQL conditions
for the truth-group classifier, so a pair's `levels` mean the same in `links` and in
`pair_scores`. Splink numbers each comparison's levels by their order: -1 for the
null level, else from the first level's count down to 0. In Splink's terms the
commitment is the left record (`_l`) and the disbursement the right (`_r`). In
`candidates` and `trace` links the disbursement unit is `left_id`.

A unit reads its fields from its representative record, the largest by absolute
commitment plus disbursement, ties to the smaller record id. The levels:

- project number, read from the unit's number (`keys.normalise_pn`, so spaces, a
  trailing `.0`, placeholders and rounded numbers decide nothing): 4 different
  numbers under one programme (siblings), -1 where either side's number is missing
  or under three characters once lower-cased with every character other than a to
  z and digits and leading zeros dropped, 3 the same number, 2 the same in that
  lower-cased form, 1 Jaro-Winkler at least 0.92 on it, else 0.
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
- year gap: 3 one year, 2 two years, 1 three or more, else 0.

The features beside the levels are the year gap (a `candidates` column), log10 of
the disbursement over the commitment, whether the disbursement unit also commits,
and whether its start and completion dates equal the commitment's. A record that
continues a project usually repeats them, and a new activity carries its own. The
commitment date is left out because, where an agency's ids persist, a disbursement
record repeats its line's commitment date, so on the training pairs an equal date
restates the crs-id label and misleads the model on the pairs it links.
"""

from collections.abc import Mapping

import duckdb
import polars as pl
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink.internals.comparison_level_creator import ComparisonLevelCreator

from climate_finance.build.link.candidates import pn_block, with_programme
from climate_finance.build.link.text import (
    representative,
    text_norm,
    text_words,
    title_norm,
)
from climate_finance.schema import COMPARISON_LEVELS

LEVELS = tuple(COMPARISON_LEVELS.to_schema())
# The long description's leading words the text level compares.
DESCRIPTION_WORDS = 80
# Pairs compared at once, which bounds the memory a batch takes.
BATCH_PAIRS = 500_000
_RATIO = "disbursement_usd_r / commitment_usd_l"
# The date features, each as SQL over a pair. A comparison with a null date is null.
_DATE_SQL = {
    "same_start": "(expected_start_date_r = expected_start_date_l)::tinyint",
    "same_completion": "(completion_date_r = completion_date_l)::tinyint",
}
DATE_FEATURES = tuple(_DATE_SQL)
# The `unit_attributes` columns that `comparisons()` and `_DATE_SQL` read.
COMPARISON_COLUMNS = (
    "unique_id",
    "pn",
    "prog",
    "pn_norm",
    "title_norm",
    "title_toks",
    "desc_norm",
    "desc_toks",
    "purpose_code",
    "purpose3",
    "recipient_code",
    "channel_code",
    "flow_code",
    "finance_type_code",
    "commitment_usd",
    "disbursement_usd",
    "record_year",
    "expected_start_date",
    "completion_date",
)


def unit_attributes(
    units: pl.DataFrame, *, crs: pl.LazyFrame, stems: pl.DataFrame
) -> pl.DataFrame:
    """Each unit's fields for the comparison levels and for Jev.

    A unit takes its fields from its largest record by commitment plus
    disbursement, ties to the smaller record id, and its title falls back to that
    record's short description. `pn` is the unit's normalised project number and
    `prog` its programme key. `pn_norm` (from `pn`), `title_norm` and `desc_norm`
    are lower-cased with punctuation dropped, and the token lists hold the words
    that tell projects apart.

    Args:
        units: the units to describe, in the `units` schema.
        crs: the CRS records the units sum.
        stems: the programme stem rules.
    """
    fields = (
        "project_number",
        "title",
        "short_description",
        "long_description",
        "channel_code",
        "flow_code",
        "finance_t",
        "expected_start_date",
        "completion_date",
    )
    return (
        with_programme(units.lazy().drop("record_ids"), stems=stems.lazy())
        .join(
            representative(
                units,
                crs=crs,
                fields=[pl.col(name).alias(f"rep_{name}") for name in fields],
            ),
            on="unit_id",
            how="left",
        )
        .with_columns(
            pn_norm=pn_block(pl.col("project_number")),
            title_norm=title_norm(),
            desc_norm=text_norm(pl.col("rep_long_description")),
        )
        .with_columns(
            title_toks=text_words(pl.col("title_norm")),
            desc_toks=text_words(pl.col("desc_norm"), first=DESCRIPTION_WORDS),
        )
        .select(
            unique_id="unit_id",
            donor_code="donor_code",
            provider="donor_code",
            recipient_code="recipient_code",
            purpose_code="purpose_code",
            purpose3=pl.col("purpose_code") // 100,
            channel_code="rep_channel_code",
            flow_code="rep_flow_code",
            finance_type_code="rep_finance_t",
            year="year",
            record_year="year",
            expected_start_date="rep_expected_start_date",
            completion_date="rep_completion_date",
            commitment_usd="commitment_usd",
            disbursement_usd="disbursement_usd",
            pn="project_number",
            prog="programme",
            pn_norm="pn_norm",
            title_norm="title_norm",
            title_toks="title_toks",
            desc_norm="desc_norm",
            desc_toks="desc_toks",
            project_number="rep_project_number",
            title="rep_title",
            short_description="rep_short_description",
            long_description="rep_long_description",
        )
        .sort("unique_id")
        .collect()
    )


def comparisons(
    similarity: Mapping[str, str] | None = None,
) -> list[cl.CustomComparison | cl.ExactMatch]:
    """The ten comparisons of `schema.COMPARISON_LEVELS` as Splink comparisons.

    The project number comparison's first level marks siblings: different project
    numbers under one programme. Title and description form one comparison, so a
    description that repeats its title counts once. The year gap reads
    `record_year`, a copy of `year`: every Splink EM blocking rule reads `year`,
    and EM leaves out the comparisons that read a blocking column.

    Args:
        similarity: the SQL the text comparison reads each similarity of
            `similarities()` as. By default the similarity's own expression, as
            Splink needs; `comparison_levels` passes the names of columns that
            hold them.
    """
    sim = similarities() if similarity is None else similarity
    description_agrees = (
        f"(desc_norm_l is null or desc_norm_r is null or {sim['desc_jaccard']} >= 0.7)"
    )
    return [
        cl.CustomComparison(
            output_column_name="project_number",
            comparison_levels=[
                cll.CustomLevel(
                    "pn_l <> pn_r and prog_l = prog_r",
                    label_for_charts="same programme, other number",
                ),
                cll.NullLevel("pn_norm"),
                cll.ExactMatchLevel("pn").configure(
                    tf_adjustment_column="pn_norm", tf_adjustment_weight=1.0
                ),
                cll.ExactMatchLevel("pn_norm", term_frequency_adjustments=True),
                cll.JaroWinklerLevel("pn_norm", 0.92),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="text",
            comparison_levels=[
                cll.CustomLevel(
                    "title_norm_l is null and desc_norm_l is null "
                    "or title_norm_r is null and desc_norm_r is null"
                ).configure(is_null_level=True),
                _title_level(
                    f"title_norm_l = title_norm_r and {description_agrees}",
                    "title exact, description agrees or missing",
                ),
                _title_level(
                    "title_norm_l = title_norm_r", "title exact, description differs"
                ),
                cll.CustomLevel(
                    f"{sim['title_jaro_winkler']} >= 0.95 "
                    f"or {sim['title_jaccard']} >= 0.6 "
                    f"or {sim['desc_jaccard']} >= 0.7",
                    label_for_charts="title close or description close",
                ),
                cll.CustomLevel(
                    f"{sim['title_jaro_winkler']} >= 0.85 "
                    f"or {sim['title_jaccard']} >= 0.3 "
                    f"or {sim['desc_jaccard']} >= 0.4",
                    label_for_charts="title or description similar",
                ),
                cll.CustomLevel(
                    f"{sim['desc_jaccard']} >= 0.2",
                    label_for_charts="description overlaps",
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="purpose",
            comparison_levels=[
                cll.NullLevel("purpose_code"),
                cll.ExactMatchLevel("purpose_code"),
                cll.ExactMatchLevel("purpose3"),
                cll.ElseLevel(),
            ],
        ),
        cl.ExactMatch("recipient_code"),
        cl.ExactMatch("channel_code"),
        cl.ExactMatch("flow_code"),
        cl.ExactMatch("finance_type_code"),
        cl.CustomComparison(
            output_column_name="amount",
            comparison_levels=[
                cll.CustomLevel(
                    "commitment_usd_l is null or commitment_usd_l <= 0"
                ).configure(is_null_level=True),
                cll.CustomLevel(
                    f"{_RATIO} between 0.5 and 1.0", label_for_charts="0.5 to 1.0"
                ),
                cll.CustomLevel(
                    f"{_RATIO} >= 0.1 and {_RATIO} < 0.5", label_for_charts="0.1 to 0.5"
                ),
                cll.CustomLevel(f"{_RATIO} < 0.1", label_for_charts="under 0.1"),
                cll.CustomLevel(f"{_RATIO} <= 1.1", label_for_charts="1.0 to 1.1"),
                cll.ElseLevel(),
            ],
        ),
        # A later record whose own commitment equals its disbursement looks like a
        # new annual activity.
        cl.CustomComparison(
            output_column_name="later_commitment",
            comparison_levels=[
                cll.CustomLevel(
                    "commitment_usd_r is null or commitment_usd_r <= 0",
                    label_for_charts="later record has no commitment",
                ),
                cll.CustomLevel(
                    "abs(commitment_usd_r - disbursement_usd_r) "
                    "<= 0.01 * disbursement_usd_r",
                    label_for_charts="later commitment equals disbursement",
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="year_gap",
            comparison_levels=[
                cll.CustomLevel(
                    "record_year_r - record_year_l = 1", label_for_charts="gap 1"
                ),
                cll.CustomLevel(
                    "record_year_r - record_year_l = 2", label_for_charts="gap 2"
                ),
                cll.CustomLevel(
                    "record_year_r - record_year_l >= 3",
                    label_for_charts="gap 3 or more",
                ),
                cll.ElseLevel(),
            ],
        ),
    ]


def comparison_levels(pairs: pl.DataFrame, *, attributes: pl.DataFrame) -> pl.DataFrame:
    """`pairs` with the ten Int8 levels, `log_ratio`, `later_commit` and the date
    features.

    Each level is the first of its comparison's conditions in `comparisons()` that
    holds, numbered as Splink numbers it. The year gap, a feature too, is a
    `candidates` column. The date features compare the two units' representative
    records: `same_start` and `same_completion` are 1 when the dates match, else 0,
    and null where either date is missing.

    Args:
        pairs: rows with `left_id` (a disbursement unit) and `right_id` (a
            commitment unit), in any order, and the output keeps it.
        attributes: `unit_attributes` of every unit the pairs name.
    """
    sides = {
        side: ", ".join(f'"{name}" as "{name}_{side}"' for name in COMPARISON_COLUMNS)
        for side in "lr"
    }
    # Each similarity is computed once per pair, where the levels' conditions would
    # evaluate it again in every `when` a pair reaches.
    computed = ", ".join(f"{sql} as {name}" for name, sql in similarities().items())
    query = f"""
        with joined as (
            select p.pair_row, _commitments.*, _disbursements.*
            from _batch p
            left join _commitments on unique_id_l = p.right_id
            left join _disbursements on unique_id_r = p.left_id
        ),
        compared as (select *, {computed} from joined)
        select
            {", ".join(_level_cases())},
            case when commitment_usd_l > 0 and disbursement_usd_r > 0
                then log10(disbursement_usd_r / commitment_usd_l) end as log_ratio,
            (commitment_usd_r > 0)::tinyint as later_commit,
            {", ".join(f"{sql} as {name}" for name, sql in _DATE_SQL.items())}
        from compared
        order by pair_row
    """
    keys = pairs.select("left_id", "right_id").with_row_index("pair_row")
    batches = list(keys.iter_slices(BATCH_PAIRS)) or [keys]
    levels = []
    with duckdb.connect() as con:
        con.register("_attributes", attributes.select(COMPARISON_COLUMNS))
        con.sql(f"create table _commitments as select {sides['l']} from _attributes")
        con.sql(f"create table _disbursements as select {sides['r']} from _attributes")
        for batch in batches:
            # DuckDB scans a registered frame as one task, and a table on a thread
            # per row group. Each level is a function of its own pair, and the query
            # orders by pair, so the thread count changes no value or row.
            con.register("_pairs", batch)
            con.sql("create or replace table _batch as select * from _pairs")
            con.unregister("_pairs")
            levels.append(con.sql(query).pl())
    return pl.concat(
        [
            pairs,
            pl.concat(levels).select(
                *LEVELS, "log_ratio", "later_commit", *DATE_FEATURES
            ),
        ],
        how="horizontal",
    )


def levels_struct() -> pl.Expr:
    """The ten level columns packed into `schema.COMPARISON_LEVELS`."""
    return pl.struct(LEVELS).cast(COMPARISON_LEVELS)


def similarities() -> dict[str, str]:
    """The similarities the text comparison reads, by name, each as SQL over a
    pair."""
    return {
        "title_jaro_winkler": "jaro_winkler_similarity(title_norm_l, title_norm_r)",
        "title_jaccard": _jaccard("title_toks"),
        "desc_jaccard": _jaccard("desc_toks"),
    }


def _level_cases() -> list[str]:
    """Each comparison as an SQL case giving its level, named `gamma_<comparison>`,
    reading the similarities from the columns of their names."""
    cases = []
    for creator in comparisons({name: name for name in similarities()}):
        comparison = creator.get_comparison("duckdb")
        whens = " ".join(
            f"else {level.comparison_vector_value}"
            if level.sql_condition.strip().upper() == "ELSE"
            else f"when {level.sql_condition} then {level.comparison_vector_value}"
            for level in comparison.comparison_levels
        )
        cases.append(
            f"case {whens} end::tinyint as gamma_{comparison.output_column_name}"
        )
    return cases


def _jaccard(column: str) -> str:
    """Token-set Jaccard similarity of a token-list column, in Splink's SQL.

    The lists hold distinct words (`text.text_words`), so the union counts
    both lists less what they share. DuckDB's `list_intersect` gives null for a null
    left list and an empty list for a null right one, and a null list counts no
    words, so the value equals the shared words over the distinct words of both
    lists in every case.
    """
    shared = f"len(list_intersect({column}_l, {column}_r))"
    return (
        f"{shared}::double / nullif(coalesce(len({column}_l), 0)"
        f" + coalesce(len({column}_r), 0) - {shared}, 0)"
    )


def _title_level(sql: str, label: str) -> ComparisonLevelCreator:
    return cll.CustomLevel(sql, label_for_charts=label).configure(
        tf_adjustment_column="title_norm", tf_adjustment_weight=1.0
    )
