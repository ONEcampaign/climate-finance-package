"""Candidate pairs for the scoring stage, and the programme stems that mark siblings.

A candidate pairs a disbursement unit (left, as in `trace` links) with a commitment
unit a CRDF row reaches (right) of the same donor and an earlier record year, when
one of four blocking rules holds: the same recipient and purpose; the same project
number, held by at most `MAX_PN_UNITS` of the donor's disbursement units from years
after the commitment's; the same title, held by at most `MAX_TITLE_UNITS` of them; or
the same recipient and the same first two title words.

A pair is eligible for scoring when the keys left both units unlinked: no exact tier
or key-repair rule took the disbursement, no disbursement of a later record year
reaches the commitment by key, the disbursement's floor year is at or before the
commitment's placed year, and the ids of neither unit's agency persist in its record
year's period. Where an agency's ids persist, the id is the only evidence, and a
later record under another id does not continue the commitment. Those agencies'
commitments carry the id truth instead: a pair is a true continuation when the two
units share a crs id. The table keeps eligible pairs and the pairs of commitments
whose agency is `id_persistent`, which train and test the truth-group classifier.

A programme stem maps a component's project number to its programme's. A project
number's shape replaces each digit run by 9 and each letter run by A, so
7F-10454.01.03 has shape 9A-9.9.9. A rule says, for one (donor, agency, shape), how
many trailing segments to strip: one maps 7F-10454.01.03 to 7F-10454.01. Rules are
relearned from the CRS each release. A candidate rule merges, within (donor, agency,
stem, recipient), the numbers it strips with each other and with a number equal to
the stem. A merged pair is consistent when the two titles share at least
`MIN_SIMILARITY` of their words (Jaccard). A rule needs `MIN_PAIRS` merged pairs and
at most `MAX_ERROR` of them inconsistent, and per shape the passing segment count
with the lowest error wins, the smaller count on ties. A stem that gathers more than
`MAX_FANOUT` project numbers over all recipients is a budget line or portfolio code,
and every pair it merges counts as inconsistent. Two units with different project
numbers and the same programme key are siblings (`programme_level` 1).
"""

import polars as pl

from climate_finance.build.link.id_practice import ID_PERSISTENT_CLASSES, period_of
from climate_finance.build.link.keys import normalise_pn

MAX_PN_UNITS = 300
MAX_TITLE_UNITS = 100
STEM_SEGMENTS = (1, 2)
MIN_PAIRS = 20
MAX_ERROR = 0.10
MAX_FANOUT = 50
MIN_SIMILARITY = 0.5
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
# Blocking rules: a key the two units share, and the most later disbursement units
# of the donor the key may hold for a commitment to pair through it.
BLOCKING_RULES: tuple[tuple[tuple[str, ...], int | None], ...] = (
    (("recipient_code", "purpose_code"), None),
    (("pn_block",), MAX_PN_UNITS),
    (("title_block",), MAX_TITLE_UNITS),
    (("recipient_code", "title_head"), None),
)
# Project numbers and titles that name no project once lower-cased and stripped.
_NO_PN = ("notapplicable", "na", "none", "nan", "null", "0", "")
_NO_TITLE = ("not applicable", "na", "none", "n a", "nan", "null")
_SEPARATORS = ".-/_&: "
_LAST_SEGMENT = r"[.\-/_&: ]+[A-Z0-9]+$"
_AGENCY = ("donor_code", "agency_code")


def project_shape(pn: pl.Expr) -> pl.Expr:
    """A project number with each digit run replaced by 9 and each letter run by A."""
    return pn.str.replace_all("[0-9]+", "9").str.replace_all("[A-Z]+", "A")


def strip_segments(pn: pl.Expr, segments: int) -> pl.Expr:
    """`pn` without its last `segments` segments and their separators; null when
    fewer segments remain to strip or none would be left."""
    stem = pn.str.strip_chars_end(_SEPARATORS)
    for _ in range(segments):
        cut = stem.str.replace(_LAST_SEGMENT, "")
        stem = pl.when(cut != stem).then(cut)
    return pl.when(stem.str.contains("[A-Z0-9]")).then(stem)


def title_words(title: pl.Expr) -> pl.Expr:
    """The distinct words of `title` that can tell projects apart, in order."""
    return (
        _clean(title)
        .str.split(" ")
        .list.eval(
            pl.element().filter(
                (pl.element().str.len_chars() >= 3) & ~pl.element().is_in(STOPWORDS)
            )
        )
        .list.unique(maintain_order=True)
    )


def learn_programme_stems(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Programme stem rules per (donor, agency) from `crs`, in the `programme_stems`
    schema.

    `crs` holds the records the trace follows. Each (donor, agency, project number,
    recipient) takes the title of its largest record.
    """
    projects = (
        crs.with_columns(project_number=normalise_pn(pl.col("project_number")))
        .drop_nulls(["agency_code", "project_number"])
        .group_by(*_AGENCY, "project_number", "recipient_code")
        .agg(_largest_title())
        .with_columns(words=title_words(pl.col("title")))
        .drop("title")
    )
    scores = pl.concat(
        _score_stems(projects, segments=segments) for segments in STEM_SEGMENTS
    )
    return (
        scores.group_by(*_AGENCY, "pattern", "segments")
        .agg(pl.col("pairs", "consistent_pairs").sum())
        .with_columns(error=1 - pl.col("consistent_pairs") / pl.col("pairs"))
        .filter(pl.col("pairs") >= MIN_PAIRS, pl.col("error") <= MAX_ERROR)
        .sort(*_AGENCY, "pattern", "error", "segments")
        .unique([*_AGENCY, "pattern"], keep="first", maintain_order=True)
        .select(
            *_AGENCY,
            "pattern",
            pl.col("segments").cast(pl.Int8),
            pl.col("pairs", "consistent_pairs").cast(pl.Int32),
        )
    )


def _score_stems(projects: pl.LazyFrame, *, segments: int) -> pl.LazyFrame:
    """Merged and consistent pair counts of every candidate rule that strips
    `segments` segments, per (donor, agency, pattern, stem, recipient)."""
    group = [*_AGENCY, "pattern", "stem", "recipient_code"]
    stripped = projects.with_columns(
        stem=strip_segments(pl.col("project_number"), segments),
        pattern=project_shape(pl.col("project_number")),
        stripped=pl.lit(value=True),
    ).drop_nulls("stem")
    # A number equal to a stem joins that stem's groups unstripped.
    own = projects.join(
        stripped.select(group).unique(),
        left_on=[*_AGENCY, "project_number", "recipient_code"],
        right_on=[*_AGENCY, "stem", "recipient_code"],
    ).with_columns(stem=pl.col("project_number"), stripped=pl.lit(value=False))
    nodes = (
        pl.concat(
            frame.select(*group, "project_number", "stripped", "words")
            for frame in (stripped, own)
        )
        .group_by(*group, "project_number")
        .agg(pl.col("stripped").any(), pl.col("words").first())
        .with_columns(fanout=pl.col("project_number").n_unique().over(group[:-1]))
    )
    # Every pair under a wide stem counts as inconsistent, so it is counted, not built.
    members = pl.len().cast(pl.Int64)
    unstripped = members - pl.col("stripped").sum()
    wide = (
        nodes.filter(pl.col("fanout") > MAX_FANOUT)
        .group_by(group)
        .agg(
            pairs=members * (members - 1) // 2 - unstripped * (unstripped - 1) // 2,
            consistent_pairs=pl.lit(0, dtype=pl.Int64),
        )
    )
    narrow = nodes.filter(pl.col("fanout") <= MAX_FANOUT).drop("fanout")
    built = (
        narrow.join(narrow, on=group, suffix="_other")
        .filter(
            pl.col("project_number") < pl.col("project_number_other"),
            pl.col("stripped") | pl.col("stripped_other"),
        )
        .group_by(group)
        .agg(
            pairs=pl.len().cast(pl.Int64),
            consistent_pairs=(
                _jaccard(pl.col("words"), pl.col("words_other")) >= MIN_SIMILARITY
            )
            .sum()
            .cast(pl.Int64),
        )
    )
    return pl.concat([wide, built]).with_columns(segments=pl.lit(segments))


def with_programme(units: pl.LazyFrame, *, stems: pl.LazyFrame) -> pl.LazyFrame:
    """`units` with `programme`: the project number's stem under its (donor, agency,
    shape)'s rule, else the project number itself; null without a project number."""
    pn = pl.col("project_number")
    stem = pl.coalesce(
        *(
            pl.when(pl.col("segments") == segments).then(strip_segments(pn, segments))
            for segments in STEM_SEGMENTS
        ),
        pn,
    )
    return (
        units.with_columns(pattern=project_shape(pn))
        .join(
            stems.select(*_AGENCY, "pattern", "segments"),
            on=[*_AGENCY, "pattern"],
            how="left",
            maintain_order="left",
        )
        .with_columns(programme=stem)
        .drop("pattern", "segments")
    )


def generate_candidates(
    units: pl.DataFrame,
    *,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    stems: pl.DataFrame,
    first_year: int,
) -> pl.DataFrame:
    """Blocked unit pairs, in the `candidates` schema.

    Args:
        units: the link stage's units.
        crs: the CRS records the units sum, for their titles.
        links: the link stage's `trace` and `crdf_crs` links so far. Commitments
            are the units `crdf_crs` links reach; `trace` links say which units the
            keys linked.
        id_practice: each agency's class per period.
        stems: the programme stem rules.
        first_year: the first CRS year the build loads. A disbursement whose floor
            precedes it continues a commitment the build cannot see.
    """
    indexed = units.with_row_index("unit")
    reached = links.filter(task="crdf_crs").select(unit_id="right_id")
    commitments = indexed.join(reached, on="unit_id", how="semi")
    disbursements = indexed.filter(
        pl.col("disbursement_usd") > 0,
        pl.col("floor_year").is_null() | (pl.col("floor_year") >= first_year),
        pl.col("donor_code").is_in(commitments["donor_code"].unique().implode()),
    )
    sides = _sides(
        pl.concat([commitments, disbursements]).unique("unit"),
        crs=crs,
        stems=stems,
        links=links,
        id_practice=id_practice,
    )
    commitments = sides.join(commitments.select("unit"), on="unit", how="semi")
    disbursements = sides.join(disbursements.select("unit"), on="unit", how="semi")
    truth = _block(
        commitments.filter("id_persistent"), disbursements, reference=disbursements
    )
    open_pairs = _block(
        commitments.filter(~pl.col("keyed_later"), ~pl.col("ids_persist")),
        disbursements.filter(~pl.col("taken"), ~pl.col("ids_persist")),
        reference=disbursements,
    )
    pairs = pl.concat(
        [truth.with_columns(eligible=False), open_pairs.with_columns(eligible=True)]
    )
    same = {
        name: pl.col(name).is_not_null() & (pl.col(name) == pl.col(f"{name}_right"))
        for name in ("crs_id", "project_number", "programme")
    }
    programme_level = (
        pl.when(same["project_number"])
        .then(2)
        .when(same["programme"])
        .then(1)
        .otherwise(0)
        .cast(pl.Int8)
    )
    floor_held = pl.col("floor_year").is_null() | (
        pl.col("placed_year_right") >= pl.col("floor_year")
    )
    side = sides.select(
        "unit",
        "unit_id",
        "donor_code",
        "year",
        "placed_year",
        "floor_year",
        "crs_id",
        "project_number",
        "programme",
        "id_persistent",
    )
    return (
        pairs.join(side, left_on="left", right_on="unit")
        .join(side, left_on="right", right_on="unit", suffix="_right")
        .filter(~pl.col("eligible") | floor_held)
        .select(
            left_id="unit_id",
            right_id="unit_id_right",
            donor_code="donor_code",
            year_gap=(pl.col("year") - pl.col("year_right")).cast(pl.Int16),
            programme_level=programme_level,
            same_crs_id=same["crs_id"],
            id_persistent="id_persistent_right",
            eligible="eligible",
        )
    )


def _sides(
    units: pl.DataFrame,
    *,
    crs: pl.LazyFrame,
    stems: pl.DataFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
) -> pl.DataFrame:
    """Each unit's blocking keys, its programme key, and what the keys and its
    agency's id practice say of it.

    A unit's title is its largest record's. `taken` marks a disbursement a key
    linked, and `keyed_later` a commitment a key reaches from a later record year.
    `ids_persist` holds when the agency's ids persist in the record year's period,
    and `id_persistent` when its class is `id_persistent`.
    """
    titles = (
        units.select("unit", record_id="record_ids")
        .explode("record_id")
        .lazy()
        .join(
            crs.select("record_id", "title", "commitment_usd", "disbursement_usd"),
            on="record_id",
        )
        .group_by("unit")
        .agg(_largest_title())
    )
    classes = id_practice.lazy().select(
        *_AGENCY,
        "period",
        ids_persist=pl.col("id_class").is_in(ID_PERSISTENT_CLASSES),
        id_persistent=pl.col("id_class") == "id_persistent",
    )
    trace = links.filter(task="trace").select("left_id", "right_id")
    years = units.select("unit_id", "year")
    keyed_later = (
        trace.join(years, left_on="left_id", right_on="unit_id")
        .join(years, left_on="right_id", right_on="unit_id", suffix="_commitment")
        .filter(pl.col("year") > pl.col("year_commitment"))
        .get_column("right_id")
    )
    title = _clean(pl.col("title")).str.replace_all(" +", " ")
    pn = (
        pl.col("project_number")
        .str.to_lowercase()
        .str.replace_all("[^a-z0-9]", "")
        .str.strip_chars_start("0")
    )
    words = title_words(pl.col("title"))
    return (
        with_programme(
            units.lazy().select(
                "unit",
                "unit_id",
                *_AGENCY,
                "crs_id",
                "project_number",
                "recipient_code",
                "purpose_code",
                "year",
                "placed_year",
                "floor_year",
            ),
            stems=stems.lazy(),
        )
        .join(titles, on="unit", how="left")
        .with_columns(
            pn_block=pl.when(~pn.is_in(_NO_PN) & (pn.str.len_chars() >= 3)).then(pn),
            title_block=pl.when(~title.is_in(_NO_TITLE)).then(title),
            title_head=pl.when(words.list.len() >= 2).then(
                words.list.head(2).list.join(" ")
            ),
            taken=pl.col("unit_id").is_in(trace["left_id"].implode()),
            keyed_later=pl.col("unit_id").is_in(keyed_later.implode()),
            period=period_of(pl.col("year")),
        )
        .join(classes, on=[*_AGENCY, "period"], how="left")
        .with_columns(pl.col("ids_persist", "id_persistent").fill_null(value=False))
        .drop("title", "period")
        .collect()
    )


def _block(
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
    *,
    reference: pl.DataFrame,
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs of one donor, the disbursement of a
    later record year, under any blocking rule.

    Under a capped rule a commitment pairs only when its key holds at most the cap of
    the `reference` disbursement units of later years, so a common project number or
    title adds no pairs.
    """
    blocked = []
    for rule, cap in BLOCKING_RULES:
        key = ["donor_code", *rule]
        left = commitments.drop_nulls(rule).select("unit", "year", *key)
        if cap is not None:
            narrow = _narrow_keys(left, reference.drop_nulls(rule), key=key, cap=cap)
            left = left.join(narrow, on=[*key, "year"], how="semi")
        blocked.append(
            left.join(
                disbursements.drop_nulls(rule).select("unit", "year", *key),
                on=key,
                suffix="_disbursement",
            )
            .filter(pl.col("year_disbursement") > pl.col("year"))
            .select(left="unit_disbursement", right="unit")
        )
    return pl.concat(blocked).unique()


def _narrow_keys(
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
    *,
    key: list[str],
    cap: int,
) -> pl.DataFrame:
    """The (key, year) of `commitments` whose key holds at most `cap` disbursement
    units of later years."""
    later = (
        disbursements.group_by(*key, "year")
        .len("units")
        .join(commitments.select(*key, "year").unique(), on=key, suffix="_commitment")
        .filter(pl.col("year") > pl.col("year_commitment"))
        .group_by(*key, "year_commitment")
        .agg(pl.col("units").sum())
    )
    return later.filter(pl.col("units") <= cap).select(*key, year="year_commitment")


def _largest_title() -> pl.Expr:
    """The title of the group's largest record by commitment plus disbursement,
    ties to the smaller record id."""
    size = pl.col("commitment_usd").abs().fill_null(0) + pl.col(
        "disbursement_usd"
    ).abs().fill_null(0)
    return pl.col("title").sort_by(size, "record_id", descending=[True, False]).first()


def _clean(text: pl.Expr) -> pl.Expr:
    """`text` lower-cased, every run of characters other than a to z and digits a
    space, trimmed."""
    return text.str.to_lowercase().str.replace_all("[^a-z0-9]+", " ").str.strip_chars()


def _jaccard(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Shared words over all words of two word lists; null when either is null or
    both are empty."""
    union = left.list.set_union(right).list.len()
    return pl.when(union > 0).then(left.list.set_intersection(right).list.len() / union)
