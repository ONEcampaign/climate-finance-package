"""Candidate pairs for the scoring stage, and the programme stems that mark siblings.

A candidate pairs a disbursement unit (left, as in `trace` links) with a commitment
unit a CRDF row reaches (right) of the same donor and an earlier record year, when
one of four blocking rules holds: the same recipient and purpose; the same project
number, held by at most `MAX_PN_UNITS` of the donor's disbursement units from years
after the commitment's; the same title, held by at most `MAX_TITLE_UNITS` of them; or
the same recipient and the same first two title words. At `RENUMBERED_DONORS` a
fifth rule follows a project number to the crs ids it shares a record with: a
disbursement pairs with the commitments of each such crs id's line (`line_id`),
under the project-number rule's cap. A renumbered project's records under its old
crs id carry its new number, so a later disbursement under a new crs id and the new
number reaches the commitment that carries neither. With an embedder, a disbursement
also pairs with a commitment that is its embedding neighbour (`embed`). A unit's
title is its largest record's title, or that record's short description where it
has none.

A disbursement pairs once with a purpose-split line, which a donor files as one unit
per purpose, as Canada and the Adaptation Fund do (`keys.purpose_units`). The pair
takes the unit of the disbursement's purpose code. Where no CRDF row reaches that
unit, the disbursement continues no climate commitment on the line and keeps no pair
there. Where the line has no unit of that code, the pair takes the reached unit of
its purpose category, else the line's largest reached commitment. A pair whose unit
is outside the commitments it was blocked among, as a unit a key reaches from a
later record year, is left out.

A pair is eligible for scoring when the keys left both units unlinked: no exact tier
or key-repair rule took the disbursement, no disbursement of a later record year
reaches the commitment by key, the disbursement's floor year is at or before the
commitment's placed year, and the ids of neither unit's agency persist in its record
year's period. Where an agency's ids persist, the id is the only evidence, and a
later record under another id does not continue the commitment. Those agencies'
commitments carry the id truth. A pair is a true continuation when the two units
share a crs id. The table keeps eligible pairs and the pairs of commitments
whose agency is `id_persistent`, which train and test the truth-group classifier.

A template line enters no candidate pair, eligible or training (`template_lines`). A
template line is a unit with no project number whose title its donor gives, with no
project number, to units of at least `TEMPLATE_RECIPIENTS` recipients in at least
`TEMPLATE_YEARS` record years. Such a title names a reporting category. Examples are the
New Development Bank's "Aggregate" line per country and year, which sums every project
the bank funds there, a credit-line or "aggregated disbursements" line, and a sector or
purpose name. An aggregate sums several activities, so no link to one commitment is
right, and a category title says nothing of which activity a line continues. A title is
no template when at least `TEMPLATE_IDENTITY_UNITS` of its units, and
`TEMPLATE_IDENTITY_SHARE` of them, are id-persistent units whose crs id an id-persistent
unit of the donor carries in another record year. It names one activity per country, as
Japan's "The Economic and Social Development Programme" does. Key tiers and repair rules
run before candidates and keep any link they give such a line. On the archive, 701
template titles hold 244,740 units. They leave out 6,346 of 1,346,917 eligible pairs,
from 37 titles of 10 providers, and 5,827,107 of 7,213,658 training pairs, none of them
true and 5,813,108 of them Japan's "TC aggregated activities". Three recipients would
also catch regional road corridors.

A programme stem maps a component's project number to its programme's. A project
number's shape replaces each digit run by 9 and each letter run by A, so 7F-10454.01.03
has shape 9A-9.9.9. A rule says, for one (donor, agency, shape), how many trailing
segments to strip. A count of one maps 7F-10454.01.03 to 7F-10454.01. Rules are
relearned from the CRS each release. A candidate rule merges, within (donor, agency,
stem, recipient), the numbers it strips with each other and with a number equal to the
stem. A merged pair is consistent when the two titles share at least `MIN_SIMILARITY` of
their words (Jaccard). A rule needs `MIN_PAIRS` merged pairs and at most `MAX_ERROR` of
them inconsistent, and per shape the passing segment count with the lowest error wins,
the smaller count on ties. A stem that gathers more than `MAX_FANOUT` project numbers
over all recipients is a budget line or portfolio code, and every pair it merges counts
as inconsistent. Two units with different project numbers and the same programme key are
siblings (`programme_level` 1).
"""

import numpy as np
import polars as pl

from climate_finance.build.link.embed import Encoder, neighbour_pairs, record_text
from climate_finance.build.link.id_practice import (
    ID_PERSISTENT_CLASSES,
    RENUMBERED_DONORS,
    line_id,
    period_of,
)
from climate_finance.build.link.keys import floor_loaded, purpose_units
from climate_finance.build.link.text import (
    NO_TITLE,
    clean_text,
    largest_record,
    representative,
    text_words,
    word_share,
)
from climate_finance.build.normalise.project_numbers import normalise_pn

MAX_PN_UNITS = 300
MAX_TITLE_UNITS = 100
# A title its donor gives, with no project number, to units of at least this many
# recipients in at least this many record years is a template (`template_lines`).
TEMPLATE_RECIPIENTS = 4
TEMPLATE_YEARS = 2
# A title is no template when at least this many of its units, and this share of
# them, continue a crs id across record years under persistent ids. On the archive
# any share from 0.01 to 0.25 keeps every id-truth pair. A share of 0.3 loses 49.
TEMPLATE_IDENTITY_UNITS = 2
TEMPLATE_IDENTITY_SHARE = 0.1
STEM_SEGMENTS = (1, 2)
MIN_PAIRS = 20
MAX_ERROR = 0.10
MAX_FANOUT = 50
MIN_SIMILARITY = 0.5
# Blocking rules: a key the two units share, and the most later disbursement units
# of the donor the key may hold for a commitment to pair through it.
BLOCKING_RULES: tuple[tuple[tuple[str, ...], int | None], ...] = (
    (("recipient_code", "purpose_code"), None),
    (("pn_block",), MAX_PN_UNITS),
    (("title_block",), MAX_TITLE_UNITS),
    (("recipient_code", "title_head"), None),
)
# Project numbers that name no project once lower-cased and stripped.
_NO_PN = ("notapplicable", "na", "none", "nan", "null", "0", "")
_SEPARATORS = ".-/_&: "
_LAST_SEGMENT = r"[.\-/_&: ]+[A-Z0-9]+$"
_AGENCY = ("donor_code", "agency_code")


def project_shape(pn: pl.Expr) -> pl.Expr:
    """A project number with each digit run replaced by 9 and each letter run by A."""
    return pn.str.replace_all("[0-9]+", "9").str.replace_all("[A-Z]+", "A")


def strip_segments(pn: pl.Expr, segments: int) -> pl.Expr:
    """`pn` without its last `segments` segments and their separators, or null when
    fewer segments remain to strip or none would be left."""
    stem = pn.str.strip_chars_end(_SEPARATORS)
    for _ in range(segments):
        cut = stem.str.replace(_LAST_SEGMENT, "")
        stem = pl.when(cut != stem).then(cut)
    return pl.when(stem.str.contains("[A-Z0-9]")).then(stem)


def learn_programme_stems(crs: pl.LazyFrame) -> pl.LazyFrame:
    """Programme stem rules per (donor, agency) from `crs`, in the `programme_stems`
    schema.

    `crs` holds the records the trace follows. Each (donor, agency, project number,
    recipient) takes the title of its largest record.
    """
    # Collected once: the rules read the projects about eight times, and the
    # streaming engine would evaluate the grouping again for each read.
    projects = (
        crs.with_columns(project_number=normalise_pn(pl.col("project_number")))
        .drop_nulls(["agency_code", "project_number"])
        .group_by(*_AGENCY, "project_number", "recipient_code")
        .agg(largest_record("title"))
        .with_columns(words=text_words(pl.col("title")))
        .drop("title")
        .collect()
        .lazy()
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
    # Every pair under a wide stem counts as inconsistent, so the count comes from group
    # sizes and no pair is built.
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
                word_share(pl.col("words"), pl.col("words_other")) >= MIN_SIMILARITY
            )
            .sum()
            .cast(pl.Int64),
        )
    )
    return pl.concat([wide, built]).with_columns(segments=pl.lit(segments))


def with_programme(units: pl.LazyFrame, *, stems: pl.LazyFrame) -> pl.LazyFrame:
    """`units` with `programme`: the project number's stem under its (donor, agency,
    shape)'s rule, else the project number itself, and null without a project number."""
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
    embedder: Encoder | None = None,
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
            precedes it is left out (`floor_loaded`), and the blocking caps count
            only the disbursements left in. The key tiers link one where a later
            record carries its commitment's date (`floor_seen`).
        embedder: vectors of unit texts, which add embedding neighbours to the
            blocking rules. Without one, the other rules alone block.
    """
    indexed = units.with_row_index("unit").select(
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
        "disbursement_usd",
        "record_ids",
    )
    reached = links.filter(task="crdf_crs").select(unit_id="right_id")
    commitments = indexed.join(reached, on="unit_id", how="semi")
    disbursements = indexed.filter(
        pl.col("disbursement_usd") > 0,
        floor_loaded(first_year=first_year),
        pl.col("donor_code").is_in(commitments["donor_code"].unique().implode()),
    )
    sides = _sides(
        pl.concat([commitments, disbursements]).unique("unit"),
        crs=crs,
        stems=stems,
        links=links,
        id_practice=id_practice,
        texts=embedder is not None,
    )
    vectors = None
    if embedder is not None:
        texts = sides["text"].drop_nulls().unique().sort()
        vectors = embedder(texts.to_list())
        rows = pl.DataFrame(
            {
                "text": texts,
                "vector": pl.int_range(texts.len(), dtype=pl.UInt32, eager=True),
            }
        )
        sides = sides.join(rows, on="text", how="left", maintain_order="left").drop(
            "text"
        )
    # The blocking caps count every disbursement, template lines included.
    reference = sides.join(disbursements.select("unit"), on="unit", how="semi")
    later_units = {
        rule: reference.drop_nulls(rule)
        .group_by("donor_code", *rule, "year")
        .len("units")
        for rule, cap in BLOCKING_RULES
        if cap is not None
    }
    lines = sides.filter(~pl.col("unit").is_in(template_lines(sides).implode()))
    commitments = lines.join(commitments.select("unit"), on="unit", how="semi")
    disbursements = lines.join(disbursements.select("unit"), on="unit", how="semi")
    renumbered = _renumbered_lines(units)
    ids = sides.select("unit", "unit_id")

    def by_line(commitments: pl.DataFrame, disbursements: pl.DataFrame) -> pl.DataFrame:
        """The blocked pairs of `commitments` and `disbursements`, one per
        purpose-split line (`purpose_units`), as unit ids. A pair whose line unit
        is not among `commitments` is dropped."""
        blocked = _block(
            commitments,
            disbursements,
            later_units=later_units,
            renumbered=renumbered,
            vectors=vectors,
        )
        named = blocked.join(ids, left_on="left", right_on="unit").join(
            ids, left_on="right", right_on="unit", suffix="_right"
        )
        return purpose_units(
            named.select(left_id="unit_id", right_id="unit_id_right"),
            units=units,
            reached=reached["unit_id"],
        ).join(commitments.select(right_id="unit_id"), on="right_id", how="semi")

    truth = by_line(commitments.filter("id_persistent"), disbursements)
    open_pairs = by_line(
        commitments.filter(~pl.col("keyed_later"), ~pl.col("ids_persist")),
        disbursements.filter(~pl.col("taken"), ~pl.col("ids_persist")),
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
        pairs.join(side, left_on="left_id", right_on="unit_id")
        .join(side, left_on="right_id", right_on="unit_id", suffix="_right")
        .filter(~pl.col("eligible") | floor_held)
        .select(
            "left_id",
            "right_id",
            donor_code="donor_code",
            year_gap=(pl.col("year") - pl.col("year_right")).cast(pl.Int16),
            programme_level=programme_level,
            same_crs_id=same["crs_id"],
            id_persistent="id_persistent_right",
            eligible="eligible",
        )
    )


def template_lines(sides: pl.DataFrame) -> pl.Series:
    """The `unit`s of `sides` (`_sides`) that are template lines: no project number,
    and a title their donor gives to units with no project number in at least
    `TEMPLATE_RECIPIENTS` recipients and `TEMPLATE_YEARS` record years, unless at
    least `TEMPLATE_IDENTITY_UNITS` and `TEMPLATE_IDENTITY_SHARE` of the title's
    units continue a crs id, meaning the unit's ids persist and an id-persistent unit
    of the donor carries its crs id in another record year."""
    bare = sides.filter(
        pl.col("project_number").is_null(), pl.col("title_block").is_not_null()
    )
    continued = (
        sides.filter("id_persistent")
        .drop_nulls("crs_id")
        .group_by("donor_code", "crs_id")
        .agg(years=pl.col("year").n_unique())
        .filter(pl.col("years") >= 2)
        .select("donor_code", "crs_id", continues=pl.lit(value=True))
    )
    continues = pl.col("id_persistent") & pl.col("continues").fill_null(value=False)
    templates = (
        bare.join(continued, on=["donor_code", "crs_id"], how="left")
        .group_by("donor_code", "title_block")
        .agg(
            recipients=pl.col("recipient_code").n_unique(),
            years=pl.col("year").n_unique(),
            continuing=continues.sum(),
            share=continues.mean(),
        )
        .filter(
            pl.col("recipients") >= TEMPLATE_RECIPIENTS,
            pl.col("years") >= TEMPLATE_YEARS,
            (pl.col("continuing") < TEMPLATE_IDENTITY_UNITS)
            | (pl.col("share") < TEMPLATE_IDENTITY_SHARE),
        )
    )
    return bare.join(templates, on=["donor_code", "title_block"], how="semi")["unit"]


def _sides(
    units: pl.DataFrame,
    *,
    crs: pl.LazyFrame,
    stems: pl.DataFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    texts: bool = False,
) -> pl.DataFrame:
    """Each unit's blocking keys, its programme key, and what the keys and its
    agency's id practice say of it.

    A unit's title is its largest record's, or that record's short description
    where it has no title. With `texts`, `text` holds the largest record's
    embedding text (`record_text`). `taken` marks a disbursement a key
    linked, and `keyed_later` a commitment a key reaches from a later record year.
    `ids_persist` holds when the agency's ids persist in the record year's period,
    and `id_persistent` when its class is `id_persistent`.
    """
    titles = representative(
        units,
        crs=crs,
        fields=[
            pl.coalesce("title", "short_description").alias("title"),
            *([record_text().alias("text")] if texts else []),
        ],
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
    title = clean_text(pl.col("title"))
    words = text_words(pl.col("title"))
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
        .join(titles, on="unit_id", how="left")
        .with_columns(
            pn_block=pn_block(pl.col("project_number")),
            title_block=pl.when(~title.is_in(NO_TITLE)).then(title),
            title_head=pl.when(words.list.len() >= 2).then(
                words.list.head(2).list.join(" ")
            ),
            taken=pl.col("unit_id").is_in(trace["left_id"].implode()),
            keyed_later=pl.col("unit_id").is_in(keyed_later.implode()),
            period=period_of(pl.col("year")),
            line=_renumbered_line(),
        )
        .join(classes, on=[*_AGENCY, "period"], how="left")
        .with_columns(pl.col("ids_persist", "id_persistent").fill_null(value=False))
        .drop("title", "period")
        .collect()
    )


def _renumbered_lines(units: pl.DataFrame) -> pl.DataFrame:
    """Each `RENUMBERED_DONORS` project number's `pn_block` with the line of every
    crs id a unit files under it: donor_code, pn_block and line."""
    return (
        units.filter(pl.col("donor_code").is_in(RENUMBERED_DONORS))
        .select(
            "donor_code",
            pn_block=pn_block(pl.col("project_number")),
            line=_renumbered_line(),
        )
        .drop_nulls()
        .unique()
    )


def _block(
    commitments: pl.DataFrame,
    disbursements: pl.DataFrame,
    *,
    later_units: dict[tuple[str, ...], pl.DataFrame],
    renumbered: pl.DataFrame,
    vectors: np.ndarray | None,
) -> pl.DataFrame:
    """(disbursement, commitment) unit pairs of one donor, the disbursement of a
    later record year, under any blocking rule, and embedding neighbours
    (`embed.neighbour_pairs`) when `vectors` holds the units' text vectors.

    Under a capped rule a commitment pairs only when its key holds at most the cap of
    the reference disbursement units of later years, which `later_units` counts per
    rule, key and year, so a common project number or title adds no pairs. A
    commitment of a `renumbered` line (`_renumbered_lines`) also takes each project
    number its line is filed under as its `pn_block`.
    """
    rules = [(commitments, rule, cap) for rule, cap in BLOCKING_RULES]
    bridged = commitments.drop_nulls("line").select(
        "unit", "year", "donor_code", "line"
    )
    rules.append(
        (
            bridged.join(renumbered, on=["donor_code", "line"]),
            ("pn_block",),
            MAX_PN_UNITS,
        )
    )
    blocked = []
    for blocking, rule, cap in rules:
        key = ["donor_code", *rule]
        left = blocking.drop_nulls(rule).select("unit", "year", *key)
        if cap is not None:
            narrow = _narrow_keys(left, later_units[rule], key=key, cap=cap)
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
    if vectors is not None:
        blocked.append(neighbour_pairs(commitments, disbursements, vectors=vectors))
    return pl.concat(blocked).unique()


def _narrow_keys(
    commitments: pl.DataFrame,
    units: pl.DataFrame,
    *,
    key: list[str],
    cap: int,
) -> pl.DataFrame:
    """The (key, year) of `commitments` whose key holds at most `cap` disbursement
    units of later years, of which `units` holds the count per (key, year)."""
    later = (
        units.join(
            commitments.select(*key, "year").unique(), on=key, suffix="_commitment"
        )
        .filter(pl.col("year") > pl.col("year_commitment"))
        .group_by(*key, "year_commitment")
        .agg(pl.col("units").sum())
    )
    return later.filter(pl.col("units") <= cap).select(*key, year="year_commitment")


def pn_block(project_number: pl.Expr) -> pl.Expr:
    """`project_number` lower-cased, with every character other than a to z and
    digits and leading zeros dropped, or null when it names no project or keeps under
    three characters."""
    pn = (
        project_number.str.to_lowercase()
        .str.replace_all("[^a-z0-9]", "")
        .str.strip_chars_start("0")
    )
    return pl.when(~pn.is_in(_NO_PN) & (pn.str.len_chars() >= 3)).then(pn)


def _renumbered_line() -> pl.Expr:
    """The unit's `line_id` at `RENUMBERED_DONORS`, else null."""
    donor = pl.col("donor_code")
    return pl.when(donor.is_in(RENUMBERED_DONORS)).then(
        line_id(donor, pl.col("crs_id"))
    )
