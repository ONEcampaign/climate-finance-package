"""The truth-group classifier and the provider profile.

Where an agency's ids persist, a later record continues a commitment exactly when it
carries the commitment's line id (`line_id`: the crs id, with a zero-padded line
suffix unpadded and a UK copy's letters dropped), so those agencies' candidate pairs
are labelled truth. A crs id that a later record files under another project number
and an unrelated title names another project. A provider with at least
`MIN_TRUE_PAIRS` true pairs among its training pairs is in the truth group, where
the group's training pairs can be cross-fitted (`training_folds`): one
gradient-boosted classifier, trained on the truth group's labelled pairs, scores the
eligible pairs of its other agencies. Providers without that truth are scored by
Splink, alone or, below `SPLINK_MIN_COMMITMENTS` open commitments or
`SPLINK_MIN_PAIRS` eligible pairs, pooled.

Some agencies number a project's lines inside its crs id and renumber them from year
to year: Canada's ids are a ten-character project root and a four-digit line, a line
can pass to another recipient, and a later year can report the same recipient under a
line no record commits. Where a project's later records contradict its ids this way
(see `contradicted_commitments`), the project number and recipient take the id's
place for lines the id cannot follow (see `id_truth`). Where the project committed
two lines to one recipient in a year, a new line of that recipient continues one of
them and nothing says which. Those pairs are `unlabelled` and neither train nor
measure the classifier. Canada also tells lines apart by purpose, so a Canadian
disbursement continues the commitments of its own project, recipient and purpose
where there are any (see `purpose_lines`). Where a donor changes a project's number
or agency and then issues a new crs id, the new line continues what the records that
bridge the change continue, and at the Adaptation Fund, which leaves no bridge, what
its title continues (see `carried_continuations`).

Labelled pairs split into halves by the md5 of their commitment's donor and line id
(the unit id where it has none). The classifier trains on one half, with up to
`FOLDS` folds grouped by the same key, and the other half measures it. The
out-of-fold scores fit an isotonic map: the provider's own where it has at least
`MIN_CALIBRATION_PAIRS` true and as many false training pairs, else the pooled one.
The link threshold maximises F1 on out-of-fold scores calibrated by maps fitted on
the other folds, so no map scores the pairs it was fitted on.

F1 is read on links, as `classifier_links` makes them: each disbursement unit's best
pair (`best_commitments`) links when it reaches the threshold, and is right when it
is true. A held-out unit's best pair is taken over both halves (`heldout_links`),
since the build weighs every pair of a unit. Recall counts every disbursement unit
with a true commitment in the half, so a true pair blocking did not produce counts as
a miss.

A private donor, as the OECD classes it (`private_donors`), files each grant under
one project number, so the classifier scores its eligible pairs only where the two
units carry the same number (`confirmable_pairs`). Its scoring group still counts
them all. A pair under two numbers is two grants or a
grant the donor renumbered, and neither the number nor the generic titles of
foundation grants confirm it. The Hewlett Foundation (1624) moved from numbers such
as `41867` to ones such as `2020-1471` in its 2021 records, and 4 of 15 of its
links under two numbers, read by hand, were right.

An eligible pair links when its probability reaches the threshold and it is not a
sibling (`programme_level` 1). Each disbursement unit joins the one commitment it
scores highest, ties to the earlier placed year, then the smaller unit id
(`best_commitments`). A commitment's links of one method form one group, so
allocation fills it under the commitment's face cap. Splink links follow the same
rule (`splink_models.splink_links`). With `jev_links` on, Jev adjudicates the
eligible pairs in the band (`jev.adjudicate_band`), and those it answered link as
`classifier_jev` on their combined probability.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import TYPE_CHECKING

import numpy as np
import polars as pl
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.model_selection import GroupKFold

from climate_finance import schema
from climate_finance.build.link.candidates import word_share
from climate_finance.build.link.id_practice import (
    RENUMBERED_DONORS,
    line_id,
    period_of,
)
from climate_finance.build.link.keys import (
    floor_loaded,
    partner_recipient,
    separator_free_pn,
)
from climate_finance.build.link.levels import (
    DATE_FEATURES,
    LEVELS,
    comparison_levels,
    levels_struct,
    title_words,
    unit_attributes,
)

if TYPE_CHECKING:
    from climate_finance.build.link.jev import JevBand

MIN_TRUE_PAIRS = 30
MIN_CALIBRATION_PAIRS = 30
SPLINK_MIN_COMMITMENTS = 300
SPLINK_MIN_PAIRS = 2_000
FOLDS = 5
# The fewest folds that cross-fit the classifier: with fewer split keys the truth
# group is not trained, and its providers are scored by Splink.
MIN_FOLDS = 2
THRESHOLDS = np.round(np.arange(0.01, 1.0, 0.01), 2)
FEATURES = (
    *LEVELS,
    "year_gap",
    "log_ratio",
    "later_commit",
    "programme_level",
    *DATE_FEATURES,
)
PARAMS = {
    "max_iter": 400,
    "learning_rate": 0.05,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 40,
    "l2_regularization": 1.0,
    "early_stopping": False,
    "random_state": 0,
}
METHOD = "classifier"
JEV_METHOD = "classifier_jev"
MODEL_VERSION = f"truth_hgb/scikit-learn-{sklearn.__version__}"
# The values of a pair's `split`.
TRAIN, HELDOUT, ELIGIBLE, UNLABELLED = "train", "heldout", "eligible", "unlabelled"
_SIBLING = 1
_PAIR = ("left_id", "right_id")
# Canada tells a project's lines apart by purpose as well as recipient: from 2018
# it reports several lines under one crs id split by purpose, and in 2016 it
# rotated purposes among a project's crs ids.
PURPOSE_LINE_DONORS = (301,)
# The title word share (`word_share`) from which a renumbered disbursement
# continues a commitment.
RENUMBERED_TITLE_SHARE = 0.5
# The title word share below which a crs id under another project number names
# another project.
REUSED_TITLE_SHARE = 0.2
# What a later disbursement shares with the commitment it continues: the line id,
# or where the project contradicts its ids, the project and recipient.
_BY_ID = ["donor_code", "crs_id"]
_BY_PROJECT = ["donor_code", "agency_code", "project_number", "recipient_code"]


@dataclass(frozen=True)
class TruthScores:
    """The classifier's calibrated scores and what it chose.

    `pairs` holds the truth group's candidate pairs with their features, `split`
    and `p_model`: cross-fitted out-of-fold probabilities for training pairs, the
    final model's for the others. `calibration` names, per truth-group provider,
    the isotonic map its pairs took (`own` or `pooled`).
    """

    pairs: pl.DataFrame
    threshold: float
    calibration: pl.DataFrame


@dataclass(frozen=True)
class Scored:
    """The scoring step's output: the classifier's links, the scoring columns of
    the provider profile, and the truth group's scores (None when the group is
    empty)."""

    links: pl.DataFrame
    profile: pl.DataFrame
    scores: TruthScores | None


def score_candidates(
    candidates: pl.DataFrame,
    *,
    units: pl.DataFrame,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    stems: pl.DataFrame,
    first_year: int,
    rejected: pl.DataFrame | None = None,
) -> Scored:
    """Sort providers into scoring groups and link what the truth group scores.

    Args:
        candidates: the `candidates` table.
        units: the link stage's units.
        crs: the CRS records the units sum.
        links: the `trace` and `crdf_crs` links of the steps before scoring.
        id_practice: each agency's class per period.
        stems: the programme stem rules.
        first_year: the first CRS year the build loads.
        rejected: the trace pairs (`left_id`, `right_id`) review decisions
            rejected, which never link (`linked_pairs`).
    """
    contradicted = contradicted_commitments(units, first_year=first_year)
    truth = id_truth(
        units,
        crs=crs,
        links=links,
        id_practice=id_practice,
        first_year=first_year,
        contradicted=contradicted,
    )
    pairs = with_split(candidates, units=units, truth=truth)
    groups = scoring_groups(pairs)
    if not (groups["scoring_group"] == "truth").any():
        columns = schema.LINKS.columns.items()
        empty = pl.DataFrame(schema={name: column.dtype for name, column in columns})
        return Scored(
            links=empty, profile=scoring_profile(groups, scores=None), scores=None
        )
    members = groups.filter(scoring_group="truth")["provider_code"].implode()
    pairs = confirmable_pairs(
        pairs.filter(pl.col("donor_code").is_in(members)), units=units
    )
    compared = units.join(
        pl.concat([pairs["left_id"], pairs["right_id"]]).to_frame("unit_id"),
        on="unit_id",
        how="semi",
    )
    features = comparison_levels(
        pairs, attributes=unit_attributes(compared, crs=crs, stems=stems)
    )
    scores = score_truth_group(features, groups=groups, truth=truth)
    return Scored(
        links=classifier_links(scores, rejected=rejected),
        profile=scoring_profile(groups, scores=scores),
        scores=scores,
    )


def confirmable_pairs(pairs: pl.DataFrame, *, units: pl.DataFrame) -> pl.DataFrame:
    """`pairs` (`with_split` candidates) less the eligible pairs of `private_donors`
    whose two units do not carry the same project number (`separator_free_pn`)."""
    pn = separator_free_pn(pl.col("project_number"))
    return (
        pairs.join(
            units.select(left_id="unit_id", pn_left=pn),
            on="left_id",
            how="left",
            maintain_order="left",
        )
        .join(
            units.select(right_id="unit_id", pn_right=pn),
            on="right_id",
            how="left",
            maintain_order="left",
        )
        .filter(
            (pl.col("split") != ELIGIBLE)
            | ~pl.col("donor_code").is_in(private_donors())
            | (pl.col("pn_left") == pl.col("pn_right")).fill_null(value=False)
        )
        .drop("pn_left", "pn_right")
    )


@cache
def private_donors() -> tuple[int, ...]:
    """The donor codes the OECD classes as private donors."""
    donors = pl.read_csv(
        (files("climate_finance") / "data" / "oecd_classification.csv").read_bytes(),
        columns=["code", "type"],
        schema_overrides={"code": pl.Int32},
    )
    return tuple(donors.filter(type="Private donor")["code"])


def split_key(donor_code: pl.Expr, crs_id: pl.Expr, unit_id: pl.Expr) -> pl.Expr:
    """The donor and crs id, else the unit id, as the key that halves and folds
    labelled pairs."""
    return pl.concat_str(
        donor_code.cast(pl.String), pl.coalesce(crs_id, unit_id), separator="|"
    )


def is_heldout(keys: pl.Series) -> pl.Series:
    """Whether each split key falls in the held-out half, which holds when the md5 of
    the key, read as a number from its first eight hex digits, is odd."""
    distinct = keys.unique().sort()
    odd = [
        int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % 2 == 1 for key in distinct
    ]
    lookup = pl.DataFrame(
        {"key": distinct, "heldout": pl.Series(odd, dtype=pl.Boolean)}
    )
    return keys.to_frame("key").join(
        lookup, on="key", how="left", maintain_order="left"
    )["heldout"]


def id_truth(
    units: pl.DataFrame,
    *,
    crs: pl.LazyFrame,
    links: pl.DataFrame,
    id_practice: pl.DataFrame,
    first_year: int,
    contradicted: pl.DataFrame,
) -> pl.DataFrame:
    """Every (disbursement, commitment) unit pair the ids say continues, blocked or
    not, and the pairs whose continuation the ids leave open.

    Ids are compared as `line_id`s, so a UK copy and a zero-padded line suffix
    carry their line's id.

    A commitment counts when a `crdf_crs` link reaches it, it has a crs id, and its
    agency is `id_persistent` in its record year's period. It continues in every
    disbursement unit from a later record year whose floor year is not before
    `first_year` and that has its donor and line id, unless the two units have
    different project numbers and titles that share under `REUSED_TITLE_SHARE` of
    their words (`word_share`): the donor reused the id for another project.

    Where `contradicted` names the commitment, its own id continues in such a unit
    unless the unit's recipient is one another line of the project committed to in
    the commitment's year: the line passed to that recipient. The commitment also
    continues in every such unit of its donor, agency, project number and recipient
    under a new line: a crs id that never committed to the unit's recipient in or
    before the unit's record year, and that commits elsewhere or is first recorded
    after the commitment's year. Where the commitment is ambiguous, those new-line
    pairs are `ambiguous`: they continue one of the recipient's lines, and nothing
    says which.

    A new line that neither the id nor the commitment's project number reaches
    continues what its own project's earlier records continue, and at
    `RENUMBERED_DONORS` what its title continues (see `carried_continuations`).
    A `PURPOSE_LINE_DONORS` unit that continues commitments of its own project,
    recipient and purpose continues those alone (see `purpose_lines`).

    Args:
        crs: the CRS records the units sum, for their titles.
        contradicted: `contradicted_commitments`.

    Returns:
        left_id, right_id, donor_code and ambiguous, sorted by pair.
    """
    units = _with_lines(units)
    persistent = id_practice.filter(id_class="id_persistent").select(
        "donor_code", "agency_code", "period"
    )
    commitments = (
        units.join(
            links.filter(task="crdf_crs").select(unit_id="right_id"),
            on="unit_id",
            how="semi",
        )
        .drop_nulls("crs_id")
        .with_columns(period=period_of(pl.col("year")))
        .join(persistent, on=["donor_code", "agency_code", "period"], how="semi")
        .join(contradicted, on="unit_id", how="left")
    )
    disbursements = _later_lines(units, first_year=first_year)
    later = pl.col("year_left") > pl.col("year")
    by_id = (
        commitments.select("unit_id", "year", "ambiguous", *_BY_PROJECT, "crs_id")
        .join(
            disbursements.select(
                "unit_id", "year", "project_number", "recipient_code", *_BY_ID
            ),
            on=_BY_ID,
            suffix="_left",
        )
        .filter(later)
    )
    renumbering = units.filter(pl.col("donor_code").is_in(RENUMBERED_DONORS))
    other_project = by_id.filter(
        pl.col("project_number_left").ne_missing(pl.col("project_number"))
    )
    titled = pl.concat(
        [
            other_project.select("unit_id"),
            other_project.select(unit_id="unit_id_left"),
            renumbering.select("unit_id"),
        ]
    ).unique()
    titles = title_words(units.join(titled, on="unit_id", how="semi"), crs=crs)
    reused = _with_title_share(other_project, titles=titles).filter(
        pl.col("title_share") < REUSED_TITLE_SHARE
    )
    by_id = by_id.join(reused, on=["unit_id", "unit_id_left"], how="anti")
    project = ["donor_code", "agency_code", "project_number", "year"]
    passed = (
        by_id.filter(
            pl.col("ambiguous").is_not_null(),
            pl.col("recipient_code_left") != pl.col("recipient_code"),
        )
        .join(
            units.filter(pl.col("commitment_usd") > 0).select(
                *project, "crs_id", recipient_code_left="recipient_code"
            ),
            on=[*project, "recipient_code_left"],
            suffix="_sibling",
        )
        .filter(pl.col("crs_id_sibling") != pl.col("crs_id"))
        .unique(["unit_id", "unit_id_left"])
    )
    new_line = ~pl.col("anchored") & (
        pl.col("committed") | (pl.col("seen_year") > pl.col("year"))
    )
    by_project = (
        commitments.drop_nulls("ambiguous")
        .select("unit_id", "year", "crs_id", "ambiguous", *_BY_PROJECT)
        .join(
            disbursements.select(
                "unit_id",
                "year",
                "crs_id",
                "anchored",
                "committed",
                "seen_year",
                *_BY_PROJECT,
            ),
            on=_BY_PROJECT,
            suffix="_left",
        )
        .filter(
            later,
            pl.col("crs_id_left") != pl.col("crs_id"),
            new_line.fill_null(value=False),
        )
    )
    found = pl.concat(
        [
            by_id.join(passed, on=["unit_id", "unit_id_left"], how="anti").select(
                left_id="unit_id_left",
                right_id="unit_id",
                donor_code="donor_code",
                ambiguous=pl.lit(value=False),
            ),
            by_project.select(
                left_id="unit_id_left",
                right_id="unit_id",
                donor_code="donor_code",
                ambiguous="ambiguous",
            ),
        ]
    )
    carried = carried_continuations(
        units,
        continuations=found,
        commitments=commitments,
        titles=titles,
        first_year=first_year,
    )
    lines = purpose_lines(units, commitments=commitments, first_year=first_year)
    return (
        pl.concat([found, carried])
        .join(lines, on="left_id", how="anti")
        .vstack(lines)
        .sort(*_PAIR)
    )


def carried_continuations(
    units: pl.DataFrame,
    *,
    continuations: pl.DataFrame,
    commitments: pl.DataFrame,
    titles: pl.DataFrame,
    first_year: int,
) -> pl.DataFrame:
    """The commitments a new line continues, read from its project's earlier records
    or, at `RENUMBERED_DONORS`, from its title.

    A donor can change the project number or agency it reports a project under and
    then issue a new crs id, so that neither the commitment's id nor its project
    number reaches the new line. The Adaptation Fund commits crs id 2017000003 under
    project number `FSM/RIE/COASTAL/2015/1`, reports that id under `AF00000107` in
    2020, and from 2021 reports `AF00000107` under crs id 2021000032; the UK commits
    crs id 2019000029 under DFID (agency 1), and from 2021 reports it under FCDO
    (agency 17). The records that bridge the change carry the new (donor, agency,
    project number, recipient) and continue the commitment.

    A disbursement unit no pair of `continuations` holds, whose floor year is not
    before `first_year` and whose crs id the donor never commits, continues each
    commitment of its recipient that a held pair continues from a disbursement unit
    of its donor, agency, project number and recipient in or before its record year,
    where the commitment's record year is before its crs id's first. A bridge from
    another recipient's commitment says nothing, since a line can pass between
    recipients.

    Where no record bridges the change, a disbursement unit of a
    `RENUMBERED_DONORS` donor that no pair of `continuations` holds continues each
    of `commitments` of its donor from an earlier record year under another crs id
    and another project number whose title shares at least `RENUMBERED_TITLE_SHARE`
    of its words (`word_share`), and whose recipient is the unit's or one that
    `regional_codes` names. Two of the fund's projects that shared one project number
    have dissimilar titles. The fund's titles follow a template, so two countries'
    projects can share most of their words, and a regional project disburses to
    its countries.

    Where a unit's carried commitments have more than one crs id, the unit
    continues one of the lines and nothing says which: its pairs are `ambiguous`.

    Args:
        units: the units, with `_with_lines` ids.
        continuations: left_id, right_id and ambiguous of the pairs known to
            continue; ambiguous pairs carry nothing.
        commitments: the commitment units truth follows.
        titles: unit_id and title_toks (`title_words`) of at least the
            `RENUMBERED_DONORS` units.

    Returns:
        left_id, right_id, donor_code and ambiguous.
    """
    disbursements = _later_lines(units, first_year=first_year).drop_nulls(
        ["crs_id", "project_number"]
    )
    history = (
        continuations.filter(~pl.col("ambiguous"))
        .join(
            disbursements.select(*_BY_PROJECT, left_id="unit_id", year_held="year"),
            on="left_id",
        )
        .join(
            units.select(
                "recipient_code",
                right_id="unit_id",
                year_right="year",
                line="crs_id",
            ),
            on=["right_id", "recipient_code"],
        )
        .select(*_BY_PROJECT, "year_held", "right_id", "year_right", "line")
    )
    bridged = (
        disbursements.filter(~pl.col("committed"))
        .join(continuations, left_on="unit_id", right_on="left_id", how="anti")
        .select(*_BY_PROJECT, "unit_id", "year", "seen_year")
        .join(history, on=_BY_PROJECT)
        .filter(
            pl.col("year_held") <= pl.col("year"),
            pl.col("year_right") < pl.col("seen_year"),
        )
        .select("unit_id", "donor_code", "right_id", "line")
    )
    fields = [
        "unit_id",
        "donor_code",
        "year",
        "crs_id",
        "project_number",
        "recipient_code",
    ]
    renumbered = (
        _with_title_share(
            commitments.filter(pl.col("donor_code").is_in(RENUMBERED_DONORS))
            .select(fields)
            .join(
                disbursements.join(
                    continuations, left_on="unit_id", right_on="left_id", how="anti"
                ).select(fields),
                on="donor_code",
                suffix="_left",
            )
            .filter(
                pl.col("year_left") > pl.col("year"),
                pl.col("crs_id_left") != pl.col("crs_id"),
                pl.col("project_number_left") != pl.col("project_number"),
                partner_recipient(
                    commitment=pl.col("recipient_code"),
                    disbursement=pl.col("recipient_code_left"),
                ),
            ),
            titles=titles,
        )
        .filter(pl.col("title_share") >= RENUMBERED_TITLE_SHARE)
        .select("donor_code", unit_id="unit_id_left", right_id="unit_id", line="crs_id")
    )
    return (
        pl.concat([bridged, renumbered.select(bridged.columns)])
        .unique(["unit_id", "right_id"])
        .select(
            left_id="unit_id",
            right_id="right_id",
            donor_code="donor_code",
            ambiguous=pl.col("line").n_unique().over("unit_id") > 1,
        )
    )


def purpose_lines(
    units: pl.DataFrame, *, commitments: pl.DataFrame, first_year: int
) -> pl.DataFrame:
    """The commitments a `PURPOSE_LINE_DONORS` disbursement unit continues on its
    own line: its project, recipient and purpose.

    Canada tells a project's lines apart by purpose as well as recipient: from 2018
    it reports several lines under one crs id split by purpose, and in 2016 it
    rotated purposes among a project's crs ids while the disbursements kept to the
    purposes. A disbursement unit as `_later_lines` finds them continues each of
    `commitments` of its donor, agency, project number, recipient and purpose from
    an earlier record year that carries its crs id, where that id committed to its
    recipient and purpose in or before its record year, and otherwise each that
    carries another crs id, where its own id commits elsewhere or is first recorded
    after the commitment's year. Where the commitments of one record year have more
    than one crs id, the unit continues one of them and nothing says which: those
    pairs are `ambiguous`.

    Args:
        units: the units, with `line_id`s.
        commitments: the commitment units truth follows.

    Returns:
        left_id, right_id, donor_code and ambiguous.
    """
    line = [*_BY_PROJECT, "purpose_code"]
    place = ["donor_code", "crs_id", "recipient_code", "purpose_code"]
    units = units.filter(pl.col("donor_code").is_in(PURPOSE_LINE_DONORS))
    anchors = (
        units.filter(pl.col("commitment_usd") > 0)
        .drop_nulls("crs_id")
        .group_by(place)
        .agg(placed=pl.col("year").min())
    )
    disbursements = (
        _later_lines(units, first_year=first_year)
        .join(anchors, on=place, how="left")
        .select(
            "unit_id",
            "year",
            "crs_id",
            "committed",
            "seen_year",
            *line,
            own=(pl.col("placed") <= pl.col("year")).fill_null(value=False),
        )
    )
    same_id = pl.col("crs_id_left") == pl.col("crs_id")
    new_line = pl.col("committed") | (pl.col("seen_year") > pl.col("year"))
    return (
        commitments.join(units, on="unit_id", how="semi")
        .select("unit_id", "year", "crs_id", *line)
        .join(disbursements, on=line, suffix="_left")
        .filter(
            pl.col("year_left") > pl.col("year"),
            pl.when("own").then(same_id).otherwise(~same_id & new_line),
        )
        .select(
            left_id="unit_id_left",
            right_id="unit_id",
            donor_code="donor_code",
            ambiguous=pl.col("crs_id").n_unique().over("unit_id_left", "year") > 1,
        )
    )


def _with_lines(units: pl.DataFrame) -> pl.DataFrame:
    """`units` with each crs id as its `line_id`."""
    return units.with_columns(crs_id=line_id(pl.col("donor_code"), pl.col("crs_id")))


def _with_title_share(pairs: pl.DataFrame, *, titles: pl.DataFrame) -> pl.DataFrame:
    """`pairs` of `unit_id_left` and `unit_id` with the `word_share` of their titles
    as `title_share`, null where either has none."""
    return (
        pairs.join(
            titles.rename({"unit_id": "unit_id_left", "title_toks": "toks_left"}),
            on="unit_id_left",
            how="left",
        )
        .join(titles, on="unit_id", how="left")
        .with_columns(title_share=word_share(pl.col("toks_left"), pl.col("title_toks")))
        .drop("toks_left", "title_toks")
    )


def _later_lines(units: pl.DataFrame, *, first_year: int) -> pl.DataFrame:
    """The disbursement units a commitment can continue in: those whose floor year
    is not before `first_year`, each with what its crs id says of it.

    `anchored` holds when the id committed to the unit's recipient in or before the
    unit's record year, `committed` when the id commits anywhere, and `seen_year` is
    the id's first record year.
    """
    commitments = units.filter(pl.col("commitment_usd") > 0).drop_nulls("crs_id")
    line = ["donor_code", "crs_id"]
    anchors = commitments.group_by(*line, "recipient_code").agg(
        anchor_year=pl.col("year").min()
    )
    committed = commitments.select(*line).unique().with_columns(committed=True)
    seen = units.drop_nulls("crs_id").group_by(line).agg(seen_year=pl.col("year").min())
    return (
        units.filter(
            pl.col("disbursement_usd") > 0,
            floor_loaded(first_year=first_year),
        )
        .join(anchors, on=[*line, "recipient_code"], how="left")
        .join(committed, on=line, how="left")
        .join(seen, on=line, how="left")
        .with_columns(
            anchored=(pl.col("anchor_year") <= pl.col("year")).fill_null(value=False),
            committed=pl.col("committed").fill_null(value=False),
        )
    )


def contradicted_commitments(units: pl.DataFrame, *, first_year: int) -> pl.DataFrame:
    """The commitment units whose crs id the project's later records contradict.

    Ids are compared as `line_id`s. Within one (donor, agency, project number), a
    commitment of record year Y is contradicted by a disbursement unit of a later
    record year, whose floor year is not before `first_year`, that either
    - has the commitment's recipient and a crs id the donor never commits and first
      records after Y: the line was renumbered, or
    - has the commitment's crs id and the recipient another crs id of the project
      committed in Y: the lines passed between recipients.

    Every commitment of a contradicted (donor, agency, project number, Y) is returned,
    since its siblings' labels are wrong too.

    Returns:
        unit_id, sorted, and `ambiguous`: whether the project committed more than one
        crs id to the commitment's recipient in Y, so that the project number and
        recipient cannot say which line a later record continues.
    """
    units = _with_lines(units)
    project = ["donor_code", "agency_code", "project_number"]
    commitments = units.filter(pl.col("commitment_usd") > 0)
    lines = (
        commitments.drop_nulls(["crs_id", "project_number"])
        .select(*project, "year", "crs_id", "recipient_code")
        .unique()
    )
    disbursed = (
        _later_lines(units, first_year=first_year)
        .drop_nulls(["crs_id", "project_number"])
        .select(
            *project,
            "crs_id",
            "recipient_code",
            "committed",
            "seen_year",
            year_later="year",
        )
        .unique()
    )
    later = pl.col("year_later") > pl.col("year")
    renumbered = lines.join(
        disbursed.filter(~pl.col("committed")), on=[*project, "recipient_code"]
    ).filter(later, pl.col("seen_year") > pl.col("year"))
    siblings = lines.select(
        *project, "year", sibling_id="crs_id", recipient_later="recipient_code"
    )
    moved = (
        lines.join(
            disbursed.rename({"recipient_code": "recipient_later"}),
            on=[*project, "crs_id"],
        )
        .filter(later, pl.col("recipient_later") != pl.col("recipient_code"))
        .join(siblings, on=[*project, "year", "recipient_later"])
        .filter(pl.col("sibling_id") != pl.col("crs_id"))
    )
    contradicted = pl.concat(
        [frame.select(*project, "year") for frame in (renumbered, moved)]
    ).unique()
    ambiguous = (
        pl.col("crs_id")
        .drop_nulls()
        .n_unique()
        .over(*project, "recipient_code", "year")
        > 1
    )
    return (
        commitments.join(contradicted, on=[*project, "year"], how="semi")
        .select("unit_id", ambiguous=ambiguous)
        .sort("unit_id")
    )


def with_split(
    candidates: pl.DataFrame,
    *,
    units: pl.DataFrame,
    truth: pl.DataFrame,
) -> pl.DataFrame:
    """`candidates` with `split`, `true_pair`, and the commitment's `split_key` (of
    its `line_id`) and `placed_year`.

    A labelled pair (`id_persistent`) is `unlabelled` when `truth` (`id_truth`)
    holds it as ambiguous, else `train` or `heldout` by its split key. Any other
    pair is `eligible`. `true_pair` says whether `truth` holds the pair and not as
    ambiguous.
    """
    line = line_id(pl.col("donor_code"), pl.col("crs_id"))
    commitments = units.select(
        right_id="unit_id",
        split_key=split_key(pl.col("donor_code"), line, pl.col("unit_id")),
        placed_year="placed_year",
    )
    held = truth.select(*_PAIR, "ambiguous")
    pairs = (
        candidates.join(commitments, on="right_id", how="left", maintain_order="left")
        .join(held, on=_PAIR, how="left", maintain_order="left")
        .with_columns(true_pair=pl.col("ambiguous").not_().fill_null(value=False))
    )
    heldout = is_heldout(pairs["split_key"])
    return pairs.with_columns(
        split=pl.when(~pl.col("id_persistent"))
        .then(pl.lit(ELIGIBLE))
        .when(pl.col("ambiguous").fill_null(value=False))
        .then(pl.lit(UNLABELLED))
        .when(heldout)
        .then(pl.lit(HELDOUT))
        .otherwise(pl.lit(TRAIN))
    ).drop("ambiguous")


def scoring_groups(pairs: pl.DataFrame) -> pl.DataFrame:
    """Each provider's scoring group and the counts that set it.

    A provider with at least `MIN_TRUE_PAIRS` true training pairs is in the truth
    group when the training pairs of all such providers can be cross-fitted
    (`training_folds`); otherwise none is, and each takes the Splink group its
    eligible pairs set.

    Args:
        pairs: `with_split` candidates.

    Returns:
        provider_code, scoring_group, true_pairs (true training pairs), commitments
        (commitments in eligible pairs) and pairs (eligible pairs), one row per
        provider with candidate pairs.
    """
    eligible = pl.col("split") == ELIGIBLE
    counts = pairs.group_by(provider_code="donor_code").agg(
        true_pairs=((pl.col("split") == TRAIN) & pl.col("true_pair")).sum(),
        commitments=pl.col("right_id").filter(eligible).n_unique(),
        pairs=eligible.sum(),
    )
    admitted = counts.filter(pl.col("true_pairs") >= MIN_TRUE_PAIRS)["provider_code"]
    trainable = not admitted.is_empty() and (
        training_folds(
            pairs.filter(
                pl.col("split") == TRAIN, pl.col("donor_code").is_in(admitted.implode())
            ).sort(*_PAIR)
        )
        is not None
    )
    return counts.select(
        "provider_code",
        scoring_group=pl.when(
            (pl.col("true_pairs") >= MIN_TRUE_PAIRS) & pl.lit(trainable)
        )
        .then(pl.lit("truth"))
        .when(
            (pl.col("commitments") >= SPLINK_MIN_COMMITMENTS)
            & (pl.col("pairs") >= SPLINK_MIN_PAIRS)
        )
        .then(pl.lit("splink"))
        .otherwise(pl.lit("pooled_splink")),
        true_pairs=pl.col("true_pairs").cast(pl.Int32),
        commitments=pl.col("commitments").cast(pl.Int32),
        pairs=pl.col("pairs").cast(pl.Int32),
    ).sort("provider_code")


def score_truth_group(
    features: pl.DataFrame, *, groups: pl.DataFrame, truth: pl.DataFrame
) -> TruthScores:
    """Train, calibrate and apply the classifier for the truth group.

    Args:
        features: `with_split` candidates with `comparison_levels`.
        groups: `scoring_groups`.
        truth: `id_truth`, for the true pairs blocking missed.
    """
    members = groups.filter(scoring_group="truth")
    pairs = features.filter(
        pl.col("donor_code").is_in(members["provider_code"].implode())
    ).sort(*_PAIR)
    train = pairs.filter(split=TRAIN)
    x, y = _matrix(train), train["true_pair"].to_numpy()
    donors = train["donor_code"].to_numpy()
    folds = training_folds(train)
    if folds is None:
        msg = "the truth group's training pairs cannot be cross-fitted"
        raise ValueError(msg)
    raw = np.empty(len(y))
    for fit_rows, score_rows in folds:
        raw[score_rows] = _predict(_fit(x[fit_rows], y[fit_rows]), x[score_rows])
    crossed = np.empty(len(y))
    for fit_rows, score_rows in folds:
        fold_maps = _isotonic_maps(raw[fit_rows], y[fit_rows], donors[fit_rows])
        crossed[score_rows] = _calibrate(fold_maps, raw[score_rows], donors[score_rows])
    best = best_commitments(train.with_columns(p_model=pl.Series(crossed)))
    threshold = best_threshold(
        best["p_model"].to_numpy(),
        best["true_pair"].to_numpy(),
        n_true=true_disbursements(truth, commitments=train["right_id"]),
    )

    model = _fit(x, y)
    maps = _isotonic_maps(raw, y, donors)
    rest = pairs.filter(pl.col("split") != TRAIN)
    p_rest = _calibrate(
        maps, _predict(model, _matrix(rest)), rest["donor_code"].to_numpy()
    )
    scored = pl.concat(
        [
            train.with_columns(p_model=pl.Series(crossed)),
            rest.with_columns(p_model=pl.Series(p_rest)),
        ]
    ).sort(*_PAIR)
    own = pl.col("provider_code").is_in([donor for donor in maps if donor is not None])
    calibration = members.select(
        "provider_code",
        calibration=pl.when(own).then(pl.lit("own")).otherwise(pl.lit("pooled")),
    )
    return TruthScores(pairs=scored, threshold=threshold, calibration=calibration)


def training_folds(train: pl.DataFrame) -> list[tuple[np.ndarray, np.ndarray]] | None:
    """The folds that cross-fit the classifier on `train`, the truth group's
    training pairs: `FOLDS` grouped by split key, or one per key where there are
    fewer, as (fitting rows, scoring rows). None when there are fewer than
    `MIN_FOLDS` keys, or when a fold's fitting rows lack true or false pairs, as
    no classifier is fitted on one outcome."""
    keys = train["split_key"].to_numpy()
    n_splits = min(FOLDS, len(np.unique(keys)))
    if n_splits < MIN_FOLDS:
        return None
    y = train["true_pair"].to_numpy()
    folds = list(GroupKFold(n_splits=n_splits).split(keys, y, keys))
    if any(len(np.unique(y[fit_rows])) < 2 for fit_rows, _ in folds):
        return None
    return folds


def best_commitments(
    pairs: pl.DataFrame, *, probability: str = "p_model"
) -> pl.DataFrame:
    """Each disbursement unit's best pair among `pairs` that are not siblings: the
    highest `probability`, ties to the earlier placed year, then the smaller
    commitment unit id."""
    return (
        pairs.filter(pl.col("programme_level") != _SIBLING)
        .sort(
            "left_id",
            probability,
            "placed_year",
            "right_id",
            descending=[False, True, False, False],
        )
        .unique("left_id", keep="first", maintain_order=True)
    )


def heldout_links(pairs: pl.DataFrame) -> pl.DataFrame:
    """The held-out half's links: each disbursement unit with a held-out pair takes
    its best pair (`best_commitments`) among its labelled pairs of both halves, as
    the build weighs every pair of a unit. A best pair that is true and in the
    training half continues a commitment of that half and is dropped. A false one
    is the unit's link, wrong whichever half holds it.

    Every score ranked is out-of-sample: a held-out pair takes the final model's,
    and a training pair the model of the fold that scored it. The final model's
    scores of training pairs would put in-sample scores into the ranking.

    Args:
        pairs: `TruthScores.pairs`.
    """
    labelled = pairs.filter(pl.col("split").is_in([TRAIN, HELDOUT]))
    measured = labelled.filter(split=HELDOUT).select("left_id").unique()
    best = best_commitments(labelled.join(measured, on="left_id", how="semi"))
    return best.filter((pl.col("split") == HELDOUT) | ~pl.col("true_pair"))


def true_disbursements(truth: pl.DataFrame, *, commitments: pl.Series) -> int:
    """How many disbursement units `truth` (`id_truth`) says continue one of
    `commitments`, ambiguous pairs aside: the links a perfect classifier makes."""
    return (
        truth.filter(~pl.col("ambiguous"))
        .join(commitments.unique().to_frame("right_id"), on="right_id", how="semi")[
            "left_id"
        ]
        .n_unique()
    )


def linked_pairs(
    pairs: pl.DataFrame,
    *,
    threshold: float,
    jev_threshold: float | None = None,
    rejected: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Each disbursement unit's link among `pairs`: its best pair
    (`best_commitments` on `probability`) among those that reach their threshold,
    `jev_threshold` on `probability` where Jev answered (`p_jev` not null) and
    `threshold` on `p_model` elsewhere. The pairs of `rejected` (`left_id`,
    `right_id`), which review decisions rejected, are left out first, so a unit
    whose best pair was rejected links its next best."""
    if rejected is not None:
        pairs = pairs.join(
            rejected.select(*_PAIR), on=list(_PAIR), how="anti", maintain_order="left"
        )
    reached = pl.col("p_model") >= threshold
    if jev_threshold is not None:
        reached = (
            pl.when(pl.col("p_jev").is_not_null())
            .then(pl.col("probability") >= jev_threshold)
            .otherwise(reached)
        )
    return best_commitments(pairs.filter(reached), probability="probability")


def classifier_links(
    scores: TruthScores,
    *,
    jev: JevBand | None = None,
    rejected: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """The classifier's `trace` links, in the `links` schema.

    With `jev` (`jev.adjudicate_band`), an eligible pair Jev answered takes its
    combined probability and links at the combined threshold, as `classifier_jev`;
    every other pair keeps the classifier's probability and threshold. No pair of
    `rejected` links (`linked_pairs`).
    """
    eligible = scores.pairs.filter(pl.col("split") == ELIGIBLE)
    if jev is None:
        weighed = eligible.with_columns(
            probability="p_model", p_jev=pl.lit(None, dtype=pl.Float64)
        )
    else:
        weighed = eligible.join(jev.answers, on=list(_PAIR), how="left").with_columns(
            probability=pl.coalesce("p_combined", "p_model")
        )
    best = linked_pairs(
        weighed,
        threshold=scores.threshold,
        jev_threshold=None if jev is None else jev.threshold,
        rejected=rejected,
    )
    method = (
        pl.when(pl.col("p_jev").is_not_null())
        .then(pl.lit(JEV_METHOD))
        .otherwise(pl.lit(METHOD))
    )
    given = {
        "task": pl.lit("trace"),
        "left_id": pl.col("left_id"),
        "right_id": pl.col("right_id"),
        "group_id": pl.concat_str(method, "right_id", separator="|"),
        "probability": pl.col("probability"),
        "link_method": method,
        "p_model": pl.col("p_model"),
        "p_jev": pl.col("p_jev"),
        "programme_level": pl.col("programme_level"),
        "levels": levels_struct(),
        "model_version": pl.lit(MODEL_VERSION),
        "review": pl.lit(value=False),
    }
    if jev is not None:
        given["jev_model"] = pl.when(pl.col("p_jev").is_not_null()).then(
            pl.lit(jev.model)
        )
    return best.select(
        given.get(name, pl.lit(None)).cast(column.dtype).alias(name)
        for name, column in schema.LINKS.columns.items()
    ).sort(*_PAIR)


def scoring_profile(
    groups: pl.DataFrame, *, scores: TruthScores | None
) -> pl.DataFrame:
    """The scoring columns of `provider_profile`: `groups` with each truth-group
    provider's calibration and threshold. `scores` is None when the truth group is
    empty."""
    if scores is None:
        profile = groups.with_columns(
            calibration=pl.lit(None, dtype=pl.String),
            threshold=pl.lit(None, dtype=pl.Float64),
        )
    else:
        truth = pl.col("scoring_group") == "truth"
        profile = groups.join(scores.calibration, on="provider_code", how="left")
        profile = profile.with_columns(
            threshold=pl.when(truth).then(pl.lit(scores.threshold))
        )
    return profile.select(
        "provider_code",
        "scoring_group",
        "true_pairs",
        "commitments",
        "pairs",
        "calibration",
        "threshold",
    ).sort("provider_code")


def provider_profile(crosswalk: pl.DataFrame, *, scoring: pl.DataFrame) -> pl.DataFrame:
    """`provider_profile` from the crosswalk columns (`crosswalk_profile`) and the
    scoring columns (`scoring_profile`), one row per provider in either."""
    profile = crosswalk.join(scoring, on="provider_code", how="full", coalesce=True)
    return profile.select(list(schema.PROVIDER_PROFILE.columns)).sort("provider_code")


def _matrix(pairs: pl.DataFrame) -> np.ndarray:
    """The features as floats: levels shifted by one so missing is category 0, and
    a missing log ratio or date feature NaN."""
    shifted = [pl.col(level) + 1 for level in LEVELS]
    others = FEATURES[len(LEVELS) :]
    return pairs.select(*shifted, *others).to_numpy().astype(np.float64)


def _fit(x: np.ndarray, y: np.ndarray) -> HistGradientBoostingClassifier:
    """A gradient-boosting model of `y` on `x`. A feature missing on every row, such
    as a date no record carries, is fitted as a constant 0, since scikit-learn
    cannot bin a column with no values."""
    empty = np.isnan(x).all(axis=0)
    if empty.any():
        x = x.copy()
        x[:, empty] = 0.0
    model = HistGradientBoostingClassifier(
        categorical_features=list(range(len(LEVELS))), **PARAMS
    )
    return model.fit(x, y)


def _predict(model: HistGradientBoostingClassifier, x: np.ndarray) -> np.ndarray:
    return model.predict_proba(x)[:, 1]


def _isotonic_maps(
    raw: np.ndarray, y: np.ndarray, donors: np.ndarray
) -> dict[int | None, IsotonicRegression]:
    """The pooled map (key None) and each donor's own where its pairs allow."""
    maps = {None: _isotonic(raw, y)}
    for donor in np.unique(donors):
        own = donors == donor
        true = int(y[own].sum())
        if min(true, int(own.sum()) - true) >= MIN_CALIBRATION_PAIRS:
            maps[int(donor)] = _isotonic(raw[own], y[own])
    return maps


def _isotonic(x: np.ndarray, y: np.ndarray) -> IsotonicRegression:
    return IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(x, y)


def _calibrate(
    maps: dict[int | None, IsotonicRegression], raw: np.ndarray, donors: np.ndarray
) -> np.ndarray:
    """`raw` through each donor's own map, else the pooled one."""
    out = np.empty(len(raw))
    for donor in np.unique(donors):
        rows = donors == donor
        out[rows] = maps.get(int(donor), maps[None]).predict(raw[rows])
    return out


def best_threshold(p: np.ndarray, y: np.ndarray, *, n_true: int) -> float:
    """The grid threshold of highest F1, the lowest on ties, of links with
    probabilities `p` and correctness `y`, against `n_true` right links in all."""
    true = y.astype(bool)
    f1 = [
        2 * int((p[true] >= t).sum()) / (int((p >= t).sum()) + n_true)
        for t in THRESHOLDS
    ]
    return float(THRESHOLDS[int(np.argmax(f1))])
