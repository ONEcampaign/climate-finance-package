"""BTR Table III.1 rows to same-year CRS records of the reporting party.

A party's BTR row names an activity it reported to the UNFCCC; its CRS records are
those of the party's DAC provider code in the same year, Bi_Multi 2 left out. Two
tiers link them:

- `btr_id`: an id the party writes into the title or the additional information
  (`ID_PATTERN`: a BMZ number "2022.7476.9", a Norwegian crs id "(2019000014)", an
  FCDO programme number "202495") that equals, once lower-cased and stripped to
  letters and digits, a CRS record's project number, the project number's root
  before its first hyphen, or its crs id. The row links to every such record of the
  year whose recipient is the row's or a regional or unspecified one, so a UK
  programme reaches each component the CRS splits it into ("202495-110",
  "202495-112") and a row for Kenya leaves the programme's Ghana component.
- `btr_splink`: one Fellegi-Sunter model per party over six comparisons (title
  words against the CRS title and descriptions, recipient, amount, sector,
  instrument, and the Rio marker against the type of support), over the records
  with a commitment or a disbursement. Its u comes from random sampling, its m from
  two EM sessions blocked on strong evidence, since a session blocked on recipient
  alone converged on non-matches. The model reads no ids, as they are cut from the
  title before it compares text.

Rows the reader flags `is_aggregate`, such as Finland's channel totals, total
spending under many commitments, so neither tier reads them.

Raw EM probabilities do not link. The ids calibrate the model: each id row's best
candidate is labelled by whether it is one of the row's id records, and an isotonic
map of label on match weight gives the probability, the party's own map where its
labels hold `MIN_MAP_OUTCOMES` of each outcome. Each party's model has its own
scale of weight, so a party without its own map reads the map pooled over every
party's labels only from `POOLED_FLOOR` up, where every party's labels agree, and
only while its own labels there agree with the pooled map (`POOLED_CHECK_SHARE`).
A map with fewer of either outcome links nothing, and a weight below a map's lowest
label reads 0. A row the id tier did not link takes its best candidate when the
calibrated probability is at least `LINK_PROBABILITY`. Unlinked rows are UNFCCC
only.

Id rows score high, so few labels sit at the low weights where most id-less rows
do. `calibration_draw` draws id-less rows there for people to label, and, for a
party without its own map, rows from `POOLED_FLOOR` up to check its pooled links.
Their decisions on a row's best candidate join the id labels, each weighted by the
id-less rows of its party and stratum over the decisions there. With no decisions
the maps are the id labels' alone.

Recipients link on the geo resolver's DAC code (`orgs.recipient_code`), so
"Vietnam" and "Turkey" find Viet Nam and Türkiye. A recipient that names no single
country (a region, a list of countries, a name the resolver does not know, a code
outside the recipient list such as 999 for "Global") and a CRS regional or
unspecified code (`keys.regional_codes`) both read as `aggregate`.
"""

import logging
import math
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, NamedTuple

import duckdb
import numpy as np
import polars as pl
import splink
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from sklearn.isotonic import IsotonicRegression
from splink import DuckDBAPI, Linker, SettingsCreator
from splink.blocking_rule_library import CustomRule
from splink.internals.comparison_creator import ComparisonCreator

from climate_finance import schema
from climate_finance.build.geo import recipient_code
from climate_finance.build.link.jev import spread_sample
from climate_finance.build.link.keys import (
    country_codes,
    links_frame,
    regional_codes,
)
from climate_finance.build.link.packet import btr_digests
from climate_finance.build.link.splink_models import (
    MIN_EM_PAIRS,
    isotonic_map,
)
from climate_finance.build.link.text import text_words
from climate_finance.build.readers.btr import LEVEL_TOLERANCE, WEIGHT_LEVELS

_log = logging.getLogger(__name__)

LINK_PROBABILITY = 0.5
# Labels of each outcome a calibration map needs. On the archive, five parties hold
# their own map (Germany 909 right and 75 wrong, Denmark 733 and 67, France 210 and
# 62, the UK 252 and 159, Norway 1,221 and 99), and the others read the pooled map,
# 3,837 and 487, from `POOLED_FLOOR` up. Of those, only the Netherlands holds many
# id labels of its own (511 right and 24 wrong).
MIN_MAP_OUTCOMES = 30
# The lowest match weight at which a party without its own map reads the pooled map.
# Fitted without each labelled party in turn, the pooled map reads that party's id
# labels from weight 5 up at a precision of 0.93 to 0.99 (Germany, Denmark, France,
# the UK, Norway and the Netherlands), and every one of them is right at 0.85 or more
# in each band of weight from 5 up. Below weight -3 it misreads them by 0.19 to 0.56
# (France's labels there are right at 0.09 and read 0.64, Denmark's at 0.66 and read
# 0.35), and between 3 and 5 Norway's are right at 0.66 and read 0.87 to 0.92.
POOLED_FLOOR = 5.0
# A party without its own map keeps its pooled links while its labels from
# `POOLED_FLOOR` up, once there are `MIN_CHECK_LABELS`, are right at a weighted share
# of at least `POOLED_CHECK_SHARE` (`_pool_check`). The share sits under the 0.85
# of the worst band of weight a held-out party showed from the floor up, and above
# Norway's 0.66 between 3 and 5, the shift a party on another scale shows. Thirty is
# the draw of the stratum from the floor up.
POOLED_CHECK_SHARE = 0.8
MIN_CHECK_LABELS = 30
# Id-less rows drawn for people to label, in the strata of `CALIBRATION_STRATA`
# (each from its lower edge up to its upper). The three from -12 to -3 share
# `CALIBRATION_DRAWS` a party, spread evenly. They cover the weights where Germany's
# id labels hold 24, 4 of them right, against 316 id-less rows, none linked; thirty
# a stratum let each form an isotonic block of thirty. The last, from
# `POOLED_FLOOR` up, gives `MIN_CHECK_LABELS` a party whose id labels give it no
# map of its own, so its labels can check its pooled links (`_pool_check`).
CALIBRATION_STRATA = (
    (-12.0, -9.0),
    (-9.0, -6.0),
    (-6.0, -3.0),
    (POOLED_FLOOR, math.inf),
)
CALIBRATION_DRAWS = 90
# Ids parties write into titles and additional information.
ID_PATTERN = r"\d{4}\.\d{4}\.\d|\d{6,}(?:-\d+)?|[A-Z]{2,4}-?\d{2,}[\w/\-]*"
# An id shorter than this, once stripped, names nothing.
MIN_ID_CHARS = 5
# Ids and tags cut from a title before the model compares it: Germany's
# "BMZ-Nr. 2022.7476.9", Norway's "(2019000014)", the UK's leading "202495", the
# USA's leading "2021," and trailing ", Mitigation".
TITLE_TAGS = (
    r",?\s*BMZ-Nr\.?\s*[\d\.\-]+",
    r"\(\s*\d{6,}\s*\)",
    r"^\s*\d{6}\s*-?\s+",
    r"^\s*20\d\d\s*,\s*",
    r"(?i),\s*(mitigation|adaptation|cross-cutting)\s*$",
    r",\s*20\d\d\.?\s*$",
)
# The CRS text words the title is compared with.
TEXT_WORDS = 120
AGGREGATE = "aggregate"
# Splink puts the input with the smaller table name on the left.
LEFT, RIGHT = "a_btr", "b_crs"
SEED = 7
U_MAX_PAIRS = 5e6
# EM sessions, each a rule and its fallback. Exact title trains every comparison
# but text; recipient with amounts within 2% trains text. Where few titles match
# exactly (UK programmes), EM drives an m to zero, and the fallback blocks on
# recipient and two shared title words.
_SAME_YEAR = "l.year = r.year"
_AMOUNT_R = "case when l.status = 'disbursed' then r.amt_d else r.amt_c end"
EM_SESSIONS = (
    (
        f"{_SAME_YEAR} and l.title_norm = r.title_norm",
        f"{_SAME_YEAR} and l.rec_key = r.rec_key "
        "and len(list_intersect(l.title_toks, r.text_toks)) >= 2",
    ),
    (
        f"{_SAME_YEAR} and l.rec_key = r.rec_key "
        f"and abs(l.amt / nullif({_AMOUNT_R}, 0) - 1) <= 0.02",
    ),
)
BLOCKING_RULES = (
    f"{_SAME_YEAR} and l.rec_key = r.rec_key",
    f"{_SAME_YEAR} and l.title_norm = r.title_norm",
)
# Sector buckets: words in a BTR sector label, and CRS purpose categories.
SECTOR_WORDS = (
    ("energy", "energ|power|electric|renewable|solar|wind"),
    ("transport", "transport"),
    ("industry", "industr|mining|construction"),
    ("forestry", "forest|land use"),
    ("agriculture", "agri|food|rural|fish|livestock"),
    ("water", "water|sanit"),
    ("health", "health"),
    ("education", "educat|training"),
    ("drr", "disaster|emergency|humanitarian"),
    ("environment", "environment|biodivers|conserv"),
    ("governance", "govern|policy|public admin|civil society"),
    ("cross", "cross|multi"),
)
SECTOR_PURPOSES = {
    "energy": (230, 231, 232),
    "transport": (210,),
    "industry": (321, 322, 323),
    "forestry": (312,),
    "agriculture": (311, 313, 520),
    "water": (140,),
    "health": (120, 121, 122, 123, 130),
    "education": (110, 111, 112, 113, 114),
    "drr": (720, 730, 740),
    "environment": (410,),
    "governance": (150, 151, 152),
    "cross": (430,),
}
# The Splink inputs' columns, the same on both sides.
_INPUT = {
    "unique_id": pl.String,
    "year": pl.Int16,
    "rec_key": pl.String,
    "title_norm": pl.String,
    "desc_norm": pl.String,
    "title_desc": pl.String,
    "title_toks": pl.List(pl.String),
    "text_toks": pl.List(pl.String),
    "amt": pl.Float64,
    "status": pl.String,
    "amt_c": pl.Float64,
    "amt_d": pl.Float64,
    "sector": pl.String,
    "instr": pl.String,
    "type": pl.String,
    "ad": pl.Int8,
    "mi": pl.Int8,
}
_PAIR = ("row_id", "record_id")
_BEST = {
    "party": pl.String,
    "row_id": pl.String,
    "record_id": pl.String,
    "match_weight": pl.Float64,
    "p_raw": pl.Float64,
    "model_version": pl.String,
}


class BtrLinks(NamedTuple):
    """`link_btr`'s output: the `links` rows of task `btr_crs`, the id-less rows
    drawn for people to label (`btr_calibration`), and the `btr_crs` decisions
    whose fingerprints match the release (`current_decisions`), None when
    `link_btr` was given no decisions."""

    links: pl.DataFrame
    draw: pl.DataFrame
    labels: pl.DataFrame | None


def link_btr(
    btr: pl.LazyFrame, *, crs: pl.LazyFrame, labels: pl.DataFrame | None = None
) -> BtrLinks:
    """`links` rows of task `btr_crs`, from BTR row (left) to CRS record (right),
    and the `btr_calibration` draw (`calibration_draw`).

    Args:
        btr: The `btr` table.
        crs: The normalised `crs` table.
        labels: Queue decisions as `decisions.read_decision_labels` gives them,
            whose `btr_crs` labels that still match the release
            (`current_decisions`) join calibration (`calibrate`).
    """
    rows = btr_rows(btr)
    records = crs_records(crs, rows=rows)
    by_id = link_by_ids(rows, records=records)
    parties = rows.filter(pl.col("donor_code").is_not_null())["party"].unique().sort()
    best = pl.concat(
        [pl.DataFrame(schema=_BEST)]
        + [score_party(rows.filter(party=party), records=records) for party in parties]
    )
    current = None if labels is None else current_decisions(labels, btr=btr, crs=crs)
    by_model = (
        calibrate(best, truth=by_id, labels=current)
        .join(by_id, on="row_id", how="anti")
        .filter(pl.col("probability") >= LINK_PROBABILITY)
    )
    links = _as_links(
        pl.concat(
            [
                by_id.with_columns(
                    link_method=pl.lit("btr_id"), probability=pl.lit(1.0)
                ),
                by_model.select(
                    *_PAIR,
                    "probability",
                    "model_version",
                    link_method=pl.lit("btr_splink"),
                    p_model="p_raw",
                ),
            ],
            how="diagonal",
        )
    )
    draw = calibration_draw(best, truth=by_id).select(
        pl.col("row_id").alias("left_id"),
        pl.col("record_id").alias("right_id"),
        "party",
        "match_weight",
        "draw_rank",
    )
    draw = draw.join(
        btr_digests(draw.select("left_id", "right_id"), btr=btr, crs=crs),
        on=["left_id", "right_id"],
        maintain_order="left",
    )
    return BtrLinks(
        links=links,
        draw=draw,
        labels=current,
    )


def current_decisions(
    labels: pl.DataFrame, *, btr: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.DataFrame:
    """The `btr_crs` decisions of `labels` whose fingerprints (`left_digest`,
    `right_digest`) equal the digests of what a packet shows of the BTR row and the
    CRS record now (`packet.btr_digests`).

    A BTR row id names a party, a year and a position in the party's workbook, so
    a revised workbook can put another activity under the same id. A decision
    reaches only the row and record its labeller saw, and one with no fingerprint,
    or on a side that changed, is left out, so its pair returns to the queue.
    """
    decided = labels.filter(task="btr_crs")
    pair = ["left_id", "right_id"]
    now = btr_digests(decided.select(pair).unique(), btr=btr, crs=crs)
    return decided.join(
        now,
        on=[*pair, "left_digest", "right_digest"],
        how="semi",
        maintain_order="left",
    )


def btr_rows(btr: pl.LazyFrame) -> pl.DataFrame:
    """BTR rows with their party's DAC provider code as `donor_code` and their
    recipient key (`rec_key`), `is_aggregate` rows left out."""
    rows = (
        btr.filter(~pl.col("is_aggregate"))
        .select(
            "row_id",
            "party",
            "year",
            "recipient",
            "title",
            "additional_information",
            "face_usd",
            "status",
            "sector",
            "instrument",
            "support_type",
            donor_code="provider_code",
        )
        .collect()
    )
    countries = {
        name: str(code)
        for name in rows["recipient"].drop_nulls().unique()
        if (code := recipient_code(name)) in country_codes()
    }
    return rows.with_columns(
        rec_key=pl.when(pl.col("recipient").is_not_null()).then(
            pl.col("recipient").replace_strict(
                countries, default=AGGREGATE, return_dtype=pl.String
            )
        ),
    )


def crs_records(crs: pl.LazyFrame, *, rows: pl.DataFrame) -> pl.DataFrame:
    """The CRS records a BTR row can link to: its party's, of its year, Bi_Multi 2
    left out. The id tier reads every one, since a programme number names its
    programme whatever the record's amounts; the model reads those with a
    commitment or a disbursement."""
    scope = rows.select("donor_code", "year").drop_nulls().unique()
    return (
        crs.join(scope.lazy(), on=["donor_code", "year"], how="semi")
        .filter(pl.col("bi_multi") != 2)
        .select(
            "record_id",
            "donor_code",
            "year",
            "crs_id",
            "project_number",
            "title",
            "short_description",
            "long_description",
            "recipient_code",
            "purpose_code",
            "finance_t",
            "commitment_usd",
            "disbursement_usd",
            "rio_adaptation",
            "rio_mitigation",
        )
        .sort("record_id")
        .collect()
    )


def link_by_ids(rows: pl.DataFrame, *, records: pl.DataFrame) -> pl.DataFrame:
    """Each BTR row and every same-year record of its party that an id written in
    its title or additional information names: `row_id`, `record_id`. A row for one
    country keeps the records of that country and of regional or unspecified
    recipients."""
    ids = (
        rows.select(
            "row_id",
            "donor_code",
            "year",
            "rec_key",
            key=pl.concat_str(
                pl.col("title").fill_null(""),
                pl.col("additional_information").fill_null(""),
                separator=" ",
            ).str.extract_all(ID_PATTERN),
        )
        .explode("key")
        .with_columns(key=_id_key(pl.col("key")))
        .filter(pl.col("key").str.len_chars() >= MIN_ID_CHARS)
    )
    project_number = pl.col("project_number").fill_null("")
    keys = (
        pl.concat(
            [
                records.select(
                    "record_id",
                    "donor_code",
                    "year",
                    "recipient_code",
                    key=_id_key(source),
                )
                for source in (
                    project_number,
                    project_number.str.split("-").list.first(),
                    pl.col("crs_id").fill_null(""),
                )
            ]
        )
        .filter(pl.col("key").str.len_chars() >= MIN_ID_CHARS)
        .unique()
    )
    rec_key, recipient = pl.col("rec_key"), pl.col("recipient_code")
    partners = (
        rec_key.is_null()
        | (rec_key == AGGREGATE)
        | recipient.is_null()
        | recipient.is_in(regional_codes())
        | (recipient.cast(pl.String) == rec_key)
    )
    return (
        ids.join(keys, on=["donor_code", "year", "key"])
        .filter(partners)
        .select(_PAIR)
        .unique()
        .sort(_PAIR)
    )


def score_party(rows: pl.DataFrame, *, records: pl.DataFrame) -> pl.DataFrame:
    """Each of one party's BTR rows with its best-scoring candidate record:
    `party`, `row_id`, `record_id`, `match_weight`, `p_raw`, `model_version`.
    Empty when the party has no records or no EM session completes, the second
    logged as a warning."""
    party = rows["party"][0]
    own = records.filter(
        pl.col("donor_code") == rows["donor_code"][0],
        (pl.col("commitment_usd").fill_null(0) > 0)
        | (pl.col("disbursement_usd").fill_null(0) > 0),
    )
    if own.is_empty():
        return pl.DataFrame(schema=_BEST)
    con = duckdb.connect()
    try:
        inputs = _register(con, left=_btr_input(rows), right=_crs_input(own))
        model = _fit(con, inputs, party=party)
        if model is None:
            _log.warning(
                "BTR %s: no EM session completed, so none of its %d rows links by "
                "the model against its %d CRS records",
                party,
                rows.height,
                own.height,
            )
            return pl.DataFrame(schema=_BEST)
        predictions = _linker(inputs, model).inference.predict()
        scored = con.sql(
            "select unique_id_l as row_id, unique_id_r as record_id, match_weight, "
            f"match_probability as p_raw from {predictions.physical_name}"
        ).pl()
    finally:
        con.close()
    return (
        scored.sort(
            "row_id", "match_weight", "record_id", descending=[False, True, False]
        )
        .unique("row_id", keep="first", maintain_order=True)
        .select(
            pl.lit(party).alias("party"),
            "row_id",
            "record_id",
            "match_weight",
            "p_raw",
            model_version=pl.lit(f"splink-{splink.__version__}:btr:{party}"),
        )
    )


def calibrate(
    best: pl.DataFrame, *, truth: pl.DataFrame, labels: pl.DataFrame | None = None
) -> pl.DataFrame:
    """`best` (one scored candidate per BTR row, with `party` and `match_weight`)
    with `probability`, null where no map reads the row.

    A row with id records (`truth`: row_id, record_id) labels its best candidate 1
    when the candidate is one of them, else 0. A `human` or `agent` decision of task
    `btr_crs` in `labels` (`decisions.read_decision_labels`) on an id-less row's best
    candidate labels it too, 1 when accepted (`_decided`). A party with
    `MIN_MAP_OUTCOMES` of each label takes its own isotonic map.

    Any other party reads one pooled map at weights from `POOLED_FLOOR` up, and its
    rows below the floor read null. The pool holds the labels of the parties with
    their own map and of each other party whose labels from the floor up pass the
    check (`_pool_check`). A party whose labels fail it links nothing by the model,
    and its labels leave the pool, so a party on another scale cannot move the map
    the others read. A party with fewer labels there than the check needs reads the
    pool unchecked and adds nothing to it. Until people label the draw's stratum
    from the floor up, that is most parties without their own map. A pool holding
    fewer than `MIN_MAP_OUTCOMES` of either outcome links nothing.

    Each party logs the map its id-less links read: its own map or a checked pool
    at INFO, an unchecked pool, a failed check or no map as a warning. A weight
    below the map's lowest labelled weight reads 0, since no label vouches for it.
    """
    columns = ["party", "match_weight", "y", "weight", "left_id", "right_id"]
    by_id = _id_labels(best, truth=truth)
    labelled = (
        by_id
        if labels is None
        else pl.concat([by_id, _decided(best, truth=truth, labels=labels)])
    ).select(columns)
    maps = {
        party: _map(labelled.filter(party=party))
        for party in best["party"].unique(maintain_order=True)
    }
    checks = {
        party: _pool_check(labelled.filter(party=party))
        for party, own in maps.items()
        if own is None
    }
    pool = [party for party, own in maps.items() if own is not None] + [
        party for party, check in checks.items() if check.passes
    ]
    pooled = _map(labelled.filter(pl.col("party").is_in(pool)))
    frames = [best.clear().with_columns(probability=pl.lit(None, dtype=pl.Float64))]
    for (party,), scored in best.group_by("party", maintain_order=True):
        own, check = maps[party], checks.get(party)
        if own is not None:
            mapping, floor = own, -math.inf
        elif pooled is not None and check is not None and not check.fails:
            mapping, floor = pooled, POOLED_FLOOR
        else:
            _log_unlinked(party, rows=scored.height, check=check, pooled=pooled)
            frames.append(
                scored.with_columns(probability=pl.lit(None, dtype=pl.Float64))
            )
            continue
        weights = scored["match_weight"].to_numpy()
        probability = pl.Series(
            np.where(weights < mapping.X_min_, 0.0, mapping.predict(weights)),
            dtype=pl.Float64,
        )
        read = scored.with_columns(
            probability=pl.when(pl.col("match_weight") >= floor).then(probability)
        )
        linked = (
            read.join(truth, on="row_id", how="anti")
            .filter(pl.col("probability") >= LINK_PROBABILITY)
            .height
        )
        _log_linked(party, linked=linked, check=check)
        frames.append(read)
    return pl.concat(frames).sort("row_id")


class _Check(NamedTuple):
    """A party's labels from `POOLED_FLOOR` up: how many, how many right, and
    their share right weighted as `calibrate` weighs them."""

    labels: int
    right: int
    share: float

    @property
    def ran(self) -> bool:
        return self.labels >= MIN_CHECK_LABELS

    @property
    def passes(self) -> bool:
        return self.ran and self.share >= POOLED_CHECK_SHARE

    @property
    def fails(self) -> bool:
        return self.ran and self.share < POOLED_CHECK_SHARE


def _pool_check(labelled: pl.DataFrame) -> _Check:
    """The check of a party without its own map on its labels (`labelled`) from
    `POOLED_FLOOR` up. It runs once they number `MIN_CHECK_LABELS`, and passes while
    their weighted share right is at least `POOLED_CHECK_SHARE`.

    On thirty unweighted labels, as the draw's stratum from the floor up gives, a
    party whose rows there are right at 0.93, the lowest share a held-out party
    showed, fails with probability 0.004, and one right at 0.85, the lowest band of
    any held-out party, with probability 0.15. A party right at 0.7 passes with
    probability 0.16, and one right at 0.6 with 0.017. Until it runs, the party's
    pooled links rest on the held-out evidence behind `POOLED_FLOOR` alone.
    """
    above = labelled.filter(pl.col("match_weight") >= POOLED_FLOOR)
    if above.is_empty():
        return _Check(labels=0, right=0, share=0.0)
    return _Check(
        labels=above.height,
        right=int(above["y"].sum()),
        share=above.select(
            (pl.col("y") * pl.col("weight")).sum() / pl.col("weight").sum()
        ).item(),
    )


def _log_linked(party: str, *, linked: int, check: _Check | None) -> None:
    """Log the map a party's `linked` id-less rows read: its own (`check` None), a
    checked pool, or an unchecked one, the last as a warning."""
    if check is None:
        _log.info("BTR %s: %d id-less rows link on its own map", party, linked)
    elif check.ran:
        _log.info(
            "BTR %s: %d id-less rows link on the pooled map from weight %g, checked "
            "on its %d labels there, %d right (weighted share %.2f)",
            party,
            linked,
            POOLED_FLOOR,
            check.labels,
            check.right,
            check.share,
        )
    else:
        _log.warning(
            "BTR %s: %d id-less rows link on the pooled map from weight %g "
            "unchecked, on %d of the %d labels there a check needs",
            party,
            linked,
            POOLED_FLOOR,
            check.labels,
            MIN_CHECK_LABELS,
        )


def _log_unlinked(
    party: str, *, rows: int, check: _Check | None, pooled: IsotonicRegression | None
) -> None:
    """Warn that none of a party's `rows` links by the model: its labels failed the
    pool's check, or no map holds enough labels."""
    if check is not None and check.fails:
        _log.warning(
            "BTR %s: %d of its %d labels from weight %g up are right (weighted "
            "share %.2f, under %.2f), so none of its %d rows links by the pooled "
            "map and its labels leave the pool",
            party,
            check.right,
            check.labels,
            POOLED_FLOOR,
            check.share,
            POOLED_CHECK_SHARE,
            rows,
        )
    elif pooled is None:
        _log.warning(
            "BTR %s: no calibration map holds %d labels of each outcome, so "
            "none of its %d rows links by the model",
            party,
            MIN_MAP_OUTCOMES,
            rows,
        )


def _id_labels(best: pl.DataFrame, *, truth: pl.DataFrame) -> pl.DataFrame:
    """The best candidate of each row with id records in `truth`, labelled `y` 1
    when it is one of them, else 0, each of `weight` 1."""
    return (
        best.join(truth.select("row_id").unique(), on="row_id", how="semi")
        .join(truth.with_columns(y=pl.lit(1.0)), on=_PAIR, how="left")
        .select(
            "party",
            "match_weight",
            pl.col("y").fill_null(0.0),
            weight=pl.lit(1.0),
            left_id="row_id",
            right_id="record_id",
        )
    )


def calibration_draw(best: pl.DataFrame, *, truth: pl.DataFrame) -> pl.DataFrame:
    """The id-less rows of `best` drawn for people to label: `party`, `row_id`,
    `record_id` (its best candidate), `match_weight` and `draw_rank`.

    Each party gives `CALIBRATION_DRAWS` of its rows with no id records in `truth`
    from the strata of `CALIBRATION_STRATA` below `POOLED_FLOOR`, spread evenly over
    them, a stratum short of its share giving all its rows. A party whose id labels
    hold fewer than `MIN_MAP_OUTCOMES` of either outcome also gives
    `MIN_CHECK_LABELS` from the stratum from `POOLED_FLOOR` up, which a party with
    its own map never reads the pooled map in. Within a stratum, rows are taken in
    the order of a digest of the pair (`jev.spread_sample`), so the same scores give
    the same draw. `draw_rank` numbers a party's draw from 1 in that order, the
    strata in turn, so the first k ranks of a party are a random subset of each of
    its strata.
    """
    top = pl.col("stratum") == len(CALIBRATION_STRATA) - 1
    idless = (
        best.join(truth.select("row_id").unique(), on="row_id", how="anti")
        .with_columns(stratum=_stratum())
        .filter(_drawn_stratum(best, truth=truth))
        .select("party", "row_id", "record_id", "match_weight", "stratum")
        .with_columns(left_id="row_id", right_id="record_id")
    )
    columns = ["party", "row_id", "record_id", "match_weight"]
    draws = [idless.clear().select(*columns, draw_rank=pl.lit(0, dtype=pl.Int32))]
    for (_,), rows in idless.sort("party").group_by("party", maintain_order=True):
        drawn = pl.concat(
            [
                spread_sample(
                    rows.filter(~top), band=pl.col("stratum"), n=CALIBRATION_DRAWS
                ),
                spread_sample(rows.filter(top), band=top, n=MIN_CHECK_LABELS),
            ]
        )
        draws.append(
            drawn.with_columns(turn=pl.int_range(pl.len()).over("stratum"))
            .sort("turn", "stratum")
            .with_row_index("draw_rank", offset=1)
            .select(*columns, pl.col("draw_rank").cast(pl.Int32))
        )
    return pl.concat(draws).sort("party", "row_id")


def _decided(
    best: pl.DataFrame, *, truth: pl.DataFrame, labels: pl.DataFrame
) -> pl.DataFrame:
    """The `human` and `agent` decisions of task `btr_crs` in `labels` on the best
    candidate of a row with no id records in `truth`, as calibration labels.

    A `stratified` decision (one on a pair `calibration_draw` drew) whose pair scores
    inside a stratum the draw takes a share of for its party (`_drawn_stratum`),
    the open-ended one from `POOLED_FLOOR` up included, weighs the id-less rows of
    its party and stratum over the stratified decisions there. Any other weighs 1,
    including a drawn pair that now scores outside its party's strata. A decision
    on a record other than the row's best candidate says nothing about the
    candidate, so it is left out.
    """
    idless = best.join(truth.select("row_id").unique(), on="row_id", how="anti")
    decided = labels.filter(
        pl.col("pair_label_source").is_in(schema.DECIDERS), task="btr_crs"
    ).select(
        row_id="left_id",
        record_id="right_id",
        y=pl.col("is_match").cast(pl.Float64),
        stratified="stratified",
    )
    stratum = ["party", "stratum"]
    drawn = pl.col("stratified") & _drawn_stratum(best, truth=truth)
    return (
        idless.with_columns(stratum=_stratum())
        .with_columns(rows=pl.len().over(stratum))
        .join(decided, on=_PAIR)
        .select(
            "party",
            "match_weight",
            "y",
            weight=pl.when(drawn)
            .then(pl.col("rows") / drawn.sum().over(stratum))
            .otherwise(1.0),
            left_id="row_id",
            right_id="record_id",
        )
    )


def _drawn_stratum(best: pl.DataFrame, *, truth: pl.DataFrame) -> pl.Expr:
    """Whether a row's `stratum` (`_stratum`) is one `calibration_draw` takes a
    share of for its `party`: every stratum, but the one from `POOLED_FLOOR` up only
    for a party whose id labels in `best` and `truth` hold fewer than
    `MIN_MAP_OUTCOMES` of either outcome, as a party with its own map never reads
    the pooled map."""
    own = (
        _id_labels(best, truth=truth)
        .group_by("party")
        .agg(right=pl.col("y").sum(), wrong=(1 - pl.col("y")).sum())
        .filter(pl.min_horizontal("right", "wrong") >= MIN_MAP_OUTCOMES)["party"]
    )
    stratum = pl.col("stratum")
    return stratum.is_not_null() & (
        (stratum < len(CALIBRATION_STRATA) - 1) | ~pl.col("party").is_in(own.implode())
    )


def _stratum() -> pl.Expr:
    """The index of the stratum of `CALIBRATION_STRATA` a row's match weight falls
    in, from its lower edge up to its upper, null outside every stratum."""
    weight = pl.col("match_weight")
    index = pl.lit(None, dtype=pl.Int32)
    for i, (low, high) in reversed(list(enumerate(CALIBRATION_STRATA))):
        index = (
            pl.when(weight.is_between(low, high, closed="left"))
            .then(i)
            .otherwise(index)
        )
    return index


def _map(labelled: pl.DataFrame) -> IsotonicRegression | None:
    """The isotonic map of `labelled`, or None when it holds fewer than
    `MIN_MAP_OUTCOMES` of either outcome."""
    right = labelled["y"].sum()
    if min(right, labelled.height - right) < MIN_MAP_OUTCOMES:
        return None
    return isotonic_map(labelled)


def linked_pairs(
    links: pl.DataFrame, *, btr: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.DataFrame:
    """The linked pairs `readers.btr.estimate_party_weights` reads, one per BTR
    row: party, the row's year and face value, the CRS amount it is set against (the
    disbursement for a disbursed row, else the commitment, summed over the row's
    records), the records' highest Rio marker, and `significant_on_both`, true where
    every record is Significant for both objectives."""
    pairs = links.filter(task="btr_crs").select(row_id="left_id", record_id="right_id")
    disbursed = pl.col("status") == "disbursed"
    return (
        pairs.lazy()
        .join(
            btr.select("row_id", "party", "year", "status", btr_usd="face_usd"),
            on="row_id",
        )
        .join(
            crs.select(
                "record_id",
                "commitment_usd",
                "disbursement_usd",
                rio=pl.max_horizontal("rio_adaptation", "rio_mitigation"),
                both=(pl.col("rio_adaptation") == 1) & (pl.col("rio_mitigation") == 1),
            ),
            on="record_id",
        )
        .group_by("row_id", "party", "year", "btr_usd")
        .agg(
            crs_usd=pl.when(disbursed.first())
            .then(pl.col("disbursement_usd").sum())
            .otherwise(pl.col("commitment_usd").sum()),
            rio_marker=pl.col("rio").max().fill_null(0),
            significant_on_both=pl.col("both").fill_null(value=False).all(),
        )
        .sort("row_id")
        .select(
            "party", "year", "btr_usd", "crs_usd", "rio_marker", "significant_on_both"
        )
        .collect()
    )


def _fit(
    con: duckdb.DuckDBPyConnection, inputs: list[Any], *, party: str
) -> dict[str, Any] | None:
    """The party's fitted model as Splink's settings dict, or None when no EM
    session completes. A rule blocking fewer than `MIN_EM_PAIRS` pairs gives way to
    its fallback. u and EM run on one DuckDB thread, so two builds fit the same
    model."""
    pairs = _count(
        con,
        f"select sum(a.n * b.n) from (select year, count(*) n from {LEFT} group by 1) "
        f"a join (select year, count(*) n from {RIGHT} group by 1) b using (year)",
    )
    if not pairs:
        return None
    # One true pair per BTR row among the same-year pairs.
    prior = min(_count(con, f"select count(*) from {LEFT}") / pairs, 0.5)
    linker = _linker(inputs, _settings(prior=prior))
    model = linker.misc.save_model_to_json()
    completed = 0
    con.sql("set threads = 1")
    try:
        linker.training.estimate_u_using_random_sampling(
            max_pairs=U_MAX_PAIRS, seed=SEED
        )
        model = linker.misc.save_model_to_json()
        for options in EM_SESSIONS:
            for rule in options:
                blocked = f"select count(*) from {LEFT} l join {RIGHT} r on {rule}"
                if _count(con, blocked) < MIN_EM_PAIRS:
                    continue
                try:
                    linker.training.estimate_parameters_using_expectation_maximisation(
                        rule
                    )
                except ValueError as error:
                    # Splink refuses an m EM drives to zero after overwriting part
                    # of the model, so the session rolls back.
                    _log.info("BTR %s: EM on %r failed: %s", party, rule, error)
                    linker = _linker(inputs, deepcopy(model))
                    continue
                model = linker.misc.save_model_to_json()
                completed += 1
                break
    finally:
        con.sql("reset threads")
    return model if completed else None


def _linker(inputs: list[Any], settings: SettingsCreator | dict[str, Any]) -> Linker:
    return Linker(inputs, settings, log_level=logging.ERROR)


def _settings(*, prior: float) -> SettingsCreator:
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        probability_two_random_records_match=prior,
        blocking_rules_to_generate_predictions=[
            CustomRule(rule) for rule in BLOCKING_RULES
        ],
        comparisons=comparisons(),
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
    )


def comparisons() -> list[ComparisonCreator | dict[str, Any]]:
    """Text, recipient, amount, sector, instrument and Rio marker comparisons. `_l`
    is the BTR row, `_r` the CRS record."""
    found = (
        "len(list_intersect(title_toks_l, text_toks_r))::double "
        "/ nullif(len(title_toks_l), 0)"
    )
    amount_r = "case when status_l = 'disbursed' then amt_d_r else amt_c_r end"
    ratio = f"amt_l / nullif({amount_r}, 0)"
    # A party that counts a Significant activity at 0.4 or 0.5 reports that share
    # of the CRS amount.
    weighted = " or ".join(
        f"abs({ratio} / {level} - 1) <= {LEVEL_TOLERANCE}"
        for level in WEIGHT_LEVELS
        if level < 1
    )
    return [
        cl.CustomComparison(
            output_column_name="text",
            comparison_levels=[
                cll.CustomLevel(
                    "title_norm_l is null "
                    "or (title_norm_r is null and desc_norm_r is null)"
                ).configure(is_null_level=True),
                cll.CustomLevel(
                    "title_norm_l = title_norm_r or title_norm_l = desc_norm_r "
                    "or title_norm_l = title_desc_r",
                    label_for_charts="title exact",
                ),
                cll.CustomLevel(
                    f"{found} >= 0.8 "
                    "or jaro_winkler_similarity(title_norm_l, title_norm_r) >= 0.93",
                    label_for_charts="title words >= 0.8 in CRS text",
                ),
                cll.CustomLevel(
                    f"{found} >= 0.5", label_for_charts="title words >= 0.5"
                ),
                cll.CustomLevel(
                    f"{found} >= 0.25", label_for_charts="title words >= 0.25"
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="recipient",
            comparison_levels=[
                cll.NullLevel("rec_key"),
                cll.CustomLevel(
                    f"rec_key_l = rec_key_r and rec_key_l <> '{AGGREGATE}'",
                    label_for_charts="same country",
                ),
                cll.CustomLevel("rec_key_l = rec_key_r", label_for_charts="aggregate"),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="amount",
            comparison_levels=[
                cll.CustomLevel(
                    f"amt_l is null or amt_l <= 0 or {amount_r} is null "
                    f"or {amount_r} <= 0"
                ).configure(is_null_level=True),
                cll.CustomLevel(f"abs({ratio} - 1) <= 0.02", label_for_charts="2%"),
                cll.CustomLevel(weighted, label_for_charts="at a party weight"),
                cll.CustomLevel(f"abs({ratio} - 1) <= 0.15", label_for_charts="15%"),
                cll.CustomLevel(
                    f"{ratio} between 0.2 and 0.85", label_for_charts="20-85%"
                ),
                cll.CustomLevel(
                    f"{ratio} between 0.05 and 5", label_for_charts="within 20x"
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="sector",
            comparison_levels=[
                cll.CustomLevel(
                    "sector_l is null or sector_r is null or sector_l = 'other'"
                ).configure(is_null_level=True),
                cll.ExactMatchLevel("sector"),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="instrument",
            comparison_levels=[
                cll.NullLevel("instr"),
                cll.ExactMatchLevel("instr"),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="marker",
            comparison_levels=[
                cll.CustomLevel("type_l is null").configure(is_null_level=True),
                cll.CustomLevel(
                    "(type_l = 'adaptation' and ad_r > 0) "
                    "or (type_l = 'mitigation' and mi_r > 0) "
                    "or (type_l = 'both' and ad_r > 0 and mi_r > 0)",
                    label_for_charts="Rio marker agrees",
                ),
                cll.CustomLevel(
                    "ad_r > 0 or mi_r > 0", label_for_charts="other climate marker"
                ),
                cll.ElseLevel(),
            ],
        ),
    ]


def _btr_input(rows: pl.DataFrame) -> pl.DataFrame:
    """One party's BTR rows as the model's left input."""
    title = pl.col("title")
    for tag in TITLE_TAGS:
        title = title.str.replace_all(tag, "")
    words = text_words(title)
    support = pl.col("support_type").str.to_lowercase()
    instrument = pl.col("instrument").str.to_lowercase()
    return _conform(
        rows.select(
            "year",
            "rec_key",
            "status",
            unique_id="row_id",
            title_norm=_joined(words),
            title_toks=words,
            amt="face_usd",
            sector=_btr_sector(pl.col("sector")),
            instr=pl.when(instrument.str.contains("grant"))
            .then(pl.lit("grant"))
            .when(instrument.str.contains("loan|credit"))
            .then(pl.lit("loan"))
            .when(instrument.str.contains("equity|share"))
            .then(pl.lit("equity"))
            .when(instrument.is_not_null())
            .then(pl.lit("other")),
            type=pl.when(support.str.contains("cross"))
            .then(pl.lit("both"))
            .when(support.str.contains("adapt"))
            .then(pl.lit("adaptation"))
            .when(support.str.contains("mitig"))
            .then(pl.lit("mitigation")),
        )
    )


def _crs_input(records: pl.DataFrame) -> pl.DataFrame:
    """One party's CRS records as the model's right input."""
    title, short = pl.col("title"), pl.col("short_description")
    finance = pl.col("finance_t") // 100
    return _conform(
        records.select(
            "year",
            unique_id="record_id",
            rec_key=pl.when(pl.col("recipient_code").is_in(regional_codes()))
            .then(pl.lit(AGGREGATE))
            .otherwise(pl.col("recipient_code").cast(pl.String)),
            title_norm=_joined(text_words(title)),
            desc_norm=_joined(text_words(short)),
            title_desc=_joined(
                text_words(
                    pl.concat_str(title, short, separator=" ", ignore_nulls=True)
                )
            ),
            text_toks=text_words(
                pl.concat_str(
                    title, short, "long_description", separator=" ", ignore_nulls=True
                )
            ).list.head(TEXT_WORDS),
            amt_c="commitment_usd",
            amt_d="disbursement_usd",
            sector=_crs_sector(pl.col("purpose_code")),
            instr=pl.when(finance == 1)
            .then(pl.lit("grant"))
            .when(finance == 4)
            .then(pl.lit("loan"))
            .when(finance.is_in([5, 6]))
            .then(pl.lit("equity"))
            .when(finance.is_not_null())
            .then(pl.lit("other")),
            ad=pl.col("rio_adaptation").fill_null(0),
            mi=pl.col("rio_mitigation").fill_null(0),
        )
    )


def _conform(frame: pl.DataFrame) -> pl.DataFrame:
    """`frame` with every Splink input column, typed nulls where absent."""
    return frame.select(
        pl.col(name).cast(dtype)
        if name in frame.columns
        else pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in _INPUT.items()
    )


def _joined(words: pl.Expr) -> pl.Expr:
    """Title words joined by spaces, null when there are none."""
    text = words.list.join(" ")
    return pl.when(text != "").then(text)


def _btr_sector(sector: pl.Expr) -> pl.Expr:
    text = sector.str.to_lowercase().str.replace_all("[^a-z0-9]+", " ")
    bucket = pl.when(sector.is_null()).then(pl.lit(None, dtype=pl.String))
    for name, pattern in SECTOR_WORDS:
        bucket = bucket.when(text.str.contains(pattern)).then(pl.lit(name))
    return bucket.otherwise(pl.lit("other"))


def _crs_sector(purpose: pl.Expr) -> pl.Expr:
    category = purpose // 100
    bucket = pl.when(purpose.is_null()).then(pl.lit(None, dtype=pl.String))
    for name, categories in SECTOR_PURPOSES.items():
        bucket = bucket.when(category.is_in(categories)).then(pl.lit(name))
    return bucket.otherwise(pl.lit("other"))


def _id_key(text: pl.Expr) -> pl.Expr:
    return text.str.to_lowercase().str.replace_all("[^a-z0-9]", "")


def _register(
    con: duckdb.DuckDBPyConnection, *, left: pl.DataFrame, right: pl.DataFrame
) -> list[Any]:
    db_api = DuckDBAPI(connection=con)
    inputs = []
    for table, frame in ((LEFT, left), (RIGHT, right)):
        con.register("_frame", frame)
        con.sql(f"create or replace table {table} as select * from _frame")
        con.unregister("_frame")
        inputs.append(db_api.register(table, dataset_display_name=table))
    return inputs


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.sql(sql).fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def _as_links(pairs: pl.DataFrame) -> pl.DataFrame:
    """`pairs` (row_id, record_id, link_method, probability, and optionally p_model
    and model_version) as `links` rows of task `btr_crs`."""
    given: Mapping[str, pl.Expr] = {
        "task": pl.lit("btr_crs"),
        "left_id": pl.col("row_id"),
        "right_id": pl.col("record_id"),
        "group_id": pl.col("row_id"),
        "review": pl.lit(value=False),
        **{
            name: pl.col(name)
            for name in ("link_method", "probability", "p_model", "model_version")
            if name in pairs.columns
        },
    }
    return links_frame(pairs, given).sort("left_id", "right_id")
