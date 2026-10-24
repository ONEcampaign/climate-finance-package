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
labels hold `MIN_MAP_OUTCOMES` of each outcome, else one pooled over every party's
labels. A map with fewer of either outcome links nothing, and a weight below a
map's lowest label reads 0. A row the id tier did not link takes its best candidate
when the calibrated probability is at least `LINK_PROBABILITY`. Unlinked rows are
UNFCCC only.

Recipients link on the geo resolver's DAC code (`orgs.recipient_code`), so
"Vietnam" and "Turkey" find Viet Nam and Türkiye. A recipient that names no single
country (a region, a list of countries, a name the resolver does not know) and a CRS
regional or unspecified code (`keys.regional_codes`) both read as `aggregate`.
"""

import logging
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

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
from climate_finance.build.link.keys import regional_codes
from climate_finance.build.link.splink_models import (
    MIN_EM_PAIRS,
    isotonic_map,
)
from climate_finance.build.link.text import text_words
from climate_finance.build.readers.btr import LEVEL_TOLERANCE, WEIGHT_LEVELS

_log = logging.getLogger(__name__)

LINK_PROBABILITY = 0.5
# Labels of each outcome a calibration map needs. On the archive, four parties hold
# their own map (Germany 882 right and 55 wrong, France 210 and 65, the UK 184 and
# 86, Norway 1,156 and 84); Denmark (407 and 19) and the Netherlands (324 and 10)
# take the pooled map, 3,164 and 320.
MIN_MAP_OUTCOMES = 30
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


def link_btr(btr: pl.LazyFrame, *, crs: pl.LazyFrame) -> pl.DataFrame:
    """`links` rows of task `btr_crs`, from BTR row (left) to CRS record (right).

    Args:
        btr: The `btr` table.
        crs: The normalised `crs` table.
    """
    rows = btr_rows(btr)
    records = crs_records(crs, rows=rows)
    by_id = link_by_ids(rows, records=records)
    parties = rows.filter(pl.col("donor_code").is_not_null())["party"].unique().sort()
    best = pl.concat(
        [pl.DataFrame(schema=_BEST)]
        + [score_party(rows.filter(party=party), records=records) for party in parties]
    )
    by_model = (
        calibrate(best, truth=by_id)
        .join(by_id, on="row_id", how="anti")
        .filter(pl.col("probability") >= LINK_PROBABILITY)
    )
    return _as_links(
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
    regional = regional_codes()
    countries = {
        name: str(code)
        for name in rows["recipient"].drop_nulls().unique()
        if (code := recipient_code(name)) is not None and code not in regional
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


def calibrate(best: pl.DataFrame, *, truth: pl.DataFrame) -> pl.DataFrame:
    """`best` (one scored candidate per BTR row, with `party` and `match_weight`)
    with `probability`, null for a party with no map.

    A row with id records (`truth`: row_id, record_id) labels its best candidate 1
    when the candidate is one of them, else 0. A party with `MIN_MAP_OUTCOMES` of
    each label takes its own isotonic map, any other the map pooled over every
    party's labels when the pool holds as many. A party left without a map is
    logged as a warning. A weight below the map's lowest labelled weight reads 0,
    since no label vouches for it.
    """
    labelled = (
        best.join(truth.select("row_id").unique(), on="row_id", how="semi")
        .join(truth.with_columns(y=pl.lit(1.0)), on=_PAIR, how="left")
        .with_columns(
            pl.col("y").fill_null(0.0),
            weight=pl.lit(1.0),
            left_id="row_id",
            right_id="record_id",
        )
    )
    pooled = _map(labelled)
    frames = [best.clear().with_columns(probability=pl.lit(None, dtype=pl.Float64))]
    for (party,), scored in best.group_by("party", maintain_order=True):
        own = _map(labelled.filter(party=party))
        mapping = pooled if own is None else own
        if mapping is None:
            _log.warning(
                "BTR %s: no calibration map holds %d labels of each outcome, so "
                "none of its %d rows links by the model",
                party,
                MIN_MAP_OUTCOMES,
                scored.height,
            )
            frames.append(
                scored.with_columns(probability=pl.lit(None, dtype=pl.Float64))
            )
            continue
        weights = scored["match_weight"].to_numpy()
        frames.append(
            scored.with_columns(
                probability=pl.Series(
                    np.where(weights < mapping.X_min_, 0.0, mapping.predict(weights)),
                    dtype=pl.Float64,
                )
            )
        )
    return pl.concat(frames).sort("row_id")


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
    row: party, the row's face value, the CRS amount it is set against (the
    disbursement for a disbursed row, else the commitment, summed over the row's
    records) and the records' highest Rio marker."""
    pairs = links.filter(task="btr_crs").select(row_id="left_id", record_id="right_id")
    disbursed = pl.col("status") == "disbursed"
    return (
        pairs.lazy()
        .join(btr.select("row_id", "party", "status", btr_usd="face_usd"), on="row_id")
        .join(
            crs.select(
                "record_id",
                "commitment_usd",
                "disbursement_usd",
                rio=pl.max_horizontal("rio_adaptation", "rio_mitigation"),
            ),
            on="record_id",
        )
        .group_by("row_id", "party", "btr_usd")
        .agg(
            crs_usd=pl.when(disbursed.first())
            .then(pl.col("disbursement_usd").sum())
            .otherwise(pl.col("commitment_usd").sum()),
            rio_marker=pl.col("rio").max().fill_null(0),
        )
        .sort("row_id")
        .select("party", "btr_usd", "crs_usd", "rio_marker")
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
    return pairs.select(
        given.get(name, pl.lit(None)).cast(column.dtype).alias(name)
        for name, column in schema.LINKS.columns.items()
    ).sort("left_id", "right_id")
