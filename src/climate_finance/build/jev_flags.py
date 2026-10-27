"""The flag stage: Jev's four questions about every climate-marked CRS record.

With `jev_flags` on, Jev reads the title and long description of each CRS record
from `jev_flags_first_year` to `years.crs_last_complete` with a Rio mitigation or
adaptation marker of 1 or 2 and a positive commitment or disbursement, and answers
whether the activity aims at mitigation, at adaptation, could work against climate
goals, and would surprise a reader as climate finance. The `jev_flags` table holds
the answers beside the record's own facts. The flags change no total, and no stage or
query weighs a record by them.

The cuts (`schema.NO_AIM_CUT`, `schema.SURPRISING_CUT`) were measured on Jev's
answers to these requests byte for byte, and a client cache of those answers
replays them. A change to any string asks Jev again and needs a new evaluation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import polars as pl

from climate_finance.build.errors import ConfigError
from climate_finance.build.fetch import fetched_years, last_complete_year
from climate_finance.build.label import ask_each_text, project_text
from climate_finance.schema import JEV_FLAGS, NO_AIM_CUT, SURPRISING_CUT

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient
    from climate_finance.build.stages import StageContext, StageOutput

# The aim questions follow the OECD Rio marker handbook definitions of mitigation
# (reduce or limit greenhouse gas emissions, or enhance sinks) and adaptation
# (reduce vulnerability to current and expected climate change impacts, including
# climate variability). They are lenient, so that a low answer means the text
# offers no climate rationale at all.
MITIGATION_AIM = {
    "type": "noul",
    "instructions": (
        "Does this aid activity, as described, aim to mitigate climate change: to "
        "reduce or limit greenhouse gas emissions, or to protect or enhance carbon "
        "sinks such as forests and soils?"
    ),
    "criteria": {
        "true": (
            "The text names a climate change mitigation or emissions objective, or the "
            "activity is in itself a mitigation activity, such as renewable energy, "
            "energy efficiency, low-carbon transport, methane capture, reforestation "
            "or forest protection. Support for climate change policy, negotiations, "
            "finance or institutions, such as a contribution to a climate fund or to "
            "the UNFCCC process, also counts."
        ),
        "false": (
            "The text names no such objective and the activity described is not in "
            "itself a mitigation activity, or the text is too vague to tell. General "
            "environmental, economic or social aims do not count."
        ),
    },
}
ADAPTATION_AIM = {
    "type": "noul",
    "instructions": (
        "Does this aid activity, as described, aim to help people, economies or "
        "ecosystems adapt to climate change: to reduce their vulnerability to current "
        "or expected climate change impacts, including climate variability, or to "
        "strengthen their resilience to them?"
    ),
    "criteria": {
        "true": (
            "The text names climate change, climate variability or climate risk as a "
            "reason for the activity, or the activity directly addresses a climate "
            "hazard such as drought, floods, sea level rise, heat or changing "
            "rainfall. "
            "Support for climate change policy, negotiations, finance or institutions, "
            "such as a contribution to a climate fund or to the UNFCCC process, also "
            "counts."
        ),
        "false": (
            "The text names no such aim, or is too vague to tell. Generic development, "
            "poverty, health, governance, environment or disaster work without a link "
            "to climate hazards does not count."
        ),
    },
}
COUNTERPRODUCTIVE = {
    "type": "noul",
    "instructions": (
        "Could this activity plausibly work against climate goals, by increasing "
        "greenhouse gas emissions or locking in emission-intensive assets?"
    ),
    "criteria": {
        "true": (
            "The activity builds, expands or finances fossil fuel exploration, "
            "extraction, transport, refining or fossil-fuelled power; expands "
            "airports, aviation or road capacity for motor traffic; expands "
            "livestock herds or emission-intensive industry; or clears forest, "
            "peatland or other natural land."
        ),
        "false": (
            "The activity does none of these, or its main effect is to cut emissions "
            "or to reduce climate risk. A small purchase such as a single vehicle does "
            "not count."
        ),
    },
}
SURPRISING = {
    "type": "noul",
    "instructions": (
        "Would a well-informed general reader be surprised to learn that this "
        "activity is counted as climate finance, that is, as money spent to cut "
        "greenhouse gas emissions or to help countries adapt to climate change?"
    ),
    "criteria": {
        "true": (
            "The description shows no evident link to cutting emissions or to coping "
            "with climate change impacts, as with a retail business, a hotel, a "
            "cultural event, administrative costs, or a fossil fuel project."
        ),
        "false": (
            "A link to cutting emissions or to coping with climate change impacts is "
            "evident from the description."
        ),
    },
}
# All four go in one request, under these names.
FLAG_QUESTIONS = {
    "mitigation_aim": MITIGATION_AIM,
    "adaptation_aim": ADAPTATION_AIM,
    "counterproductive": COUNTERPRODUCTIVE,
    "surprising": SURPRISING,
}
STATE_KEY = "aid_activity_description"
# Texts shorter than this go unasked, as a title alone tells Jev too little.
MIN_TEXT_CHARS = 60


def run_flag(context: StageContext) -> StageOutput:
    """Write `jev_flags` when `jev_flags` is on, and nothing otherwise.

    Raises:
        ConfigError: when the CRS the build reads starts after
            `jev_flags_first_year`, or `years.crs_last_complete` lies outside it.
    """
    config = context.config
    if not config.jev_flags:
        return {}
    first_year = config.jev_flags_first_year
    crs_first = fetched_years(context)["crs"][0]
    if crs_first > first_year:
        raise ConfigError(
            f"jev_flags_first_year {first_year} lies before the first CRS year the "
            f"build reads, {crs_first}; set an earlier CRS year or a later "
            "jev_flags_first_year."
        )
    records = flag_records(
        context.read("normalise", "crs"),
        first_year=first_year,
        last_year=last_complete_year(context),
    )
    return {"jev_flags": jev_flags(records.collect(), client=context.jev)}


def flag_records(crs: pl.LazyFrame, *, first_year: int, last_year: int) -> pl.LazyFrame:
    """The CRS records Jev flags, with their facts and `project_text`.

    A record qualifies from `first_year` to `last_year` with a Rio mitigation or
    adaptation marker of 1 or 2 and a positive commitment or disbursement.
    """
    return crs.filter(
        pl.col("year").is_between(first_year, last_year),
        (pl.col("rio_mitigation") >= 1) | (pl.col("rio_adaptation") >= 1),
        (pl.col("commitment_usd") > 0) | (pl.col("disbursement_usd") > 0),
    ).select(
        "record_id",
        "year",
        "agency_code",
        "recipient_code",
        "purpose_code",
        "flow_code",
        "project_number",
        "crs_id",
        "title",
        "commitment_usd",
        "disbursement_usd",
        "rio_adaptation",
        "rio_mitigation",
        provider_code="donor_code",
        text=project_text(pl.col("title"), pl.col("long_description")),
    )


def flag_request(text: str) -> dict[str, object]:
    """The request that asks Jev `FLAG_QUESTIONS` about `text`."""
    return {"state": {STATE_KEY: text}, "questions": FLAG_QUESTIONS}


def _read_answers(answers: Mapping[str, Any]) -> tuple[float, ...]:
    return tuple(float(answers[name]["noul"]) for name in FLAG_QUESTIONS)


def jev_flags(records: pl.DataFrame, *, client: JevClient) -> pl.DataFrame:
    """`JEV_FLAGS` rows for `records` (`flag_records`), asking Jev about each
    distinct text of at least `MIN_TEXT_CHARS` once."""
    short = pl.col("text").str.len_chars() < MIN_TEXT_CHARS
    answers = ask_each_text(
        records.filter(~short).select("record_id", "text"),
        client=client,
        request=flag_request,
        read=_read_answers,
        columns=tuple(f"{name}_p" for name in FLAG_QUESTIONS),
    )
    no_aim = 1 - pl.max_horizontal("mitigation_aim_p", "adaptation_aim_p")
    return (
        records.join(answers, on="record_id", how="left", validate="1:1")
        .with_columns(short_text=short, no_aim_p=no_aim, model=pl.lit(client.model))
        .with_columns(
            no_aim=pl.col("no_aim_p") >= NO_AIM_CUT,
            surprising=pl.col("surprising_p") >= SURPRISING_CUT,
        )
        .select(list(JEV_FLAGS.columns))
    )
