"""The label stage: inherited markers and objective labels.

A disbursement inherits the Rio markers and climate share of the CRDF commitment it
fills, and keeps the CRS record's own markers beside them. `marker_source` says how
the two relate: `components` where the commitment carries climate components (a
climate share, which the weighting reads in place of markers), `commitment` where
the record's own markers match the inherited ones, and `record_differs` where they
do not.

Each linked record's reported objectives enter `objectives`: the CRDF objective of
the climate-component commitments it fills (`crdf`, level 2), and the record's own
Rio markers (`marker`).

With `jev_labels` on, Jev reads the title and long description of every record
allocated to a commitment without climate components, as the human-coded projects
give them, and answers three adaptation questions in one request.
Its answers to the same questions on Weikmans et al.'s human-coded projects fit two
things: the cut on the lenient answer that labels a record adaptation, and
`jev_level_map`, an isotonic map from Jev's expected coefficient (0.4 x
p(significant) + 1.0 x p(principal)) to the human-coded coefficient. Mitigation has
no human-coded set yet, so Jev labels adaptation only.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from sklearn.isotonic import IsotonicRegression

from climate_finance.build.fetch import ADAPTATION_LABELS_FILE
from climate_finance.methods import ONE
from climate_finance.schema import JEV_LEVEL_MAP, OBJECTIVE_LABELS, OBJECTIVES

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient
    from climate_finance.build.stages import StageContext, StageOutput

# The level a climate-component commitment gives its CRDF objective.
COMPONENT_LEVEL = 2

# The adaptation questions, written from the OECD Rio marker handbook and frozen
# before the held-out evaluation on Weikmans et al.'s projects. A cached answer is
# keyed on this wording, so a change asks Jev again and needs a new evaluation. The
# client sorts the choice's options, so Jev always reads them in alphabetical order.
STRICT = (
    "Does this aid activity state an aim of reducing vulnerability to, or helping "
    "people or systems cope with, the impacts of climate change (adaptation)? Answer "
    "yes only if climate change impacts or climate risk are named or clearly implied "
    "as the reason for the activity. Answer no if climate is not mentioned, or only "
    "environment, development, or general disaster work is described."
)
LENIENT = (
    "Does this aid activity contribute in any way to adaptation to climate change: "
    "reducing vulnerability to climate variability or climate change impacts, "
    "building resilience, or improving coping with droughts, floods, sea level rise "
    "or changing weather?"
)
LEVEL = (
    "Which best describes how strongly this aid activity targets adaptation to "
    "climate change (Rio marker for adaptation)? Judge only from the text given."
)
LEVEL_CRITERIA = {
    "principal": "Climate change adaptation is a fundamental objective of the "
    "activity: it would not have been undertaken without that aim, and the text "
    "says so.",
    "significant": "Adaptation to climate change is a stated objective but "
    "secondary: the activity has another main purpose and also reduces "
    "vulnerability to climate impacts or builds adaptive capacity.",
    "not_targeted": "The activity does not state an adaptation aim. Generic "
    "mentions of environment, development, poverty or disaster without a link to "
    "climate change impacts do not count.",
    "insufficient_info": "The text is too short or too vague to tell whether the "
    "activity has an adaptation aim (for example only a title, a code, or an "
    "administrative note).",
}
ADAPTATION_QUESTIONS = {
    "strict": {"type": "noul", "instructions": STRICT},
    "lenient": {"type": "noul", "instructions": LENIENT},
    "level": {"type": "choice", "instructions": LEVEL, "criteria": LEVEL_CRITERIA},
}
# Cuts on the lenient answer that the threshold fit tries.
THRESHOLDS = tuple(step / 20 for step in range(1, 20))
# Weikmans et al.'s adaptation level per project: 0, 1 or 2, and 99 where the text
# gave too little to tell, which counts as 0.
WEIKMANS_LEVEL = "Our re-evaluation (climateAdaptation)"
NO_TEXT = "(no text)"


def run_label(context: StageContext) -> StageOutput:
    """Write `objectives` and `jev_level_map`.

    The objectives hold the CRDF and marker labels of linked records. With
    `jev_labels` on they also hold Jev's adaptation labels, and `jev_level_map`
    holds the level map fitted on the human-coded labels fetch copied into `raw/`.
    Off, the map is written empty.
    """
    allocations = context.read("fill", "allocations")
    commitments = context.read("normalise", "crdf")
    crs = context.read("normalise", "crs")
    objectives = reported_objectives(allocations, commitments=commitments, crs=crs)
    if not context.config.jev_labels:
        return {"objectives": objectives, "jev_level_map": _empty_level_map()}

    human = read_adaptation_labels(context.raw_dir / ADAPTATION_LABELS_FILE)
    fitted = human.join(
        ask_jev(human.select("label_id", "text"), client=context.jev),
        on="label_id",
        validate="1:1",
    )
    records = jev_records(allocations, commitments=commitments, crs=crs).collect()
    labels = jev_objectives(
        ask_jev(records, client=context.jev),
        threshold=fit_adaptation_threshold(fitted),
    )
    return {
        "objectives": pl.concat([objectives.collect(), labels]),
        "jev_level_map": fit_jev_level_map(fitted, model=context.jev.model),
    }


def _empty_level_map() -> pl.DataFrame:
    return pl.DataFrame(
        schema={name: column.dtype for name, column in JEV_LEVEL_MAP.columns.items()}
    )


def inherit_markers(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """Each disbursement's markers and climate share from the commitment it fills.

    Args:
        allocations: allocations; rows without a CRDF commitment are left out.
        commitments: CRDF rows, by `row_id`.
        crs: CRS records, by `record_id`.

    Returns:
        The allocations to CRDF commitments with `inherited_rio_adaptation`,
        `inherited_rio_mitigation`, `record_rio_adaptation`,
        `record_rio_mitigation`, `marker_source` and `climate_share`.
    """
    inherited = commitments.select(
        "row_id",
        "climate_share",
        inherited_rio_adaptation="rio_adaptation",
        inherited_rio_mitigation="rio_mitigation",
    )
    own = crs.select(
        "record_id",
        record_rio_adaptation="rio_adaptation",
        record_rio_mitigation="rio_mitigation",
    )
    agree = pl.col("record_rio_adaptation").eq_missing(
        pl.col("inherited_rio_adaptation")
    ) & pl.col("record_rio_mitigation").eq_missing(pl.col("inherited_rio_mitigation"))
    return (
        allocations.filter(pl.col("row_id").is_not_null())
        .join(inherited, on="row_id", how="left", validate="m:1")
        .join(own, on="record_id", how="left", validate="m:1")
        .with_columns(
            marker_source=pl.when(pl.col("climate_share").is_not_null())
            .then(pl.lit("components"))
            .when(agree)
            .then(pl.lit("commitment"))
            .otherwise(pl.lit("record_differs"))
        )
    )


def reported_objectives(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """CRDF and marker objective labels for every record allocated to a CRDF
    commitment.

    A record filling climate-component commitments takes each one's CRDF objective
    at `COMPONENT_LEVEL`, and every linked record takes its own Rio markers, where
    set, as `marker` labels. A record holds one label per objective and source, at
    the highest level.
    """
    linked = allocations.filter(pl.col("row_id").is_not_null()).select(
        "row_id", "record_id"
    )
    components = commitments.filter(
        pl.col("climate_share").is_not_null(), pl.col("objective").is_in(OBJECTIVES)
    ).select("row_id", "objective")
    crdf = linked.join(components, on="row_id").select(
        "record_id",
        "objective",
        level=pl.lit(COMPONENT_LEVEL, pl.Int8),
        label_source=pl.lit("crdf"),
    )
    markers = (
        linked.select("record_id")
        .unique()
        .join(
            crs.select("record_id", "rio_adaptation", "rio_mitigation"), on="record_id"
        )
        .unpivot(
            index="record_id",
            on=["rio_adaptation", "rio_mitigation"],
            variable_name="objective",
            value_name="level",
        )
        .drop_nulls("level")
        .select(
            "record_id",
            pl.col("objective").str.strip_prefix("rio_"),
            "level",
            label_source=pl.lit("marker"),
        )
    )
    return (
        pl.concat([crdf, markers])
        .group_by("record_id", "objective", "label_source")
        .agg(pl.col("level").max())
        .with_columns(
            probability=pl.lit(None, pl.Float64),
            expected_coefficient=pl.lit(None, pl.Float64),
        )
        .select(list(OBJECTIVE_LABELS.columns))
    )


def read_adaptation_labels(path: Path) -> pl.DataFrame:
    """Weikmans et al.'s human-coded adaptation levels with each project's text.

    The text is the file's own `text`, else the title and long description, else
    `NO_TEXT`. Projects without a level are left out.

    Returns:
        `label_id` (the project's row in the file), `text` and `level` (0, 1 or 2).
    """
    text = pl.coalesce(
        "text", project_text(pl.col("projecttitle"), pl.col("longdescription"))
    )
    return (
        pl.read_csv(path, infer_schema_length=0)
        .with_row_index("label_id")
        .filter(pl.col(WEIKMANS_LEVEL).is_not_null())
        .select(
            pl.col("label_id").cast(pl.Int64),
            text=pl.when(text.str.len_chars() > 0)
            .then(text)
            .otherwise(pl.lit(NO_TEXT)),
            level=pl.col(WEIKMANS_LEVEL)
            .cast(pl.Int8)
            .replace_strict({0: 0, 1: 1, 2: 2, 99: 0}, return_dtype=pl.Int8),
        )
    )


def jev_records(
    allocations: pl.LazyFrame, *, commitments: pl.LazyFrame, crs: pl.LazyFrame
) -> pl.LazyFrame:
    """The records Jev labels, with their `project_text`.

    A record qualifies when it is allocated to a commitment without climate
    components, whose markers it inherits, and has a title or long description.
    """
    marked = commitments.filter(pl.col("climate_share").is_null()).select("row_id")
    return (
        allocations.join(marked, on="row_id", how="semi")
        .select("record_id")
        .unique()
        .join(
            crs.select(
                "record_id",
                text=project_text(pl.col("title"), pl.col("long_description")),
            ),
            on="record_id",
        )
        .filter(pl.col("text").str.len_chars() > 0)
    )


def project_text(title: pl.Expr, description: pl.Expr) -> pl.Expr:
    """A project's title and long description, joined by a space and stripped, as
    Weikmans et al.'s file builds its text."""
    return pl.concat_str(
        title.fill_null(""), pl.lit(" "), description.fill_null("")
    ).str.strip_chars()


def ask_jev(texts: pl.DataFrame, *, client: JevClient) -> pl.DataFrame:
    """Jev's answers to `ADAPTATION_QUESTIONS` for each text.

    Args:
        texts: a `text` column and any id columns. Each distinct text is asked once.
        client: the build's Jev client.

    Returns:
        The id columns with Jev's `lenient` answer, `p_significant`, `p_principal`
        and `expected_coefficient`, 0.4 x p_significant + 1.0 x p_principal.
    """
    unique = texts.select(pl.col("text").unique().sort())
    responses = client.score(
        [
            {"state": {"project": text}, "questions": ADAPTATION_QUESTIONS}
            for text in unique["text"]
        ]
    )
    answers = unique.with_columns(
        lenient=pl.Series([_answer(r, "lenient")["noul"] for r in responses]),
        p_significant=pl.Series([_level(r, "significant") for r in responses]),
        p_principal=pl.Series([_level(r, "principal") for r in responses]),
    ).with_columns(
        expected_coefficient=ONE.significant * pl.col("p_significant")
        + ONE.principal * pl.col("p_principal")
    )
    return texts.join(answers, on="text", how="left", validate="m:1").drop("text")


def _answer(response: dict[str, object], question: str) -> dict[str, object]:
    answers = response["answers"]
    assert isinstance(answers, dict)
    return answers[question]


def _level(response: dict[str, object], option: str) -> float:
    probabilities = _answer(response, "level")["probabilities"]
    assert isinstance(probabilities, dict)
    return float(probabilities[option])


def fit_adaptation_threshold(fitted: pl.DataFrame) -> float:
    """The cut on Jev's lenient answer with the highest adaptation F1.

    Args:
        fitted: human-coded projects with their `level` and Jev's `lenient` answer.

    Returns:
        The best of `THRESHOLDS`, a project counting as adaptation when its human
        level is above 0. A tie goes to the higher cut.
    """
    human = pl.col("level") > 0

    def f1(threshold: float) -> pl.Expr:
        jev = pl.col("lenient") >= threshold
        return (2 * (jev & human).sum() / (jev.sum() + human.sum())).alias(
            str(threshold)
        )

    scores = fitted.select(f1(threshold) for threshold in THRESHOLDS).row(0)
    return max(zip(scores, THRESHOLDS, strict=True))[1]


def fit_jev_level_map(fitted: pl.DataFrame, *, model: str) -> pl.DataFrame:
    """The isotonic map from Jev's expected coefficient to the human coefficient.

    Args:
        fitted: human-coded projects with their `level` and Jev's
            `expected_coefficient`.
        model: the Jev model that answered.

    Returns:
        `jev_level_map` rows: the map's points for `adaptation`. Human levels
        count at ONE's coefficients, 0.4 for significant and 1.0 for principal.
    """
    points = fitted.sort("expected_coefficient", "level")
    coefficient = points["level"].replace_strict(
        {0: 0.0, 1: ONE.significant, 2: ONE.principal}, return_dtype=pl.Float64
    )
    isotonic = IsotonicRegression(out_of_bounds="clip").fit(
        points["expected_coefficient"].to_numpy(), coefficient.to_numpy()
    )
    return pl.DataFrame(
        {
            "objective": "adaptation",
            "jev_model": model,
            "expected_coefficient": isotonic.X_thresholds_,
            "level": isotonic.y_thresholds_,
        },
        schema={name: column.dtype for name, column in JEV_LEVEL_MAP.columns.items()},
    )


def jev_objectives(answers: pl.DataFrame, *, threshold: float) -> pl.DataFrame:
    """Jev's adaptation labels, one per record.

    A record is labelled adaptation when its lenient answer reaches `threshold`, at
    level 2 when Jev finds principal likelier than significant and 1 otherwise, and
    at level 0 below it. `probability` is the lenient answer, which is not
    calibrated; the level map calibrates `expected_coefficient` instead.
    """
    adaptation = pl.col("lenient") >= threshold
    principal = pl.col("p_principal") > pl.col("p_significant")
    return answers.select(
        "record_id",
        objective=pl.lit("adaptation"),
        level=pl.when(adaptation & principal)
        .then(pl.lit(2, pl.Int8))
        .when(adaptation)
        .then(pl.lit(1, pl.Int8))
        .otherwise(pl.lit(0, pl.Int8)),
        label_source=pl.lit("jev"),
        probability="lenient",
        expected_coefficient="expected_coefficient",
    )
