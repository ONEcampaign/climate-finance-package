"""Jev's reading of a candidate pair, and the scales that turn it into a probability.

Jev (`jev.model` in `build.toml`) reads both units' records with the facts derived
from them and answers the frozen v1 `relation` question: are the two records the
same activity? Its p(same activity) is evidence whose scale shifts between tasks and
wording, so id truth or people's labels set the probability. Each
request is built from the canonical bulk fields of `levels.unit_attributes`, and the
client's cache keys on its canonical text.

`adjudicate_band` asks Jev about the truth-group classifier's eligible pairs whose
calibrated probability lies in `jev.band`, `[low, high)`, and about two sets of
training-half pairs of single-id donors (`single_id_donors`), labelled by id truth:

- the band pairs, on which a logistic regression of the label on logit(model) and
  logit(Jev) gives an eligible band pair its final probability. A band pair links
  when that probability reaches the classifier's threshold (`score.LINK_THRESHOLD`),
  so Jev's answer moves a pair across the cut it would face without one. The grid
  threshold at which the links the build makes among the training pairs of
  single-id donors have the highest F1 is kept (`JevBand.best_threshold`), and the
  band refuses with `LinkThresholdError` when F1 at the cut falls more than
  `score.F1_TOLERANCE` below it. F1 is flat over the cut: on the archive it varies
  by under 0.001 between 0.30 and 0.49, and one flipped label moved its best
  threshold by 0.04.
- `SCALE_SAMPLE` pairs spread over the bands of the classifier's probability
  (`SCALE_BANDS`), on which a logistic regression of the label on logit(Jev) alone
  is the scale that turns Jev's answers on a Splink model's calibration sample into
  labels (`jev_answers`). A Splink calibration sample spreads over EM match weight,
  from pairs that share nothing to pairs that share everything, so the scale is
  fitted on pairs spread the same way. Band pairs alone, where Jev separates
  weakly, give a slope of 0.31 and no Splink probability reaches 0.9.

Every request goes to the client in one batch, so the cost cap refuses all of it or
none. Siblings (`programme_level` 1) never link, so Jev is not asked about band
siblings; the scale sample keeps them, as a Splink calibration sample does.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
from sklearn.linear_model import LogisticRegression

from climate_finance.build.errors import BuildError
from climate_finance.build.link.levels import unit_attributes
from climate_finance.build.link.score import (
    ELIGIBLE,
    TRAIN,
    TruthScores,
    best_commitments,
    checked_best_threshold,
)
from climate_finance.build.normalise.ids import content_id

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient

# Jev's pair questions, wording v1.
RELATION_QUESTION = (
    "Each record is one yearly line of a donor's aid database (OECD CRS). The earlier "
    "record carries a commitment; the later record is from a later year. How do the "
    "two records relate as aid activities? Judge from the titles, descriptions, "
    "project numbers, recipients and purposes; use derived_facts for dates and "
    "amounts."
)
RELATION_LEVELS = (
    "Different activities: the later record belongs to another project, programme or "
    "component, even if the donor, country or sector is the same.",
    "Related, and possibly the same: the records share a programme or theme, but the "
    "later record may be a separate component, phase, new grant or sibling of the "
    "earlier one.",
    "Same activity: the later record is the same project or grant as the earlier "
    "record, reported again in a later year.",
)
DISBURSEMENT_QUESTION = (
    "Is the later record a disbursement of the same activity that the earlier record "
    "committed funds to? Answer yes when it is the same project or grant continuing "
    "in a later year (a later payment or a repeat of the same record). Answer no when "
    "it is a different project, a new commitment for a new phase, or a separate "
    "component."
)
SIBLING_QUESTION = (
    "Is the later record a sibling sub-activity of the same umbrella programme as the "
    "earlier record, that is, a separate numbered component, phase or grant under one "
    "programme, rather than the same activity itself?"
)
PAIR_QUESTIONS: dict[str, object] = {
    "relation": {
        "type": "score",
        "instructions": RELATION_QUESTION,
        "criteria": list(RELATION_LEVELS),
    },
    "disbursement": {"type": "noul", "instructions": DISBURSEMENT_QUESTION},
    "sibling": {"type": "noul", "instructions": SIBLING_QUESTION},
}
# Characters of each text field Jev reads.
JEV_TEXT_CHARS = 450
# Probabilities are clipped this far from 0 and 1 before their logit is taken.
LOGIT_CLIP = 1e-3
# A donor is single-id when at least this share of its training pairs under one
# project number are true. Elsewhere one project spans several crs ids, so id truth
# calls pairs that Jev rightly reads as one activity different, and those donors
# stay out of the fit.
SINGLE_ID_SHARE = 0.5
# The fewest training pairs of each outcome either fit needs.
MIN_FIT_OUTCOMES = 5
# Training pairs Jev answers for the Splink scale, spread over the classifier's
# probability in the bands these edges bound.
SCALE_SAMPLE = 1_000
SCALE_BANDS = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
# The project number level of two units under the same number (`levels`).
_SAME_PN = 3
_SIBLING = 1
_PAIR = ["left_id", "right_id"]

# What answers a Splink model's calibration sample: given the sample's pairs and the
# units' attributes, each pair's label `y` in [0, 1] and its `pair_label_source`.
type CalibrationAnswers = Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame]


class JevFitError(BuildError):
    """Id truth holds too few band pairs to set Jev's scale."""


@dataclass(frozen=True)
class Logistic:
    """A logistic regression on the logits of probabilities, applied as a polars
    expression: 1 / (1 + exp(-(intercept + sum of coefs x logit(p))))."""

    coefs: tuple[float, ...]
    intercept: float

    def __call__(self, *probabilities: pl.Expr) -> pl.Expr:
        z = pl.lit(self.intercept)
        for coef, p in zip(self.coefs, probabilities, strict=True):
            clipped = p.clip(LOGIT_CLIP, 1 - LOGIT_CLIP)
            z = z + coef * (clipped / (1 - clipped)).log()
        return 1 / (1 + (-z).exp())


def fit_logistic(frame: pl.DataFrame, *, inputs: list[str], y: str) -> Logistic:
    """The logistic of `y` on the logits of the `inputs` columns, unpenalised."""
    x = np.column_stack([_logit(frame[name].to_numpy()) for name in inputs])
    fit = LogisticRegression(C=1e6).fit(x, frame[y].to_numpy())
    return Logistic(
        coefs=tuple(float(coef) for coef in fit.coef_[0]),
        intercept=float(fit.intercept_[0]),
    )


def _logit(p: np.ndarray) -> np.ndarray:
    clipped = np.clip(p, LOGIT_CLIP, 1 - LOGIT_CLIP)
    return np.log(clipped / (1 - clipped))


@dataclass(frozen=True)
class JevBand:
    """Jev's adjudication of the classifier's band.

    `answers` holds each eligible band pair's `p_jev` and final `p_combined`, which
    links when it reaches the classifier's threshold. `best_threshold` is the grid
    threshold of highest F1 for the links the build makes among the labelled
    pairs. `scale` maps Jev's answer alone to a probability, fitted on the scale
    sample, and `model` names the Jev model that answered.
    """

    answers: pl.DataFrame
    combined: Logistic
    best_threshold: float
    scale: Logistic
    model: str


def adjudicate_band(
    scores: TruthScores,
    *,
    units: pl.DataFrame,
    crs: pl.LazyFrame,
    stems: pl.DataFrame,
    band: tuple[float, float],
    client: JevClient,
) -> JevBand:
    """Ask Jev about the classifier's band and combine its answers with the model.

    Args:
        scores: the truth group's scores (`score.score_truth_group`).
        units: the link stage's units.
        crs: the CRS records the units sum.
        stems: the programme stem rules.
        band: the classifier probabilities Jev adjudicates, `[low, high)`.
        client: the build's Jev client.

    Raises:
        JevFitError: when the training band pairs or the scale sample of single-id
            donors hold fewer than `MIN_FIT_OUTCOMES` of either outcome.
        LinkThresholdError: when F1 of the links among the labelled pairs at the
            classifier's threshold is more than `F1_TOLERANCE` below its best on
            the grid.
        CostCapExceededError: when the uncached requests' estimate exceeds the cap.
        JevCacheMissError: when the build is offline and a request is not cached.
    """
    low, high = band
    pairs = scores.pairs
    in_band = pl.col("p_model").is_between(low, high, closed="left") & (
        pl.col("programme_level") != _SIBLING
    )
    labelled = pairs.filter(
        pl.col("split") == TRAIN,
        pl.col("donor_code").is_in(single_id_donors(pairs).implode()),
    ).select(*_PAIR, "p_model", "programme_level", "placed_year", "true_pair")
    fit_on = labelled.filter(in_band).select(*_PAIR, "p_model", y="true_pair")
    scale_on = spread_sample(
        labelled, band=probability_band(pl.col("p_model")), n=SCALE_SAMPLE
    ).select(*_PAIR, "p_model", y="true_pair")
    for name, frame in (("training band pairs", fit_on), ("scale pairs", scale_on)):
        outcomes = int(frame["y"].sum())
        if min(outcomes, frame.height - outcomes) < MIN_FIT_OUTCOMES:
            raise JevFitError(
                f"{frame.height} {name} of single-id donors, {outcomes} true: the "
                f"fit needs {MIN_FIT_OUTCOMES} of each outcome."
            )
    eligible = pairs.filter(in_band, pl.col("split") == ELIGIBLE).select(
        *_PAIR, "p_model"
    )
    asked = (
        pl.concat([eligible, fit_on.drop("y"), scale_on.drop("y")])
        .unique(_PAIR)
        .sort(_PAIR)
    )
    ids = pl.concat([asked["left_id"], asked["right_id"]]).unique()
    attributes = unit_attributes(
        units.filter(pl.col("unit_id").is_in(ids.implode())), crs=crs, stems=stems
    )
    answered = asked.select(
        *_PAIR, p_jev=ask_jev(asked, attributes=attributes, client=client)
    )
    fitted = fit_on.join(answered, on=_PAIR).sort(_PAIR)
    combined = fit_logistic(fitted, inputs=["p_model", "p_jev"], y="y")
    p_combined = combined(pl.col("p_model"), pl.col("p_jev"))
    tuned = labelled.join(
        fitted.select(*_PAIR, "p_jev"), on=_PAIR, how="left"
    ).with_columns(probability=pl.coalesce(p_combined, "p_model"))
    return JevBand(
        answers=eligible.join(answered, on=_PAIR)
        .select(*_PAIR, "p_jev", p_combined=p_combined)
        .sort(_PAIR),
        combined=combined,
        best_threshold=best_link_threshold(tuned, threshold=scores.threshold),
        scale=fit_logistic(
            scale_on.join(answered, on=_PAIR).sort(_PAIR), inputs=["p_jev"], y="y"
        ),
        model=client.model,
    )


def best_link_threshold(pairs: pl.DataFrame, *, threshold: float) -> float:
    """The grid threshold of highest F1 of the links the build makes among `pairs`
    (`score.linked_pairs`), checked against `threshold`, the cut they link at
    (`score.checked_best_threshold`).

    A link is right when its pair is true, and recall counts the disbursement units
    with a true pair among `pairs`. A disbursement unit links one commitment, so a
    false pair that its true pair outscores costs nothing, and a true pair that
    another pair outscores gains nothing.

    Args:
        pairs: labelled pairs with `true_pair`, `programme_level`, `placed_year`,
            and the combined `probability` where Jev answered, else `p_model`.
        threshold: the classifier's threshold.

    Raises:
        LinkThresholdError: F1 at `threshold` is more than `F1_TOLERANCE` below
            its best on the grid.
    """
    # Jev's labelled pairs are the single-id donors' alone, so recall counts their
    # true pairs.
    n_true = pairs.filter("true_pair")["left_id"].n_unique()
    best = best_commitments(pairs, probability="probability")
    return checked_best_threshold(
        best["probability"].to_numpy(),
        best["true_pair"].to_numpy(),
        n_true=n_true,
        threshold=threshold,
    )


def spread_sample(frame: pl.DataFrame, *, band: pl.Expr, n: int) -> pl.DataFrame:
    """`n` rows of `frame` spread evenly over the values of `band`: the first pair
    of every band, then the second, and so on, so a band smaller than its share
    gives all its pairs and the others make up the rest.

    Within a band, pairs are taken in the order of a digest of their ids, so every
    build draws the same pairs from the same frame.
    """
    order = content_id(_PAIR, order_by=_PAIR)
    return (
        frame.with_columns(_band=band, _order=order)
        .with_columns(_rank=pl.col("_order").rank("ordinal").over("_band"))
        .sort("_rank", "_band")
        .head(n)
        .drop("_band", "_order", "_rank")
    )


def probability_band(p: pl.Expr) -> pl.Expr:
    """The band of `SCALE_BANDS` a probability falls in, from 0."""
    return pl.sum_horizontal(p >= edge for edge in SCALE_BANDS)


def single_id_donors(pairs: pl.DataFrame) -> pl.Series:
    """The donors at least `SINGLE_ID_SHARE` of whose training pairs under one
    project number are true, read from `TruthScores.pairs`."""
    same_pn = pairs.filter(split=TRAIN, gamma_project_number=_SAME_PN)
    shares = same_pn.group_by("donor_code").agg(share=pl.col("true_pair").mean())
    return shares.filter(pl.col("share") >= SINGLE_ID_SHARE)["donor_code"].sort()


def jev_answers(*, client: JevClient, scale: Logistic) -> CalibrationAnswers:
    """Jev's labels for a Splink model's calibration sample: its answer on each
    pair through `scale`, with source `jev`."""

    def answer(pairs: pl.DataFrame, attributes: pl.DataFrame) -> pl.DataFrame:
        jev = ask_jev(pairs, attributes=attributes, client=client)
        return pairs.select(*_PAIR, jev=jev).select(
            *_PAIR, y=scale(pl.col("jev")), pair_label_source=pl.lit("jev")
        )

    return answer


def pair_request(
    earlier: Mapping[str, Any], later: Mapping[str, Any]
) -> dict[str, object]:
    """Jev's request about a commitment unit and a later unit, wording v1.

    Each record gives its unit's fields, texts cut to `JEV_TEXT_CHARS`, and
    `derived_facts` states the comparisons of dates and amounts.
    """
    first, second = _jev_record(earlier), _jev_record(later)
    commitment = first["commitment_usd"]
    share = (
        None
        if commitment <= 0
        else float(f"{second['disbursement_usd'] / commitment:.3g}")
    )
    facts = {
        "years_between_records": second["year"] - first["year"],
        "same_provider": first["provider"] == second["provider"],
        "same_project_number": bool(
            first["project_number"]
            and first["project_number"] == second["project_number"]
        ),
        "same_title": first["title"].lower() == second["title"].lower(),
        "same_recipient": first["recipient"] == second["recipient"],
        "same_purpose_code": first["purpose_code"] == second["purpose_code"],
        "later_disbursement_as_share_of_earlier_commitment": share,
        "later_record_also_has_its_own_commitment": second["commitment_usd"] > 0,
    }
    return {
        "state": {
            "earlier_record": first,
            "later_record": second,
            "derived_facts": facts,
        },
        "questions": PAIR_QUESTIONS,
    }


def _jev_record(unit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "year": unit["year"],
        "provider": unit["donor_code"],
        "recipient": unit["recipient_code"],
        "purpose_code": unit["purpose_code"],
        "channel_code": unit["channel_code"],
        "flow_code": unit["flow_code"],
        "finance_type_code": unit["finance_type_code"],
        "project_number": _jev_text(unit["project_number"]) or None,
        "title": _jev_text(unit["title"]),
        "short_description": _jev_text(unit["short_description"]),
        "long_description": _jev_text(unit["long_description"]),
        "commitment_usd": round(unit["commitment_usd"] or 0),
        "disbursement_usd": round(unit["disbursement_usd"] or 0),
    }


def _jev_text(text: str | None) -> str:
    cleaned = " ".join((text or "").split())
    return cleaned[:JEV_TEXT_CHARS] + ("..." if len(cleaned) > JEV_TEXT_CHARS else "")


def pair_requests(
    pairs: pl.DataFrame, *, attributes: pl.DataFrame
) -> list[dict[str, object]]:
    """Jev's request for each (`left_id`, `right_id`) pair, in order: the
    commitment (right) is the earlier record."""
    named = attributes.join(
        pl.concat([pairs["left_id"], pairs["right_id"]]).to_frame("unique_id"),
        on="unique_id",
        how="semi",
    )
    by_id = {row["unique_id"]: row for row in named.iter_rows(named=True)}
    return [
        pair_request(by_id[right], by_id[left])
        for left, right in pairs.select(_PAIR).iter_rows()
    ]


def ask_jev(
    pairs: pl.DataFrame, *, attributes: pl.DataFrame, client: JevClient
) -> pl.Series:
    """Jev's p(same activity) for each (`left_id`, `right_id`) pair, in order."""
    responses = client.score(pair_requests(pairs, attributes=attributes))
    return pl.Series(
        [_same_activity(response) for response in responses], dtype=pl.Float64
    )


def _same_activity(response: Mapping[str, Any]) -> float:
    return float(response["answers"]["relation"]["probabilities"]["2"])
