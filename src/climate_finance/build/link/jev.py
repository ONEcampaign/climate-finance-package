"""Jev's reading of a candidate pair, and the scales that turn it into a probability.

Jev (`jev.model` in `build.toml`) reads both units' records with the facts derived
from them and answers the frozen v1 `relation` question: are the two records the
same activity? Its p(same activity) is evidence, not a probability, because its
scale shifts between tasks and wording, so id truth or people's labels set it. Each
request is built from the canonical bulk fields of `levels.unit_attributes`, and the
client's cache keys on its canonical text.

`adjudicate_band` asks Jev about the truth-group classifier's eligible pairs whose
calibrated probability lies in `jev.band`, `[low, high)`, and about the training
half's band pairs of single-id donors (`single_id_donors`). On those training pairs,
labelled by id truth, it fits two logistic regressions: one on logit(model) and
logit(Jev), which gives an eligible band pair its final probability, and one on
logit(Jev) alone, the scale that turns Jev's answers on a Splink model's calibration
sample into labels (`jev_answers`). A band pair links when its final probability
reaches the threshold that maximises F1 on the training pairs. Every request goes
to the client in one batch, so the cost cap refuses all of it or none.

Siblings (`programme_level` 1) never link, so Jev is not asked about them.
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
    best_threshold,
)

if TYPE_CHECKING:
    from climate_finance.build.jev_client import JevClient

# Jev's pair questions, wording v1, as the prototypes asked them.
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
# The fewest training band pairs of each outcome the fit needs.
MIN_FIT_OUTCOMES = 5
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
    links when it reaches `threshold`. `scale` maps Jev's answer alone to a
    probability, and `model` names the Jev model that answered.
    """

    answers: pl.DataFrame
    combined: Logistic
    threshold: float
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
        JevFitError: when the training band pairs of single-id donors hold fewer
            than `MIN_FIT_OUTCOMES` of either outcome.
        CostCapExceededError: when the uncached requests' estimate exceeds the cap.
        JevCacheMissError: when the build is offline and a request is not cached.
    """
    low, high = band
    pairs = scores.pairs
    in_band = pl.col("p_model").is_between(low, high, closed="left") & (
        pl.col("programme_level") != _SIBLING
    )
    fit_on = pairs.filter(
        in_band,
        pl.col("split") == TRAIN,
        pl.col("donor_code").is_in(single_id_donors(pairs).implode()),
    ).select(*_PAIR, "p_model", y="true_pair")
    outcomes = int(fit_on["y"].sum())
    if min(outcomes, fit_on.height - outcomes) < MIN_FIT_OUTCOMES:
        raise JevFitError(
            f"{fit_on.height} training band pairs of single-id donors, {outcomes} "
            f"true: Jev's scale needs {MIN_FIT_OUTCOMES} of each outcome."
        )
    eligible = pairs.filter(in_band, pl.col("split") == ELIGIBLE).select(
        *_PAIR, "p_model"
    )
    asked = pl.concat([eligible, fit_on.drop("y")]).sort(_PAIR)
    ids = pl.concat([asked["left_id"], asked["right_id"]]).unique()
    attributes = unit_attributes(
        units.filter(pl.col("unit_id").is_in(ids.implode())), crs=crs, stems=stems
    )
    answered = asked.select(
        *_PAIR, p_jev=ask_jev(asked, attributes=attributes, client=client)
    )
    fitted = fit_on.join(answered, on=_PAIR).sort(_PAIR)
    combined = fit_logistic(fitted, inputs=["p_model", "p_jev"], y="y")
    y = fitted["y"].to_numpy()
    p_fitted = fitted.select(combined(pl.col("p_model"), pl.col("p_jev"))).to_series()
    return JevBand(
        answers=eligible.join(answered, on=_PAIR)
        .select(
            *_PAIR, "p_jev", p_combined=combined(pl.col("p_model"), pl.col("p_jev"))
        )
        .sort(_PAIR),
        combined=combined,
        threshold=best_threshold(p_fitted.to_numpy(), y, n_true=int(y.sum())),
        scale=fit_logistic(fitted, inputs=["p_jev"], y="y"),
        model=client.model,
    )


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
    by_id = {row["unique_id"]: row for row in attributes.iter_rows(named=True)}
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
