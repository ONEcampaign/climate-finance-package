"""Jev's combined classifier on the held-out half of the archive's labelled pairs.

The link stage scores as in `test_score_archive`, with key repair and recoveries on.
Each held-out disbursement unit ranks its labelled pairs of both halves
(`heldout_links`), every pair in the band (`jev.in_band`) taking its combined
probability, as `classifier_links` does with `coalesce(p_combined, p_model)`, and
links at `LINK_THRESHOLD`. Every combined probability ranked is out-of-sample, as
`p_model` is. A held-out pair takes the logistic the build deploys, fitted on its
training pairs (`jev.band_fit_pairs`). A training pair takes the logistic fitted on
those of them in the other folds of `training_folds`, the folds whose classifier
scored it. Single-id donors are chosen once over every training pair, as the build
chooses them. Link precision
is the share of these links that are true, and monetary precision the share of
their disbursement units' `disbursement_usd`. Both are read overall and per
provider, for the combined classifier and the classifier alone on the same units.

Jev answers from the cache in `CF_E2E_JEV_CACHE`, the directory holding
`jev-1.14.0.jsonl`, offline and with no live call. With `CF_E2E_JEV_LIVE_CAP_USD`
set, the client asks Jev the uncached requests under that cost cap and appends the
answers to the cache, which later offline runs replay. Needs CF_E2E_DATA pointing at
the scoping `data/`.
"""

import os
from pathlib import Path

import polars as pl
import pytest
from archive_build import archive_config

from climate_finance.build.config import Jev
from climate_finance.build.jev_client import JevClient
from climate_finance.build.link.jev import (
    Logistic,
    ask_jev,
    band_fit_pairs,
    fit_logistic,
    in_band,
    labelled_pairs,
)
from climate_finance.build.link.levels import unit_attributes
from climate_finance.build.link.score import (
    HELDOUT,
    LINK_THRESHOLD,
    TRAIN,
    Scored,
    heldout_links,
    training_folds,
)
from climate_finance.build.stages import table_path

JEV_CACHE = "CF_E2E_JEV_CACHE"
LIVE_CAP_USD = "CF_E2E_JEV_LIVE_CAP_USD"
LIVE = bool(os.environ.get(LIVE_CAP_USD))

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not os.environ.get(JEV_CACHE), reason=f"skipped: no {JEV_CACHE}"
    ),
    # A live run reaches Jev, which only an integration test may.
    *([pytest.mark.integration] if LIVE else []),
]

_PAIR = ["left_id", "right_id"]
# A provider's figures are pinned when either classifier makes this many held-out
# links of its commitments.
MIN_PROVIDER_LINKS = 100
# Each classifier's held-out links, link precision and monetary precision. A
# disbursement pairs with a purpose-split line once (`line_units`). Jev's
# adjudication lowers precision out of sample overall: precision is 0.9246 for
# the classifier alone and 0.9204 with Jev, monetary precision 0.9503 and
# 0.9417.
OVERALL = {
    "base": (25387, 0.9246, 0.9503),
    "jev": (25619, 0.9204, 0.9417),
}
# The same figures for each provider with `MIN_PROVIDER_LINKS`, by donor code. Jev
# changes precision most for Canada (301), from 0.7898 to 0.7529, and Germany (5),
# from 0.8474 to 0.8230, and raises it most for Italy (6), from 0.8948 to 0.9149.
# Slovenia (61) makes under `MIN_PROVIDER_LINKS` held-out links. Denmark (3) and
# Canada (301) stay below 0.8 under both.
PROVIDERS = {
    1: {"base": (802, 0.8791, 0.9529), "jev": (802, 0.8803, 0.9555)},
    2: {"base": (310, 0.9645, 0.9279), "jev": (313, 0.9553, 0.9245)},
    3: {"base": (1065, 0.6329, 0.6242), "jev": (1075, 0.6223, 0.6105)},
    4: {"base": (1425, 0.9663, 0.9859), "jev": (1409, 0.9737, 0.986)},
    5: {"base": (603, 0.8474, 0.8649), "jev": (644, 0.823, 0.8057)},
    6: {"base": (1663, 0.8948, 0.8821), "jev": (1634, 0.9149, 0.8924)},
    7: {"base": (1270, 0.9913, 0.9968), "jev": (1269, 0.9921, 0.9969)},
    8: {"base": (2511, 0.9976, 0.9989), "jev": (2511, 0.9976, 0.9989)},
    10: {"base": (1358, 0.9691, 0.9877), "jev": (1358, 0.9691, 0.9877)},
    11: {"base": (3222, 0.9957, 0.997), "jev": (3219, 0.9966, 0.9982)},
    12: {"base": (1957, 0.9663, 0.9789), "jev": (1949, 0.9702, 0.9805)},
    18: {"base": (1529, 0.9509, 0.9411), "jev": (1529, 0.9509, 0.9411)},
    50: {"base": (874, 0.9954, 0.9992), "jev": (874, 0.9954, 0.9992)},
    301: {"base": (2398, 0.7898, 0.6589), "jev": (2630, 0.7529, 0.6472)},
    701: {"base": (1719, 0.8441, 0.9582), "jev": (1738, 0.8418, 0.9564)},
    742: {"base": (842, 0.9656, 0.9826), "jev": (838, 0.9654, 0.9824)},
    820: {"base": (379, 0.942, 0.9782), "jev": (377, 0.9469, 0.9825)},
    918: {"base": (132, 0.9545, 0.9883), "jev": (132, 0.9545, 0.9883)},
    1601: {"base": (979, 0.9877, 0.995), "jev": (979, 0.9877, 0.995)},
}


@pytest.fixture(scope="module")
def jev(archive: Path) -> Jev:
    """The deployed Jev settings of `build.toml`."""
    return archive_config(archive, key_repair=True, recoveries=True).jev


@pytest.fixture(scope="module")
def client(jev: Jev) -> JevClient:
    """Offline on the cache, or live under `CF_E2E_JEV_LIVE_CAP_USD`."""
    return JevClient(
        model=jev.model,
        cost_cap_usd=float(os.environ[LIVE_CAP_USD]) if LIVE else 0.0,
        cache_dir=Path(os.environ[JEV_CACHE]),
        offline=not LIVE,
    )


@pytest.fixture(scope="module")
def weighed(
    release_dir: Path,
    scored: Scored,
    crs: pl.LazyFrame,
    jev: Jev,
    client: JevClient,
) -> pl.DataFrame:
    """The labelled pairs of the held-out disbursement units, both halves, with
    `p_model`, the combined `probability`, and the left unit's
    `disbursement_usd`."""
    assert scored.scores is not None
    pairs = scored.scores.pairs
    fit_on = band_fit_pairs(labelled_pairs(pairs), band=jev.band)
    labelled = pairs.filter(pl.col("split").is_in([TRAIN, HELDOUT]))
    ranked = labelled.join(
        labelled.filter(split=HELDOUT).select("left_id").unique(),
        on="left_id",
        how="semi",
    )
    asked = (
        pl.concat(
            [fit_on.select(_PAIR), ranked.filter(in_band(jev.band)).select(_PAIR)]
        )
        .unique()
        .sort(_PAIR)
    )
    units = pl.read_parquet(table_path(release_dir, "link", "units"))
    ids = pl.concat([asked["left_id"], asked["right_id"]]).unique()
    attributes = unit_attributes(
        units.filter(pl.col("unit_id").is_in(ids.implode())),
        crs=crs,
        stems=pl.read_parquet(table_path(release_dir, "link", "programme_stems")),
    )
    answered = asked.with_columns(
        p_jev=ask_jev(asked, attributes=attributes, client=client)
    )
    print(
        f"Jev: {asked.height} pairs asked, {client.live_calls} live, "
        f"USD {client.spent_usd:.4f} spent"
    )
    if not LIVE:
        assert client.live_calls == 0

    fitted = fit_on.join(answered, on=_PAIR)
    answers = ranked.join(answered, on=_PAIR)
    # As `adjudicate_band` fits it.
    deployed = _combined(fitted)
    scored_pairs = [
        answers.filter(split=HELDOUT).select(
            *_PAIR, p_combined=deployed(pl.col("p_model"), pl.col("p_jev"))
        )
    ]
    # The folds of `score_truth_group`, on its training pairs in the same order.
    train = pairs.filter(split=TRAIN).sort(_PAIR)
    folds = training_folds(train)
    assert folds is not None
    for fit_rows, score_rows in folds:
        fold = _combined(
            fitted.join(train[fit_rows].select(_PAIR), on=_PAIR, how="semi")
        )
        scored_pairs.append(
            answers.join(train[score_rows].select(_PAIR), on=_PAIR, how="semi").select(
                *_PAIR, p_combined=fold(pl.col("p_model"), pl.col("p_jev"))
            )
        )
    combined = pl.concat(scored_pairs)
    assert combined.height == answers.height
    return (
        ranked.join(combined, on=_PAIR, how="left")
        .select(
            pl.exclude("p_combined"),
            probability=pl.coalesce("p_combined", "p_model"),
        )
        .join(
            units.select(left_id="unit_id", disbursement_usd="disbursement_usd"),
            on="left_id",
            how="left",
        )
    )


def _combined(fitted: pl.DataFrame) -> Logistic:
    """The combined logistic fitted on `fitted`, sorted by pair."""
    return fit_logistic(fitted.sort(_PAIR), inputs=["p_model", "p_jev"], y="y")


def _links(weighed: pl.DataFrame, *, probability: str) -> pl.DataFrame:
    """The held-out links (`heldout_links`) ranked and cut on `probability`."""
    # `heldout_links` ranks on `p_model`.
    ranked = weighed.with_columns(p_model=pl.col(probability))
    return heldout_links(ranked).filter(pl.col("p_model") >= LINK_THRESHOLD)


def _precision(links: pl.DataFrame) -> tuple[int, float, float]:
    """`links`' count, link precision and monetary precision."""
    usd = links["disbursement_usd"].fill_null(0.0)
    return (
        links.height,
        links["true_pair"].sum() / links.height,
        usd.filter(links["true_pair"]).sum() / usd.sum(),
    )


def test_jev_link_precision_on_held_out_pairs(weighed: pl.DataFrame) -> None:
    links = {
        "base": _links(weighed, probability="p_model"),
        "jev": _links(weighed, probability="probability"),
    }
    overall = {name: _precision(frame) for name, frame in links.items()}
    counts = pl.concat(frame.group_by("donor_code").len() for frame in links.values())
    pinned = counts.filter(pl.col("len") >= MIN_PROVIDER_LINKS)["donor_code"]
    providers = {
        donor: {
            name: _precision(frame.filter(donor_code=donor))
            for name, frame in links.items()
        }
        for donor in sorted(pinned.unique())
    }
    print(f"overall: {overall}")
    print(f"providers: {providers}")

    # Counts match exactly, and shares to the four decimals pinned.
    assert overall == {
        name: pytest.approx(row, abs=1e-4) for name, row in OVERALL.items()
    }
    assert providers == {
        donor: {name: pytest.approx(row, abs=1e-4) for name, row in rows.items()}
        for donor, rows in PROVIDERS.items()
    }
