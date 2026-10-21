"""Marker rules, ported from 1.2.3's `transform_markers_into_indicators` tests."""

import polars as pl
import pytest

from climate_finance.methods import (
    OECD,
    ONE,
    Method,
    label,
    transform_markers_into_indicators,
    weights,
)

DISBURSED = 800_000.0


def _indicators(adaptation: int, mitigation: int, method: Method) -> pl.DataFrame:
    activity = pl.LazyFrame(
        {
            "rio_adaptation": [adaptation],
            "rio_mitigation": [mitigation],
            "value": [DISBURSED],
        },
        schema_overrides={"rio_adaptation": pl.Int8, "rio_mitigation": pl.Int8},
    )
    return transform_markers_into_indicators(activity, method=method).collect()


def _by_objective(frame: pl.DataFrame) -> dict[str, float]:
    return dict(zip(frame["objective"], frame["value"], strict=True))


def test_oecd_counts_a_significant_marker_in_full() -> None:
    assert _by_objective(_indicators(1, 0, OECD)) == {"adaptation": DISBURSED}


def test_one_counts_a_significant_marker_at_40_percent() -> None:
    result = _by_objective(_indicators(1, 0, ONE))

    assert result == {"adaptation": pytest.approx(0.4 * DISBURSED)}


@pytest.mark.parametrize("method", [ONE, OECD])
def test_a_principal_marker_counts_in_full_under_both(method: Method) -> None:
    assert _by_objective(_indicators(0, 2, method)) == {"mitigation": DISBURSED}


@pytest.mark.parametrize(
    ("adaptation", "mitigation", "objective"),
    [(1, 2, "mitigation"), (2, 1, "adaptation")],
)
def test_the_highest_marker_takes_the_activity_at_its_level(
    adaptation: int, mitigation: int, objective: str
) -> None:
    result = _indicators(adaptation, mitigation, ONE)

    assert _by_objective(result) == {objective: DISBURSED}
    assert result["level"].to_list() == [2]


def test_equal_markers_become_cross_cutting_under_the_highest_marker() -> None:
    assert _by_objective(_indicators(2, 2, ONE)) == {"cross_cutting": DISBURSED}


def test_without_the_highest_marker_each_marker_gives_its_own_row() -> None:
    method = ONE.replace(highest_marker=False)

    result = _by_objective(_indicators(2, 1, method))

    assert result == {
        "adaptation": DISBURSED,
        "mitigation": pytest.approx(0.4 * DISBURSED),
        "cross_cutting": pytest.approx(0.4 * DISBURSED),
    }


def test_an_activity_without_markers_keeps_its_value_as_not_climate() -> None:
    result = _indicators(0, 0, ONE)

    assert _by_objective(result) == {"not_climate_relevant": DISBURSED}
    assert result["level"].to_list() == [0]


COMPONENTS = {
    "rio_adaptation": None,
    "rio_mitigation": None,
    "climate_share": 0.5,
    "share_adaptation": 0.3,
    "share_mitigation": 0.3,
    "share_overlap": 0.1,
}


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        (ONE, {"adaptation": 0.2, "mitigation": 0.2, "cross_cutting": 0.1}),
        (OECD, {"adaptation": 0.3, "mitigation": 0.3, "cross_cutting": 0.1}),
    ],
)
def test_climate_components_ignore_coefficients_and_count_the_overlap_once(
    method: Method, expected: dict[str, float]
) -> None:
    frame = pl.DataFrame([COMPONENTS], schema_overrides={"rio_adaptation": pl.Int8})

    result = frame.select(**weights(method)).row(0, named=True)

    assert result == pytest.approx({**expected, "climate": 0.5})


def test_named_methods_print_their_names_and_custom_ones_their_coefficients() -> None:
    assert label(ONE) == "ONE"
    assert label(ONE.replace(significant=0.5)).startswith("significant 0.5,")
    assert label(ONE.replace(eu="provider")) == "ONE, EU as a provider"
    assert label(OECD.replace(eu="channel")) == "OECD, EU as a channel"


def test_coefficients_outside_zero_to_one_are_refused() -> None:
    with pytest.raises(ValueError, match="principal"):
        ONE.replace(principal=1.5)
