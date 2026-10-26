"""Marker rules, ported from 1.2.3's `transform_markers_into_indicators` tests."""

from collections.abc import Callable

import polars as pl
import pytest
from make_fixture import DEU, FIXTURE_DIR, FRA, JPN, NOR

import climate_finance as cf
from climate_finance.errors import InvalidInputError
from climate_finance.methods import (
    OECD,
    ONE,
    OXFAM,
    OXFAM_HIGH,
    OXFAM_LOW,
    Method,
    MethodRange,
    label,
    transform_markers_into_indicators,
    weights,
)
from climate_finance.query import _coefficients

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


@pytest.mark.parametrize(
    ("method", "adaptation", "mitigation", "expected"),
    [
        (OECD, 1, 0, {"adaptation": DISBURSED}),
        (ONE, 1, 0, {"adaptation": pytest.approx(0.4 * DISBURSED)}),
        (ONE, 0, 2, {"mitigation": DISBURSED}),
        (OECD, 0, 2, {"mitigation": DISBURSED}),
        (OXFAM_LOW, 1, 0, {"adaptation": pytest.approx(0.3 * DISBURSED)}),
        (OXFAM_LOW, 0, 2, {"mitigation": pytest.approx(0.85 * DISBURSED)}),
        (OXFAM_LOW, 1, 1, {"cross_cutting": pytest.approx(0.3 * DISBURSED)}),
    ],
    ids=[
        "OECD significant",
        "ONE significant",
        "ONE principal",
        "OECD principal",
        "OXFAM_LOW significant",
        "OXFAM_LOW principal",
        "OXFAM_LOW both significant",
    ],
)
def test_a_single_marker_counts_at_its_coefficient(
    method: Method, adaptation: int, mitigation: int, expected: dict[str, float]
) -> None:
    assert _by_objective(_indicators(adaptation, mitigation, method)) == expected


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
    assert label(ONE.replace(significant=0.5)).startswith(
        "significant 0.5, principal 1,"
    )
    assert label(ONE.replace(eu="provider")) == "ONE, EU as a provider"
    assert label(OECD.replace(eu="channel")) == "OECD, EU as a channel"
    assert (label(OXFAM), label(OXFAM_LOW), label(OXFAM_HIGH)) == (
        "OXFAM",
        "OXFAM_LOW",
        "OXFAM_HIGH",
    )
    assert label(MethodRange(methods=(OXFAM_HIGH, OXFAM_LOW))) == (
        "range of OXFAM_HIGH / OXFAM_LOW"
    )
    assert cf.Spec(method="OXFAM").method == OXFAM


def test_methods_with_close_coefficients_keep_distinct_labels() -> None:
    close = (ONE.replace(significant=0.5000001), ONE.replace(significant=0.5000002))

    assert label(close[0]) != label(close[1])


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: MethodRange(methods=(ONE, OECD, ONE)), "more than once"),
        (lambda: ONE.replace(principal=1.5), "principal"),
        # Each call below is the wrong input under test, so a type checker flags it.
        (lambda: Method(significant=0.4, principal=1.0), "highest_marker"),  # ty: ignore[missing-argument]
        (
            lambda: Method(significant=0.4, principal=1.0, highest_marker="yes"),  # ty: ignore[invalid-argument-type]
            "highest_marker",
        ),
        (
            lambda: Method(signficant=0.4, principal=1.0, highest_marker=True),  # ty: ignore[missing-argument, unknown-argument]
            "did you mean 'significant'",
        ),
        (lambda: cf.Agency(provider=918), "agency"),  # ty: ignore[missing-argument]
        (lambda: cf.Agency(provider=918, agncy=3), "did you mean 'agency'"),  # ty: ignore[missing-argument, unknown-argument]
    ],
)
def test_a_method_or_agency_refuses_with_the_package_error(
    build: Callable[[], object], match: str
) -> None:
    with pytest.raises(InvalidInputError, match=match):
        build()


def _marker_row(
    adaptation: int, mitigation: int, jev_level: int | None
) -> pl.DataFrame:
    """A marker row, without climate components, with Jev's adaptation level."""
    return pl.DataFrame(
        [
            {
                **COMPONENTS,
                "rio_adaptation": adaptation,
                "rio_mitigation": mitigation,
                "climate_share": None,
                "jev_level": jev_level,
            }
        ],
        schema_overrides={"climate_share": pl.Float64, "jev_level": pl.Int8},
    )


@pytest.mark.parametrize(
    ("method", "markers", "jev_level", "expected"),
    [
        # Jev finds no adaptation aim in a principal adaptation row: it counts 0.
        (ONE, (2, 0), 0, {"adaptation": 0.0, "climate": 0.0}),
        # A significant Jev level counts at the method's significant coefficient.
        (ONE, (2, 0), 1, {"adaptation": 0.4, "climate": 0.4}),
        (OECD, (2, 0), 1, {"adaptation": 1.0, "climate": 1.0}),
        # Equal levels are cross-cutting under the highest-marker rule.
        (ONE, (1, 2), 2, {"cross_cutting": 1.0, "mitigation": 0.0, "climate": 1.0}),
        (
            ONE.replace(highest_marker=False),
            (1, 1),
            2,
            {"adaptation": 1.0, "mitigation": 0.4, "cross_cutting": 0.4},
        ),
        # A record Jev did not label keeps its marker.
        (ONE, (1, 0), None, {"adaptation": 0.4, "climate": 0.4}),
    ],
)
def test_a_jev_level_weighs_like_the_adaptation_marker_it_replaces(
    method: Method,
    markers: tuple[int, int],
    jev_level: int | None,
    expected: dict[str, float],
) -> None:
    frame = _marker_row(*markers, jev_level)

    result = frame.select(**weights(method.replace(relevance="jev"))).row(0, named=True)

    assert {key: result[key] for key in expected} == pytest.approx(expected)


def test_marker_relevance_ignores_a_jev_level() -> None:
    frame = _marker_row(1, 0, 2)

    assert frame.select(**weights(ONE)).row(0, named=True)["adaptation"] == 0.4


_PARTS = ("adaptation", "mitigation", "overlap")


def test_capped_coefficients_take_the_providers_up_to_the_methods() -> None:
    # (provider, agency, year, adaptation, mitigation): the fixture's coefficients
    # for each provider are in `make_fixture.PROVIDER_COEFFICIENTS`.
    rows = [
        (JPN, 2, 2022, 1, 0),  # Japan's 0.6 is above the 0.5 cap
        (JPN, 2, 2021, 0, 2),  # Japan's 0.85 principal is below the 1.0 cap
        (NOR, 1, 2022, 1, 0),  # Norway's 0.3 default is below the cap
        (FRA, 1, 2022, 1, 0),  # France's agency 1 states none, so takes the cap
        (DEU, 52, 2022, 1, 1),  # Germany's own 1.0 for both significant
        (JPN, 2, 2022, 1, 1),  # Japan has none: its significant 0.6, uncapped
        (FRA, 1, 2022, 1, 1),  # France's agency 1 has neither: counts in full
    ]
    frame = pl.LazyFrame(
        rows,
        schema={
            "provider_code": pl.Int32,
            "agency_code": pl.Int32,
            "year": pl.Int16,
            "rio_adaptation": pl.Int8,
            "rio_mitigation": pl.Int8,
        },
        orient="row",
    ).with_columns(
        pl.lit(None, pl.Float64).alias(name)
        for name in ("climate_share", *(f"share_{part}" for part in _PARTS))
    )

    weighed = _coefficients(cf.load(path=FIXTURE_DIR), frame, method=OXFAM_HIGH)
    result = weighed.select(
        "method_coefficient",
        climate=weights(
            OXFAM_HIGH,
            **{
                name: pl.col(name)
                for name in ("significant", "principal", "both_significant")
            },
        )["climate"],
    ).collect()

    assert result["climate"].to_list() == pytest.approx(
        [0.5, 0.85, 0.3, 0.5, 1.0, 0.6, 1.0]
    )
    assert result["method_coefficient"].to_list() == [
        True,
        False,
        False,
        True,
        False,
        False,
        True,
    ]
