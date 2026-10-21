"""Climate weighting: `Method`, the named methods ONE, OECD, PROVIDER, RANGE, and the
marker rules that turn Rio markers or climate components into objective weights."""

from typing import Literal, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from climate_finance.catalogue import Catalogue

type Objective = Literal["adaptation", "mitigation", "cross_cutting"]
OBJECTIVES: tuple[Objective, ...] = ("adaptation", "mitigation", "cross_cutting")
NOT_CLIMATE = "not_climate_relevant"


class Method(BaseModel):
    """Coefficients applied to face value by Rio marker level."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    significant: float = Field(ge=0.0, le=1.0)
    principal: float = Field(ge=0.0, le=1.0)
    highest_marker: bool
    coefficients: Literal["fixed", "provider"] = "fixed"
    relevance: Literal["markers", "jev"] = "markers"

    def replace(self, **changes: object) -> Self:
        """Return a copy with `changes` applied, validated again."""
        return self.model_validate({**self.model_dump(), **changes})

    def _repr_markdown_(self) -> str:
        return describe(self)


class MethodRange(BaseModel):
    """Several methods reported together; `low` and `high` are their row-wise bounds."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    methods: tuple[Method, ...] = Field(min_length=1)

    def _repr_markdown_(self) -> str:
        return describe(self)


ONE = Method(significant=0.4, principal=1.0, highest_marker=True)
OECD = Method(significant=1.0, principal=1.0, highest_marker=False)
PROVIDER = Method(
    significant=0.4, principal=1.0, highest_marker=True, coefficients="provider"
)
RANGE = MethodRange(methods=(ONE, OECD, PROVIDER))

_NAMED = {"ONE": ONE, "OECD": OECD, "PROVIDER": PROVIDER}


def label(method: Method | MethodRange) -> str:
    """The method's name if it is a named method, else its coefficients."""
    if isinstance(method, MethodRange):
        if method == RANGE:
            return "RANGE"
        return f"range of {' / '.join(label(member) for member in method.methods)}"
    for name, named in _NAMED.items():
        if method == named:
            return name
    rule = "highest marker" if method.highest_marker else "each marker"
    return (
        f"significant {method.significant:g}, principal {method.principal:g}, {rule}"
        f", {method.coefficients} coefficients, {method.relevance} relevance"
    )


def describe(method: Method | MethodRange) -> str:
    """What `method` counts, in plain language."""
    if isinstance(method, MethodRange):
        names = " / ".join(label(member) for member in method.methods)
        return (
            f"Methods {names}, reported together; low and high are the lowest and "
            "highest value across them."
        )
    rule = (
        "each activity counts once, under its higher Rio marker, with equal "
        "markers as cross-cutting"
        if method.highest_marker
        else "adaptation and mitigation count separately; cross_cutting is their "
        "overlap, which value counts once"
    )
    text = (
        f"Method {label(method)}: significant markers count "
        f"{method.significant:.0%} of face value and principal markers "
        f"{method.principal:.0%}; {rule}. Climate-component rows from development "
        "banks count their reported climate share and ignore the coefficients."
    )
    if method.coefficients == "provider":
        text += (
            " Each provider takes its own significant coefficient from the release, "
            f"or {method.significant:g} where it has none."
        )
    return text


def _coefficient(level: pl.Expr, method: Method, significant: pl.Expr) -> pl.Expr:
    return (
        pl.when(level == 2)
        .then(pl.lit(method.principal))
        .when(level == 1)
        .then(significant)
        .otherwise(pl.lit(0.0))
    )


def _markers() -> tuple[pl.Expr, pl.Expr]:
    return pl.col("rio_adaptation").fill_null(0), pl.col("rio_mitigation").fill_null(0)


def _marker_rules(
    method: Method, significant: pl.Expr
) -> dict[str, tuple[pl.Expr, pl.Expr, pl.Expr]]:
    """Per objective: whether a marker row counts towards it, its level, its weight."""
    rio_a, rio_m = _markers()
    if method.highest_marker:
        level = pl.max_horizontal(rio_a, rio_m)
        weight = _coefficient(level, method, significant)
        return {
            "adaptation": (rio_a > rio_m, level, weight),
            "mitigation": (rio_m > rio_a, level, weight),
            "cross_cutting": ((rio_a == rio_m) & (rio_a > 0), level, weight),
        }
    adaptation = _coefficient(rio_a, method, significant)
    mitigation = _coefficient(rio_m, method, significant)
    return {
        "adaptation": (rio_a > 0, rio_a, adaptation),
        "mitigation": (rio_m > 0, rio_m, mitigation),
        "cross_cutting": (
            (rio_a > 0) & (rio_m > 0),
            pl.min_horizontal(rio_a, rio_m),
            pl.min_horizontal(adaptation, mitigation),
        ),
    }


def weights(
    method: Method, *, significant: pl.Expr | None = None
) -> dict[str, pl.Expr]:
    """Fractions of a row's amount counted per objective, and in total, under `method`.

    Marker rows (null `climate_share`) weight `rio_adaptation` and `rio_mitigation`
    (nulls count as 0) by the method's coefficients; `significant` replaces the
    method's significant coefficient, for example with a per-provider column. Under
    the highest-marker rule a row goes to its higher marker and equal markers become
    cross-cutting, so the three objectives are disjoint. Without it adaptation and
    mitigation each count their own marker, and `cross_cutting` is their overlap: the
    smaller of the two weighted shares, which `climate` counts once.

    Climate-component rows (non-null `climate_share`) ignore the coefficients and use
    `share_adaptation`, `share_mitigation` and `share_overlap`, fractions of the amount
    in which adaptation and mitigation each include the overlap. The highest-marker
    rule subtracts the overlap from both.
    """
    overlap = pl.col("share_overlap")
    exclusive = overlap if method.highest_marker else pl.lit(0.0)
    component = {
        "adaptation": pl.col("share_adaptation") - exclusive,
        "mitigation": pl.col("share_mitigation") - exclusive,
        "cross_cutting": overlap,
    }
    if significant is None:
        significant = pl.lit(method.significant)
    rules = _marker_rules(method, significant)
    fractions = {
        objective: pl.when(pl.col("climate_share").is_not_null())
        .then(component[objective])
        .when(counts)
        .then(weight)
        .otherwise(0.0)
        for objective, (counts, _, weight) in rules.items()
    }
    a, m, c = (fractions[objective] for objective in OBJECTIVES)
    fractions["climate"] = a + m + c if method.highest_marker else a + m - c
    return fractions


def transform_markers_into_indicators(
    frame: pl.LazyFrame, *, method: Method, value: str = "value"
) -> pl.LazyFrame:
    """One row per activity and objective it counts towards, `value` weighted.

    Each row of `frame` needs `rio_adaptation` and `rio_mitigation`; the result
    replaces both with `objective` and `level`. Under the highest-marker rule the
    objective is the higher marker, or `cross_cutting` when they are equal. Without
    it each positive marker gives its own row, and a row with both also gives a
    `cross_cutting` row for the overlap. Rows with neither marker keep their value as
    `not_climate_relevant` at level 0.
    """
    rio_a, rio_m = _markers()
    parts = [
        frame.filter(counts).with_columns(
            objective=pl.lit(objective),
            level=level.cast(pl.Int8),
            **{value: pl.col(value) * weight},
        )
        for objective, (counts, level, weight) in _marker_rules(
            method, pl.lit(method.significant)
        ).items()
    ]
    not_climate = frame.filter((rio_a == 0) & (rio_m == 0)).with_columns(
        objective=pl.lit(NOT_CLIMATE), level=pl.lit(0, pl.Int8)
    )
    return pl.concat([*parts, not_climate]).drop("rio_adaptation", "rio_mitigation")


_catalogue = Catalogue(kind="methods")
list = _catalogue.list
info = _catalogue.info
search = _catalogue.search
