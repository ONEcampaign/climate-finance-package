"""Climate weighting: `Method`, the named methods ONE, OECD, PROVIDER, RANGE, and the
marker rules that turn Rio markers or climate components into objective weights."""

from typing import Literal, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator

from climate_finance.catalogue import Catalogue
from climate_finance.schema import OBJECTIVES

NOT_CLIMATE = "not_climate_relevant"


class Frozen(BaseModel):
    """A frozen model that rejects unknown fields and copies itself with changes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    def replace(self, **changes: object) -> Self:
        """Return a copy with `changes` applied, validated again."""
        return self.model_validate({**self.model_dump(), **changes})


class Method(Frozen):
    """Coefficients applied to face value by Rio marker level.

    `eu` sets how EU Institutions (provider 918) count in the provider view. As a
    `"channel"`, members' core contributions to the EU budget, the EDF and the EIB
    carry the EU's climate share, and 918's own rows leave a result in each year
    where it also holds such an attribution, so EU money counts once, inside its
    members' totals. A query for 918 alone returns its own spending, the EIB's
    included.
    As a `"provider"`, 918 counts the EU budget and the EDF in its own right, and
    contributions to them attribute nothing. The EIB is a channel, as the OECD counts
    it: the EIB's own rows (agency 3) leave every result, and members' contributions
    to it carry the EIB's climate share. A query for 918 alone returns the spending of
    the EU budget and the EDF.
    """

    significant: float = Field(ge=0.0, le=1.0)
    principal: float = Field(ge=0.0, le=1.0)
    highest_marker: bool
    coefficients: Literal["fixed", "provider"] = "fixed"
    relevance: Literal["markers", "jev"] = "markers"
    eu: Literal["channel", "provider"] = "channel"

    @property
    def methods(self) -> tuple[Self, ...]:
        """The method itself, so a method and a range both list their members."""
        return (self,)

    def _repr_markdown_(self) -> str:
        return describe(self)


class MethodRange(Frozen):
    """Several methods reported together. `low` and `high` are their row-wise bounds.

    Each method may appear once, since a repeated one would count its rows twice.
    """

    methods: tuple[Method, ...] = Field(min_length=1)

    @field_validator("methods")
    @classmethod
    def _distinct(cls, methods: tuple[Method, ...]) -> tuple[Method, ...]:
        if len(set(methods)) < len(methods):
            raise ValueError("a range lists a method more than once")
        return methods

    def _repr_markdown_(self) -> str:
        return describe(self)


ONE = Method(significant=0.4, principal=1.0, highest_marker=True)
# The OECD's imputed shares hold the EIB alone of the EU channels, because it counts
# the EU budget and the EDF as a provider. PROVIDER reads ONE's agency shares, so it
# keeps ONE's EU setting.
OECD = Method(significant=1.0, principal=1.0, highest_marker=False, eu="provider")
PROVIDER = Method(
    significant=0.4, principal=1.0, highest_marker=True, coefficients="provider"
)
RANGE = MethodRange(methods=(ONE, OECD, PROVIDER))

_NAMED = {"ONE": ONE, "OECD": OECD, "PROVIDER": PROVIDER}


def uses_imputed_shares(method: Method) -> bool:
    """Whether `method` attributes core contributions with the OECD's imputed
    shares, as OECD does with either EU setting. Any other method weighs the
    agency's own spending."""
    return method.replace(eu=OECD.eu) == OECD


def _number(value: float) -> str:
    """`value` in the shortest form that reads back as the same float, -0 as 0."""
    return repr(value + 0.0).removesuffix(".0")


def label(method: Method | MethodRange) -> str:
    """The method's name if it is a named method, else every field that defines it.

    Results group rows by this label, so two methods share a label only when they
    are equal: coefficients print in full, as the shortest text that reads back as
    the same float.
    """
    if isinstance(method, MethodRange):
        if method == RANGE:
            return "RANGE"
        return f"range of {' / '.join(label(member) for member in method.methods)}"
    for name, named in _NAMED.items():
        if method == named:
            return name
        if method.replace(eu=named.eu) == named:
            return f"{name}, EU as a {method.eu}"
    rule = "highest marker" if method.highest_marker else "each marker"
    return (
        f"significant {_number(method.significant)}, "
        f"principal {_number(method.principal)}, {rule}"
        f", {method.coefficients} coefficients, {method.relevance} relevance"
        f", EU as a {method.eu}"
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
            " Each provider takes the coefficients it applies when reporting to the "
            "UNFCCC for the activity's year and agency, as the release records them "
            "with their source, and these coefficients where it states none."
        )
    return text + _EU[method.eu]


_EU = {
    "channel": " In the provider view EU Institutions count as a channel: members' "
    "core contributions to the EU budget, the EDF and the EIB carry the EU's climate "
    "share, and EU Institutions' own rows, the EIB's lending among them, leave a "
    "result in each year where it holds such an attribution, so EU spending counts "
    "once.",
    "provider": " In the provider view EU Institutions count as a provider and the "
    "EIB as a channel: the rows of the EU budget and the EDF count in EU "
    "Institutions' own right and contributions to them attribute nothing, while the "
    "EIB's own rows leave and members' contributions to it carry the EIB's climate "
    "share.",
}


def _coefficient(
    level: pl.Expr, *, significant: pl.Expr, principal: pl.Expr
) -> pl.Expr:
    return (
        pl.when(level == 2)
        .then(principal)
        .when(level == 1)
        .then(significant)
        .otherwise(pl.lit(0.0))
    )


def _markers() -> tuple[pl.Expr, pl.Expr]:
    return pl.col("rio_adaptation").fill_null(0), pl.col("rio_mitigation").fill_null(0)


def _marker_rules(
    method: Method,
    *,
    significant: pl.Expr,
    principal: pl.Expr,
    both_significant: pl.Expr | None = None,
) -> dict[str, tuple[pl.Expr, pl.Expr, pl.Expr]]:
    """Per objective: whether a marker row counts towards it, its level, its weight.

    A row significant for both objectives takes `both_significant`, where it is set,
    as its significant coefficient.
    """
    rio_a, rio_m = _markers()
    if both_significant is not None:
        significant = (
            pl.when((rio_a == 1) & (rio_m == 1))
            .then(both_significant.fill_null(significant))
            .otherwise(significant)
        )
    coefficients = {"significant": significant, "principal": principal}
    if method.highest_marker:
        level = pl.max_horizontal(rio_a, rio_m)
        weight = _coefficient(level, **coefficients)
        return {
            "adaptation": (rio_a > rio_m, level, weight),
            "mitigation": (rio_m > rio_a, level, weight),
            "cross_cutting": ((rio_a == rio_m) & (rio_a > 0), level, weight),
        }
    adaptation = _coefficient(rio_a, **coefficients)
    mitigation = _coefficient(rio_m, **coefficients)
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
    method: Method,
    *,
    significant: pl.Expr | None = None,
    principal: pl.Expr | None = None,
    both_significant: pl.Expr | None = None,
) -> dict[str, pl.Expr]:
    """Fractions of a row's amount counted per objective, and in total, under `method`.

    Marker rows (null `climate_share`) weight `rio_adaptation` and `rio_mitigation`
    (nulls count as 0) by the method's coefficients. `significant` and `principal`
    replace the method's coefficients, for example with per-provider columns, and a
    row significant for both objectives takes `both_significant` where it is not
    null. Under the highest-marker rule a row goes to its higher marker and equal
    markers become cross-cutting, so the three objectives are disjoint. Without it
    adaptation and mitigation each count their own marker, and `cross_cutting` is
    their overlap, the smaller of the two weighted shares, which `climate` counts
    once.

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
    rules = _marker_rules(
        method,
        significant=pl.lit(method.significant) if significant is None else significant,
        principal=pl.lit(method.principal) if principal is None else principal,
        both_significant=both_significant,
    )
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

    Each row of `frame` needs `rio_adaptation` and `rio_mitigation`. The result
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
            method,
            significant=pl.lit(method.significant),
            principal=pl.lit(method.principal),
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
