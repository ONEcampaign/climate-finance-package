"""Climate weighting: `Method` and the named methods ONE, OECD, PROVIDER, RANGE."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict

from climate_finance.catalogue import Catalogue


class Method(BaseModel):
    """Coefficients applied to face value by Rio marker level."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    significant: float
    principal: float
    highest_marker: bool
    coefficients: Literal["fixed", "provider"] = "fixed"
    relevance: Literal["markers", "jev"] = "markers"

    def replace(self, **changes: object) -> Self:
        """Return a copy with `changes` applied, validated again."""
        return self.model_validate({**self.model_dump(), **changes})


class MethodRange(BaseModel):
    """Several methods reported together; `low` and `high` are their row-wise bounds."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    methods: tuple[Method, ...]


ONE = Method(significant=0.4, principal=1.0, highest_marker=True)
OECD = Method(significant=1.0, principal=1.0, highest_marker=False)
PROVIDER = Method(
    significant=0.4, principal=1.0, highest_marker=True, coefficients="provider"
)
RANGE = MethodRange(methods=(ONE, OECD, PROVIDER))

_catalogue = Catalogue(kind="methods")
list = _catalogue.list
info = _catalogue.info
search = _catalogue.search
