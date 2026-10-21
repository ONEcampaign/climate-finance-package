"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict

from climate_finance.methods import ONE, Method, MethodRange

type Flow = Literal["commitments", "disbursements", "grant_equivalent"]


class Spec(BaseModel):
    """Every argument of `Snapshot.spending`, as a frozen pydantic model."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    providers: tuple[str | int, ...] | None = None
    recipients: tuple[str | int, ...] | None = None
    years: tuple[int, ...] | None = None
    method: Method | MethodRange = ONE
    flow: Flow = "disbursements"
    currency: str = "USD"
    prices: int | Literal["current"] = "current"
    oda_only: bool = False

    def replace(self, **changes: object) -> Self:
        """Return a copy with `changes` applied, validated again."""
        return self.model_validate({**self.model_dump(), **changes})
