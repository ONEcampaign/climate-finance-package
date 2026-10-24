"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from typing import Literal

from climate_finance.methods import ONE, Frozen, Method, MethodRange

type Flow = Literal["commitments", "disbursements", "grant_equivalent"]
type View = Literal["recipient", "provider"]
type Currency = Literal["USD", "EUR", "GBP", "CAD"]


class Spec(Frozen):
    """Every argument of `Snapshot.spending`, as a frozen pydantic model.

    `view="recipient"` counts bilateral flows (Bi_Multi 1, 3, 7 and 8) and
    multilateral outflows (code 4). `view="provider"` counts the same bilateral flows
    plus each provider's core contributions times the agency's climate share. Both
    leave out code 2 rows, so no dollar counts twice.

    `oda_only=True` keeps rows with ODA flow codes 11, 13 and 19, drops rows without
    a flow code and the modelled line (neither can be shown to be ODA), and keeps
    attributed core contributions, which count as ODA.
    """

    providers: tuple[str | int, ...] | None = None
    recipients: tuple[str | int, ...] | None = None
    years: tuple[int, ...] | None = None
    method: Method | MethodRange = ONE
    flow: Flow = "disbursements"
    view: View = "recipient"
    currency: Currency = "USD"
    prices: int | Literal["current"] = "current"
    oda_only: bool = False

    @property
    def methods(self) -> tuple[Method, ...]:
        """The methods `method` names: itself, or each method of a range."""
        if isinstance(self.method, Method):
            return (self.method,)
        return self.method.methods
