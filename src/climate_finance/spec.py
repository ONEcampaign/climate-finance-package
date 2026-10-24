"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from typing import Any, Literal

from pydantic import SerializerFunctionWrapHandler, model_serializer

from climate_finance.methods import ONE, Frozen, Method, MethodRange

type Flow = Literal["commitments", "disbursements", "grant_equivalent"]
type View = Literal["recipient", "provider"]
type Currency = Literal["USD", "EUR", "GBP", "CAD"]
type Measure = Literal["amount", "share_of_total"]


class Spec(Frozen):
    """Every argument of `Snapshot.spending`, as a frozen pydantic model.

    `view="recipient"` counts bilateral flows (Bi_Multi 1, 3, 7 and 8) and
    multilateral outflows (code 4). `view="provider"` counts the same bilateral flows
    plus each provider's core contributions times the agency's climate share. Both
    leave out code 2 rows, so no dollar counts twice.

    `oda_only=True` keeps rows with ODA flow codes 11, 13 and 19, drops rows without
    a flow code and the modelled line (neither can be shown to be ODA), and keeps
    attributed core contributions, which count as ODA.

    `measure="share_of_total"` divides the table's amounts by all CRS flows of the
    same key, as `Result.share_of_total` describes. An amount spec leaves `measure`
    out of its serialised form.
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
    measure: Measure = "amount"

    @model_serializer(mode="wrap")
    def _drop_default_measure(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        data = handler(self)
        if self.measure == "amount":
            data.pop("measure", None)
        return data

    @property
    def methods(self) -> tuple[Method, ...]:
        """The methods `method` names: itself, or each method of a range."""
        if isinstance(self.method, Method):
            return (self.method,)
        return self.method.methods
