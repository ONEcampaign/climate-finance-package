"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from typing import Any, Literal

from pydantic import SerializerFunctionWrapHandler, model_serializer

from climate_finance.methods import ONE, Frozen, Method, MethodRange

type Flow = Literal["commitments", "disbursements", "grant_equivalent"]
type View = Literal["recipient", "provider"]
type Currency = Literal["USD", "EUR", "GBP", "CAD"]
type Measure = Literal["amount", "share_of_total"]

# Bi_Multi codes: bilateral flows, multilateral outflows and core contributions.
BILATERAL = (1, 3, 7, 8)
MULTILATERAL_OUTFLOW = 4
CORE_CONTRIBUTION = 2
# A disbursement's ODA test reads its CRS record's flow code, and a row with no CRS
# record (a commitment or a fill) its commitment's CRDF flow category: grants (11),
# loans (13) and equity (19) form category 10.
ODA_FLOW_CATEGORY = 10
ODA_FLOW_CODES = (11, 13, 19)


class Spec(Frozen):
    """Every argument of `Snapshot.spending`, as a frozen pydantic model.

    `view="recipient"` counts bilateral flows (Bi_Multi 1, 3, 7 and 8) and
    multilateral outflows (code 4). `view="provider"` counts the same bilateral flows
    plus each provider's core contributions times the agency's climate share. Both
    leave out code 2 rows, so no dollar counts twice.

    `oda_only=True` keeps commitments of the CRDF's ODA flow category (10),
    disbursements of the CRS's ODA flow codes 11, 13 and 19, and bank-ledger and
    IATI fills, which have no CRS record, of commitments in category 10. It drops
    rows without a flow category or code and the modelled line (neither can be
    shown to be ODA), and keeps attributed core contributions, which count as ODA.

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
        return self.method.methods
