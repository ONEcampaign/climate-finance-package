"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from collections.abc import Iterable, Mapping
from typing import Annotated, Any, Literal

import polars as pl
from pydantic import BeforeValidator, SerializerFunctionWrapHandler, model_serializer

from climate_finance.catalogue import Agency, Coded, Target, resolve
from climate_finance.methods import ONE, Frozen, Method, MethodRange

# Re-exported: the Bi_Multi codes the views count, which `schema` defines.
from climate_finance.schema import BILATERAL as BILATERAL
from climate_finance.schema import CORE_CONTRIBUTION as CORE_CONTRIBUTION
from climate_finance.schema import MULTILATERAL_OUTFLOW as MULTILATERAL_OUTFLOW

type Flow = Literal["commitments", "disbursements", "grant_equivalent"]
type View = Literal["recipient", "provider"]
type Currency = Literal["USD", "EUR", "GBP", "CAD"]
type Measure = Literal["amount", "share_of_total"]

# A disbursement's ODA test reads its CRS record's flow code, and a row with no CRS
# record (a commitment or a fill) its commitment's CRDF flow category: grants (11),
# loans (13) and equity (19) form category 10.
ODA_FLOW_CATEGORY = 10
ODA_FLOW_CODES = (11, 13, 19)


def _resolver(kind: Coded) -> BeforeValidator:
    """Read a field's names, codes and agencies through `resolve`. An agency in its
    serialised form, `{"provider": 918, "agency": 3}`, reads as an `Agency`."""

    def read(values: Any) -> tuple[Target, ...] | None:
        if isinstance(values, Mapping):
            return resolve(Agency.model_validate(values), kind=kind)
        if isinstance(values, Iterable) and not isinstance(values, str | Agency):
            values = [
                Agency.model_validate(value) if isinstance(value, Mapping) else value
                for value in values
            ]
        return resolve(values, kind=kind)

    return BeforeValidator(read)


type Providers = Annotated[tuple[Target, ...] | None, _resolver("providers")]
type Recipients = Annotated[tuple[int, ...] | None, _resolver("recipients")]


class Spec(Frozen):
    """Every argument of `Release.spending`, as a frozen pydantic model.

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
    same key, as `Result.share_of_total` describes.

    `untraced=False` leaves out the untraced rows of disbursements and grant
    equivalents: Rio-marked CRS disbursement that reaches no commitment. Commitments
    have none, so it changes nothing there.

    `providers` and `recipients` take names, DAC codes or, for providers,
    `Agency` values, one or many, and hold the codes and agencies they name, sorted
    and without duplicates (`catalogue.resolve`). So `providers=["Japan", 701]` and
    `providers=701` give equal specs, and a provider beside one of its agencies
    covers it. An agency serialises as `{"provider": 918, "agency": 3}`.

    A spec at the default `measure` and `untraced` leaves them out of its serialised
    form, so it serialises as a spec written before either field.
    """

    providers: Providers = None
    recipients: Recipients = None
    years: tuple[int, ...] | None = None
    method: Method | MethodRange = ONE
    flow: Flow = "disbursements"
    view: View = "recipient"
    currency: Currency = "USD"
    prices: int | Literal["current"] = "current"
    oda_only: bool = False
    measure: Measure = "amount"
    untraced: bool = True

    @model_serializer(mode="wrap")
    def _drop_later_defaults(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        data = handler(self)
        if self.measure == "amount":
            data.pop("measure", None)
        if self.untraced:
            data.pop("untraced", None)
        return data

    @property
    def methods(self) -> tuple[Method, ...]:
        """The methods `method` names: itself, or each method of a range."""
        return self.method.methods


def provider_rows(providers: tuple[Target, ...]) -> pl.Expr:
    """Rows of the providers in `providers`, and of the (provider, agency) pairs.

    A row with a null `agency_code`, such as an attributed core contribution, matches
    a provider and no agency of it.
    """
    codes = [target for target in providers if isinstance(target, int)]
    agencies = [target for target in providers if isinstance(target, Agency)]
    return pl.any_horizontal(
        pl.col("provider_code").is_in(codes),
        *(
            (pl.col("provider_code") == agency.provider)
            & (pl.col("agency_code") == agency.agency)
            for agency in agencies
        ),
    )
