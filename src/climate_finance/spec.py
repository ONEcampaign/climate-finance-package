"""`Spec`: the frozen description of one query, which reruns to an identical frame."""

from collections.abc import Iterable, Iterator, Mapping
from difflib import get_close_matches
from numbers import Integral
from typing import Annotated, Any, ClassVar, Literal, Self, TypedDict, Unpack

import polars as pl
from pydantic import (
    BaseModel,
    BeforeValidator,
    Discriminator,
    SerializerFunctionWrapHandler,
    StrictBool,
    Tag,
    model_serializer,
    model_validator,
)

from climate_finance.catalogue import (
    Agency,
    Coded,
    Target,
    agency_name,
    each_value,
    name_of,
    resolve,
    target_of,
)
from climate_finance.errors import InvalidInputError, UnknownNameError
from climate_finance.frozen import Frozen
from climate_finance.methods import (
    OECD,
    ONE,
    OXFAM,
    OXFAM_HIGH,
    OXFAM_LOW,
    PROVIDER,
    RANGE,
    Method,
    MethodRange,
    label,
)

# Re-exported: the Bi_Multi codes the views count, which `schema` defines.
from climate_finance.schema import BILATERAL as BILATERAL
from climate_finance.schema import CORE_CONTRIBUTION as CORE_CONTRIBUTION
from climate_finance.schema import MULTILATERAL_OUTFLOW as MULTILATERAL_OUTFLOW
from climate_finance.schema import NET_FLOWS

type Flow = Literal[
    "commitments",
    "disbursements",
    "grant_equivalent",
    "net_disbursements",
    "net_transfers",
]
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


def read_provider_codes(
    providers: str | int | Agency | Iterable[str | int | Agency] | None,
    *,
    reader: str,
    keyword: str = "providers",
) -> tuple[int, ...] | None:
    """The DAC provider codes `providers` names, sorted and without duplicates, or
    None for None, for `reader`, whose tables count providers and no agencies.

    Raises:
        UnknownProviderError: for a name, code or agency `resolve` refuses.
        InvalidInputError: for a name or value of an agency, naming its provider,
            which `keyword` takes instead, for a value of another type, or for an
            empty iterable.
    """
    if providers is None:
        return None
    codes: set[int] = set()
    for value in each_value(providers, kind="providers"):
        target = target_of(value, kind="providers")
        if isinstance(target, Agency):
            raise _agency_error(value, target, reader=reader, keyword=keyword)
        codes.add(target)
    return tuple(sorted(codes))


def _agency_error(
    value: object, agency: Agency, *, reader: str, keyword: str
) -> InvalidInputError:
    provider = name_of(agency.provider, kind="providers")
    name = agency_name(agency)
    return InvalidInputError(
        f"{reader} reads providers; {value!r} is the {name}, an agency of "
        f"{provider}. Pass {keyword}={provider!r}."
    )


def read_one_method(
    method: Method | MethodRange | str | Iterable[Method | str], *, reader: str
) -> Method:
    """`read_method` for `reader`, which reads one method and no range.

    Raises:
        UnknownNameError: for a name outside `cf.methods.list()`.
        InvalidInputError: for a range, naming its methods, or any other value.
    """
    read = read_method(method)
    if isinstance(read, Method):
        return read
    *first, last = (label(member) for member in read.methods)
    members = f"{', '.join(first)} and {last}" if first else last
    raise InvalidInputError(
        f"{reader} reads one method; {label(read)} holds {members}. Pass one of them."
    )


# The methods `method` takes by name, keyed by the names `cf.methods.list()` prints.
_METHODS = {
    label(method): method
    for method in (ONE, OECD, PROVIDER, RANGE, OXFAM_LOW, OXFAM_HIGH, OXFAM)
}


def read_years(years: int | Iterable[int] | None) -> tuple[int, ...] | None:
    """The years `years` names, sorted and without duplicates, or None for None.

    Raises:
        InvalidInputError: for a string, a bool or any other value that is not an
            integer or an iterable of integers, numpy integers counting as integers,
            and for an empty iterable, which would select no rows.
    """
    if years is None:
        return None
    if isinstance(years, str) or not isinstance(years, Integral | Iterable):
        raise _years_error(years)
    values = (years,) if isinstance(years, Integral) else tuple(years)
    for year in values:
        if isinstance(year, bool) or not isinstance(year, Integral):
            raise _years_error(year)
    if not values:
        raise InvalidInputError("years takes at least one year; None takes every year")
    return tuple(sorted({int(year) for year in values}))


def _years_error(value: object) -> InvalidInputError:
    return InvalidInputError(
        f"years takes an int or ints, such as 2022 or range(2015, 2023); got {value!r}"
    )


def read_method(
    method: Method | MethodRange | str | Iterable[Method | str],
) -> Method | MethodRange:
    """The method `method` names: a `Method` or `MethodRange` as given, a name from
    `cf.methods.list()` ignoring case, or a list of methods and names as a range.

    Raises:
        UnknownNameError: for a name outside `cf.methods.list()`, with close names
            as suggestions.
        InvalidInputError: for any other value, or a range inside a list.
    """
    if isinstance(method, Method | MethodRange):
        return method
    if isinstance(method, str):
        return _named_method(method)
    if not isinstance(method, Iterable):
        raise _method_error(method)
    members: list[Method] = []
    for member in method:
        read = read_method(member)
        if not isinstance(read, Method):
            raise _method_error(method)
        members.append(read)
    return MethodRange(methods=tuple(members))


def _named_method(name: str) -> Method | MethodRange:
    key = name.strip().upper()
    if key in _METHODS:
        return _METHODS[key]
    raise UnknownNameError(
        name, kind="method", suggestions=get_close_matches(key, _METHODS)
    )


def _method_error(value: object) -> InvalidInputError:
    return InvalidInputError(
        "method takes a Method, a MethodRange, a name from cf.methods.list() or a "
        f"list of methods and names; got {value!r}"
    )


def _method_or_dump(value: Any) -> Any:
    """`read_method` for every input but a serialised method, which pydantic reads."""
    return value if isinstance(value, Mapping) else read_method(value)


def read_prices(prices: int | Literal["current"]) -> int | Literal["current"]:
    """`"current"`, or the base year `prices` names as an int.

    Raises:
        InvalidInputError: for any other value.
    """
    if prices == "current":
        return "current"
    if isinstance(prices, Integral) and not isinstance(prices, bool):
        return int(prices)
    raise InvalidInputError(
        f"prices takes 'current' or a base year, such as 2022; got {prices!r}"
    )


type Providers = Annotated[tuple[Target, ...] | None, _resolver("providers")]
type Recipients = Annotated[tuple[int, ...] | None, _resolver("recipients")]
type Years = Annotated[tuple[int, ...] | None, BeforeValidator(read_years)]


def _method_kind(value: Any) -> str:
    """Which model a method reads as: a range for a serialised range, which holds
    `methods`, else a method, so a refusal names one model's fields."""
    if isinstance(value, Mapping):
        return "MethodRange" if "methods" in value else "Method"
    return type(value).__name__


type MethodField = Annotated[
    Annotated[Method, Tag("Method")] | Annotated[MethodRange, Tag("MethodRange")],
    Discriminator(_method_kind),
    BeforeValidator(_method_or_dump),
]
type Prices = Annotated[int | Literal["current"], BeforeValidator(read_prices)]


class SpecInputs(TypedDict, total=False):
    """The forms each `Spec` field takes as input. `Spec(...)` and
    `Release.spending(...)` accept them, and type checkers read them from here. `Spec`
    holds the defaults and the stored types."""

    providers: str | int | Agency | Iterable[str | int | Agency] | None
    recipients: str | int | Iterable[str | int] | None
    years: int | Iterable[int] | None
    method: Method | MethodRange | str | Iterable[Method | str]
    flow: Flow
    view: View
    currency: Currency
    prices: int | Literal["current"]
    oda_only: bool
    measure: Measure
    untraced: bool


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

    `flow="net_disbursements"` counts disbursements less the repayments of the
    loans they disbursed, and `flow="net_transfers"` less their interest too. A net
    flow refuses `measure="share_of_total"`, since the CRS totals hold no net
    flows, and a method of `relevance="jev"`, since Jev labels no repayment.

    `untraced=False` leaves out the untraced rows of disbursements and grant
    equivalents: Rio-marked CRS disbursement that reaches no commitment. Commitments
    have none, so it changes nothing there.

    `providers` and `recipients` take names, DAC codes or, for providers,
    `Agency` values, one or many, and hold the codes and agencies they name, sorted
    and without duplicates (`catalogue.resolve`). So `providers=["Japan", 701]` and
    `providers=701` give equal specs, and a provider beside one of its agencies
    covers it. An agency serialises as `{"provider": 918, "agency": 3}`.

    `years` takes an integer or integers, such as `range(2015, 2023)`, and holds
    them sorted and without duplicates. `method` takes a `Method`, a `MethodRange`, a
    name from `cf.methods.list()` ignoring case, or a list of methods and names,
    which becomes a range; it holds the `Method` or `MethodRange`, so JSON keeps the
    coefficients. `prices` takes `"current"` or a base year, and `oda_only` and
    `untraced` take True or False only. An unknown method name raises
    `UnknownNameError`. Any other refusal, an unknown keyword or a value outside a
    field's choices included, raises one `InvalidInputError` naming each field and
    the form it takes.

    A spec at the default `measure` and `untraced` leaves them out of its serialised
    form, so it serialises as a spec written before either field.
    """

    providers: Providers = None
    recipients: Recipients = None
    years: Years = None
    method: MethodField = ONE
    flow: Flow = "disbursements"
    view: View = "recipient"
    currency: Currency = "USD"
    prices: Prices = "current"
    oda_only: StrictBool = False
    measure: Measure = "amount"
    untraced: StrictBool = True

    _keyword_notes: ClassVar[Mapping[str, str]] = {
        "spec": "rerun a saved Spec with release.run(spec)"
    }

    @model_validator(mode="after")
    def _refuse_net_flow_measures(self) -> Self:
        if self.flow not in NET_FLOWS:
            return self
        if self.measure == "share_of_total":
            raise InvalidInputError(
                f"measure='share_of_total' takes a gross flow; {self.flow!r} subtracts "
                "repayments, which the CRS totals do not. Pass flow='disbursements'."
            )
        if any(method.relevance == "jev" for method in self.methods):
            raise InvalidInputError(
                f"relevance='jev' takes a gross flow; Jev labels no repayment, so "
                f"{self.flow!r} cannot weigh by it. Pass a method of "
                "relevance='markers'."
            )
        return self

    def __init__(self, **inputs: Unpack[SpecInputs]) -> None:
        # `SpecInputs` types the keywords. Type checkers derive a pydantic model's
        # constructor from the stored field types, which refuse `providers="Japan"`.
        # `BaseModel` is named because they read `super()` as `Frozen`'s fieldless
        # constructor.
        BaseModel.__init__(self, **inputs)

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

    def __repr_args__(self) -> Iterator[tuple[str, object]]:
        # Only the fields away from their defaults, a named method as the
        # expression that returns it.
        for name, field in type(self).model_fields.items():
            value = getattr(self, name)
            if value == field.default:
                continue
            named = [key for key, method in _METHODS.items() if method == value]
            yield name, _Expression(f"cf.methods.{named[0]}") if named else value

    @property
    def methods(self) -> tuple[Method, ...]:
        """The methods `method` names: itself, or each method of a range."""
        return self.method.methods


class _Expression(str):
    """Text a repr prints as is, such as `cf.methods.OECD`."""

    __slots__ = ()

    def __repr__(self) -> str:
        return str.__str__(self)


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
