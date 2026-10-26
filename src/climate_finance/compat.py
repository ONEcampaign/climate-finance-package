"""`ClimateData`: the 1.x interface as a thin, deprecated wrapper over `spending()`.

The 1.2.3 constructor, its `set_*` methods, `load_spending_data` and `get_data` map
onto one `Release.spending` call per source and flow:

- `methodology` "ONE" and "OECD" map to `methods.ONE` and `methods.OECD`; "custom"
  takes the coefficients of `set_custom_spending_methodology`.
- Flows "gross_disbursements", "commitments", "grant_equivalent" and
  "net_disbursements" map to `flow="disbursements"`, `"commitments"`,
  `"grant_equivalent"` and `"net_disbursements"`. 1.x net disbursements were the
  climate rows' disbursements less their own `usd_received`. 2.0 subtracts the
  principal repaid on the loans the disbursements count, at each loan's counted
  share.
- Every OECD source is answered from the release's traced commitments and
  disbursements in the recipient view, except "OECD_CRDF_DONOR", the provider
  perspective, which maps to `view="provider"`. The legacy names therefore select
  the traced dataset, which includes disbursements from development banks' own
  ledgers (`disbursement_source="bank_ledger"`), so "OECD_CRS" can return more than
  the CRS alone. Disbursements also hold the Rio-marked CRS disbursement that
  reaches no commitment, at the record's own markers. The "_ALLOCABLE" names return
  the same rows as their plain names, without their former filter.
- `prices="constant"` with `base_year` maps to `prices=base_year`; currency "CAN"
  maps to "CAD".
- `set_only_oda` maps to `oda_only=True` and `set_update_data` to `refresh=True`.

`get_data` returns the result tables as one pandas frame with `source` and `flow`
columns. Calls without a 2.0 equivalent raise NotImplementedError naming the
replacement, or saying none exists. The module goes in 3.0.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Literal, NoReturn, Self

import polars as pl

from climate_finance.methods import OECD, ONE, Method
from climate_finance.release import Release, load
from climate_finance.spec import Currency, Flow, View

if TYPE_CHECKING:
    import pandas as pd

DEPRECATION = (
    "ClimateData is deprecated and goes in climate-finance 3.0; use "
    "climate_finance.load().spending(...), which takes the same choices as keyword "
    "arguments and returns polars."
)
_FLOWS: dict[str, Flow] = {
    "gross_disbursements": "disbursements",
    "commitments": "commitments",
    "grant_equivalent": "grant_equivalent",
    "net_disbursements": "net_disbursements",
}
_VIEWS: dict[str, View] = {
    "OECD_CRS": "recipient",
    "OECD_CRS_ALLOCABLE": "recipient",
    "OECD_CRDF": "recipient",
    "OECD_CRDF_CRS": "recipient",
    "OECD_CRDF_CRS_ALLOCABLE": "recipient",
    "OECD_CRDF_DONOR": "provider",
}
_CURRENCIES: dict[str, Currency] = {
    "USD": "USD",
    "EUR": "EUR",
    "GBP": "GBP",
    "CAN": "CAD",
    "CAD": "CAD",
}
_PROVIDERS_BY_TYPE = "climate_finance.providers.list(), which has a type column"
# 1.x values that 2.0 cannot answer, with what to use instead.
_NO_EQUIVALENT = {
    "UNFCCC": "UNFCCC data has no ClimateData equivalent; use "
    "Release.reconcile(), which compares BTR reports with OECD data",
}


def _removed(call: str, replacement: str) -> Callable[..., NoReturn]:
    """A 1.x call that 2.0 removed: it raises NotImplementedError naming
    `replacement`."""

    def removed(*args: object, **kwargs: object) -> NoReturn:
        raise NotImplementedError(f"{call} is gone in 2.0; use {replacement}")

    removed.__name__ = removed.__qualname__ = call
    removed.__doc__ = f"Gone in 2.0; use {replacement}."
    return removed


class ClimateData:
    """The 1.2.3 constructor and methods, returning pandas.

    `release` is the release to query. Without it, `load_spending_data` opens the
    latest release with `climate_finance.load()`.

    Raises:
        ValueError: for a currency or price choice 1.2.3 refused.
    """

    def __init__(
        self,
        years: list[int] | range,
        providers: list[int | str] | int | str | None = None,
        recipients: list[int | str] | int | str | None = None,
        currency: str = "USD",
        prices: str = "current",
        base_year: int | None = None,
        *,
        release: Release | None = None,
    ) -> None:
        warnings.warn(DEPRECATION, DeprecationWarning, stacklevel=2)
        self.years = list(years)
        self.providers = _listed(providers)
        self.recipients = _listed(recipients)
        self.currency = _choice(currency, _CURRENCIES, what="currency")
        self.prices = _prices(prices, base_year=base_year)
        self._release = release
        self._custom: Method | None = None
        self._oda_only = False
        self._refresh = False
        self._tables: dict[str, pl.DataFrame] = {}

    def __repr__(self) -> str:
        years = f"{min(self.years)}-{max(self.years)}" if self.years else "no years"
        loaded = ", ".join(self._tables) or "nothing loaded"
        return f"ClimateData for {years}: {loaded}"

    def set_only_oda(self) -> Self:
        """Count ODA only (`oda_only=True`)."""
        self._oda_only = True
        return self

    def set_update_data(self) -> Self:
        """Fetch the newest release index on load (`refresh=True`)."""
        self._refresh = True
        return self

    def set_oecd_spending_methodology(self) -> Self:
        """Do nothing, as in 1.2.3, where `load_spending_data(methodology=)` decides."""
        return self

    def set_one_spending_methodology(self) -> Self:
        """Do nothing, as in 1.2.3, where `load_spending_data(methodology=)` decides."""
        return self

    def set_custom_spending_methodology(
        self,
        coefficients: tuple[float, float],
        highest_marker: bool = True,
    ) -> Self:
        """Set the (significant, principal) coefficients of
        `load_spending_data(methodology="custom")`."""
        significant, principal = coefficients
        self._custom = Method(
            significant=significant,
            principal=principal,
            highest_marker=highest_marker,
        )
        return self

    def load_spending_data(
        self,
        methodology: str = "ONE",
        flows: str | list[str] = "gross_disbursements",
        source: str | list[str] = "OECD_CRS",
    ) -> Self:
        """Run `spending()` once per source and flow.

        As in 1.2.3, `methodology` overrides the `set_*_spending_methodology` calls,
        and "custom" needs `set_custom_spending_methodology` first.

        Raises:
            ValueError: for a methodology, flow or source 1.2.3 did not know, or
                "custom" without custom coefficients.
            NotImplementedError: for a flow or source with no 2.0 equivalent.
        """
        method = self._methodology(methodology)
        chosen = [
            (flow, _choice(flow, _FLOWS, what="flow")) for flow in _as_list(flows)
        ]
        views = {
            name: _choice(name, _VIEWS, what="source") for name in _as_list(source)
        }
        release = self._release or load(refresh=self._refresh)
        # Sources that share a view share one query per flow.
        by_view: dict[View, pl.DataFrame] = {}
        for name, view in views.items():
            if view not in by_view:
                by_view[view] = pl.concat(
                    release.spending(
                        providers=self.providers,
                        recipients=self.recipients,
                        years=self.years,
                        method=method,
                        flow=spending_flow,
                        view=view,
                        currency=self.currency,
                        prices=self.prices,
                        oda_only=self._oda_only,
                    ).table.with_columns(flow=pl.lit(flow))
                    for flow, spending_flow in chosen
                )
            self._tables[name] = by_view[view]
        return self

    def get_data(self) -> pd.DataFrame:
        """The loaded tables as one pandas frame, with `source` and `flow` columns.

        Raises:
            AttributeError: when nothing has been loaded, as in 1.2.3.
        """
        if not self._tables:
            raise AttributeError("No data has been loaded.")
        frame = pl.concat(
            table.with_columns(source=pl.lit(name))
            for name, table in self._tables.items()
        )
        return frame.to_pandas()

    get_multilateral_spending_shares = staticmethod(
        _removed(
            "get_multilateral_spending_shares",
            "spending(view='provider') attributes core contributions by each "
            "agency's climate share",
        )
    )
    load_multilateral_imputations_data = staticmethod(
        _removed(
            "load_multilateral_imputations_data",
            "spending(view='provider', method=cf.methods.OECD) attributes core "
            "contributions by the OECD's imputed shares",
        )
    )
    as_share_of_total = staticmethod(
        _removed("as_share_of_total", "spending(...).share_of_total()")
    )
    convert_to_shares = staticmethod(
        _removed("convert_to_shares", "spending(...).share_of_total()")
    )
    available_methodologies = staticmethod(
        _removed("available_methodologies", "climate_finance.methods.list()")
    )
    available_flows = staticmethod(
        _removed("available_flows", "climate_finance.flows.list()")
    )
    available_sources = staticmethod(
        _removed("available_sources", "a release, which holds one traced source")
    )
    available_prices = staticmethod(
        _removed("available_prices", "prices='current' or a base year")
    )
    available_currencies = staticmethod(
        _removed("available_currencies", "currency='USD', 'EUR', 'GBP' or 'CAD'")
    )
    available_providers = staticmethod(
        _removed("available_providers", "climate_finance.providers.list()")
    )
    get_dac_providers = staticmethod(_removed("get_dac_providers", _PROVIDERS_BY_TYPE))
    get_non_dac_providers = staticmethod(
        _removed("get_non_dac_providers", _PROVIDERS_BY_TYPE)
    )
    get_multilateral_providers = staticmethod(
        _removed("get_multilateral_providers", _PROVIDERS_BY_TYPE)
    )
    get_private_providers = staticmethod(
        _removed("get_private_providers", _PROVIDERS_BY_TYPE)
    )

    def _methodology(self, methodology: str) -> Method:
        if methodology == "ONE":
            return ONE
        if methodology == "OECD":
            return OECD
        if methodology != "custom":
            raise ValueError(
                f"{methodology!r} is not a valid methodology; choose ONE, OECD or "
                "custom"
            )
        if self._custom is None:
            raise ValueError(
                "You must set the custom methodology using the "
                "set_custom_spending_methodology method"
            )
        return self._custom


set_climate_finance_data_path = _removed(
    "set_climate_finance_data_path",
    "the CLIMATE_FINANCE_CACHE environment variable or load(cache_dir=...)",
)
get_unfccc_data = _removed("get_unfccc_data", "Release.reconcile()")


def _listed(values: list[int | str] | int | str | None) -> list[int | str] | None:
    if values is None or isinstance(values, list):
        return values
    return [values]


def _as_list(values: str | list[str]) -> list[str]:
    return [values] if isinstance(values, str) else values


def _choice[T](value: str, choices: Mapping[str, T], *, what: str) -> T:
    """The 2.0 value of a 1.x choice, or NotImplementedError when 2.0 has none."""
    if value in _NO_EQUIVALENT:
        raise NotImplementedError(_NO_EQUIVALENT[value])
    if value not in choices:
        raise ValueError(
            f"{value!r} is not a valid {what}; choose from {', '.join(choices)}"
        )
    return choices[value]


def _prices(prices: str, *, base_year: int | None) -> int | Literal["current"]:
    """`prices` and `base_year` as the 2.0 `prices` argument, checked as in 1.2.3."""
    if prices == "current":
        if base_year is not None:
            raise ValueError("You cannot provide a base year when using current prices")
        return "current"
    if prices == "constant":
        if base_year is None:
            raise ValueError("You must provide a base year when using constant prices")
        return base_year
    raise ValueError(f"{prices!r} is not a valid prices; choose current or constant")
