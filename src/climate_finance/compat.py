"""`ClimateData`: the 1.x interface as a thin, deprecated wrapper over `spending()`.

The 1.2.3 constructor, its `set_*` methods, `load_spending_data` and `get_data` map
onto one `Snapshot.spending` call per source and flow:

- `methodology` "ONE" and "OECD" map to `methods.ONE` and `methods.OECD`; "custom"
  takes the coefficients of `set_custom_spending_methodology`.
- Flows "gross_disbursements", "commitments" and "grant_equivalent" map to
  `flow="disbursements"`, `"commitments"` and `"grant_equivalent"`.
- Every OECD source is answered from the release's traced commitments and
  disbursements in the recipient view, except "OECD_CRDF_DONOR", the provider
  perspective, which maps to `view="provider"`. The legacy names therefore select
  the traced dataset, which includes disbursements from development banks' own
  ledgers (`disbursement_source="bank_ledger"`), so "OECD_CRS" can return more than
  the CRS alone. The "_ALLOCABLE" names return the same rows as their plain names,
  without their former filter.
- `prices="constant"` with `base_year` maps to `prices=base_year`; currency "CAN"
  maps to "CAD".
- `set_only_oda` maps to `oda_only=True` and `set_update_data` to `refresh=True`.

`get_data` returns the result tables as one pandas frame with `source` and `flow`
columns. Calls without a 2.0 equivalent raise NotImplementedError naming the
replacement, or saying none exists. The module goes in 3.0.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal, NoReturn, Self

import polars as pl

from climate_finance.methods import OECD, ONE, Method
from climate_finance.result import Table
from climate_finance.snapshot import Snapshot, load
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
    "net_disbursements": "net disbursements have no 2.0 equivalent; releases hold "
    "gross disbursements only",
    "UNFCCC": "UNFCCC data has no ClimateData equivalent; use "
    "Snapshot.reconcile(), which compares BTR reports with OECD data",
}


class ClimateData:
    """The 1.2.3 constructor and methods, returning pandas.

    `snapshot` is the release to query. Without it, `load_spending_data` opens the
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
        snapshot: Snapshot | None = None,
    ) -> None:
        warnings.warn(DEPRECATION, DeprecationWarning, stacklevel=2)
        self.years = list(years)
        self.providers = _listed(providers)
        self.recipients = _listed(recipients)
        self.currency = _choice(currency, _CURRENCIES, what="currency")
        self.prices = _prices(prices, base_year=base_year)
        self._snapshot = snapshot
        self._method = ONE
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
        """Use `methods.OECD`."""
        self._method = OECD
        return self

    def set_one_spending_methodology(self) -> Self:
        """Use `methods.ONE`."""
        self._method = ONE
        return self

    def set_custom_spending_methodology(
        self,
        coefficients: tuple[float, float],
        highest_marker: bool = True,
    ) -> Self:
        """Use a custom `Method` with (significant, principal) coefficients."""
        significant, principal = coefficients
        self._custom = Method(
            significant=significant,
            principal=principal,
            highest_marker=highest_marker,
        )
        self._method = self._custom
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
        self._method = self._methodology(methodology)
        snapshot = self._snapshot or load(refresh=self._refresh)
        for name in _as_list(source):
            view = _choice(name, _VIEWS, what="source")
            frames = []
            for flow in _as_list(flows):
                result = snapshot.spending(
                    providers=self.providers,
                    recipients=self.recipients,
                    years=self.years,
                    method=self._method,
                    flow=_choice(flow, _FLOWS, what="flow"),
                    view=view,
                    currency=self.currency,
                    prices=self.prices,
                    oda_only=self._oda_only,
                )
                frames.append(result.table.to_polars().with_columns(flow=pl.lit(flow)))
            self._tables[name] = pl.concat(frames)
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
        return Table(frame.lazy()).to_pandas()

    def get_multilateral_spending_shares(
        self, *args: object, **kwargs: object
    ) -> NoReturn:
        """Gone in 2.0."""
        _gone(
            "get_multilateral_spending_shares",
            "spending(view='provider') attributes core contributions by each "
            "agency's climate share",
        )

    def load_multilateral_imputations_data(
        self, *args: object, **kwargs: object
    ) -> NoReturn:
        """Gone in 2.0."""
        _gone(
            "load_multilateral_imputations_data",
            "spending(view='provider', method=cf.methods.OECD) attributes core "
            "contributions by the OECD's imputed shares",
        )

    def as_share_of_total(self, *args: object, **kwargs: object) -> NoReturn:
        """Gone in 2.0."""
        _gone("as_share_of_total", "spending(...).share_of_total()")

    def convert_to_shares(self, *args: object, **kwargs: object) -> NoReturn:
        """Gone in 2.0."""
        _gone("convert_to_shares", "spending(...).share_of_total()")

    @staticmethod
    def available_methodologies() -> NoReturn:
        """Gone in 2.0."""
        _gone("available_methodologies", "climate_finance.methods.list()")

    @staticmethod
    def available_flows() -> NoReturn:
        """Gone in 2.0."""
        _gone("available_flows", "climate_finance.flows.list()")

    @staticmethod
    def available_sources() -> NoReturn:
        """Gone in 2.0."""
        _gone("available_sources", "a release, which holds one traced source")

    @staticmethod
    def available_prices() -> NoReturn:
        """Gone in 2.0."""
        _gone("available_prices", "prices='current' or a base year")

    @staticmethod
    def available_currencies() -> NoReturn:
        """Gone in 2.0."""
        _gone("available_currencies", "currency='USD', 'EUR', 'GBP' or 'CAD'")

    @staticmethod
    def available_providers(*args: object, **kwargs: object) -> NoReturn:
        """Gone in 2.0."""
        _gone("available_providers", "climate_finance.providers.list()")

    @staticmethod
    def get_dac_providers(*args: object, **kwargs: object) -> NoReturn:
        """Gone in 2.0."""
        _gone("get_dac_providers", _PROVIDERS_BY_TYPE)

    @staticmethod
    def get_non_dac_providers() -> NoReturn:
        """Gone in 2.0."""
        _gone("get_non_dac_providers", _PROVIDERS_BY_TYPE)

    @staticmethod
    def get_multilateral_providers(*args: object, **kwargs: object) -> NoReturn:
        """Gone in 2.0."""
        _gone("get_multilateral_providers", _PROVIDERS_BY_TYPE)

    @staticmethod
    def get_private_providers() -> NoReturn:
        """Gone in 2.0."""
        _gone("get_private_providers", _PROVIDERS_BY_TYPE)

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


def set_climate_finance_data_path(path: object) -> NoReturn:
    """Gone in 2.0."""
    _gone(
        "set_climate_finance_data_path",
        "the CLIMATE_FINANCE_CACHE environment variable or load(cache_dir=...)",
    )


def get_unfccc_data(*args: object, **kwargs: object) -> NoReturn:
    """Gone in 2.0."""
    _gone("get_unfccc_data", "Snapshot.reconcile()")


def _gone(call: str, replacement: str) -> NoReturn:
    raise NotImplementedError(f"{call} is gone in 2.0; use {replacement}")


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
