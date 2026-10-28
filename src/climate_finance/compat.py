"""`ClimateData`: the 1.x interface as a thin, deprecated wrapper over `spending()`.

The 1.2.3 constructor, its `set_*` methods, `load_spending_data` and `get_data` map
onto one `Release.spending` call per source and flow, for the choices whose meaning
2.0 keeps:

- `methodology` "ONE" and "OECD" map to `methods.ONE` and `methods.OECD`; "custom"
  takes the coefficients of `set_custom_spending_methodology`.
- Flows "gross_disbursements", "commitments" and "grant_equivalent" map to
  `flow="disbursements"`, `"commitments"` and `"grant_equivalent"`.
- Source "OECD_CRDF_CRS", the CRDF's commitments and the CRS disbursements
  matched to them, maps to the recipient view's traced rows (`untraced=False`).
  The traced disbursements include those from development banks' own ledgers
  (`disbursement_source="bank_ledger"`), which stand in for the CRS records of the
  same commitments.
- Sources "OECD_CRDF", the CRDF's recipient perspective, and "OECD_CRDF_DONOR", its
  provider perspective, held commitments only in 1.x, so they map to
  `flow="commitments"` in the recipient and the provider view. 1.x returned no rows
  for their other flows, which 2.0 would fill, so those raise `RemovedError`.
- `prices="constant"` with `base_year` maps to `prices=base_year`; currency "CAN"
  maps to "CAD".
- `set_only_oda` maps to `oda_only=True` and `set_update_data` to `refresh=True`.
- `set_climate_finance_data_path` sets the `cache_dir` a `ClimateData` without
  `release` downloads into.

A choice whose meaning 2.0 changed raises `RemovedError` naming the 2.0 call to use:
flow "net_disbursements", which 2.0 nets by loan, source "OECD_CRS", every
Rio-marked CRS record, which 2.0 counts only beside the CRDF's commitments, and the
"_ALLOCABLE" sources, whose filter 2.0 does not hold. So do calls without a 2.0
equivalent, such as source "UNFCCC" and `get_unfccc_data`, and `load_spending_data`
without `source`, which 1.x read as "OECD_CRS".

`get_data` returns the result tables as one pandas frame with `source` and `flow`
columns. The module goes in 3.0.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NoReturn, Self

import polars as pl

from climate_finance._removed import REMOVED_MODULES
from climate_finance.errors import (
    ClimateFinanceDeprecationWarning,
    InvalidInputError,
    RemovedError,
)
from climate_finance.methods import OECD, ONE, Method
from climate_finance.release import Release, load
from climate_finance.spec import Currency, Flow, View

if TYPE_CHECKING:
    import pandas as pd

DEPRECATION = (
    "ClimateData is deprecated since climate-finance 2.0 and goes in 3.0; use "
    "climate_finance.load().spending(...), which returns polars, as the migration "
    "guide shows."
)
_FLOWS: dict[str, Flow] = {
    "gross_disbursements": "disbursements",
    "commitments": "commitments",
    "grant_equivalent": "grant_equivalent",
}
_VIEWS: dict[str, View] = {
    "OECD_CRDF": "recipient",
    "OECD_CRDF_CRS": "recipient",
    "OECD_CRDF_DONOR": "provider",
}
# The CRDF sources, which held commitments only in 1.x, with the 2.0 call for their
# other flows.
_COMMITMENTS_ONLY = {
    "OECD_CRDF": "load_spending_data(source='OECD_CRDF_CRS') or "
    "release.spending(flow=..., untraced=False)",
    "OECD_CRDF_DONOR": "release.spending(flow=..., view='provider', untraced=False)",
}
_CURRENCIES: dict[str, Currency] = {
    "USD": "USD",
    "EUR": "EUR",
    "GBP": "GBP",
    "CAN": "CAD",
    "CAD": "CAD",
}
_PROVIDERS_BY_TYPE = "climate_finance.providers.list(), which has a type column"
_UNFCCC = REMOVED_MODULES["unfccc"]
_TRACED = (
    "load_spending_data(source='OECD_CRDF_CRS') counts the CRDF's commitments and "
    "the CRS disbursements traced to them, and release.spending(...) counts those "
    "with the Rio-marked disbursement traced to no commitment"
)
# 1.x choices whose meaning 2.0 changed or has no equivalent for, with what to use.
_CHANGED = {
    "net_disbursements": "1.x subtracted each climate row's own usd_received, and "
    "2.0 subtracts the principal repaid on each counted loan at its counted share. "
    "Use release.spending(flow='net_disbursements')",
    "OECD_CRS": "it counted every Rio-marked CRS record, which no 2.0 query "
    f"reproduces. {_TRACED}",
    "OECD_CRS_ALLOCABLE": "2.0 holds no allocable filter, and the CRS alone has no "
    f"2.0 query. {_TRACED}",
    "OECD_CRDF_CRS_ALLOCABLE": "2.0 holds no allocable filter. Use "
    "load_spending_data(source='OECD_CRDF_CRS') or "
    "release.spending(untraced=False)",
    "UNFCCC": _UNFCCC,
}
# The cache directory `set_climate_finance_data_path` sets, which a `ClimateData`
# without `release` downloads into, as 1.x read its data from the data path.
_cache_dir: Path | None = None


def _removed(call: str, replacement: str) -> Callable[..., NoReturn]:
    """A 1.x call that 2.0 removed: it raises RemovedError naming `replacement`."""

    def removed(*args: object, **kwargs: object) -> NoReturn:
        raise RemovedError(call, replacement=replacement)

    removed.__name__ = removed.__qualname__ = call
    removed.__doc__ = f"Gone in 2.0; use {replacement}."
    return removed


class ClimateData:
    """The 1.2.3 constructor and methods, returning pandas.

    `release` is the release to query. Without it, `load_spending_data` opens the
    latest release with `climate_finance.load()`, in the directory
    `set_climate_finance_data_path` sets.

    Raises:
        InvalidInputError: a `ValueError`, for a currency or price choice 1.2.3
            refused.
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
        warnings.warn(DEPRECATION, ClimateFinanceDeprecationWarning, stacklevel=2)
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
        source: str | list[str] | None = None,
    ) -> Self:
        """Run `spending()` once per source and flow.

        As in 1.2.3, `methodology` overrides the `set_*_spending_methodology` calls,
        and "custom" needs `set_custom_spending_methodology` first. 1.2.3 read a
        missing `source` as "OECD_CRS", whose meaning 2.0 changed, so a call
        without `source` raises `RemovedError`.

        Raises:
            InvalidInputError: a `ValueError`, for a methodology, flow or source
                1.2.3 did not know, or "custom" without custom coefficients.
            RemovedError: for a flow or source whose meaning 2.0 changed, or that
                has no 2.0 equivalent, and for a call without `source`, naming the
                2.0 call to use.
        """
        method = self._methodology(methodology)
        chosen = [
            (flow, _choice(flow, _FLOWS, what="flows")) for flow in _as_list(flows)
        ]
        if source is None:
            raise RemovedError(
                "load_spending_data() without source=, which 1.x read as "
                "source='OECD_CRS'",
                replacement="that default changed meaning in 2.0: "
                + _CHANGED["OECD_CRS"],
            )
        views = {
            name: _choice(name, _VIEWS, what="source") for name in _as_list(source)
        }
        for name in views.keys() & _COMMITMENTS_ONLY.keys():
            for flow, _ in chosen:
                if flow != "commitments":
                    raise RemovedError(
                        f"source={name!r} with flows={flow!r}",
                        replacement=f"1.x's {name} held commitments only, so it "
                        f"returned no rows. Use {_COMMITMENTS_ONLY[name]}",
                    )
        release = self._release or load(cache_dir=_cache_dir, refresh=self._refresh)
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
                        untraced=False,
                    ).table.with_columns(flow=pl.lit(flow))
                    for flow, spending_flow in chosen
                )
            self._tables[name] = by_view[view]
        return self

    def get_data(self) -> pd.DataFrame:
        """The loaded tables as one pandas frame, with `source` and `flow` columns.

        Raises:
            AttributeError: when nothing has been loaded, as in 1.2.3.
            ImportError: when pandas is not installed, naming the extra that adds it.
        """
        if not self._tables:
            raise AttributeError("No data has been loaded.")
        try:
            import pandas  # noqa: F401 - checked here so the error names the extra
        except ImportError as error:
            raise ImportError(
                "ClimateData.get_data() returns pandas, which is not installed; "
                'pip install "climate-finance[pandas]" adds it'
            ) from error
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
            raise InvalidInputError(
                f"{methodology!r} is not a valid methodology; choose ONE, OECD or "
                "custom"
            )
        if self._custom is None:
            raise InvalidInputError(
                "You must set the custom methodology using the "
                "set_custom_spending_methodology method"
            )
        return self._custom


def set_climate_finance_data_path(path: str | Path) -> None:
    """Download the releases of every `ClimateData` without `release` into `path`,
    as `climate_finance.load(cache_dir=path)` does."""
    warnings.warn(
        "set_climate_finance_data_path is deprecated since climate-finance 2.0 and "
        "goes in 3.0; pass cache_dir= to climate_finance.load()",
        ClimateFinanceDeprecationWarning,
        stacklevel=2,
    )
    # One setting per process, as 1.x's data path was.
    global _cache_dir
    _cache_dir = Path(path).resolve()


get_unfccc_data = _removed("get_unfccc_data", _UNFCCC)


def _listed(values: list[int | str] | int | str | None) -> list[int | str] | None:
    if values is None or isinstance(values, list):
        return values
    return [values]


def _as_list(values: str | list[str]) -> list[str]:
    return [values] if isinstance(values, str) else values


def _choice[T](value: str, choices: Mapping[str, T], *, what: str) -> T:
    """The 2.0 value of a 1.x choice, or RemovedError when 2.0 changed its meaning
    or has none."""
    if value in _CHANGED:
        raise RemovedError(f"{what}={value!r}", replacement=_CHANGED[value])
    if value not in choices:
        raise InvalidInputError(
            f"{what} takes {', '.join(choices)} in 2.0; got {value!r}"
        )
    return choices[value]


def _prices(prices: str, *, base_year: int | None) -> int | Literal["current"]:
    """`prices` and `base_year` as the 2.0 `prices` argument, checked as in 1.2.3."""
    if prices == "current":
        if base_year is not None:
            raise InvalidInputError(
                "You cannot provide a base year when using current prices"
            )
        return "current"
    if prices == "constant":
        if base_year is None:
            raise InvalidInputError(
                "You must provide a base year when using constant prices"
            )
        return base_year
    raise InvalidInputError(
        f"{prices!r} is not a valid prices; choose current or constant"
    )
