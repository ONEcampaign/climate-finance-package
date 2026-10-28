"""Climate finance figures from OECD and UNFCCC data, traced to the project."""

import warnings as _warnings
from importlib import import_module as _import_module
from importlib.metadata import version as _version

from climate_finance import errors, methods
from climate_finance._removed import refuse_removed_modules as _refuse_removed_modules
from climate_finance.catalogue import Agency, flows, providers, recipients
from climate_finance.errors import (
    UnknownNameError,
    UnknownProviderError,
    UnknownRecipientError,
)
from climate_finance.methods import Method
from climate_finance.release import Release, load, load_example
from climate_finance.result import Comparison, Quality, Reconciliation, Result, compare
from climate_finance.spec import Spec, SpecInputs

__version__ = _version("climate-finance")

__all__ = [
    "Agency",
    "Comparison",
    "Method",
    "Quality",
    "Reconciliation",
    "Release",
    "Result",
    "Spec",
    "SpecInputs",
    "UnknownNameError",
    "UnknownProviderError",
    "UnknownRecipientError",
    "__version__",
    "compare",
    "errors",
    "flows",
    "load",
    "load_example",
    "methods",
    "providers",
    "recipients",
]

_refuse_removed_modules()

# 1.x names, served from climate_finance.compat with a deprecation warning until 3.0,
# with what replaces each.
_COMPAT = {
    "ClimateData": "climate_finance.load().spending(...)",
    "set_climate_finance_data_path": "climate_finance.load(cache_dir=...)",
    "get_unfccc_data": "Release.reconcile() for the years of the BTR reports",
}


def __getattr__(name: str) -> object:
    if name not in _COMPAT:
        raise AttributeError(f"module 'climate_finance' has no attribute {name!r}")
    _warnings.warn(
        f"climate_finance.{name} is deprecated since 2.0 and goes in 3.0; use "
        f"{_COMPAT[name]}",
        errors.ClimateFinanceDeprecationWarning,
        stacklevel=2,
    )
    return getattr(_import_module("climate_finance.compat"), name)
