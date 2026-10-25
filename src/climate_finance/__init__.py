"""Climate finance figures from OECD and UNFCCC data, traced to the project."""

import warnings as _warnings
from importlib import import_module as _import_module
from importlib.metadata import version as _version

from climate_finance import errors, methods
from climate_finance.catalogue import flows, providers, recipients
from climate_finance.errors import (
    UnknownNameError,
    UnknownProviderError,
    UnknownRecipientError,
)
from climate_finance.methods import Method
from climate_finance.release import Release, load
from climate_finance.result import Comparison, Quality, Reconciliation, Result, compare
from climate_finance.spec import Spec

__version__ = _version("climate-finance")

__all__ = [
    "Comparison",
    "Method",
    "Quality",
    "Reconciliation",
    "Release",
    "Result",
    "Spec",
    "UnknownNameError",
    "UnknownProviderError",
    "UnknownRecipientError",
    "__version__",
    "compare",
    "errors",
    "flows",
    "load",
    "methods",
    "providers",
    "recipients",
]

# 1.x names, served from climate_finance.compat with a DeprecationWarning until 3.0.
_COMPAT = ("ClimateData", "set_climate_finance_data_path", "get_unfccc_data")


def __getattr__(name: str) -> object:
    if name not in _COMPAT:
        raise AttributeError(f"module 'climate_finance' has no attribute {name!r}")
    _warnings.warn(
        f"climate_finance.{name} is deprecated and goes in 3.0; "
        f"import it from climate_finance.compat, which names its replacement",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(_import_module("climate_finance.compat"), name)
