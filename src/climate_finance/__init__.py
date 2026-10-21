"""Climate finance figures from OECD and UNFCCC data, traced to the project."""

from importlib.metadata import version

from climate_finance import methods
from climate_finance.catalogue import flows, providers
from climate_finance.methods import Method
from climate_finance.result import compare
from climate_finance.snapshot import load
from climate_finance.spec import Spec

__version__ = version("climate-finance")

__all__ = [
    "Method",
    "Spec",
    "__version__",
    "compare",
    "flows",
    "load",
    "methods",
    "providers",
]
