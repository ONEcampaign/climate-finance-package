"""The 1.x modules 2.0 removed, refused on import with what replaces each.

`refuse_removed_modules` adds a finder to the end of `sys.meta_path`, so it sees an
import of `climate_finance.<name>` only after every other finder has missed it, and
raises `RemovedModuleError` for a 1.x module there.
"""

import sys
from collections.abc import Sequence
from importlib.machinery import ModuleSpec
from types import ModuleType

from climate_finance.errors import RemovedModuleError

_QUERY = "query a release with climate_finance.load() and Release.spending()"
# Each 1.x module under `climate_finance`, with what to use in 2.0.
REMOVED_MODULES = {
    "common": f"its helpers have no 2.0 equivalent; {_QUERY}",
    "config": "pass cache_dir= to climate_finance.load() to choose where releases "
    "download",
    "core": "ClimateData imports from climate_finance.compat until 3.0, and "
    "climate_finance.load().spending() replaces it",
    "methodologies": "pass method=cf.methods.ONE, cf.methods.OECD or a "
    "cf.Method(...) to Release.spending()",
    "oecd": "a release holds the matched OECD data, which cf-build in the build "
    f"extra fetches; {_QUERY}",
    "unfccc": "UNFCCC Biennial Report data has no 2.0 equivalent. "
    "Release.reconcile() compares Biennial Transparency Report totals, which start "
    "in 2021, with OECD figures",
    "validation": f"its checks have no 2.0 equivalent; {_QUERY}",
}


class _RemovedModuleFinder:
    """Raises `RemovedModuleError` for a 1.x module no other finder found."""

    @classmethod
    def find_spec(
        cls,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        package, _, rest = fullname.partition(".")
        module = rest.partition(".")[0]
        if package == "climate_finance" and module in REMOVED_MODULES:
            raise RemovedModuleError(
                f"climate_finance.{module}", replacement=REMOVED_MODULES[module]
            )
        return None


def refuse_removed_modules() -> None:
    """Add the finder that refuses the 1.x modules, once."""
    if _RemovedModuleFinder not in sys.meta_path:
        sys.meta_path.append(_RemovedModuleFinder)
