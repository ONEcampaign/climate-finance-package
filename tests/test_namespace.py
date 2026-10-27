"""The public namespace: `__all__` lists the vocabulary and `dir(cf)` no imports."""

import sys

import climate_finance as cf
from climate_finance import methods


def test_all_lists_the_vocabulary_and_every_name_resolves() -> None:
    assert {"Agency", "Release", "Quality", "Reconciliation", "errors"} <= set(
        cf.__all__
    )
    assert all(hasattr(cf, name) for name in cf.__all__)
    assert cf.errors.ClimateFinanceError.__name__ == "ClimateFinanceError"


def test_dir_holds_no_imported_library_names() -> None:
    """Other than `__all__`, `dir(cf)` lists only the package's own submodules."""
    extra = {name for name in dir(cf) if not name.startswith("_")} - set(cf.__all__)
    leaked = {
        name
        for name in extra
        if not getattr(sys.modules.get(f"climate_finance.{name}"), "__name__", None)
    }
    assert not leaked
    assert not hasattr(cf, "warnings")


def test_methods_all_names_what_the_docs_use() -> None:
    assert all(hasattr(methods, name) for name in methods.__all__)
    assert {"ONE", "OECD", "PROVIDER", "RANGE", "list", "info", "search"} <= set(
        methods.__all__
    )
