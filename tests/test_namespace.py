"""The public namespace: `__all__` lists the vocabulary and `dir(cf)` no imports."""

import sys

import climate_finance as cf
from climate_finance import methods

# The exact public names, changed only in a reviewed pull request.
EXPORTS = {
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
    "methods",
    "providers",
    "recipients",
}
METHODS_EXPORTS = {
    "Method",
    "MethodRange",
    "OECD",
    "ONE",
    "OXFAM",
    "OXFAM_HIGH",
    "OXFAM_LOW",
    "PROVIDER",
    "RANGE",
    "describe",
    "info",
    "label",
    "list",
    "search",
}


def test_all_is_exactly_the_public_vocabulary_and_every_name_resolves() -> None:
    assert set(cf.__all__) == EXPORTS
    assert len(cf.__all__) == len(EXPORTS)
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


def test_methods_all_is_exactly_the_public_methods() -> None:
    assert set(methods.__all__) == METHODS_EXPORTS
    assert len(methods.__all__) == len(METHODS_EXPORTS)
    assert all(hasattr(methods, name) for name in methods.__all__)
