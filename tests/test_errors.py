"""The error classes: codes, pickling, and the built-in classes they extend."""

import pickle
from importlib import import_module
from pathlib import Path

import pytest
from make_fixture import FIXTURE_DIR

import climate_finance as cf
from climate_finance.errors import ClimateFinanceError

# Every module that defines a `ClimateFinanceError` subclass.
ERROR_MODULES = (
    "climate_finance.errors",
    "climate_finance.build.errors",
    "climate_finance.build.jev_client",
    "climate_finance.build.listings",
    "climate_finance.build.names",
    "climate_finance.build.normalise.crdf",
    "climate_finance.build.link.embed",
    "climate_finance.build.link.jev",
    "climate_finance.build.link.labelling",
)
# An instance of each class whose constructor takes more than a message.
CONSTRUCTED = {
    "UnknownNameError": lambda cls: cls(
        "Japn", kind="provider", suggestions=["Japan"], note=None
    ),
    "UnknownProviderError": lambda cls: cls("Japn", suggestions=["Japan"]),
    "UnknownRecipientError": lambda cls: cls("Kenia", suggestions=["Kenya"]),
    "WithheldTableError": lambda cls: cls("btr", note="Build it with cf-build."),
    "ReleaseFormatError": lambda cls: cls("2027a", data_format=2, supported=1),
    "FormatTableError": lambda cls: cls("untraced", data_format=1),
    "FormatQueryError": lambda cls: cls("2026a", data_format=2, needed=3),
    "SchemaError": lambda cls: cls("commitments", {"year": "missing"}),
    "RemovedError": lambda cls: cls("get_unfccc_data", replacement="none"),
    "RemovedModuleError": lambda cls: cls("climate_finance.core", replacement="none"),
    "StageInputMissingError": lambda cls: cls(
        stage="link", table="commitments", path=Path("stages/commitments.parquet")
    ),
    "StaleStageError": lambda cls: cls(stage="link", reason="reads other settings"),
    "CostCapExceededError": lambda cls: cls(estimate_usd=12.5, cap_usd=10.0),
    "LinkThresholdError": lambda cls: cls(
        threshold=0.5, f1=0.8, best_threshold=0.4, best_f1=0.9
    ),
    "ReleaseInvariantError": lambda cls: cls(["a problem"]),
    "GateFailedError": lambda cls: cls(["a gate row"]),
    "JevCacheMissError": lambda cls: cls(misses=3, model="jev-1"),
    "EmbeddingCacheMissError": lambda cls: cls(misses=3, model="e5", revision="r1"),
}


def _error_classes() -> list[type[ClimateFinanceError]]:
    classes = {
        value
        for name in ERROR_MODULES
        for value in vars(import_module(name)).values()
        if isinstance(value, type) and issubclass(value, ClimateFinanceError)
    }
    return sorted(classes, key=lambda cls: cls.__name__)


ERROR_CLASSES = _error_classes()


def _instance(cls: type[ClimateFinanceError]) -> ClimateFinanceError:
    custom = any(
        "__init__" in vars(base)
        for base in cls.__mro__[: cls.__mro__.index(ClimateFinanceError)]
    )
    if not custom:
        return cls("Something went wrong.")
    assert cls.__name__ in CONSTRUCTED, f"add a {cls.__name__} to CONSTRUCTED"
    return CONSTRUCTED[cls.__name__](cls)


@pytest.mark.parametrize("cls", ERROR_CLASSES, ids=lambda cls: cls.__name__)
def test_every_error_pickles_with_its_attributes(
    cls: type[ClimateFinanceError],
) -> None:
    error = _instance(cls)
    error.add_note("raised in a worker")

    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is cls
    assert restored.args == error.args
    assert vars(restored) == vars(error)
    assert str(restored) == str(error)


def test_every_error_has_its_own_code_and_ends_its_message_with_it() -> None:
    codes = {cls.code: cls for cls in ERROR_CLASSES}

    assert len(codes) == len(ERROR_CLASSES)
    for cls in ERROR_CLASSES:
        assert str(_instance(cls)).endswith(f" [{cls.code}]")


def test_the_codes_of_the_query_errors_hold() -> None:
    # Handlers match the codes, so a renamed class keeps its code by setting `code`.
    codes = {
        cls.__name__: cls.code
        for cls in ERROR_CLASSES
        if cls.__module__ == "climate_finance.errors"
    }

    assert codes == {
        "ClimateFinanceError": "climate_finance",
        "FormatQueryError": "format_query",
        "FormatTableError": "format_table",
        "InvalidInputError": "invalid_input",
        "MissingLabelsError": "missing_labels",
        "ReleaseFormatError": "release_format",
        "ReleaseIntegrityError": "release_integrity",
        "RemovedError": "removed",
        "RemovedModuleError": "removed_module",
        "SchemaError": "schema",
        "SourceUnavailableError": "source_unavailable",
        "UnknownNameError": "unknown_name",
        "UnknownProviderError": "unknown_provider",
        "UnknownRecipientError": "unknown_recipient",
        "WithheldTableError": "withheld_table",
    }


def test_a_bad_value_is_a_value_error_and_an_unknown_keyword_a_type_error() -> None:
    release = cf.load(path=FIXTURE_DIR)

    with pytest.raises(ValueError, match="flow takes") as bad_value:
        release.spending(flow="commitment")  # ty: ignore[invalid-argument-type]
    with pytest.raises(TypeError, match="no keyword 'provider'") as bad_keyword:
        release.spending(provider="Japan")

    assert isinstance(bad_value.value, cf.errors.InvalidInputError)
    assert isinstance(bad_keyword.value, cf.errors.InvalidInputError)
