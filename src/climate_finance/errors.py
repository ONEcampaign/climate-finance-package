"""Errors and warnings raised by the analysis layer and shared with the build."""

import re
from collections.abc import Mapping, Sequence
from typing import ClassVar, Self, override

# The suggestions a message shows before it trails off.
_SHOWN = 5
# The boundaries between the words of a class name, as in `UnknownName` and `CRDFRow`.
_WORD_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


class ClimateFinanceError(Exception):
    """Base class for every climate_finance error.

    `code` is the class's stable, machine-readable name, which handlers and tests
    match. It is the class name in snake case without its `Error` suffix, such as
    `unknown_provider` for `UnknownProviderError`. The message ends with it in
    brackets, as `[unknown_provider]`, and may change between versions where the code
    does not. Every subclass pickles with its attributes, so an error raised in a
    worker process reaches the parent intact.
    """

    code: ClassVar[str] = "climate_finance"

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if "code" not in cls.__dict__:
            cls.code = _WORD_BOUNDARY.sub(
                "_", cls.__name__.removesuffix("Error")
            ).lower()

    @override
    def __str__(self) -> str:
        message = super().__str__()
        return f"{message} [{self.code}]" if message else f"[{self.code}]"

    @override
    def __reduce__(
        self,
    ) -> tuple[object, tuple[type[Self], tuple[object, ...]], dict[str, object]]:
        # Unpickling calls the class with `args` by default, which fails for a class
        # whose constructor takes keywords. Rebuild without the constructor and
        # restore the attributes, `__notes__` included.
        return _rebuild, (type(self), self.args), self.__dict__


def _rebuild[E: ClimateFinanceError](cls: type[E], args: tuple[object, ...]) -> E:
    """An instance of `cls` holding `args`, before its attributes are restored."""
    return cls.__new__(cls, *args)


class SourceUnavailableError(ClimateFinanceError):
    """A source file or service could not be read."""


class InvalidInputError(ClimateFinanceError, ValueError, TypeError):
    """An argument in a form or of a value the call does not take, such as
    `years="2022"`, an unknown keyword, or an agency where a call reads providers.
    The message names the form the call takes. It is a `ValueError`, as Python's
    refusal of a bad value is, and a `TypeError`, as its refusal of an unexpected
    keyword is."""


class UnknownNameError(ClimateFinanceError, LookupError):
    """A name is not in its listing, or names several entries. `suggestions` holds
    close names or, for a name of several entries, every entry it names. `note`,
    when given, replaces the message's "Unknown <kind>" opening."""

    def __init__(
        self,
        name: str,
        *,
        kind: str,
        suggestions: Sequence[str] = (),
        note: str | None = None,
    ) -> None:
        self.name = name
        self.kind = kind
        self.suggestions = tuple(suggestions)
        self.note = note
        shown = [*self.suggestions[:_SHOWN]]
        if len(self.suggestions) > _SHOWN:
            shown.append("...")
        hint = f" Did you mean: {', '.join(shown)}?" if shown else ""
        super().__init__(f"{note or f'Unknown {kind} {name!r}.'}{hint}")


class UnknownProviderError(UnknownNameError):
    """A provider name is not in `cf.providers`, or names several providers or
    agencies."""

    def __init__(
        self, name: str, *, suggestions: Sequence[str] = (), note: str | None = None
    ) -> None:
        super().__init__(name, kind="provider", suggestions=suggestions, note=note)


class UnknownRecipientError(UnknownNameError):
    """A recipient name is not in `cf.recipients`, or names several recipients."""

    def __init__(
        self, name: str, *, suggestions: Sequence[str] = (), note: str | None = None
    ) -> None:
        super().__init__(name, kind="recipient", suggestions=suggestions, note=note)


class ReleaseIntegrityError(ClimateFinanceError):
    """A release table is missing, its sha256 differs from release.json, or it
    breaks a rule the query relies on, such as one coefficient per year."""


class WithheldTableError(ClimateFinanceError, LookupError):
    """A release table the published release leaves out. `note` says how to build
    it locally."""

    def __init__(self, table: str, *, note: str) -> None:
        self.table = table
        self.note = note
        super().__init__(f"The release leaves out {table!r}. {note}")


class ReleaseFormatError(ClimateFinanceError):
    """A release's `release.json` records a data format newer than `supported`,
    the newest this version of climate-finance reads."""

    def __init__(self, release: str, *, data_format: int, supported: int) -> None:
        self.release = release
        self.data_format = data_format
        self.supported = supported
        super().__init__(
            f"Release {release!r} has data format {data_format}, and this version "
            f"of climate-finance reads data formats up to {supported}. Upgrade "
            f"climate-finance to a version that reads data format {data_format}: "
            "pip install -U climate-finance"
        )


class FormatTableError(WithheldTableError):
    """A release table the release's data format, `data_format`, predates."""

    def __init__(self, table: str, *, data_format: int) -> None:
        self.data_format = data_format
        super().__init__(
            table,
            note=(
                f"Its data format, {data_format}, predates the table. Load a "
                "release of a later data format to read it."
            ),
        )


class MissingLabelsError(ClimateFinanceError):
    """A method reads labels the release does not hold."""


class SchemaError(ClimateFinanceError):
    """A table does not match its schema. `problems` maps each bad column to why."""

    def __init__(self, table: str, problems: Mapping[str, str]) -> None:
        self.table = table
        self.problems = dict(problems)
        detail = "; ".join(f"{column}: {why}" for column, why in self.problems.items())
        super().__init__(f"Table {table!r} fails its schema: {detail}")


class RemovedError(ClimateFinanceError):
    """A 1.x call, choice or module that 2.0 removed, or whose meaning 2.0 changed.
    `removed` names it, and `replacement` says what to use in 2.0 or that nothing
    replaces it."""

    def __init__(self, removed: str, *, replacement: str) -> None:
        self.removed = removed
        self.replacement = replacement
        super().__init__(f"{removed} is gone in climate-finance 2.0; {replacement}")


class RemovedModuleError(RemovedError, ModuleNotFoundError):
    """A 1.x module that 2.0 removed, such as `climate_finance.unfccc`. It is a
    `ModuleNotFoundError`, so an `except ImportError` guard still catches it. Its
    `name` is None, because a module name there would let
    `from climate_finance import unfccc` swallow the error and raise a bare
    `ImportError`."""


class ClimateFinanceWarning(UserWarning):
    """Base class for every climate_finance warning. Turn them all into errors with
    `warnings.filterwarnings("error", category=cf.errors.ClimateFinanceWarning)`."""


class CoverageWarning(ClimateFinanceWarning):
    """A query asks for years its release tables hold no rows for, or names a
    provider that has no rows the query counts. Silence it with
    `warnings.filterwarnings("ignore", category=cf.errors.CoverageWarning)`."""


class DoubleCountWarning(ClimateFinanceWarning):
    """A result whose figures count some money twice when added to the results of
    separate queries, as EU Institutions' provider-view figures do. Silence it
    with `warnings.filterwarnings("ignore", category=cf.errors.DoubleCountWarning)`.
    """


class ClimateFinanceDeprecationWarning(ClimateFinanceWarning, DeprecationWarning):
    """A call deprecated in version `since` that goes in `expected_removal`. Python
    shows it for calls in `__main__`, such as a script or a notebook cell, and
    hides it elsewhere, as it does any `DeprecationWarning`."""

    since: ClassVar[str] = "2.0"
    expected_removal: ClassVar[str] = "3.0"
