"""Errors raised by the analysis layer and shared with the build."""

from collections.abc import Mapping, Sequence

# The suggestions a message shows before it trails off.
_SHOWN = 5


class ClimateFinanceError(Exception):
    """Base class for every climate_finance error."""


class SourceUnavailableError(ClimateFinanceError):
    """A source file or service could not be read."""


class InvalidInputError(ClimateFinanceError, TypeError):
    """An argument in a form or of a value the call does not take, such as
    `years="2022"`, an unknown keyword, or an agency where a call reads providers.
    The message names the form the call takes. It is a `TypeError`, as Python's
    refusal of an unexpected keyword is, so pydantic validators pass it through."""


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


class MissingLabelsError(ClimateFinanceError):
    """A method reads labels the release does not hold."""


class SchemaError(ClimateFinanceError):
    """A table does not match its schema. `problems` maps each bad column to why."""

    def __init__(self, table: str, problems: Mapping[str, str]) -> None:
        self.table = table
        self.problems = dict(problems)
        detail = "; ".join(f"{column}: {why}" for column, why in self.problems.items())
        super().__init__(f"Table {table!r} fails its schema: {detail}")


class CoverageWarning(UserWarning):
    """A query asks for years its release tables hold no rows for. Silence it with
    `warnings.filterwarnings("ignore", category=cf.errors.CoverageWarning)`."""
