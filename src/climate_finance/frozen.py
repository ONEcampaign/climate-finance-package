"""`Frozen`: the base of `Spec`, `Method`, `MethodRange` and `Agency`, whose
refusals come as one `InvalidInputError` naming each field and the form it takes."""

from collections.abc import Iterable, Mapping
from difflib import get_close_matches
from typing import Any, ClassVar, Self, override

from pydantic import BaseModel, ConfigDict, ValidationError
from pydantic.config import ExtraValues

from climate_finance.errors import InvalidInputError


class Frozen(BaseModel):
    """A frozen model that rejects unknown fields and copies itself with changes.

    Building one, from keywords, a mapping or JSON, raises `InvalidInputError` for
    an unknown keyword, malformed JSON or a value a field does not take.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # What the refusal of an unknown keyword says in place of a close name, by
    # keyword, such as `Spec`'s note on `spec=`.
    _keyword_notes: ClassVar[Mapping[str, str]] = {}

    # The entry points restate pydantic's refusal once validation is over. Raised
    # inside a validator, `InvalidInputError`, a `ValueError`, would come back
    # wrapped in pydantic's error, and a nested model's refusal would end pydantic's
    # tries at a union such as `Method | MethodRange`. The error names a nested
    # field by its path, as `method.significant`.

    def __init__(self, **data: object) -> None:
        try:
            super().__init__(**data)
        except ValidationError as error:
            raise _restated(error, model=type(self)) from None

    # pydantic's mark for an `__init__` that validates as `BaseModel.__init__` does,
    # as `RootModel.__init__` carries. Without it, pydantic validates a nested model
    # through this `__init__`, which would restate inside the outer validation.
    __init__.__pydantic_base_init__ = True  # ty: ignore[unresolved-attribute]

    def replace(self, **changes: object) -> Self:
        """Return a copy with `changes` applied, validated again."""
        return self.model_validate({**self.model_dump(), **changes})

    @override
    @classmethod
    def model_validate(
        cls,
        obj: Any,
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        from_attributes: bool | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Read a model from a mapping of its fields.

        Raises:
            InvalidInputError: for a field the model refuses.
        """
        try:
            return super().model_validate(
                obj,
                strict=strict,
                extra=extra,
                from_attributes=from_attributes,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as error:
            raise _restated(error, model=cls) from None

    @override
    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Read a model from JSON, such as a saved `result.spec.model_dump_json()`.

        Raises:
            InvalidInputError: for malformed JSON, or a field the model refuses.
        """
        try:
            return super().model_validate_json(
                json_data,
                strict=strict,
                extra=extra,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as error:
            raise _restated(error, model=cls) from None


def _restated(error: ValidationError, *, model: type[Frozen]) -> InvalidInputError:
    """One error naming each field `error` refuses and the form it takes."""
    problems = []
    for detail in error.errors(include_url=False):
        # A capitalised part tags a union member, as `Method` in `method.Method.eu`.
        # Field names are lower case.
        field = ".".join(
            str(part) for part in detail["loc"] if not str(part)[:1].isupper()
        )
        field = field or model.__name__
        context = detail.get("ctx", {})
        if detail["type"] == "extra_forbidden" and len(detail["loc"]) == 1:
            problems.append(
                unknown_keyword(
                    field,
                    owner=model.__name__,
                    choices=model.model_fields,
                    note=model._keyword_notes.get(field),
                )
            )
        elif detail["type"] == "json_invalid":
            problems.append(f"{model.__name__} reads JSON; {detail['msg']}")
        elif detail["type"] == "literal_error":
            problems.append(
                f"{field} takes {context['expected']}; got {detail['input']!r}"
            )
        elif "error" in context:  # a validator's own ValueError, already worded
            # A package error's `str()` ends in its code, which the restated
            # error adds once.
            problems.append(str(context["error"].args[0]))
        else:
            message = detail["msg"]
            problems.append(
                f"{field}: {message[0].lower()}{message[1:]}; got {detail['input']!r}"
            )
    return InvalidInputError("; ".join(problems))


def unknown_keyword(
    name: str, *, owner: str, choices: Iterable[str], note: str | None = None
) -> str:
    """The refusal of keyword `name` by `owner`, which takes `choices`: `note`, a
    close name, or every choice."""
    choices = list(choices)
    if note is None:
        close = get_close_matches(name, choices, n=1)
        note = (
            f"did you mean {close[0]!r}?" if close else f"it takes {', '.join(choices)}"
        )
    return f"{owner} takes no keyword {name!r}; {note}"
