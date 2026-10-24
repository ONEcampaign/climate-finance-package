"""The build settings each stage reads from `StageContext.config` while it runs."""

from collections.abc import Callable, Iterator, Sequence
from dataclasses import replace

import pytest
from pydantic import BaseModel

from climate_finance.build.config import BuildConfig, _Section
from climate_finance.build.provenance import Provenance
from climate_finance.build.stages import StageDef


def record_reads(
    monkeypatch: pytest.MonkeyPatch, config: BuildConfig, registry: Sequence[StageDef]
) -> tuple[tuple[StageDef, ...], dict[str, set[str]]]:
    """`registry` with each stage recording, by stage name, the dotted path of every
    setting of `config` that its function or seal reads.

    A setting below one the stage declares is recorded as the declared one. A read
    of a model method or of `__dict__` reads every setting below the model.
    Reads outside a stage's function and seal, and the provenance checks of the
    stages it reads, go unrecorded.
    """
    prefixes: dict[int, str] = {}
    _walk(config, "", prefixes)
    reads: dict[str, set[str]] = {stage.name: set() for stage in registry}
    declared = {stage.name: stage.settings for stage in registry}
    # The stage running, or None inside a provenance check.
    running: list[str | None] = []
    attribute = object.__getattribute__

    def getattribute(model: BaseModel, name: str) -> object:
        value = attribute(model, name)
        prefix = prefixes.get(id(model))
        if not running or (stage := running[-1]) is None or prefix is None:
            return value
        if name in type(model).model_fields:
            read = [] if isinstance(value, BaseModel) else [prefix + name]
        elif name == "__dict__" or not name.startswith("_"):
            read = list(_leaves(model, prefix))
        else:
            read = []
        reads[stage].update(_covering(path, declared[stage]) for path in read)
        return value

    def tracked[**P, R](name: str | None, call: Callable[P, R]) -> Callable[P, R]:
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            running.append(name)
            try:
                return call(*args, **kwargs)
            finally:
                running.pop()

        return run

    monkeypatch.setattr(_Section, "__getattribute__", getattribute)
    monkeypatch.setattr(Provenance, "read_stage", tracked(None, Provenance.read_stage))
    wrapped = tuple(
        replace(
            stage,
            run=tracked(stage.name, stage.run),
            seal=None if stage.seal is None else tracked(stage.name, stage.seal),
        )
        for stage in registry
    )
    return wrapped, reads


def _covering(path: str, settings: Sequence[str]) -> str:
    """The setting of `settings` that `path` is or sits below, else `path`."""
    return next(
        (
            setting
            for setting in settings
            if path == setting or path.startswith(f"{setting}.")
        ),
        path,
    )


def _walk(model: BaseModel, prefix: str, prefixes: dict[int, str]) -> None:
    prefixes[id(model)] = prefix
    for name in type(model).model_fields:
        if isinstance(value := getattr(model, name), BaseModel):
            _walk(value, f"{prefix}{name}.", prefixes)


def _leaves(model: BaseModel, prefix: str) -> Iterator[str]:
    for name in type(model).model_fields:
        value = object.__getattribute__(model, name)
        if isinstance(value, BaseModel):
            yield from _leaves(value, f"{prefix}{name}.")
        else:
            yield prefix + name
