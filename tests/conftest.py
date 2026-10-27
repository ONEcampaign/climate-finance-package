"""Test-wide rules: sockets closed beyond loopback outside `integration`, `slow` needs
CF_E2E_DATA, and a base install (CF_BASE_INSTALL=1) skips the modules that import an
optional extra.

Imports only the standard library and pytest, so it loads in a base-only install.
"""

import os
import re
import socket
from collections.abc import Callable, Generator
from functools import cache
from importlib.metadata import requires

import pytest

BASE_INSTALL = os.environ.get("CF_BASE_INSTALL") == "1"

# The e2e tests build a release from the archive, so they always need the build extra.
collect_ignore = ["e2e"] if BASE_INSTALL else []

# Import names that differ from the distribution name.
IMPORT_NAMES = {"scikit_learn": "sklearn", "pyyaml": "yaml"}
# The extras a base install leaves out. A module that needs `embed` still
# fails on import.
OPTIONAL_EXTRAS = ("build", "pandas")


@cache
def _extra_modules() -> frozenset[str]:
    """The import names of the distributions in the optional extras, read from the
    package's own metadata so a new dependency is covered without an edit here."""
    names = set()
    for requirement in requires("climate-finance") or []:
        extra = re.search(r"extra == '(\w+)'", requirement)
        if extra and extra[1] in OPTIONAL_EXTRAS:
            name = re.split(r"[<>=!~ ;\[]", requirement)[0]
            name = re.sub(r"[-.]+", "_", name).lower()
            names.add(IMPORT_NAMES.get(name, name))
    assert names, "climate-finance metadata lists no optional-extra dependency"
    return frozenset(names)


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(
    collector: pytest.Collector,
) -> Generator[None, pytest.CollectReport, pytest.CollectReport]:
    """On a base install, skip a module whose import fails for want of an optional
    extra. Any other collection failure still fails."""
    report = yield
    if not (BASE_INSTALL and report.failed and isinstance(collector, pytest.Module)):
        return report
    cause = report.call.excinfo.value.__cause__
    if isinstance(cause, ModuleNotFoundError) and cause.name:
        missing = cause.name.partition(".")[0]
        if missing in _extra_modules():
            reason = f"Skipped: needs the optional {missing!r} package"
            return pytest.CollectReport(
                collector.nodeid, "skipped", (str(collector.path), 0, reason), None
            )
    return report


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if os.environ.get("CF_E2E_DATA"):
        return
    skip = pytest.mark.skip(reason="skipped: no CF_E2E_DATA")
    for item in items:
        if item.get_closest_marker("slow"):
            item.add_marker(skip)


# Hosts a test may reach without the `integration` mark: a server the test runs itself.
LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})


@pytest.fixture(autouse=True)
def _block_sockets(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse outbound connections other than to loopback in every test not marked
    `integration`."""
    if request.node.get_closest_marker("integration"):
        return
    connect, connect_ex = socket.socket.connect, socket.socket.connect_ex

    def refuse(method: Callable[..., object]) -> Callable[..., object]:
        def guarded(self: socket.socket, address: object) -> object:
            if isinstance(address, tuple) and address[0] in LOOPBACK:
                return method(self, address)
            raise OSError("Network access in a test not marked integration")

        return guarded

    monkeypatch.setattr(socket.socket, "connect", refuse(connect))
    monkeypatch.setattr(socket.socket, "connect_ex", refuse(connect_ex))
