"""Test-wide rules: sockets closed outside `integration`, `slow` needs CF_E2E_DATA.

Imports only the standard library and pytest, so it loads in a base-only install.
"""

import os
import socket

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if os.environ.get("CF_E2E_DATA"):
        return
    skip = pytest.mark.skip(reason="skipped: no CF_E2E_DATA")
    for item in items:
        if item.get_closest_marker("slow"):
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _block_sockets(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse outbound connections in every test not marked `integration`."""
    if request.node.get_closest_marker("integration"):
        return

    def refuse(*args: object, **kwargs: object) -> None:
        raise OSError("Network access in a test not marked integration")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
