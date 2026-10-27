"""Test-wide rules: sockets closed beyond loopback outside `integration`, `slow` needs
CF_E2E_DATA.

Imports only the standard library and pytest, so it loads in a base-only install.
"""

import os
import socket
from collections.abc import Callable

import pytest


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
