"""Tests for the context-managed ``sdmx1`` client wrapper.

Avoids the network entirely by stubbing both ``sdmx.Client`` and the
``build_session`` factory so the test owns the session that
``_install_wb_host_rewrite`` patches.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests


@pytest.fixture
def fake_session_state(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace ``sdmx.Client`` and ``build_session`` with controllable stubs.

    Returns a dict with ``session`` (the requests.Session given to the
    sdmx_client) and ``send_calls`` (the URLs the wrapped ``send`` saw).
    """
    state: dict[str, Any] = {"send_calls": []}

    session = requests.Session()

    def _record(request: Any, **_: Any) -> None:
        state["send_calls"].append(request.url)
        return None

    session.send = _record  # type: ignore[method-assign]
    state["session"] = session

    fake_client = MagicMock()
    fake_client.session = MagicMock()  # ignored — sdmx_client overwrites it
    fake_client.session.cookies = requests.cookies.RequestsCookieJar()
    fake_client.session.headers = {}

    import sdmx

    from parsimony_sdmx.providers import sdmx_client as sdmx_client_module

    monkeypatch.setattr(sdmx, "Client", lambda **_kwargs: fake_client)
    monkeypatch.setattr(sdmx_client_module, "build_session", lambda _http=None: session)
    return state


def _make_request(url: str) -> Any:
    return requests.Request(method="GET", url=url).prepare()


class TestWbUrlRewrite:
    def test_rewrites_bad_wb_host_when_enabled(
        self, fake_session_state: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.providers.sdmx_client import sdmx_client

        with sdmx_client("WB_WDI", wb_url_rewrite=True) as client:
            client.session.send(_make_request("https://dataapi.worldbank.org/sdmx/data/WDI"))

        assert fake_session_state["send_calls"] == [
            "https://api.worldbank.org/sdmx/data/WDI"
        ]

    def test_default_does_not_rewrite(
        self, fake_session_state: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.providers.sdmx_client import sdmx_client

        with sdmx_client("WB_WDI") as client:
            client.session.send(_make_request("https://dataapi.worldbank.org/sdmx/data/WDI"))

        assert fake_session_state["send_calls"] == [
            "https://dataapi.worldbank.org/sdmx/data/WDI"
        ]

    def test_other_hosts_are_left_alone(
        self, fake_session_state: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.providers.sdmx_client import sdmx_client

        with sdmx_client("ECB", wb_url_rewrite=True) as client:
            client.session.send(
                _make_request("https://data-api.ecb.europa.eu/service/data/YC")
            )

        assert fake_session_state["send_calls"] == [
            "https://data-api.ecb.europa.eu/service/data/YC"
        ]

    def test_substring_match_does_not_trigger_rewrite(
        self, fake_session_state: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.providers.sdmx_client import sdmx_client

        with sdmx_client("WB_WDI", wb_url_rewrite=True) as client:
            client.session.send(
                _make_request("https://api.example.com/?u=dataapi.worldbank.org")
            )

        assert fake_session_state["send_calls"] == [
            "https://api.example.com/?u=dataapi.worldbank.org"
        ]
