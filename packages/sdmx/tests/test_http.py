import socket
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.http import (
    DEFAULT_USER_AGENT,
    RETRY_STATUS_FORCELIST,
    HttpConfig,
    _keepalive_socket_options,
    bounded_get,
    build_session,
)


class TestHttpConfig:
    def test_defaults(self) -> None:
        c = HttpConfig()
        assert c.connect_timeout == 10.0
        assert c.read_timeout == 120.0
        assert c.max_retries == 3
        assert c.timeout == (10.0, 120.0)

    def test_frozen(self) -> None:
        from dataclasses import FrozenInstanceError

        c = HttpConfig()
        with pytest.raises(FrozenInstanceError):
            c.max_retries = 5  # type: ignore[misc]


class TestBuildSession:
    def test_mounts_https_adapter(self) -> None:
        s = build_session()
        adapter = s.get_adapter("https://example.com/")
        assert isinstance(adapter, HTTPAdapter)

    def test_user_agent_set(self) -> None:
        s = build_session()
        assert s.headers["User-Agent"] == DEFAULT_USER_AGENT

    def test_custom_user_agent(self) -> None:
        s = build_session(HttpConfig(user_agent="custom/1.0"))
        assert s.headers["User-Agent"] == "custom/1.0"

    def test_retry_configured_with_forcelist(self) -> None:
        s = build_session()
        adapter = s.get_adapter("https://example.com/")
        assert isinstance(adapter, HTTPAdapter)
        retry = adapter.max_retries
        assert isinstance(retry, Retry)
        # urllib3 Retry stores status_forcelist as a list/set/tuple
        assert tuple(retry.status_forcelist or ()) == RETRY_STATUS_FORCELIST
        assert retry.total == 3
        assert retry.backoff_factor == 1.0

    def test_retry_honours_retry_after(self) -> None:
        adapter = build_session().get_adapter("https://example.com/")
        assert isinstance(adapter, HTTPAdapter)
        retry = adapter.max_retries
        assert isinstance(retry, Retry)
        assert retry.respect_retry_after_header is True

    def test_retry_limited_to_safe_methods(self) -> None:
        adapter = build_session().get_adapter("https://example.com/")
        assert isinstance(adapter, HTTPAdapter)
        retry = adapter.max_retries
        assert isinstance(retry, Retry)
        assert retry.allowed_methods is not None
        assert "GET" in retry.allowed_methods
        assert "POST" not in retry.allowed_methods

    def test_pool_sized_from_config(self) -> None:
        s = build_session(HttpConfig(pool_connections=4, pool_maxsize=4))
        adapter = s.get_adapter("https://example.com/")
        assert isinstance(adapter, HTTPAdapter)
        # HTTPAdapter stores these on _pool_connections / _pool_maxsize
        assert adapter._pool_connections == 4  # type: ignore[attr-defined]
        assert adapter._pool_maxsize == 4  # type: ignore[attr-defined]


class TestKeepaliveSocketOptions:
    """Regression guard for #44: the option list is built by probing for
    platform constants, so import must not raise on macOS/Windows where
    TCP_KEEPIDLE (and friends) are absent. These tests reproduce the non-Linux
    paths on the Linux CI box by stripping the constants with monkeypatch.
    """

    def test_always_starts_with_so_keepalive(self) -> None:
        opts = _keepalive_socket_options()
        assert opts[0] == (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def test_no_none_option_codes(self) -> None:
        # A None opt-code would mean getattr fell through into the tuple —
        # setsockopt would raise. Every appended code must be a real int.
        for _level, opt, _val in _keepalive_socket_options():
            assert isinstance(opt, int)

    def test_linux_has_full_probe_tuning(self) -> None:
        # On the Linux CI host all four knobs are present.
        if not hasattr(socket, "TCP_KEEPIDLE"):
            pytest.skip("TCP_KEEPIDLE not available on this platform")
        opts = _keepalive_socket_options()
        codes = {opt for _level, opt, _val in opts}
        assert socket.TCP_KEEPIDLE in codes
        assert socket.TCP_KEEPINTVL in codes
        assert socket.TCP_KEEPCNT in codes

    def test_macos_falls_back_to_tcp_keepalive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # macOS has no TCP_KEEPIDLE but exposes TCP_KEEPALIVE for the idle timer.
        monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
        monkeypatch.setattr(socket, "TCP_KEEPALIVE", 0x10, raising=False)
        opts = _keepalive_socket_options()
        codes = {opt for _level, opt, _val in opts}
        assert 0x10 in codes  # the fake TCP_KEEPALIVE idle knob was used
        assert opts[0] == (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def test_windows_degrades_to_plain_keepalive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Windows exposes none of the per-probe setsockopt knobs; only
        # SO_KEEPALIVE should remain and import must not raise.
        for name in ("TCP_KEEPIDLE", "TCP_KEEPALIVE", "TCP_KEEPINTVL", "TCP_KEEPCNT"):
            monkeypatch.delattr(socket, name, raising=False)
        opts = _keepalive_socket_options()
        assert opts == [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]


class TestBoundedGet:
    def _mock_response(self, body: bytes, status: int = 200) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = status

        def iter_content(chunk_size: int = 1024) -> Iterator[bytes]:
            for i in range(0, len(body), chunk_size):
                yield body[i : i + chunk_size]

        resp.iter_content = iter_content
        if status >= 400:
            resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_http_url_rejected(self) -> None:
        s = build_session()
        with pytest.raises(SdmxFetchError, match="Non-HTTPS"):
            bounded_get(s, "http://example.com/data")

    def test_ftp_url_rejected(self) -> None:
        s = build_session()
        with pytest.raises(SdmxFetchError, match="Non-HTTPS"):
            bounded_get(s, "ftp://example.com/data")

    def test_happy_path_returns_bytes(self) -> None:
        s = build_session()
        body = b"hello" * 1000
        resp = self._mock_response(body)
        with patch.object(s, "get", return_value=resp) as mock_get:
            out = bounded_get(s, "https://example.com/data")
        assert out == body
        # Verify the get was called with streaming + timeout tuple
        _, kwargs = mock_get.call_args
        assert kwargs["stream"] is True
        assert kwargs["timeout"] == (10.0, 120.0)

    def test_custom_timeouts_passed(self) -> None:
        s = build_session()
        resp = self._mock_response(b"ok")
        cfg = HttpConfig(connect_timeout=5.0, read_timeout=30.0)
        with patch.object(s, "get", return_value=resp) as mock_get:
            bounded_get(s, "https://example.com/data", config=cfg)
        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == (5.0, 30.0)

    def test_byte_cap_aborts_stream(self) -> None:
        s = build_session()
        huge_body = b"x" * (2 * 1024 * 1024)  # 2 MiB
        resp = self._mock_response(huge_body)
        cfg = HttpConfig(max_response_bytes=1024)
        with patch.object(s, "get", return_value=resp), pytest.raises(SdmxFetchError, match="exceeded"):
            bounded_get(s, "https://example.com/data", config=cfg)

    def test_4xx_raises_fetch_error(self) -> None:
        s = build_session()
        resp = self._mock_response(b"", status=404)
        with patch.object(s, "get", return_value=resp), pytest.raises(requests.exceptions.HTTPError):
            bounded_get(s, "https://example.com/data")

    def test_request_exception_wrapped(self) -> None:
        s = build_session()
        with (
            patch.object(s, "get", side_effect=requests.exceptions.ConnectionError("no route")),
            pytest.raises(SdmxFetchError, match="GET"),
        ):
            bounded_get(s, "https://example.com/data")

    def test_extra_headers_forwarded(self) -> None:
        s = build_session()
        resp = self._mock_response(b"ok")
        with patch.object(s, "get", return_value=resp) as mock_get:
            bounded_get(
                s,
                "https://example.com/data",
                extra_headers={"Accept": "application/xml"},
            )
        _, kwargs = mock_get.call_args
        assert kwargs["headers"] == {"Accept": "application/xml"}

    def test_response_closed(self) -> None:
        s = build_session()
        resp = self._mock_response(b"ok")
        with patch.object(s, "get", return_value=resp):
            bounded_get(s, "https://example.com/data")
        resp.close.assert_called_once()
