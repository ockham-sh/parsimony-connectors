"""Tests for the registry fetch/cache/fallback layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from parsimony_mcp.cli.registry import (
    CACHE_TTL_SECONDS,
    DEFAULT_REGISTRY_URL,
    RegistryError,
    RegistrySource,
    fetch_registry,
)


@pytest.fixture(autouse=True)
def _bypass_real_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve any hostname to a public-looking address by default.

    The SSRF allow-list calls ``socket.getaddrinfo``; without this
    fixture every test that uses ``example.test`` or similar
    unresolvable hostnames would trip a gaierror before the mocked
    transport runs. Tests that want to exercise the SSRF rejection
    re-monkeypatch ``getaddrinfo`` themselves.
    """
    monkeypatch.setattr(
        "parsimony_mcp.cli.registry.socket.getaddrinfo",
        lambda host, port, *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 0))],
    )


# ---------------------------------------------------------------------- helpers


def _valid_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "connectors": [
            {
                "package": "parsimony-fred",
                "display": "FRED",
                "summary": "Federal Reserve Economic Data",
                "homepage": "https://fred.stlouisfed.org",
                "pricing": "free",
                "rate_limits": "120 req/min",
                "tags": ["macro", "tool"],
                "env_vars": [
                    {"name": "FRED_API_KEY", "get_url": None, "required": True}
                ],
            }
        ],
    }


def _client_with_response(
    *, status: int = 200, body: bytes | None = None, captured: list[httpx.Request] | None = None
) -> httpx.Client:
    """Return an ``httpx.Client`` backed by ``MockTransport``."""
    payload = body if body is not None else json.dumps(_valid_payload()).encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured.append(request)
        return httpx.Response(status, content=payload)

    return httpx.Client(transport=httpx.MockTransport(handler))


def _client_raising(exc: Exception) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    return httpx.Client(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------- fresh-cache path


def test_fresh_cache_returns_without_network(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"
    cache.write_text(json.dumps(_valid_payload()))
    # mtime = now → within TTL

    registry, source = fetch_registry(
        url=DEFAULT_REGISTRY_URL,
        cache_path=cache,
        client=_client_raising(AssertionError("network must not be hit")),
    )

    assert source.origin == "cache-fresh"
    assert registry.connectors[0].package == "parsimony-fred"


def test_stale_cache_triggers_refetch(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"
    cache.write_text(json.dumps(_valid_payload()))

    captured: list[httpx.Request] = []
    stale_age = CACHE_TTL_SECONDS + 1.0
    registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=cache,
        client=_client_with_response(captured=captured),
        now=cache.stat().st_mtime + stale_age,
    )

    assert source.origin == "network"
    assert registry.connectors[0].package == "parsimony-fred"
    assert len(captured) == 1


# ---------------------------------------------------------------------- network + write-through


def test_network_fetch_writes_cache_atomically(tmp_path: Path) -> None:
    cache = tmp_path / "cache" / "registry.json"  # subdir doesn't exist yet

    _registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=cache,
        client=_client_with_response(),
    )

    assert source.origin == "network"
    assert source.cache_path == cache
    assert cache.is_file()
    on_disk = json.loads(cache.read_text())
    assert on_disk["schema_version"] == 1


def test_cache_file_mode_is_0600(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"
    fetch_registry(
        url="https://example.test/registry.json",
        cache_path=cache,
        client=_client_with_response(),
    )
    mode = cache.stat().st_mode & 0o777
    assert mode == 0o600


# ---------------------------------------------------------------------- fall-through on fetch failure


def test_stale_cache_returned_on_fetch_failure(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"
    cache.write_text(json.dumps(_valid_payload()))

    registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=cache,
        client=_client_raising(httpx.ConnectError("offline")),
        now=cache.stat().st_mtime + CACHE_TTL_SECONDS + 10,
    )

    assert source.origin == "cache-stale"
    assert registry.connectors[0].package == "parsimony-fred"


def test_no_cache_plus_fetch_failure_raises(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"  # does not exist

    with pytest.raises(RegistryError, match="network error"):
        fetch_registry(
            url="https://example.test/registry.json",
            cache_path=cache,
            client=_client_raising(httpx.ConnectError("offline")),
        )


# ---------------------------------------------------------------------- retry semantics


def test_single_retry_on_transient_5xx(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, content=b"busy")
        return httpx.Response(200, content=json.dumps(_valid_payload()).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=None,  # no cache path — forces single-source fetch
        client=client,
    )
    assert source.origin == "network"
    assert calls["n"] == 2
    assert registry.schema_version == 1


def test_4xx_does_not_retry(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, content=b"not found")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(RegistryError, match="HTTP 404"):
        fetch_registry(
            url="https://example.test/registry.json",
            cache_path=None,
            client=client,
        )
    assert calls["n"] == 1


# ---------------------------------------------------------------------- schema validation


def test_future_schema_version_surfaces_actionable_error(tmp_path: Path) -> None:
    body = json.dumps({"schema_version": 2, "connectors": []}).encode()
    with pytest.raises(RegistryError, match="incompatible schema version"):
        fetch_registry(
            url="https://example.test/registry.json",
            cache_path=None,
            client=_client_with_response(body=body),
        )


def test_malformed_json_surfaces_actionable_error(tmp_path: Path) -> None:
    with pytest.raises(RegistryError, match="schema validation"):
        fetch_registry(
            url="https://example.test/registry.json",
            cache_path=None,
            client=_client_with_response(body=b"{not json"),
        )


def test_corrupt_cache_ignored_not_raised(tmp_path: Path) -> None:
    cache = tmp_path / "registry.json"
    cache.write_text("garbage")

    registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=cache,
        client=_client_with_response(),
    )
    assert source.origin == "network"
    assert registry.connectors[0].package == "parsimony-fred"


# ---------------------------------------------------------------------- SSRF allow-list


@pytest.mark.parametrize(
    "bad_url, match",
    [
        ("http://example.com/registry.json", "https://"),
        ("file:///etc/passwd", "https://"),
        ("https://user:pass@example.com/r.json", "userinfo"),
    ],
)
def test_url_scheme_and_userinfo_rejected(bad_url: str, match: str, tmp_path: Path) -> None:
    with pytest.raises(RegistryError, match=match):
        fetch_registry(
            url=bad_url,
            cache_path=None,
            client=_client_with_response(),
        )


def test_private_ip_host_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Simulate a host resolving to 169.254.169.254 (cloud metadata).
    monkeypatch.setattr(
        "parsimony_mcp.cli.registry.socket.getaddrinfo",
        lambda host, port: [(None, None, None, None, ("169.254.169.254", 0))],
    )
    with pytest.raises(RegistryError, match="non-global address"):
        fetch_registry(
            url="https://attacker.test/registry.json",
            cache_path=None,
            client=_client_with_response(),
        )


# ---------------------------------------------------------------------- custom URL does not touch default cache


def test_custom_url_does_not_write_default_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Redirect DEFAULT_CACHE_PATH to a tmpdir location; the function
    # is called with cache_path=None, so even if something is wrong
    # the default cache file must not appear.
    fake_default = tmp_path / "default-cache.json"
    monkeypatch.setattr("parsimony_mcp.cli.registry.DEFAULT_CACHE_PATH", fake_default)

    fetch_registry(
        url="https://example.test/registry.json",
        cache_path=None,
        client=_client_with_response(),
    )
    assert not fake_default.exists()


# ---------------------------------------------------------------------- dataclass sanity


def test_source_is_frozen() -> None:
    src = RegistrySource(origin="network", url="https://x", cache_path=None, cache_age_seconds=None)
    with pytest.raises((AttributeError, Exception)):
        src.origin = "other"  # type: ignore[misc]
