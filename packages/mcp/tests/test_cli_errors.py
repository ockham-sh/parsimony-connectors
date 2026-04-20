"""Tests for the three-part error prose convention."""

from __future__ import annotations

from parsimony_mcp.cli._errors import (
    registry_dns_failure,
    registry_malformed,
    registry_schema_mismatch,
    registry_upstream_unreachable,
    uv_compiler_missing,
    uv_missing,
)


def _assert_three_parts(text: str) -> None:
    """Every UserError renders as 'error: ...' + DO NOT: + DO:."""
    assert text.startswith("error: ")
    assert "\n  DO NOT: " in text
    assert "\n  DO:     " in text


def test_registry_dns_failure_shape() -> None:
    text = registry_dns_failure("https://example.test/r.json").render()
    _assert_three_parts(text)
    assert "DNS" in text
    assert "offline" in text


def test_registry_upstream_shape_with_status() -> None:
    text = registry_upstream_unreachable("https://x/y", 502).render()
    _assert_three_parts(text)
    assert "HTTP 502" in text
    assert "--registry" in text  # caller-actionable command surfaced


def test_registry_upstream_without_status() -> None:
    text = registry_upstream_unreachable("https://x/y", None).render()
    _assert_three_parts(text)
    assert "HTTP" not in text.split("DO NOT")[0]  # no status in the 'what' clause


def test_registry_malformed_shape() -> None:
    text = registry_malformed("https://x/y", "missing schema_version").render()
    _assert_three_parts(text)
    assert "malformed" in text


def test_registry_schema_mismatch_shape() -> None:
    text = registry_schema_mismatch("https://x/y", client_version=1).render()
    _assert_three_parts(text)
    assert "v1" in text
    assert "upgrade" in text.lower()


def test_uv_missing_shape() -> None:
    text = uv_missing().render()
    _assert_three_parts(text)
    assert "--skip-install" in text


def test_uv_compiler_shape() -> None:
    text = uv_compiler_missing().render()
    _assert_three_parts(text)
    assert "C compiler" in text
