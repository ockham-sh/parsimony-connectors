"""Self-tests for :class:`CredentialDeclarationSuite` against toy connectors.

Three well-declared toys (keyed, keyless, optional-key) prove correct
declarations pass every applicable check; three mis-declared toys prove
each check fails loudly when declaration and runtime disagree.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import pytest
from parsimony.connector import connector
from parsimony.errors import UnauthorizedError
from parsimony_test_support.suites import CredentialDeclarationSuite

TOY_URL = "https://toy.example/data"


def _get_json(params: dict[str, str], headers: dict[str, str] | None = None) -> Any:
    return httpx.get(TOY_URL, params=params, headers=headers).json()


# ---------------------------------------------------------------------------
# Well-declared toys
# ---------------------------------------------------------------------------


@connector(secrets=("api_key",), requires=("TOY_API_KEY",))
def toy_keyed(series: str, api_key: str = "") -> Any:
    """Keyed toy: arg → env fallback, fast-fails when missing, key as query param."""
    key = api_key or os.environ.get("TOY_API_KEY", "")
    if not key:
        raise UnauthorizedError("toy", env_var="TOY_API_KEY")
    return _get_json({"series": series, "api_key": key})


@connector
def toy_keyless(path: str) -> Any:
    """Keyless toy: always calls."""
    return _get_json({"path": path})


@connector(secrets=("api_key",))
def toy_optional_key(series: str, api_key: str = "") -> Any:
    """Optional-key toy: bound key sent as a header when present."""
    headers = {"X-Api-Key": api_key} if api_key else None
    return _get_json({"series": series}, headers=headers)


class TestToyKeyed(CredentialDeclarationSuite):
    connector = toy_keyed
    call_kwargs = {"series": "gdp"}
    route_url = TOY_URL


class TestToyKeyless(CredentialDeclarationSuite):
    connector = toy_keyless
    call_kwargs = {"path": "events"}
    route_url = TOY_URL


class TestToyOptionalKey(CredentialDeclarationSuite):
    connector = toy_optional_key
    call_kwargs = {"series": "gdp"}
    route_url = TOY_URL


# ---------------------------------------------------------------------------
# Mis-declared toys — each must make the matching check fail loudly
# ---------------------------------------------------------------------------


@connector(requires=("TOY_NEVER_CHECKED",))
def toy_never_fast_fails(series: str) -> Any:
    """Mis-declared toy: declares a requirement but never fast-fails."""
    return _get_json({"series": series})


@connector
def toy_undeclared_fast_fail(series: str) -> Any:
    """Mis-declared toy: fast-fails while declaring requires=()."""
    raise UnauthorizedError("toy", env_var="TOY_SECRET")


@connector(requires=("TOY_TYPO_KEY",))
def toy_typo_declaration(series: str) -> Any:
    """Mis-declared toy: fast-fails naming a different env var than declared."""
    if not os.environ.get("TOY_API_KEY"):
        raise UnauthorizedError("toy", env_var="TOY_API_KEY")
    return _get_json({"series": series})


class _NeverFastFailsSuite(CredentialDeclarationSuite):
    connector = toy_never_fast_fails
    call_kwargs = {"series": "x"}
    route_url = TOY_URL


class _UndeclaredFastFailSuite(CredentialDeclarationSuite):
    connector = toy_undeclared_fast_fail
    call_kwargs = {"series": "x"}
    route_url = TOY_URL


class _TypoDeclarationSuite(CredentialDeclarationSuite):
    connector = toy_typo_declaration
    call_kwargs = {"series": "x"}
    route_url = TOY_URL


def test_missing_fast_fail_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(pytest.fail.Exception):
        _NeverFastFailsSuite().test_declared_requirement_fast_fails(monkeypatch)


def test_undeclared_fast_fail_is_caught() -> None:
    with pytest.raises(pytest.fail.Exception):
        _UndeclaredFastFailSuite().test_undeclared_does_not_fast_fail()


def test_typo_declaration_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TOY_API_KEY", raising=False)
    with pytest.raises(AssertionError):
        _TypoDeclarationSuite().test_declared_requirement_fast_fails(monkeypatch)
