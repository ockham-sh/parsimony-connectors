"""Support utilities for live-API integration tests.

Integration tests are marked ``@pytest.mark.integration`` and are skipped
by default (the root ``pyproject.toml`` passes ``-m 'not integration'``).
Run them explicitly with::

    uv run pytest -m integration

Or to target one package::

    uv run pytest packages/fred -m integration

Credentials come from:

1. Already-set environment variables (CI provides these via GitHub Secrets).
2. A ``.env`` file located via ``_resolve_env_path()`` — defaults to
   ``../terminal/.env`` (local dev convenience for this workspace).

The loader never overrides an already-set env var — CI > .env.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

_TERMINAL_ENV_RELATIVE = Path("..") / "terminal" / ".env"


def _resolve_env_path() -> Path | None:
    """Find a usable ``.env`` file for integration tests.

    Honours ``PARSIMONY_TEST_ENV_FILE`` if set (absolute or relative to CWD).
    Otherwise walks upward from CWD looking for ``terminal/.env`` (the
    workspace convention), then ``.env``. Returns ``None`` if nothing
    suitable is found — the loader is then a no-op and individual tests
    fall back to whatever is already in ``os.environ``.
    """
    override = os.environ.get("PARSIMONY_TEST_ENV_FILE")
    if override:
        p = Path(override).expanduser()
        return p if p.is_file() else None

    # Walk up the tree: CWD, parent, grandparent, ... up to filesystem root.
    # Look for sibling `terminal/.env` first (workspace convention), then
    # a local `.env`.
    cwd = Path.cwd().resolve()
    for directory in [cwd, *cwd.parents]:
        candidate = directory / "terminal" / ".env"
        if candidate.is_file():
            return candidate
        candidate = directory / ".env"
        if candidate.is_file() and directory != cwd:
            # Only return a `.env` from a parent, not CWD (too easy to
            # grab a stray project env by accident).
            return candidate
    return None


def load_integration_env() -> dict[str, str]:
    """Load integration-test env vars from ``.env`` without clobbering os.environ.

    Returns the mapping of values found in the ``.env``. Existing
    ``os.environ`` entries take precedence (``override=False``).
    """
    try:
        from dotenv import dotenv_values, load_dotenv
    except ImportError as exc:
        raise RuntimeError(
            "parsimony-test-support requires python-dotenv for integration tests"
        ) from exc

    env_path = _resolve_env_path()
    if env_path is None:
        return {}

    load_dotenv(env_path, override=False)
    return dict(dotenv_values(env_path))


def require_env(*var_names: str) -> dict[str, str]:
    """Return a dict of the named env vars, or skip the test if any is missing.

    Usage::

        creds = require_env("FRED_API_KEY")
        conn = fred_fetch.bind(api_key=creds["FRED_API_KEY"])
    """
    load_integration_env()  # lazy-load on first call

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for name in var_names:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            resolved[name] = value

    if missing:
        pytest.skip(f"Missing env var(s) for integration test: {', '.join(missing)}")

    return resolved


# Marker helper — in case tests want to build their own skipif chains.
def env_available(*var_names: str) -> bool:
    """True iff every named env var is set (after loading ``.env``)."""
    load_integration_env()
    return all(os.environ.get(n) for n in var_names)
