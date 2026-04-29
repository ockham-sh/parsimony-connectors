"""Support utilities for live-API integration tests.

Integration tests are marked ``@pytest.mark.integration`` and are skipped
by default (the root ``pyproject.toml`` passes ``-m 'not integration'``).
Run them explicitly with::

    uv run pytest -m integration

Or to target one package::

    uv run pytest packages/fred -m integration

Credentials are read from ``os.environ``. The shell is expected to have
the relevant variables already set — by direnv in local dev, by GitHub
Secrets in CI. This module does not load any ``.env`` file itself.
"""

from __future__ import annotations

import os

import pytest


def require_env(*var_names: str) -> dict[str, str]:
    """Return a dict of the named env vars, or skip the test if any is missing.

    Usage::

        creds = require_env("FRED_API_KEY")
        conn = fred_fetch.bind(api_key=creds["FRED_API_KEY"])
    """
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
    """True iff every named env var is set."""
    return all(os.environ.get(n) for n in var_names)
