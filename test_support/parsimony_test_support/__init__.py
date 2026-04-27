"""Shared test harness for the parsimony-connectors monorepo.

This package is NOT published to PyPI. Connectors add it as a dev-only
workspace dep. It provides:

* :data:`STATUS_TO_EXC` — canonical HTTP-status → ConnectorError mapping.
* :data:`CANARY_KEY` — sentinel API-key string used to assert secrets don't
  leak into exception messages, provenance, or ``to_llm()`` output.
* :func:`assert_no_secret_leak` — structural check on a Result, Provenance,
  or exception.
* :func:`assert_provenance_shape` — well-formed Provenance assertion.
* :func:`load_integration_env` — load ``.env`` for integration tests.
* :func:`require_env` — pytest-skip helper for missing credentials.
"""

from __future__ import annotations

from parsimony_test_support.harness import (
    CANARY_KEY,
    STATUS_TO_EXC,
    assert_no_secret_leak,
    assert_provenance_shape,
)
from parsimony_test_support.integration import load_integration_env, require_env
from parsimony_test_support.suites import ErrorMappingSuite

__all__ = [
    "CANARY_KEY",
    "ErrorMappingSuite",
    "STATUS_TO_EXC",
    "assert_no_secret_leak",
    "assert_provenance_shape",
    "load_integration_env",
    "require_env",
]
