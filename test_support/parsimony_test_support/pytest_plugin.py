"""Pytest plugin, registered through the ``pytest11`` entry point.

It loads in every test run of the monorepo, since each package's tests run with
``parsimony-test-support`` installed, so no package needs a ``conftest.py`` for it.
"""

from __future__ import annotations

import os

import pytest


def pytest_configure(config: pytest.Config) -> None:
    # Connector calls in tests must not write parsimony run folders into the real home.
    # Set before collection, so subprocesses the tests start inherit it too.
    os.environ["PARSIMONY_RECORD"] = "off"
