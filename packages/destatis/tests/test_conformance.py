"""Conformance test — release-blocking."""

from __future__ import annotations

import parsimony_destatis
from parsimony.testing import assert_plugin_valid


def test_conforms_to_parsimony_plugin_contract() -> None:
    assert_plugin_valid(parsimony_destatis)
