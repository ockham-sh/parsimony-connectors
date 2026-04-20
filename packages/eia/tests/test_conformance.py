"""Conformance test — release-blocking."""

from __future__ import annotations

from parsimony.testing import assert_plugin_valid

import parsimony_eia


def test_conforms_to_parsimony_plugin_contract() -> None:
    assert_plugin_valid(parsimony_eia)
