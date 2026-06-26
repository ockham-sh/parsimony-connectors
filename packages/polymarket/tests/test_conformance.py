"""Conformance test — release-blocking.

Runs the five plugin-contract checks via :func:`assert_plugin_valid` and,
through :class:`ProviderTestSuite`, also verifies the package is registered
under the ``parsimony.providers`` entry-point group as ``polymarket``.
"""

from __future__ import annotations

from parsimony.testing import ProviderTestSuite, assert_plugin_valid

import parsimony_polymarket


def test_conforms_to_parsimony_plugin_contract() -> None:
    assert_plugin_valid(parsimony_polymarket)


class TestPolymarketProvider(ProviderTestSuite):
    module_path = "parsimony_polymarket"
    entry_point_name = "polymarket"
