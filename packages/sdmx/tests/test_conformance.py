"""Release-blocking conformance test for parsimony-sdmx.

If this fails, the plugin violates the parsimony plugin contract and must
not be published. Contract: ``parsimony.testing.assert_plugin_valid``
(connectors_exported + descriptions_non_empty + env_vars_map_to_deps).
"""

from __future__ import annotations

from parsimony.testing import assert_plugin_valid

import parsimony_sdmx


def test_parsimony_sdmx_conforms_to_plugin_contract() -> None:
    assert_plugin_valid(parsimony_sdmx)
