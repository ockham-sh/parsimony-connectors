"""Release-blocking conformance test for parsimony-fred.

If this fails, the plugin violates the parsimony plugin contract and must
not be published. See:
https://github.com/ockham-sh/parsimony/blob/main/docs/plugin-contract.md
"""

from __future__ import annotations

from parsimony.testing import assert_plugin_valid

import parsimony_fred


def test_parsimony_fred_conforms_to_plugin_contract() -> None:
    assert_plugin_valid(parsimony_fred)
