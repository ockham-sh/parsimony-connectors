"""Public-surface contract for parsimony-treasury.

The discovered plugin surface is ``CONNECTORS`` (4 connectors); the only public entry
points are ``CONNECTORS`` + ``load`` (hence ``__all__``). Transport / parse internals live
in submodules and are NOT re-exported at the package root.
"""

from __future__ import annotations

import parsimony_treasury


def test_minimal_all() -> None:
    assert parsimony_treasury.__all__ == ["CONNECTORS", "load"]


def test_connectors_count_and_names() -> None:
    assert len(parsimony_treasury.CONNECTORS) == 4
    names = {c.name for c in parsimony_treasury.CONNECTORS}
    assert names == {"treasury_fetch", "treasury_rates_fetch", "enumerate_treasury", "treasury_search"}


def test_internal_seams_not_re_exported_at_root() -> None:
    # The transport/parse/registry seams are importable from their submodules for tests,
    # but must not leak onto the package root as public attributes.
    for internal in (
        "_TREASURY_RATE_FEEDS",  # the ODM registry lives in rate_feeds, not the root
        "is_measure_field",
        "fiscal_measure_rows",
        "parse_treasury_rates_xml",
        "_list_datasets",
        "build_treasury_rate_rows",
    ):
        assert not hasattr(parsimony_treasury, internal), f"{internal} leaked onto package root"
