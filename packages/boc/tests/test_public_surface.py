"""Public-surface contract for parsimony-boc.

Unlike the minimal-facade packages, ``parsimony_boc`` deliberately re-exports its
output configs and connector callables at the top level (a documented facade
over the ``_http``/``outputs``/``connectors``/``search`` submodules). This test
pins that surface so accidental additions/removals are caught.
"""

from __future__ import annotations

import parsimony_boc

EXPECTED_ALL = [
    "BOC_ENUMERATE_OUTPUT",
    "BOC_FETCH_OUTPUT",
    "BOC_SEARCH_OUTPUT",
    "CONNECTORS",
    "PARSIMONY_BOC_CATALOG_URL_ENV",
    "boc_fetch",
    "boc_search",
    "enumerate_boc",
    "load",
]


def test_connectors_count() -> None:
    assert len(parsimony_boc.CONNECTORS) == 3


def test_connector_names() -> None:
    assert {c.name for c in parsimony_boc.CONNECTORS} == {"boc_fetch", "enumerate_boc", "boc_search"}


def test_public_surface_is_pinned() -> None:
    assert parsimony_boc.__all__ == EXPECTED_ALL
    for name in EXPECTED_ALL:
        assert hasattr(parsimony_boc, name), name
