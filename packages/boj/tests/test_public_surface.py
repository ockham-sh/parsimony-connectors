"""Public-surface contract for parsimony-boj.

``parsimony_boj`` is a documented facade over the ``_http`` / ``outputs`` /
``databases`` / ``connectors`` / ``search`` submodules. This test pins the public
``__all__`` so accidental additions/removals are caught, and confirms the private
test/catalog-build seams stay importable at the top level (without leaking into
``__all__``).
"""

from __future__ import annotations

import parsimony_boj

EXPECTED_ALL = [
    "BOJ_ENUMERATE_OUTPUT",
    "BOJ_FETCH_OUTPUT",
    "CONNECTORS",
    "PARSIMONY_BOJ_CATALOG_URL_ENV",
    "boj_databases_search",
    "boj_fetch",
    "boj_series_search",
    "enumerate_boj",
    "load",
]

# Re-exported for the test suite + catalog_build helpers, intentionally NOT in __all__.
PRIVATE_SEAMS = ["_BOJ_DATABASES", "_resolve_boj_database", "fetch_boj_enumeration_rows_for_db"]


def test_connectors_count() -> None:
    assert len(parsimony_boj.CONNECTORS) == 4


def test_connector_names() -> None:
    assert {c.name for c in parsimony_boj.CONNECTORS} == {
        "boj_fetch",
        "enumerate_boj",
        "boj_databases_search",
        "boj_series_search",
    }


def test_public_surface_is_pinned() -> None:
    assert parsimony_boj.__all__ == EXPECTED_ALL
    for name in EXPECTED_ALL:
        assert hasattr(parsimony_boj, name), name


def test_private_seams_importable_but_not_public() -> None:
    for name in PRIVATE_SEAMS:
        assert hasattr(parsimony_boj, name), name
        assert name not in parsimony_boj.__all__, name
