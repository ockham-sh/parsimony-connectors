"""Public-surface contract for parsimony-snb.

The discovered plugin surface is ``CONNECTORS`` (3 connectors); the only public
entry points are ``CONNECTORS`` + ``load`` (hence ``__all__``). Transport / parse
internals live in submodules and are NOT re-exported at the package root.
"""

from __future__ import annotations

import parsimony_snb


def test_minimal_all() -> None:
    assert parsimony_snb.__all__ == ["CONNECTORS", "load"]


def test_connectors_count_and_names() -> None:
    assert len(parsimony_snb.CONNECTORS) == 3
    names = {c.name for c in parsimony_snb.CONNECTORS}
    assert names == {"snb_fetch", "enumerate_snb", "snb_search"}


def test_internal_seams_not_re_exported_at_root() -> None:
    # The transport/parse seams are importable from their submodules for tests, but
    # must not leak onto the package root as public attributes.
    for internal in (
        "_KNOWN_CUBES",  # the old frozen registry must be gone, not just hidden
        "parse_sitemap",
        "parse_snb_csv",
        "get_cube_info",
        "_list_cubes",
        "warehouse_api_id",
    ):
        assert not hasattr(parsimony_snb, internal), f"{internal} leaked onto package root"
