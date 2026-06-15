"""Public-surface contract for parsimony-rba.

The discovered plugin surface is ``CONNECTORS`` (3 connectors); the only public
entry points are ``CONNECTORS`` + ``load`` (hence ``__all__``). Transport/parse
internals live in submodules and are NOT re-exported at the package root.
"""

from __future__ import annotations

import parsimony_rba


def test_minimal_all() -> None:
    assert parsimony_rba.__all__ == ["CONNECTORS", "load"]


def test_connectors_count_and_names() -> None:
    assert len(parsimony_rba.CONNECTORS) == 3
    names = {c.name for c in parsimony_rba.CONNECTORS}
    assert names == {"rba_fetch", "enumerate_rba", "rba_search"}


def test_internal_seams_not_re_exported_at_root() -> None:
    # The transport/parse seams are importable from their submodules for tests, but
    # must not leak onto the package root as public attributes.
    for internal in ("_curl_get", "_make_session", "_parse_rba_csv", "_discover_csv_links"):
        assert not hasattr(parsimony_rba, internal), f"{internal} leaked onto package root"
