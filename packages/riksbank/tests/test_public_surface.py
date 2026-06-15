"""The package's public surface is the thin facade — internals stay in submodules."""

from __future__ import annotations

import parsimony_riksbank as r


def test_dunder_all_is_minimal() -> None:
    assert r.__all__ == ["CONNECTORS", "load"]


def test_connectors_count_and_names() -> None:
    assert len(r.CONNECTORS) == 7
    assert {c.name for c in r.CONNECTORS} == {
        "riksbank_fetch",
        "riksbank_swestr_fetch",
        "riksbank_monetary_policy_fetch",
        "riksbank_turnover_fetch",
        "riksbank_holdings_fetch",
        "enumerate_riksbank",
        "riksbank_search",
    }


def test_load_returns_connectors() -> None:
    assert r.load() is r.CONNECTORS
    bound = r.load(catalog_url="file:///tmp/x")
    assert {c.name for c in bound} == {c.name for c in r.CONNECTORS}


def test_implementation_internals_not_on_root() -> None:
    """Family registries and parsers live in submodules, not the facade."""
    for name in (
        "build_swestr_rows",
        "build_turnover_rows",
        "build_holdings_rows",
        "parse_forecast_rows",
        "parse_turnover_rows",
        "parse_holdings_rows",
        "_list_swea",
        "get_json_literal_query",
    ):
        assert not hasattr(r, name), f"{name} should not be exported from the package root"
