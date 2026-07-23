"""Live integration tests for parsimony-polymarket.

Hits the real public Gamma (``https://gamma-api.polymarket.com``) and CLOB
(``https://clob.polymarket.com``) read APIs. Skipped by default — the root
``pyproject.toml`` sets ``-m 'not integration'``. Run explicitly with::

    uv run pytest packages/polymarket -m integration

No credentials required — both APIs are public, so these tests need no env
vars and run in CI without secrets.
"""

from __future__ import annotations

import pytest
from parsimony.errors import EmptyDataError
from parsimony_test_support import assert_provenance_shape

from parsimony_polymarket import (
    polymarket_event,
    polymarket_market,
    polymarket_price_history,
    polymarket_search_events,
)

pytestmark = pytest.mark.integration

_SEARCH_COLUMNS = {
    "slug",
    "title",
    "description",
    "markets_count",
    "volume",
    "liquidity",
    "active",
    "closed",
}


def test_polymarket_search_events_live() -> None:
    result = polymarket_search_events(query="inflation", limit=5)

    assert_provenance_shape(result, expected_source="polymarket_search_events", required_param_keys=["query"])
    df = result.raw
    assert not df.empty, "Gamma /public-search returned an empty DataFrame"
    assert set(df.columns) == _SEARCH_COLUMNS
    assert df["slug"].astype(str).str.len().gt(0).all(), "blank event slug"
    assert df["title"].astype(str).str.len().gt(0).any(), "no real event title text"
    # The query term should actually appear somewhere in the ranked titles.
    assert df["title"].str.contains("inflation", case=False, na=False).any()
    assert len(df) <= 5, "limit not respected"


def test_polymarket_navigation_chain_live() -> None:
    # search -> event -> market -> price history, entirely against the live APIs.
    events = polymarket_search_events(query="inflation", limit=5).raw
    ev_slug = str(events.iloc[0]["slug"])

    markets = polymarket_event(slug=ev_slug).raw
    assert not markets.empty, f"event {ev_slug!r} exposed no markets"
    mk_slug = str(markets.iloc[0]["market_slug"])

    outcomes = polymarket_market(slug=mk_slug).raw
    assert not outcomes.empty, f"market {mk_slug!r} exposed no outcome tokens"
    assert set(outcomes.columns) == {"clob_token_id", "outcome"}

    # Find an outcome token that actually has a price series (some resolved
    # markets return an empty history) and assert the tidy time-series shape.
    hist = None
    for token in outcomes["clob_token_id"].astype(str):
        try:
            hist = polymarket_price_history(token_id=token, interval="1w", fidelity=60)
        except EmptyDataError:
            continue
        break
    if hist is None:
        pytest.skip("no outcome token on the sampled market returned a price history")

    assert_provenance_shape(hist, expected_source="polymarket_price_history", required_param_keys=["token_id"])
    hdf = hist.raw
    assert not hdf.empty
    assert set(hdf.columns) == {"token", "timestamp", "probability"}
    assert hdf["probability"].between(0.0, 1.0).all(), "probability outside [0,1]"
    assert str(hdf["timestamp"].dtype).startswith("datetime64")
