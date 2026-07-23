"""Credential-declaration conformance for parsimony-polymarket.

Polymarket's Gamma + CLOB APIs are keyless: every connector declares
``requires=()`` and no ``secrets=``. ``test_undeclared_does_not_fast_fail``
proves each HTTP verb reaches the network with nothing configured; the
declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_polymarket import (
    polymarket_event,
    polymarket_market,
    polymarket_price_history,
    polymarket_search_events,
)


class TestPolymarketSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = polymarket_search_events
    call_kwargs = {"query": "inflation"}
    route_url = "https://gamma-api.polymarket.com/public-search"


class TestPolymarketEventCredentialDeclaration(CredentialDeclarationSuite):
    connector = polymarket_event
    call_kwargs = {"slug": "x"}
    route_url = "https://gamma-api.polymarket.com/events/slug/x"


class TestPolymarketMarketCredentialDeclaration(CredentialDeclarationSuite):
    connector = polymarket_market
    call_kwargs = {"slug": "x"}
    route_url = "https://gamma-api.polymarket.com/markets/slug/x"


class TestPolymarketPriceHistoryCredentialDeclaration(CredentialDeclarationSuite):
    connector = polymarket_price_history
    call_kwargs = {"token_id": "x"}
    route_url = "https://clob.polymarket.com/prices-history"
