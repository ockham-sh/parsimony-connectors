"""Credential-declaration conformance for parsimony-tiingo.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb. Each
proves the verb's ``requires=("TIINGO_API_KEY",)`` declaration matches runtime:
the bare call fast-fails naming that env var, and an env- or bind-supplied key
reaches the outgoing request (tiingo carries it in the ``Authorization: Token``
header).

Routes are the tiingo base (``https://api.tiingo.com``) plus each verb's endpoint
path (with the default ticker interpolated where the path embeds one); the suite
matches any query string.

``enumerate_tiingo`` is the one verb the full suite does not fit: it uses the API
key only as a symmetric fast-fail gate (``_client(api_key)`` is built and
discarded), then downloads the supported-tickers snapshot from a *public,
unauthenticated* CDN on a separate host. Its ``requires=("TIINGO_API_KEY",)``
declaration is correct — it fast-fails naming that env var — but the credential
never rides the outgoing request, so the suite's request-canary checks are
structurally inapplicable. The applicable half of the contract (the fast-fail) is
covered by :func:`test_enumerate_tiingo_fast_fails` below.
"""

from __future__ import annotations

import pytest
from parsimony.errors import UnauthorizedError
from parsimony_test_support import CredentialDeclarationSuite

from parsimony_tiingo import (
    enumerate_tiingo,
    tiingo_crypto_prices,
    tiingo_crypto_top,
    tiingo_eod,
    tiingo_fundamentals_definitions,
    tiingo_fundamentals_meta,
    tiingo_fx_prices,
    tiingo_fx_top,
    tiingo_iex,
    tiingo_iex_historical,
    tiingo_meta,
    tiingo_news,
    tiingo_search,
)

_BASE = "https://api.tiingo.com"


class TestTiingoSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_search
    call_kwargs = {"query": "apple"}
    route_url = f"{_BASE}/tiingo/utilities/search"


class TestTiingoEodCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_eod
    call_kwargs = {"ticker": "AAPL"}
    route_url = f"{_BASE}/tiingo/daily/AAPL/prices"


class TestTiingoIexCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_iex
    call_kwargs = {"tickers": "AAPL"}
    route_url = f"{_BASE}/iex/"


class TestTiingoIexHistoricalCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_iex_historical
    call_kwargs = {"ticker": "AAPL"}
    route_url = f"{_BASE}/iex/AAPL/prices"


class TestTiingoMetaCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_meta
    call_kwargs = {"ticker": "AAPL"}
    route_url = f"{_BASE}/tiingo/daily/AAPL"


class TestTiingoFundamentalsMetaCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_fundamentals_meta
    call_kwargs = {"tickers": "AAPL"}
    route_url = f"{_BASE}/tiingo/fundamentals/meta"


class TestTiingoFundamentalsDefinitionsCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_fundamentals_definitions
    call_kwargs: dict[str, str] = {}
    route_url = f"{_BASE}/tiingo/fundamentals/definitions"


class TestTiingoNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_news
    call_kwargs: dict[str, str] = {}
    route_url = f"{_BASE}/tiingo/news"


class TestTiingoCryptoPricesCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_crypto_prices
    call_kwargs = {"tickers": "btcusd"}
    route_url = f"{_BASE}/tiingo/crypto/prices"


class TestTiingoCryptoTopCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_crypto_top
    call_kwargs = {"tickers": "btcusd"}
    route_url = f"{_BASE}/tiingo/crypto/top"


class TestTiingoFxPricesCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_fx_prices
    call_kwargs = {"tickers": "eurusd"}
    route_url = f"{_BASE}/tiingo/fx/prices"


class TestTiingoFxTopCredentialDeclaration(CredentialDeclarationSuite):
    connector = tiingo_fx_top
    call_kwargs = {"tickers": "eurusd"}
    route_url = f"{_BASE}/tiingo/fx/top"


def test_enumerate_tiingo_fast_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """The enumerator honours its ``requires`` declaration by fast-failing.

    Unlike the keyed verbs above, ``enumerate_tiingo`` downloads a public CDN
    snapshot that carries no credential, so the suite's request-canary checks do
    not apply (see the module docstring). What *is* checked is the applicable
    half of the contract: with no key configured the bare call raises
    :class:`UnauthorizedError` naming ``TIINGO_API_KEY`` before any network call.
    """
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        enumerate_tiingo()
    assert exc_info.value.env_var == "TIINGO_API_KEY"
