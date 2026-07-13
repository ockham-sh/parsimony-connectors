"""Offline tests for the CoinGecko connectors.

Every verb is exercised with a mocked transport (respx). CoinGecko auth is the
``x-cg-demo-api-key`` request header; the canonical error-mapping contract is
covered on ``coingecko_search`` (see ``test_error_mapping_coingecko.py``).
These tests assert: happy-path row shaping, EmptyData/Parse guards, inline
parameter validation, the no-key ``UnauthorizedError`` fast-fail (shared
``_client`` → all 11 verbs), the dual-meaning **401** body-disambiguation
(plan-restriction code → PaymentRequiredError; bad key → UnauthorizedError),
and that the bound key is stripped from provenance (the Theme-B secrets fix).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)

from parsimony_coingecko import (
    CONNECTORS,
    coingecko_coin_detail,
    coingecko_market_chart,
    coingecko_market_chart_range,
    coingecko_markets,
    coingecko_ohlc,
    coingecko_price,
    coingecko_search,
    coingecko_token_price_onchain,
    coingecko_top_gainers_losers,
    coingecko_trending,
    enumerate_coingecko,
    load,
)

_KEY = "live-looking-key-abc123"
_BASE = "https://api.coingecko.com/api/v3"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 11


def test_every_verb_declares_api_key_secret() -> None:
    # The headline Theme-B fix: every verb strips api_key from provenance.
    for c in CONNECTORS:
        assert "api_key" in c.secrets, f"{c.name} is missing secrets=('api_key',)"


def test_tool_tagged_connectors_have_long_first_line() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


def test_load_binds_key_across_collection() -> None:
    bundle = load(api_key=_KEY)
    for c in bundle:
        assert "api_key" not in c.exposed_signature.parameters, c.name


# ---------------------------------------------------------------------------
# coingecko_search (tool-tagged) — Theme-B provenance check lives here
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_search_returns_coin_rows_and_strips_key() -> None:
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "coins": [
                    {"id": "bitcoin", "name": "Bitcoin", "symbol": "BTC", "market_cap_rank": 1, "thumb": "btc.png"},
                    {"id": "ethereum", "name": "Ethereum", "symbol": "ETH", "market_cap_rank": 2, "thumb": "eth.png"},
                ]
            },
        )
    )
    bound = coingecko_search.bind(api_key=_KEY)
    result = bound(query="btc")

    assert result.provenance.source == "coingecko_search"
    # Theme-B: the bound key must not appear in provenance.
    assert _KEY not in str(result.provenance.params)
    assert "api_key" not in result.provenance.params
    assert list(result.data["id"]) == ["bitcoin", "ethereum"]


@respx.mock
def test_coingecko_search_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/search").mock(return_value=httpx.Response(200, json={"coins": []}))
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(query="zzzznotacoin")


def test_coingecko_search_rejects_empty_query() -> None:
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="query"):
        bound(query="   ")


# ---------------------------------------------------------------------------
# Dual-meaning 401 (verified live): plan-restriction code → PaymentRequired;
# everything else (incl. a bad key) → UnauthorizedError. The 429 path → RateLimit.
# ---------------------------------------------------------------------------


@respx.mock
def test_401_bad_key_maps_to_unauthorized_without_leaking_key() -> None:
    # A genuinely invalid / missing key: 401 with error_code 10002 (NOT a plan code).
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(401, json={"status": {"error_code": 10002, "error_message": "API Key Missing"}})
    )
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound(query="x")
    assert exc_info.value.provider == "coingecko"
    assert exc_info.value.env_var == "COINGECKO_API_KEY"
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_401_plan_restriction_code_maps_to_payment_required() -> None:
    # A plan gate (PRO-only endpoint): 401 with error_code 10005, nested under "status".
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(401, json={"status": {"error_code": 10005, "error_message": "PRO only"}})
    )
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        bound(query="x")
    assert exc_info.value.provider == "coingecko"
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_401_plan_restriction_nested_under_error_maps_to_payment_required() -> None:
    # The other body shape CoinGecko uses: {"error": {"status": {...}}} (error_code 10012).
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(
            401, json={"error": {"status": {"error_code": 10012, "error_message": "365-day limit"}}}
        )
    )
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError):
        bound(query="x")


@respx.mock
def test_429_maps_to_rate_limit_without_leaking_key() -> None:
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "10"}, json={"error": "rate limited"})
    )
    bound = coingecko_search.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        bound(query="x")
    assert exc_info.value.retry_after == 10.0
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# coingecko_trending
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_trending_returns_rows() -> None:
    respx.get(f"{_BASE}/search/trending").mock(
        return_value=httpx.Response(
            200,
            json={
                "coins": [
                    {"item": {"id": "bonk", "name": "Bonk", "symbol": "BONK", "market_cap_rank": 112, "score": 0}},
                ]
            },
        )
    )
    bound = coingecko_trending.bind(api_key=_KEY)
    result = bound()
    assert result.data.iloc[0]["id"] == "bonk"
    assert result.data.iloc[0]["score"] == 0


@respx.mock
def test_coingecko_trending_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/search/trending").mock(return_value=httpx.Response(200, json={"coins": []}))
    bound = coingecko_trending.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound()


# ---------------------------------------------------------------------------
# coingecko_top_gainers_losers (PRO-only on Demo)
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_top_gainers_losers_merges_directions() -> None:
    respx.get(f"{_BASE}/coins/top_gainers_losers").mock(
        return_value=httpx.Response(
            200,
            json={
                "top_gainers": [{"id": "a", "name": "A", "symbol": "A", "usd_price_percent_change": 50.0}],
                "top_losers": [{"id": "b", "name": "B", "symbol": "B", "usd_price_percent_change": -20.0}],
            },
        )
    )
    bound = coingecko_top_gainers_losers.bind(api_key=_KEY)
    result = bound()
    df = result.data
    assert set(df["direction"]) == {"gainer", "loser"}
    assert df[df["id"] == "a"].iloc[0]["usd_price_percent_change"] == 50.0


@respx.mock
def test_coingecko_top_gainers_losers_pro_only_maps_to_payment_required() -> None:
    # Demo key on a PRO endpoint: 401 + error_code 10005 → PaymentRequiredError.
    respx.get(f"{_BASE}/coins/top_gainers_losers").mock(
        return_value=httpx.Response(401, json={"status": {"error_code": 10005, "error_message": "PRO only"}})
    )
    bound = coingecko_top_gainers_losers.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError):
        bound()


# ---------------------------------------------------------------------------
# coingecko_price
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_price_returns_rows_per_coin() -> None:
    respx.get(f"{_BASE}/simple/price").mock(
        return_value=httpx.Response(
            200,
            json={
                "bitcoin": {"usd": 65000.0, "usd_market_cap": 1.28e12, "usd_24h_vol": 3.0e10, "usd_24h_change": 1.2},
            },
        )
    )
    bound = coingecko_price.bind(api_key=_KEY)
    result = bound(ids="bitcoin")
    df = result.data
    assert list(df["id"]) == ["bitcoin"]
    assert df.iloc[0]["usd"] == 65000.0


@respx.mock
def test_coingecko_price_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/simple/price").mock(return_value=httpx.Response(200, json={}))
    bound = coingecko_price.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(ids="nonexistent")


def test_coingecko_price_rejects_empty_ids() -> None:
    bound = coingecko_price.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="ids"):
        bound(ids="  ")


# ---------------------------------------------------------------------------
# coingecko_markets
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_markets_returns_ranked_rows() -> None:
    respx.get(f"{_BASE}/coins/markets").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "bitcoin",
                    "name": "Bitcoin",
                    "symbol": "btc",
                    "market_cap_rank": 1,
                    "current_price": 65000.0,
                    "market_cap": 1.28e12,
                    "total_volume": 3.0e10,
                    "high_24h": 66000.0,
                    "low_24h": 64000.0,
                    "price_change_percentage_24h": 1.2,
                    "ath": 73000.0,
                    "atl": 0.049,
                    "circulating_supply": 19_700_000,
                    "total_supply": 21_000_000,
                    "last_updated": "2026-04-20T10:00:00Z",
                }
            ],
        )
    )
    bound = coingecko_markets.bind(api_key=_KEY)
    result = bound()
    assert result.data.iloc[0]["id"] == "bitcoin"
    assert result.data.iloc[0]["current_price"] == 65000.0


@respx.mock
def test_coingecko_markets_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/coins/markets").mock(return_value=httpx.Response(200, json=[]))
    bound = coingecko_markets.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound()


def test_coingecko_markets_rejects_bad_per_page() -> None:
    bound = coingecko_markets.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="per_page"):
        bound(per_page=500)


# ---------------------------------------------------------------------------
# coingecko_coin_detail (schemaless dict return)
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_coin_detail_returns_dict() -> None:
    respx.get(f"{_BASE}/coins/bitcoin").mock(
        return_value=httpx.Response(200, json={"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"})
    )
    bound = coingecko_coin_detail.bind(api_key=_KEY)
    result = bound(coin_id="bitcoin")
    assert result.data["id"] == "bitcoin"


@respx.mock
def test_coingecko_coin_detail_missing_id_raises_parse_error() -> None:
    respx.get(f"{_BASE}/coins/bitcoin").mock(return_value=httpx.Response(200, json={"name": "weird"}))
    bound = coingecko_coin_detail.bind(api_key=_KEY)
    with pytest.raises(ParseError):
        bound(coin_id="bitcoin")


def test_coingecko_coin_detail_rejects_path_traversal() -> None:
    bound = coingecko_coin_detail.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="unsafe"):
        bound(coin_id="../etc/passwd")


# ---------------------------------------------------------------------------
# coingecko_market_chart
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_market_chart_merges_price_cap_volume() -> None:
    respx.get(f"{_BASE}/coins/bitcoin/market_chart").mock(
        return_value=httpx.Response(
            200,
            json={
                "prices": [[1_700_000_000_000, 40000.0], [1_700_000_600_000, 40100.0]],
                "market_caps": [[1_700_000_000_000, 7.6e11], [1_700_000_600_000, 7.6e11]],
                "total_volumes": [[1_700_000_000_000, 1.5e10], [1_700_000_600_000, 1.4e10]],
            },
        )
    )
    bound = coingecko_market_chart.bind(api_key=_KEY)
    result = bound(coin_id="bitcoin", days="1")
    df = result.data
    assert len(df) == 2
    assert "market_cap" in df.columns
    assert df.iloc[0]["price"] == 40000.0


@respx.mock
def test_coingecko_market_chart_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/coins/bitcoin/market_chart").mock(return_value=httpx.Response(200, json={"prices": []}))
    bound = coingecko_market_chart.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(coin_id="bitcoin", days="1")


# ---------------------------------------------------------------------------
# coingecko_market_chart_range (plan-gated >365d on Demo)
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_market_chart_range_returns_df() -> None:
    respx.get(f"{_BASE}/coins/bitcoin/market_chart/range").mock(
        return_value=httpx.Response(
            200,
            json={
                "prices": [[1_700_000_000_000, 40000.0]],
                "market_caps": [[1_700_000_000_000, 7.6e11]],
                "total_volumes": [[1_700_000_000_000, 1.5e10]],
            },
        )
    )
    bound = coingecko_market_chart_range.bind(api_key=_KEY)
    result = bound(coin_id="bitcoin", from_date="2024-01-01", to_date="2024-01-02")
    assert result.data.iloc[0]["price"] == 40000.0


@respx.mock
def test_coingecko_market_chart_range_over_365d_maps_to_payment_required() -> None:
    # Demo plan returns 401 + error_code 10012 for ranges older than 365 days.
    respx.get(f"{_BASE}/coins/bitcoin/market_chart/range").mock(
        return_value=httpx.Response(
            401, json={"error": {"status": {"error_code": 10012, "error_message": "365-day limit"}}}
        )
    )
    bound = coingecko_market_chart_range.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError):
        bound(coin_id="bitcoin", from_date="2020-01-01", to_date="2020-06-01")


def test_coingecko_market_chart_range_rejects_bad_date() -> None:
    bound = coingecko_market_chart_range.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="ISO date"):
        bound(coin_id="bitcoin", from_date="not-a-date", to_date="2024-01-01")


# ---------------------------------------------------------------------------
# coingecko_ohlc
# ---------------------------------------------------------------------------


@respx.mock
def test_coingecko_ohlc_returns_candles() -> None:
    respx.get(f"{_BASE}/coins/bitcoin/ohlc").mock(
        return_value=httpx.Response(
            200,
            json=[[1_700_000_000_000, 40000.0, 40500.0, 39800.0, 40200.0]],
        )
    )
    bound = coingecko_ohlc.bind(api_key=_KEY)
    result = bound(coin_id="bitcoin", days=7)
    df = result.data
    assert df.iloc[0]["open"] == 40000.0
    assert df.iloc[0]["close"] == 40200.0


@respx.mock
def test_coingecko_ohlc_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/coins/bitcoin/ohlc").mock(return_value=httpx.Response(200, json=[]))
    bound = coingecko_ohlc.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(coin_id="bitcoin", days=7)


# ---------------------------------------------------------------------------
# coingecko_token_price_onchain (GeckoTerminal)
# ---------------------------------------------------------------------------

_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


@respx.mock
def test_coingecko_token_price_onchain_returns_rows() -> None:
    respx.get(f"{_BASE}/onchain/simple/networks/eth/token_price/{_USDT}").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"attributes": {"token_prices": {_USDT: "0.9998"}}}},
        )
    )
    bound = coingecko_token_price_onchain.bind(api_key=_KEY)
    result = bound(network="eth", contract_addresses=_USDT)
    df = result.data
    assert df.iloc[0]["contract_address"] == _USDT
    # price_usd is a string in the payload; the connector coerces it to numeric.
    assert df.iloc[0]["price_usd"] == pytest.approx(0.9998)


@respx.mock
def test_coingecko_token_price_onchain_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/onchain/simple/networks/eth/token_price/{_USDT}").mock(
        return_value=httpx.Response(200, json={"data": {"attributes": {"token_prices": {}}}})
    )
    bound = coingecko_token_price_onchain.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(network="eth", contract_addresses=_USDT)


def test_coingecko_token_price_onchain_rejects_unsafe_network() -> None:
    bound = coingecko_token_price_onchain.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="network"):
        bound(network="../etc", contract_addresses=_USDT)


def test_coingecko_token_price_onchain_rejects_unsafe_addresses() -> None:
    bound = coingecko_token_price_onchain.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="contract_addresses"):
        bound(network="eth", contract_addresses="0x../../passwd")


# ---------------------------------------------------------------------------
# enumerate_coingecko (bounded — never assert full counts)
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_coingecko_emits_catalog_rows() -> None:
    respx.get(f"{_BASE}/coins/list").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc", "platforms": {"": ""}},
                {"id": "tether", "name": "Tether", "symbol": "usdt", "platforms": {"ethereum": _USDT}},
            ],
        )
    )
    bound = enumerate_coingecko.bind(api_key=_KEY)
    result = bound()
    df = result.data
    # Exact-match enumerator columns.
    assert list(df.columns) == ["id", "name", "symbol", "platforms"]
    assert set(df["id"]) == {"bitcoin", "tether"}
    # platforms is real METADATA when populated.
    assert df[df["id"] == "tether"].iloc[0]["platforms"] != ""


@respx.mock
def test_enumerate_coingecko_non_list_raises_parse_error() -> None:
    respx.get(f"{_BASE}/coins/list").mock(return_value=httpx.Response(200, json={"unexpected": True}))
    bound = enumerate_coingecko.bind(api_key=_KEY)
    with pytest.raises(ParseError):
        bound()


@respx.mock
def test_enumerate_coingecko_401_bad_key_maps_to_unauthorized() -> None:
    # The enumerator now routes through the package transport (was bare httpx).
    respx.get(f"{_BASE}/coins/list").mock(
        return_value=httpx.Response(401, json={"status": {"error_code": 10002, "error_message": "API Key Missing"}})
    )
    bound = enumerate_coingecko.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound()
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# No-key fast-fail — shared _client, so EVERY keyed verb must raise
# UnauthorizedError(env_var="COINGECKO_API_KEY") before any network call.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("connector_fn", "kwargs"),
    [
        (coingecko_search, {"query": "btc"}),
        (coingecko_trending, {}),
        (coingecko_top_gainers_losers, {}),
        (coingecko_price, {"ids": "bitcoin"}),
        (coingecko_markets, {}),
        (coingecko_coin_detail, {"coin_id": "bitcoin"}),
        (coingecko_market_chart, {"coin_id": "bitcoin", "days": "1"}),
        (coingecko_market_chart_range, {"coin_id": "bitcoin", "from_date": "2024-01-01", "to_date": "2024-01-02"}),
        (coingecko_ohlc, {"coin_id": "bitcoin"}),
        (coingecko_token_price_onchain, {"network": "eth", "contract_addresses": _USDT}),
        (enumerate_coingecko, {}),
    ],
)
def test_no_key_raises_unauthorized(connector_fn, kwargs, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector_fn(**kwargs)
    assert exc_info.value.env_var == "COINGECKO_API_KEY"
    assert exc_info.value.provider == "coingecko"


def test_no_key_case_covers_all_eleven_verbs() -> None:
    # Guard against silently dropping a verb from the parametrize list above.
    assert len(CONNECTORS) == 11
