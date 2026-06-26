"""Live integration tests for parsimony-coingecko.

Hits the real ``https://api.coingecko.com/api/v3`` (and GeckoTerminal on-chain)
endpoints. Skipped by default (root ``pyproject.toml`` sets ``-m 'not
integration'``). Run with::

    set -a; . /path/to/.env; set +a
    uv run pytest packages/coingecko -m integration

Requires ``COINGECKO_API_KEY`` (a Demo-tier key works for most verbs).

Coverage philosophy (contract checklist 15 — "no surprises"): EVERY verb has a
live test that (a) binds the real key and asserts the secret does not leak, and
(b) asserts real content, not just that columns exist. The enumerator is
BOUNDED — it asserts on membership / a head slice, never the full ~17k row
count.

Demo-plan limits (verified live, 2026-06-03):
* ``coingecko_top_gainers_losers`` is PRO-only → a Demo key returns **401**
  with ``error_code=10005``, surfaced as :class:`PaymentRequiredError`.
* ``coingecko_market_chart_range`` beyond 365 days returns **401** with
  ``error_code=10012`` → :class:`PaymentRequiredError`.
Those two live tests ACCEPT a PaymentRequiredError as a documented outcome
rather than failing or skipping.
"""

from __future__ import annotations

import pytest
from parsimony.errors import PaymentRequiredError
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_coingecko import (
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
)

pytestmark = pytest.mark.integration

# USDT on Ethereum — a stable, long-lived on-chain token for GeckoTerminal.
_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


def _key() -> str:
    return str(require_env("COINGECKO_API_KEY")["COINGECKO_API_KEY"])


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_coingecko_search_bitcoin() -> None:
    key = _key()
    result = coingecko_search.bind(api_key=key)(query="bitcoin")

    assert_provenance_shape(result, expected_source="coingecko_search", required_param_keys=["query"])
    df = result.data
    assert not df.empty, "search for 'bitcoin' returned no rows"
    assert "bitcoin" in set(df["id"]), f"bitcoin missing from results: {list(df['id'])[:10]}"
    assert df["name"].str.len().gt(0).any(), "name column is empty for every result"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_trending() -> None:
    key = _key()
    result = coingecko_trending.bind(api_key=key)()

    assert_provenance_shape(result, expected_source="coingecko_trending")
    df = result.data
    assert not df.empty, "trending returned no rows"
    assert df["name"].str.len().gt(0).any(), "trending name column is empty"
    # score is the value of this discovery feed — must carry real numbers.
    assert df["score"].notna().any(), "trending score is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_top_gainers_losers_pro_only_or_payment_required() -> None:
    key = _key()
    bound = coingecko_top_gainers_losers.bind(api_key=key)
    try:
        result = bound()
    except PaymentRequiredError as exc:
        # Documented Demo-tier outcome: this endpoint is PRO-only.
        assert exc.provider == "coingecko"
        assert key not in str(exc)
        return

    # PRO key path: real rows with both directions present.
    assert_provenance_shape(result, expected_source="coingecko_top_gainers_losers")
    df = result.data
    assert not df.empty, "top gainers/losers returned no rows on a plan that grants access"
    assert df["usd_price_percent_change"].notna().any(), "percent change is entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


def test_coingecko_price_btc_eth() -> None:
    key = _key()
    result = coingecko_price.bind(api_key=key)(ids="bitcoin,ethereum")

    assert_provenance_shape(result, expected_source="coingecko_price", required_param_keys=["ids"])
    df = result.data
    assert set(df["id"]) == {"bitcoin", "ethereum"}
    # The dynamic per-currency column must carry a real price.
    assert "usd" in df.columns, f"usd column missing: {list(df.columns)}"
    assert df["usd"].notna().any(), "usd price is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_markets_top_ranked() -> None:
    key = _key()
    result = coingecko_markets.bind(api_key=key)(per_page=5)

    assert_provenance_shape(result, expected_source="coingecko_markets")
    df = result.data
    assert not df.empty, "markets returned no rows"
    assert df["current_price"].notna().any(), "current_price is entirely NaN"
    assert df["market_cap"].notna().any(), "market_cap is entirely NaN"
    assert df["name"].str.len().gt(0).any(), "name column is empty"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_coin_detail_bitcoin() -> None:
    key = _key()
    result = coingecko_coin_detail.bind(api_key=key)(coin_id="bitcoin")

    assert_provenance_shape(result, expected_source="coingecko_coin_detail", required_param_keys=["coin_id"])
    data = result.data
    assert isinstance(data, dict)
    assert data["id"] == "bitcoin"
    # Description (the value of this verb over coingecko_markets) must be real prose.
    desc = data.get("description", {})
    assert isinstance(desc, dict) and len(desc.get("en", "")) > 0, "coin detail description is empty"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Historical
# ---------------------------------------------------------------------------


def test_coingecko_market_chart_btc_1d() -> None:
    key = _key()
    result = coingecko_market_chart.bind(api_key=key)(coin_id="bitcoin", days="1")

    assert_provenance_shape(result, expected_source="coingecko_market_chart", required_param_keys=["coin_id"])
    df = result.data
    assert not df.empty, "market chart returned no rows"
    assert df["price"].notna().any(), "price is entirely NaN"
    assert df["market_cap"].notna().any(), "market_cap is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_market_chart_range_recent_or_payment_required() -> None:
    key = _key()
    bound = coingecko_market_chart_range.bind(api_key=key)
    try:
        # A recent (< 365-day) window — allowed on the Demo plan.
        result = bound(coin_id="bitcoin", from_date="2026-05-01", to_date="2026-05-20")
    except PaymentRequiredError as exc:
        # If the recent window were somehow gated, accept the documented outcome.
        assert exc.provider == "coingecko"
        assert key not in str(exc)
        return

    assert_provenance_shape(result, expected_source="coingecko_market_chart_range", required_param_keys=["coin_id"])
    df = result.data
    assert not df.empty, "market chart range returned no rows"
    assert df["price"].notna().any(), "price is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_coingecko_market_chart_range_over_365d_is_payment_required() -> None:
    key = _key()
    bound = coingecko_market_chart_range.bind(api_key=key)
    # >365 days back on the Demo plan → error_code 10012 → PaymentRequiredError.
    with pytest.raises(PaymentRequiredError) as exc_info:
        bound(coin_id="bitcoin", from_date="2020-01-01", to_date="2020-06-01")
    assert exc_info.value.provider == "coingecko"
    assert key not in str(exc_info.value)


def test_coingecko_ohlc_btc() -> None:
    key = _key()
    result = coingecko_ohlc.bind(api_key=key)(coin_id="bitcoin", days=7)

    assert_provenance_shape(result, expected_source="coingecko_ohlc", required_param_keys=["coin_id"])
    df = result.data
    assert not df.empty, "ohlc returned no rows"
    assert df["close"].notna().any(), "close is entirely NaN"
    assert df["open"].notna().any(), "open is entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# On-chain (GeckoTerminal) — works on the Demo plan
# ---------------------------------------------------------------------------


def test_coingecko_token_price_onchain_usdt() -> None:
    key = _key()
    result = coingecko_token_price_onchain.bind(api_key=key)(network="eth", contract_addresses=_USDT)

    assert_provenance_shape(
        result, expected_source="coingecko_token_price_onchain", required_param_keys=["network", "contract_addresses"]
    )
    df = result.data
    assert not df.empty, "on-chain price returned no rows"
    assert _USDT in set(df["contract_address"])
    assert df["price_usd"].notna().any(), "price_usd is entirely NaN"
    # USDT trades near $1 — a sanity check that the value is real, not a constant placeholder.
    assert df.iloc[0]["price_usd"] > 0, "price_usd is non-positive"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Enumerator — BOUNDED. Downloads the ~17k-row list but only asserts on
# membership / a head slice, never a full count.
# ---------------------------------------------------------------------------


def test_enumerate_coingecko_bounded() -> None:
    key = _key()
    result = enumerate_coingecko.bind(api_key=key)()

    assert_provenance_shape(result, expected_source="enumerate_coingecko")
    df = result.data
    # Exact-match enumerator schema.
    assert list(df.columns) == ["id", "name", "symbol", "platforms"]
    assert not df.empty, "coin enumeration returned no rows"
    # bitcoin is a stable listed coin — membership check, not a full count.
    assert "bitcoin" in set(df["id"]), "bitcoin missing from coin list"
    head = df.head(3000)  # bound the slice we inspect
    assert head["name"].str.len().gt(0).any(), "name empty across the head slice"
    # platforms is populated for the majority of coins when include_platform=True (the default).
    assert head["platforms"].str.len().gt(2).any(), "platforms empty across the head slice"
    assert_no_secret_leak(result, secret=key)
