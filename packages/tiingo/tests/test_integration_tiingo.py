"""Live integration tests for parsimony-tiingo.

Hits the real ``https://api.tiingo.com`` endpoints (and the supported-tickers
CDN). Skipped by default (root ``pyproject.toml`` sets ``-m 'not integration'``).
Run with::

    uv run pytest packages/tiingo -m integration

Requires ``TIINGO_API_KEY`` (workspace contributors get it from ``ockham/.env``
via direnv; CI sets it from secrets).

Coverage philosophy (contract checklist 15 — "no surprises"): EVERY verb has a
live test that (a) binds the real key and asserts the secret does not leak, and
(b) asserts real content, not just that columns exist. The enumerator is
BOUNDED — it asserts on a head slice / membership, never a full ~127k row
count. ``tiingo_news`` is plan-gated (Power+); on a free-tier key it returns 403
which is surfaced as :class:`PaymentRequiredError` — the live test accepts that
as a valid documented outcome rather than failing or skipping.
"""

from __future__ import annotations

import pytest
from parsimony.errors import PaymentRequiredError
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

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

pytestmark = pytest.mark.integration


def _key() -> str:
    return str(require_env("TIINGO_API_KEY")["TIINGO_API_KEY"])


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_tiingo_search_apple_returns_aapl() -> None:
    key = _key()
    result = tiingo_search.bind(api_key=key)(query="apple")

    assert_provenance_shape(result, expected_source="tiingo_search", required_param_keys=["query"])
    df = result.data
    assert not df.empty, "search for 'apple' returned no rows"
    assert "AAPL" in set(df["ticker"]), f"AAPL missing from search results: {list(df['ticker'])[:10]}"
    # Real content, not just column presence: names must be populated.
    assert df["name"].str.len().gt(0).any(), "name column is empty for every result"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Equities
# ---------------------------------------------------------------------------


def test_tiingo_eod_aapl_window() -> None:
    key = _key()
    result = tiingo_eod.bind(api_key=key)(ticker="AAPL", start_date="2024-01-02", end_date="2024-01-10")

    assert_provenance_shape(result, expected_source="tiingo_eod", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "AAPL EOD window returned no rows"
    assert (df["ticker"] == "AAPL").all()
    # Adjusted close must carry real numeric values, not be all-NaN.
    assert df["adj_close"].notna().any(), "adj_close is entirely NaN"
    assert df["close"].notna().any(), "close is entirely NaN"
    # Window must actually constrain — Jan 2024 only.
    years = {str(d)[:4] for d in df["date"]}
    assert years == {"2024"}, f"EOD window leaked other years: {years}"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_iex_aapl_realtime() -> None:
    key = _key()
    result = tiingo_iex.bind(api_key=key)(tickers="AAPL")

    assert_provenance_shape(result, expected_source="tiingo_iex", required_param_keys=["tickers"])
    df = result.data
    assert not df.empty, "IEX quote for AAPL returned no rows"
    assert "AAPL" in set(df["ticker"])
    # tngo_last is the composite last price — must be a real number.
    assert df["tngo_last"].notna().any(), "tngo_last is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_iex_historical_aapl() -> None:
    key = _key()
    result = tiingo_iex_historical.bind(api_key=key)(ticker="AAPL", resample_freq="1hour")

    assert_provenance_shape(result, expected_source="tiingo_iex_historical", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "IEX intraday for AAPL returned no rows"
    assert (df["ticker"] == "AAPL").all()
    assert df["close"].notna().any(), "intraday close is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_meta_aapl() -> None:
    key = _key()
    result = tiingo_meta.bind(api_key=key)(ticker="AAPL")

    assert_provenance_shape(result, expected_source="tiingo_meta", required_param_keys=["ticker"])
    data = result.data
    assert isinstance(data, dict)
    assert data["ticker"].upper() == "AAPL"
    # Description must be real prose, not empty.
    assert len(data.get("description", "")) > 0, "metadata description is empty"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Fundamentals
# ---------------------------------------------------------------------------


def test_tiingo_fundamentals_meta_aapl() -> None:
    key = _key()
    result = tiingo_fundamentals_meta.bind(api_key=key)(tickers="AAPL")

    assert_provenance_shape(result, expected_source="tiingo_fundamentals_meta", required_param_keys=["tickers"])
    data = result.data
    assert isinstance(data, list) and data, "fundamentals meta returned no records"
    rec = data[0]
    assert rec["ticker"].upper() == "AAPL"
    # Sector/industry are the value of this endpoint — must be populated.
    assert rec.get("sector"), f"sector empty: {rec.get('sector')!r}"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_fundamentals_definitions() -> None:
    key = _key()
    result = tiingo_fundamentals_definitions.bind(api_key=key)()

    assert_provenance_shape(result, expected_source="tiingo_fundamentals_definitions")
    df = result.data
    assert not df.empty, "definitions returned no rows"
    assert df["data_code"].str.len().gt(0).all(), "some data_code values are empty"
    assert df["name"].str.len().gt(0).any(), "name column is empty for every definition"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# News — plan-gated. Accepts a real article set OR PaymentRequiredError (403).
# ---------------------------------------------------------------------------


def test_tiingo_news_power_plan_or_payment_required() -> None:
    key = _key()
    bound = tiingo_news.bind(api_key=key)
    try:
        result = bound(limit=5)
    except PaymentRequiredError as exc:
        # Documented free-tier outcome: 403 "no permission to access News API".
        assert exc.provider == "tiingo"
        assert key not in str(exc)
        return

    # Power+ key path: real articles with populated titles.
    assert_provenance_shape(result, expected_source="tiingo_news")
    df = result.data
    assert not df.empty, "news returned no rows on a plan that grants access"
    assert df["title"].str.len().gt(0).any(), "news titles are all empty"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Crypto
# ---------------------------------------------------------------------------


def test_tiingo_crypto_prices_btcusd_window() -> None:
    key = _key()
    result = tiingo_crypto_prices.bind(api_key=key)(
        tickers="btcusd", start_date="2024-01-01", end_date="2024-01-10", resample_freq="1day"
    )

    assert_provenance_shape(result, expected_source="tiingo_crypto_prices", required_param_keys=["tickers"])
    df = result.data
    assert not df.empty, "btcusd crypto prices returned no rows"
    assert (df["ticker"] == "btcusd").all()
    assert df["close"].notna().any(), "crypto close is entirely NaN"
    assert df["volume"].notna().any(), "crypto volume is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_crypto_top_btcusd() -> None:
    key = _key()
    result = tiingo_crypto_top.bind(api_key=key)(tickers="btcusd")

    assert_provenance_shape(result, expected_source="tiingo_crypto_top", required_param_keys=["tickers"])
    df = result.data
    assert not df.empty, "btcusd top-of-book returned no rows"
    assert (df["ticker"] == "btcusd").all()
    assert df["last_price"].notna().any(), "crypto last_price is entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Forex
# ---------------------------------------------------------------------------


def test_tiingo_fx_prices_eurusd_window() -> None:
    key = _key()
    result = tiingo_fx_prices.bind(api_key=key)(
        tickers="eurusd", start_date="2024-01-01", end_date="2024-01-10", resample_freq="1day"
    )

    assert_provenance_shape(result, expected_source="tiingo_fx_prices", required_param_keys=["tickers"])
    df = result.data
    assert not df.empty, "eurusd forex prices returned no rows"
    assert (df["ticker"] == "eurusd").all()
    assert df["close"].notna().any(), "forex close is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_tiingo_fx_top_eurusd() -> None:
    key = _key()
    result = tiingo_fx_top.bind(api_key=key)(tickers="eurusd")

    assert_provenance_shape(result, expected_source="tiingo_fx_top", required_param_keys=["tickers"])
    df = result.data
    assert not df.empty, "eurusd forex top-of-book returned no rows"
    assert "eurusd" in set(df["ticker"])
    assert df["mid_price"].notna().any(), "forex mid_price is entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Enumerator — BOUNDED. Downloads the ~127k-row CDN snapshot but only asserts
# on membership / a head slice, never a full count.
# ---------------------------------------------------------------------------


def test_enumerate_tiingo_bounded() -> None:
    key = _key()
    result = enumerate_tiingo.bind(api_key=key)()

    assert_provenance_shape(result, expected_source="enumerate_tiingo")
    df = result.data
    # Exact-match enumerator schema.
    assert list(df.columns) == [
        "ticker",
        "name",
        "asset_type",
        "exchange",
        "price_currency",
        "start_date",
        "end_date",
    ]
    assert not df.empty, "supported-tickers enumeration returned no rows"
    # AAPL is a stable supported ticker — membership check, not a full count.
    head = df.head(50000)  # bound the slice we inspect
    assert "AAPL" in set(df["ticker"]), "AAPL missing from supported tickers"
    # asset_type must carry real values for the head slice (not all empty).
    assert head["asset_type"].str.len().gt(0).any(), "asset_type empty across the head slice"
    assert_no_secret_leak(result, secret=key)
