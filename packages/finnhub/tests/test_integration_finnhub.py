"""Live integration tests for parsimony-finnhub.

Hits the real ``https://finnhub.io/api/v1`` endpoints. Skipped by default
(root ``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    set -a; . /home/espinet/ockham/.env; set +a
    uv run pytest packages/finnhub -m integration

Requires ``FINNHUB_API_KEY`` (workspace contributors get it from
``ockham/.env`` via direnv; the worktree direnv cache may be stale, so source
``.env`` explicitly as shown above; CI sets it from secrets).

Coverage philosophy (contract checklist 15 — "no surprises"): EVERY verb has a
live test that (a) binds the real key and asserts the secret does not leak
(``assert_no_secret_leak`` with the REAL key — this verifies the Theme-B
``secrets=("api_key",)`` fix), and (b) asserts real CONTENT, not just that
columns exist. The enumerator is BOUNDED — it asserts on membership / a head
slice, never a full ~30k row count.

Finnhub's free tier covers all 12 verbs (verified live), so none is expected to
return ``PaymentRequiredError``. The premium endpoints that DO 403 (``/stock/
candle`` etc.) are intentionally not exposed by this package.
"""

from __future__ import annotations

import datetime as dt

import pytest
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_finnhub import (
    enumerate_finnhub,
    finnhub_basic_financials,
    finnhub_company_news,
    finnhub_earnings,
    finnhub_earnings_calendar,
    finnhub_ipo_calendar,
    finnhub_market_news,
    finnhub_peers,
    finnhub_profile,
    finnhub_quote,
    finnhub_recommendation,
    finnhub_search,
)

pytestmark = pytest.mark.integration


def _key() -> str:
    return str(require_env("FINNHUB_API_KEY")["FINNHUB_API_KEY"])


def _recent_window(days: int = 14) -> tuple[str, str]:
    today = dt.date.today()
    return (today - dt.timedelta(days=days)).isoformat(), today.isoformat()


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_finnhub_search_apple_returns_aapl() -> None:
    key = _key()
    result = finnhub_search.bind(api_key=key)(query="apple")

    assert_provenance_shape(result, expected_source="finnhub_search", required_param_keys=["query"])
    df = result.data
    assert not df.empty, "search for 'apple' returned no rows"
    assert "AAPL" in set(df["symbol"]), f"AAPL missing: {list(df['symbol'])[:10]}"
    # Real content: descriptions (company names) must be populated.
    assert df["description"].str.len().gt(0).any(), "description empty for every result"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Market data — quote
# ---------------------------------------------------------------------------


def test_finnhub_quote_aapl() -> None:
    key = _key()
    result = finnhub_quote.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_quote", required_param_keys=["symbol"])
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"
    # current_price must be a real, positive number.
    assert df["current_price"].notna().all(), "current_price is NaN"
    assert float(df.iloc[0]["current_price"]) > 0, "current_price not positive"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Company
# ---------------------------------------------------------------------------


def test_finnhub_profile_aapl() -> None:
    key = _key()
    result = finnhub_profile.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_profile", required_param_keys=["symbol"])
    data = result.data
    assert isinstance(data, dict)
    assert data.get("ticker") == "AAPL"
    assert "Apple" in data.get("name", ""), f"unexpected name: {data.get('name')!r}"
    # marketCapitalization is the value of this endpoint — must be real.
    assert float(data.get("marketCapitalization") or 0) > 0, "marketCap missing"
    assert_no_secret_leak(result, secret=key)


def test_finnhub_peers_aapl() -> None:
    key = _key()
    result = finnhub_peers.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_peers", required_param_keys=["symbol"])
    df = result.data
    assert not df.empty, "peers returned no rows"
    assert "AAPL" in set(df["symbol"]), "AAPL not in its own peer set"
    assert len(df) > 1, "peer list has only one entry"
    assert_no_secret_leak(result, secret=key)


def test_finnhub_recommendation_aapl() -> None:
    key = _key()
    result = finnhub_recommendation.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_recommendation", required_param_keys=["symbol"])
    df = result.data
    assert not df.empty, "recommendation returned no rows"
    # Real content: the buy/hold counts must carry numbers, not be all-NaN.
    assert df["buy"].notna().any(), "buy column entirely NaN"
    assert df["hold"].notna().any(), "hold column entirely NaN"
    assert (df["buy"].fillna(0) + df["hold"].fillna(0) + df["sell"].fillna(0)).gt(0).any(), (
        "every recommendation row sums to zero"
    )
    assert_no_secret_leak(result, secret=key)


def test_finnhub_earnings_aapl() -> None:
    key = _key()
    result = finnhub_earnings.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_earnings", required_param_keys=["symbol"])
    df = result.data
    assert not df.empty, "earnings returned no rows"
    assert df["eps_actual"].notna().any(), "eps_actual entirely NaN"
    assert df["eps_estimate"].notna().any(), "eps_estimate entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_finnhub_basic_financials_aapl() -> None:
    key = _key()
    result = finnhub_basic_financials.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="finnhub_basic_financials", required_param_keys=["symbol"])
    data = result.data
    assert isinstance(data, dict)
    metric = data.get("metric", {})
    assert metric, "metric dict is empty"
    # A widely-present KPI must carry a real value.
    assert metric.get("52WeekHigh") is not None, "52WeekHigh missing from metrics"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


def test_finnhub_company_news_aapl_recent() -> None:
    key = _key()
    frm, to = _recent_window(14)
    result = finnhub_company_news.bind(api_key=key)(symbol="AAPL", from_date=frm, to_date=to)

    assert_provenance_shape(result, expected_source="finnhub_company_news", required_param_keys=["symbol"])
    df = result.data
    assert not df.empty, "no AAPL news in the recent window"
    assert df["headline"].str.len().gt(0).any(), "all headlines empty"
    assert df["datetime"].notna().any(), "all news timestamps missing"
    assert_no_secret_leak(result, secret=key)


def test_finnhub_market_news_general() -> None:
    key = _key()
    result = finnhub_market_news.bind(api_key=key)(category="general")

    assert_provenance_shape(result, expected_source="finnhub_market_news")
    df = result.data
    assert not df.empty, "market news returned no rows"
    assert df["headline"].str.len().gt(0).any(), "all market headlines empty"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Calendars
# ---------------------------------------------------------------------------


def test_finnhub_earnings_calendar_window() -> None:
    key = _key()
    today = dt.date.today()
    frm = (today - dt.timedelta(days=7)).isoformat()
    to = (today + dt.timedelta(days=30)).isoformat()
    result = finnhub_earnings_calendar.bind(api_key=key)(from_date=frm, to_date=to)

    assert_provenance_shape(result, expected_source="finnhub_earnings_calendar")
    df = result.data
    assert not df.empty, "earnings calendar returned no events"
    assert df["symbol"].str.len().gt(0).all(), "some calendar rows have no symbol"
    # At least some events should carry a date and an EPS estimate.
    assert df["date"].notna().any(), "all calendar dates missing"
    assert_no_secret_leak(result, secret=key)


def test_finnhub_ipo_calendar_window() -> None:
    key = _key()
    today = dt.date.today()
    frm = (today - dt.timedelta(days=90)).isoformat()
    to = (today + dt.timedelta(days=30)).isoformat()
    result = finnhub_ipo_calendar.bind(api_key=key)(from_date=frm, to_date=to)

    assert_provenance_shape(result, expected_source="finnhub_ipo_calendar")
    df = result.data
    assert not df.empty, "IPO calendar returned no events"
    assert df["name"].str.len().gt(0).any(), "all IPO names empty"
    # price_range preserves the verbatim string — confirm at least one is populated.
    assert df["price_range"].astype(str).str.len().gt(0).any(), "no IPO carries a price_range"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Enumerator — BOUNDED (head slice / membership, never a full ~30k count)
# ---------------------------------------------------------------------------


def test_enumerate_finnhub_bounded() -> None:
    key = _key()
    result = enumerate_finnhub.bind(api_key=key)(exchange="US")

    assert_provenance_shape(result, expected_source="enumerate_finnhub")
    df = result.data
    # Exact-match enumerator schema.
    assert list(df.columns) == [
        "symbol",
        "description",
        "display_symbol",
        "type",
        "currency",
        "mic",
        "exchange",
        "isin",
    ]
    assert not df.empty, "symbol enumeration returned no rows"
    assert "AAPL" in set(df["symbol"]), "AAPL missing from the US symbol list"
    # Real content across a bounded head slice (not a full-table scan).
    head = df.head(2000)
    assert head["description"].str.len().gt(0).any(), "description empty across head slice"
    assert (head["exchange"] == "US").all(), "exchange column not stamped"
    assert_no_secret_leak(result, secret=key)
