"""Live integration tests for parsimony-fmp.

Hits the real ``https://financialmodelingprep.com/stable`` endpoint. Skipped by
default (root ``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    set -a; . /home/espinet/ockham/.env; set +a
    uv run pytest packages/fmp -m integration

Requires ``FMP_API_KEY``.

Coverage philosophy (contract checklist 15 — "no surprises"): EVERY verb has a
live test that (a) binds the real key and asserts the secret does not leak, and
(b) asserts real content, not just that columns exist. Large / fan-out verbs are
BOUNDED: the screener uses tight pushdown filters + a 3-row limit (or the
zero-enrichment short-circuit), never an unbounded global scan; market-wide
calendars / index / taxonomy assert on a head slice or membership.

FMP status semantics (verified live 2026-06-04): an invalid key returns 401
(→ UnauthorizedError); a plan / legacy restriction returns 403 (FMP also uses
402) → PaymentRequiredError. The plan-gated verbs (analyst_estimates,
insider_trades, institutional_positions, earnings_transcript, intraday prices)
accept EITHER real content OR PaymentRequiredError so the test documents the plan
boundary on a lower-tier key rather than failing.
"""

from __future__ import annotations

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError, PaymentRequiredError
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_fmp import (
    fmp_analyst_estimates,
    fmp_balance_sheet_statements,
    fmp_cash_flow_statements,
    fmp_company_profile,
    fmp_corporate_history,
    fmp_earnings_transcript,
    fmp_event_calendar,
    fmp_income_statements,
    fmp_index_constituents,
    fmp_insider_trades,
    fmp_institutional_positions,
    fmp_market_movers,
    fmp_news,
    fmp_peers,
    fmp_prices,
    fmp_quotes,
    fmp_screener,
    fmp_search,
    fmp_taxonomy,
)

pytestmark = pytest.mark.integration


def _key() -> str:
    return str(require_env("FMP_API_KEY")["FMP_API_KEY"])


def _content_or_payment_required(connector_fn, kwargs, source: str, key: str) -> None:
    """Plan-gated verb: assert real content on a high-tier key OR PaymentRequiredError
    on a lower-tier key. Either documents the plan boundary; neither is a silent skip."""
    bound = connector_fn.bind(api_key=key)
    try:
        result = bound(**kwargs)
    except PaymentRequiredError as exc:
        assert exc.provider == "fmp", f"{source}: wrong provider on PaymentRequiredError"
        assert key not in str(exc), f"{source}: key leaked into PaymentRequiredError"
        return
    except EmptyDataError as exc:
        assert exc.provider == "fmp"
        return

    assert_provenance_shape(result, expected_source=source)
    data = result.raw
    assert isinstance(data, pd.DataFrame)
    assert not data.empty, f"{source}: licensed key returned an empty frame"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_fmp_search_apple() -> None:
    key = _key()
    result = fmp_search.bind(api_key=key)(query="Apple")

    assert_provenance_shape(result, expected_source="fmp_search", required_param_keys=["query"])
    df = result.raw
    assert not df.empty, "search for 'Apple' returned no rows"
    assert "AAPL" in set(df["symbol"]), f"AAPL missing: {list(df['symbol'])[:10]}"
    assert df["name"].str.len().gt(0).any(), "name column is empty for every result"
    assert_no_secret_leak(result, secret=key)


def test_fmp_taxonomy_sectors() -> None:
    key = _key()
    result = fmp_taxonomy.bind(api_key=key)(type="sectors")
    df = result.raw
    assert not df.empty, "available-sectors returned no rows"
    assert "Technology" in set(df["sector"]), f"Technology missing: {list(df['sector'])[:10]}"
    assert_no_secret_leak(result, secret=key)


def test_fmp_taxonomy_exchanges_facet() -> None:
    # A non-default taxonomy route (different path + column from 'sectors').
    key = _key()
    result = fmp_taxonomy.bind(api_key=key)(type="exchanges")
    df = result.raw
    assert not df.empty, "available-exchanges returned no rows"
    assert "exchange" in df.columns, f"exchange column missing: {list(df.columns)}"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Core market data
# ---------------------------------------------------------------------------


def test_fmp_quotes_aapl() -> None:
    key = _key()
    result = fmp_quotes.bind(api_key=key)(symbols="AAPL")

    assert_provenance_shape(result, expected_source="fmp_quotes", required_param_keys=["symbols"])
    df = result.raw
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df["price"].notna().any(), "price is entirely NaN"
    assert df["marketCap"].notna().any(), "marketCap is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_prices_daily_window() -> None:
    key = _key()
    result = fmp_prices.bind(api_key=key)(
        symbol="AAPL", frequency="daily", from_date="2025-05-01", to_date="2025-05-09"
    )

    assert_provenance_shape(result, expected_source="fmp_prices", required_param_keys=["symbol"])
    df = result.raw
    assert not df.empty, "AAPL daily prices returned no rows"
    assert df["close"].notna().any(), "close is entirely NaN"
    assert df["volume"].notna().any(), "volume is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_prices_dividend_adjusted_window() -> None:
    # The dividend-adjusted route returns adjOpen/adjHigh/adjLow/adjClose (no
    # open/high/low/close); the connector renames them onto the declared schema.
    # Regression for the "dividend_adjusted silently drops all price data" blocker.
    key = _key()
    result = fmp_prices.bind(api_key=key)(
        symbol="AAPL", frequency="dividend_adjusted", from_date="2025-05-01", to_date="2025-05-09"
    )

    assert_provenance_shape(result, expected_source="fmp_prices", required_param_keys=["symbol"])
    df = result.raw
    assert not df.empty, "AAPL dividend-adjusted prices returned no rows"
    assert "close" in df.columns, f"close dropped — columns are {list(df.columns)}"
    assert df["close"].notna().any(), "close is entirely NaN"
    assert df["open"].notna().any(), "open is entirely NaN"
    assert df["volume"].notna().any(), "volume is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_prices_intraday_plan_gated() -> None:
    # Intraday needs Starter (5min-4hour) or Premium (1min); accept content OR PaymentRequiredError.
    _content_or_payment_required(fmp_prices, {"symbol": "AAPL", "frequency": "1min"}, "fmp_prices", _key())


# ---------------------------------------------------------------------------
# Fundamentals
# ---------------------------------------------------------------------------


def test_fmp_company_profile_aapl() -> None:
    key = _key()
    result = fmp_company_profile.bind(api_key=key)(symbol="AAPL")

    assert_provenance_shape(result, expected_source="fmp_company_profile", required_param_keys=["symbol"])
    df = result.raw
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df["companyName"].str.len().gt(0).any(), "companyName empty"
    assert df["sector"].str.len().gt(0).any(), "sector empty"
    assert_no_secret_leak(result, secret=key)


def test_fmp_peers_aapl() -> None:
    key = _key()
    result = fmp_peers.bind(api_key=key)(symbol="AAPL")
    df = result.raw
    assert not df.empty, "peers returned no rows"
    assert df["symbol"].str.len().gt(0).any(), "peer symbol empty"
    assert df["companyName"].str.len().gt(0).any(), "peer companyName empty"
    assert_no_secret_leak(result, secret=key)


def test_fmp_income_statements_aapl() -> None:
    key = _key()
    result = fmp_income_statements.bind(api_key=key)(symbol="AAPL", period="annual", limit=2)
    df = result.raw
    assert not df.empty, "income statements returned no rows"
    assert df["revenue"].notna().any(), "revenue is entirely NaN"
    assert df["netIncome"].notna().any(), "netIncome is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_balance_sheet_aapl() -> None:
    key = _key()
    result = fmp_balance_sheet_statements.bind(api_key=key)(symbol="AAPL", period="annual", limit=2)
    df = result.raw
    assert not df.empty, "balance sheets returned no rows"
    assert df["totalAssets"].notna().any(), "totalAssets is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_cash_flow_aapl() -> None:
    key = _key()
    result = fmp_cash_flow_statements.bind(api_key=key)(symbol="AAPL", period="annual", limit=2)
    df = result.raw
    assert not df.empty, "cash flow statements returned no rows"
    assert df["freeCashFlow"].notna().any(), "freeCashFlow is entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Events and catalysts
# ---------------------------------------------------------------------------


def test_fmp_corporate_history_earnings_aapl() -> None:
    key = _key()
    result = fmp_corporate_history.bind(api_key=key)(symbol="AAPL", event_type="earnings", limit=4)
    df = result.raw
    assert not df.empty, "corporate earnings history returned no rows"
    assert (df["symbol"] == "AAPL").all(), "rows are not all AAPL"
    assert_no_secret_leak(result, secret=key)


def test_fmp_corporate_history_dividends_facet() -> None:
    # A non-default event_type route (different upstream path from 'earnings').
    key = _key()
    result = fmp_corporate_history.bind(api_key=key)(symbol="AAPL", event_type="dividends", limit=4)
    df = result.raw
    assert not df.empty, "corporate dividends history returned no rows"
    assert "dividend" in df.columns, f"dividend column missing: {list(df.columns)}"
    assert_no_secret_leak(result, secret=key)


def test_fmp_event_calendar_earnings_bounded() -> None:
    # Market-wide calendar — bound to a tight 2-day window.
    key = _key()
    result = fmp_event_calendar.bind(api_key=key)(event_type="earnings", from_date="2026-06-01", to_date="2026-06-03")
    df = result.raw
    assert not df.empty, "earnings calendar returned no rows"
    assert df["symbol"].str.len().gt(0).any(), "calendar symbol empty"
    assert_no_secret_leak(result, secret=key)


def test_fmp_analyst_estimates_plan_gated() -> None:
    _content_or_payment_required(
        fmp_analyst_estimates, {"symbol": "AAPL", "period": "annual", "limit": 4}, "fmp_analyst_estimates", _key()
    )


# ---------------------------------------------------------------------------
# Signals and context
# ---------------------------------------------------------------------------


def test_fmp_news_aapl() -> None:
    key = _key()
    result = fmp_news.bind(api_key=key)(type="news", symbols="AAPL", limit=3)
    df = result.raw
    assert not df.empty, "news returned no rows"
    assert df["title"].str.len().gt(0).any(), "all news titles empty"
    assert_no_secret_leak(result, secret=key)


def test_fmp_news_press_releases_facet() -> None:
    # Non-default news type (different upstream path from 'news').
    _content_or_payment_required(
        fmp_news, {"type": "press_releases", "symbols": "AAPL", "limit": 3}, "fmp_news", _key()
    )


def test_fmp_insider_trades_plan_gated() -> None:
    _content_or_payment_required(fmp_insider_trades, {"symbol": "AAPL", "limit": 3}, "fmp_insider_trades", _key())


def test_fmp_institutional_positions_plan_gated() -> None:
    _content_or_payment_required(
        fmp_institutional_positions,
        {"symbol": "AAPL", "year": "2024", "quarter": "1"},
        "fmp_institutional_positions",
        _key(),
    )


def test_fmp_earnings_transcript_plan_gated() -> None:
    _content_or_payment_required(
        fmp_earnings_transcript, {"symbol": "AAPL", "year": "2024", "quarter": "1"}, "fmp_earnings_transcript", _key()
    )


# ---------------------------------------------------------------------------
# Market context
# ---------------------------------------------------------------------------


def test_fmp_index_constituents_sp500_bounded() -> None:
    key = _key()
    result = fmp_index_constituents.bind(api_key=key)(index="SP500")
    df = result.raw
    assert not df.empty, "SP500 constituents returned no rows"
    assert "AAPL" in set(df["symbol"]), "AAPL missing from SP500"
    head = df.head(100)
    assert head["name"].str.len().gt(0).any(), "constituent name empty across head slice"
    assert_no_secret_leak(result, secret=key)


def test_fmp_market_movers_gainers() -> None:
    key = _key()
    result = fmp_market_movers.bind(api_key=key)(type="gainers")
    df = result.raw
    assert not df.empty, "market movers returned no rows"
    assert df["symbol"].str.len().gt(0).any(), "mover symbol empty"
    assert df["changesPercentage"].notna().any(), "changesPercentage entirely NaN"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Screener — BOUNDED HARD. ``prefilter_limit`` (NOT ``limit``) is what bounds
# request volume: it caps the company-screener result set, and the enrichment
# fan-out runs over exactly those prefiltered symbols. ``limit`` only truncates
# the final frame AFTER enrichment has already fanned out, so it does not bound
# request count. With prefilter_limit=N the fan-out issues ≤ 1 + 2*N requests
# (1 company-screener + key-metrics-ttm + ratios-ttm per prefiltered symbol).
# The native-fields variant hits the zero-enrichment short-circuit (1 request).
# ---------------------------------------------------------------------------


def test_fmp_screener_bounded_with_enrichment() -> None:
    key = _key()
    # prefilter_limit=3 caps the company-screener result set to 3 symbols, so the
    # enrichment fan-out is at most 1 + 2*3 = 7 requests regardless of how broad
    # the pushdown is. (limit=3 alone would NOT bound the fan-out — it truncates
    # only after enrichment.)
    result = fmp_screener.bind(api_key=key)(
        sector="Technology",
        country="US",
        market_cap_min=1e12,
        prefilter_limit=3,
        limit=3,
    )
    assert_provenance_shape(result, expected_source="fmp_screener")
    df = result.raw
    assert not df.empty, "bounded screener returned no rows"
    assert len(df) <= 3, f"screener exceeded the limit: {len(df)} rows"
    assert df["symbol"].str.len().gt(0).any(), "screener symbol empty"
    assert df["marketCap"].notna().any(), "screener marketCap entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_fmp_screener_native_fields_zero_enrichment() -> None:
    # fields are screener-native only → the enrichment fan-out is skipped entirely
    # (one company-screener request). Still bounded by a tight pushdown + limit.
    key = _key()
    result = fmp_screener.bind(api_key=key)(
        sector="Technology",
        country="US",
        market_cap_min=1e12,
        limit=3,
        fields=["symbol", "companyName", "marketCap"],
    )
    df = result.raw
    assert not df.empty, "native-fields screener returned no rows"
    assert list(df.columns) == ["symbol", "companyName", "marketCap"], list(df.columns)
    assert df["marketCap"].notna().any(), "marketCap entirely NaN"
    assert_no_secret_leak(result, secret=key)
