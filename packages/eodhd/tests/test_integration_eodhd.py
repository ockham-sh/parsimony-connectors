"""Live integration tests for parsimony-eodhd.

Hits the real ``https://eodhd.com/api`` endpoints. Skipped by default (root
``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    uv run pytest packages/eodhd -m integration

Requires ``EODHD_API_KEY`` (workspace contributors get it from ``ockham/.env``;
source it first: ``set -a; . ockham/.env; set +a``).

Coverage philosophy (contract checklist 15 — "no surprises"): EVERY verb has a
live test that (a) binds the real key and asserts the secret does not leak, and
(b) asserts real content, not just that columns exist. Large verbs
(eodhd_bulk_eod whole-exchange, eodhd_exchange_symbols) are BOUNDED — they
assert on a head slice / membership, never a full row count.

EODHD status semantics (verified live 2026-06-04): a free key returns real data
for the [Free+] verbs (search, exchanges, exchange_symbols, eod, live,
dividends, splits, news) and 403/423 (→ :class:`PaymentRequiredError`) for the
plan-gated verbs (intraday, bulk_eod, fundamentals, calendar, macro, macro_bulk,
technical, insider, screener). Each plan-gated test accepts EITHER real content
OR PaymentRequiredError so it documents the plan boundary rather than skipping.
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

from parsimony_eodhd import (
    eodhd_bulk_eod,
    eodhd_calendar,
    eodhd_dividends,
    eodhd_eod,
    eodhd_exchange_symbols,
    eodhd_exchanges,
    eodhd_fundamentals,
    eodhd_insider,
    eodhd_intraday,
    eodhd_live,
    eodhd_macro,
    eodhd_macro_bulk,
    eodhd_news,
    eodhd_screener,
    eodhd_search,
    eodhd_splits,
    eodhd_technical,
)

pytestmark = pytest.mark.integration


def _key() -> str:
    return str(require_env("EODHD_API_KEY")["EODHD_API_KEY"])


# ---------------------------------------------------------------------------
# [Free+] verbs — assert real content
# ---------------------------------------------------------------------------


def test_eodhd_search_apple() -> None:
    key = _key()
    result = eodhd_search.bind(api_key=key)(query="apple")

    assert_provenance_shape(result, expected_source="eodhd_search", required_param_keys=["query"])
    df = result.data
    assert not df.empty, "search for 'apple' returned no rows"
    assert "AAPL" in set(df["Code"]), f"AAPL missing: {list(df['Code'])[:10]}"
    assert df["Name"].str.len().gt(0).any(), "Name column is empty for every result"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_search_type_filter() -> None:
    # Facet coverage: a non-default type must use a VALID lowercase value.
    key = _key()
    result = eodhd_search.bind(api_key=key)(query="apple", type="etf")

    df = result.data
    assert not df.empty, "etf-filtered search returned no rows"
    assert df["Name"].str.len().gt(0).any()
    assert_no_secret_leak(result, secret=key)


def test_eodhd_exchanges() -> None:
    key = _key()
    result = eodhd_exchanges.bind(api_key=key)()

    assert_provenance_shape(result, expected_source="eodhd_exchanges")
    df = result.data
    assert not df.empty, "exchanges-list returned no rows"
    assert "US" in set(df["Code"]), "US exchange missing"
    assert df["Name"].str.len().gt(0).any(), "exchange Name is empty for every row"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_exchange_symbols_us_bounded() -> None:
    # Large response (~50k US symbols) — bound the assertion to a head slice.
    key = _key()
    result = eodhd_exchange_symbols.bind(api_key=key)(exchange="US", type="common_stock")

    assert_provenance_shape(result, expected_source="eodhd_exchange_symbols", required_param_keys=["exchange"])
    df = result.data
    assert not df.empty, "US exchange-symbol-list returned no rows"
    head = df.head(2000)
    assert head["Code"].str.len().gt(0).all(), "Code empty in head slice"
    assert head["Name"].str.len().gt(0).any(), "Name empty across head slice"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_eod_aapl_window() -> None:
    key = _key()
    # Free tier is limited to ~1 year; use a recent window.
    result = eodhd_eod.bind(api_key=key)(ticker="AAPL.US", from_date="2025-06-02", to_date="2025-06-06")

    assert_provenance_shape(result, expected_source="eodhd_eod", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "AAPL EOD window returned no rows"
    assert df["close"].notna().any(), "close is entirely NaN"
    assert df["volume"].notna().any(), "volume is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_live_aapl() -> None:
    key = _key()
    result = eodhd_live.bind(api_key=key)(ticker="AAPL.US")

    assert_provenance_shape(result, expected_source="eodhd_live", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "live quote returned no rows"
    assert df.iloc[0]["code"].upper().startswith("AAPL")
    assert df["close"].notna().any(), "live close is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_dividends_aapl() -> None:
    key = _key()
    result = eodhd_dividends.bind(api_key=key)(ticker="AAPL.US", from_date="2020-01-01")

    assert_provenance_shape(result, expected_source="eodhd_dividends", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "AAPL dividends returned no rows"
    assert df["value"].notna().any(), "dividend value is entirely NaN"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_splits_aapl() -> None:
    key = _key()
    result = eodhd_splits.bind(api_key=key)(ticker="AAPL.US", from_date="1980-01-01")

    assert_provenance_shape(result, expected_source="eodhd_splits", required_param_keys=["ticker"])
    df = result.data
    assert not df.empty, "AAPL splits returned no rows"
    assert df["split"].str.contains("/").any(), "split ratio string missing the '/' separator"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_news_aapl() -> None:
    # News is [Free+] (verified live). Accept PaymentRequiredError defensively
    # in case a key's plan revokes it.
    key = _key()
    bound = eodhd_news.bind(api_key=key)
    try:
        result = bound(ticker="AAPL.US", limit=3)
    except PaymentRequiredError as exc:
        assert exc.provider == "eodhd"
        assert key not in str(exc)
        return

    assert_provenance_shape(result, expected_source="eodhd_news")
    df = result.data
    assert not df.empty, "AAPL news returned no rows"
    assert df["title"].str.len().gt(0).any(), "news titles are all empty"
    assert_no_secret_leak(result, secret=key)


# ---------------------------------------------------------------------------
# Plan-gated verbs — accept real content OR PaymentRequiredError (403/423).
# On the free dev key these all return PaymentRequiredError; on a paid key
# they return real content. Either is a documented "no surprises" outcome.
# ---------------------------------------------------------------------------


def _content_or_payment_required(connector_fn, kwargs, source: str, key: str) -> None:
    bound = connector_fn.bind(api_key=key)
    try:
        result = bound(**kwargs)
    except PaymentRequiredError as exc:
        assert exc.provider == "eodhd", f"{source}: wrong provider on PaymentRequiredError"
        assert key not in str(exc), f"{source}: key leaked into PaymentRequiredError"
        return
    except EmptyDataError as exc:
        # A licensed key may legitimately return no rows for the chosen args.
        assert exc.provider == "eodhd"
        return

    assert_provenance_shape(result, expected_source=source)
    data = result.data
    if isinstance(data, pd.DataFrame):
        assert not data.empty, f"{source}: licensed key returned an empty frame"
    else:  # eodhd_fundamentals returns a dict
        assert data, f"{source}: licensed key returned empty data"
    assert_no_secret_leak(result, secret=key)


def test_eodhd_intraday_plan_gated() -> None:
    _content_or_payment_required(eodhd_intraday, {"ticker": "AAPL.US", "interval": "5m"}, "eodhd_intraday", _key())


def test_eodhd_bulk_eod_plan_gated_bounded() -> None:
    # Whole-exchange response — verifies the corrected path (eod-bulk-last-day);
    # the free key returns 423 → PaymentRequiredError. Never asserts a full count.
    _content_or_payment_required(eodhd_bulk_eod, {"exchange": "US"}, "eodhd_bulk_eod", _key())


def test_eodhd_fundamentals_plan_gated() -> None:
    _content_or_payment_required(eodhd_fundamentals, {"ticker": "AAPL.US"}, "eodhd_fundamentals", _key())


def test_eodhd_calendar_earnings_plan_gated() -> None:
    _content_or_payment_required(eodhd_calendar, {"type": "earnings"}, "eodhd_calendar", _key())


def test_eodhd_calendar_ipos_plan_gated() -> None:
    # Verifies the corrected IPO calendar type ("ipos", not "ipo" which 422s).
    _content_or_payment_required(eodhd_calendar, {"type": "ipos"}, "eodhd_calendar", _key())


def test_eodhd_macro_plan_gated() -> None:
    _content_or_payment_required(eodhd_macro, {"country": "USA", "indicator": "gdp_current_usd"}, "eodhd_macro", _key())


def test_eodhd_macro_bulk_plan_gated() -> None:
    _content_or_payment_required(
        eodhd_macro_bulk, {"country": "USA", "indicator": "gdp_current_usd"}, "eodhd_macro_bulk", _key()
    )


def test_eodhd_technical_plan_gated() -> None:
    # Verifies the corrected path (technical/, not technicals/ which 404s).
    _content_or_payment_required(
        eodhd_technical, {"ticker": "AAPL.US", "function": "sma", "period": 50}, "eodhd_technical", _key()
    )


def test_eodhd_insider_plan_gated() -> None:
    _content_or_payment_required(eodhd_insider, {"ticker": "AAPL.US", "limit": 5}, "eodhd_insider", _key())


def test_eodhd_screener_plan_gated() -> None:
    _content_or_payment_required(
        eodhd_screener,
        {"filters": [("market_capitalization", ">", "1000000000")], "limit": 5},
        "eodhd_screener",
        _key(),
    )
