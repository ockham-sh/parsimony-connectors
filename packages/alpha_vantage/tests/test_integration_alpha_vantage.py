"""Live integration tests for parsimony-alpha-vantage.

Hits the real ``https://www.alphavantage.co/query`` endpoint. Skipped by default
(root ``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    set -a; . ockham/.env; set +a
    uv run pytest packages/alpha_vantage -m integration

Requires ``ALPHA_VANTAGE_API_KEY``.

**Quota constraint (the honest limitation).** The free tier is a hard
**25 requests/DAY** cap *shared across all endpoints*. A full 29-verb live sweep
would exhaust it and start returning the §5.8 ``Information`` rate-limit body
(→ :class:`RateLimitError`). So the bulk of correctness coverage lives in the
offline (respx) suite; here we live-verify a SMALL representative set with real
content asserts + :func:`assert_no_secret_leak`, and treat a
:class:`RateLimitError` as a valid documented outcome for the rest (the daily cap
is real, not a bug).

**The ``Information`` ambiguity.** Alpha Vantage returns a *byte-identical*
``Information`` body for both a genuine daily-quota exhaustion AND a premium
endpoint gate (verified live 2026-06-04). Because the body cannot disambiguate
the two, the connector maps it to :class:`RateLimitError`. So
``alpha_vantage_options`` (premium-only) surfaces :class:`RateLimitError` (or
:class:`PaymentRequiredError` only if AV ever ships a distinct premium notice
without rate-limit language) — the test accepts either.
"""

from __future__ import annotations

import pandas as pd
import pytest
from parsimony.errors import (
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
)
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_alpha_vantage.connectors.connectors import (
    alpha_vantage_crypto_daily,
    alpha_vantage_daily,
    alpha_vantage_fx_rate,
    alpha_vantage_options,
    alpha_vantage_quote,
    alpha_vantage_search,
    enumerate_alpha_vantage,
)

pytestmark = pytest.mark.integration


def _key() -> str:
    return str(require_env("ALPHA_VANTAGE_API_KEY")["ALPHA_VANTAGE_API_KEY"])


def _content_or_rate_limited(connector_fn, kwargs, source: str, key: str):
    """Run a live call; accept real content, a RateLimitError (daily cap), or
    EmptyDataError as documented "no surprises" outcomes. Returns the Result on
    success, else None (the caller skips content asserts).
    """
    bound = connector_fn.bind(api_key=key)
    try:
        result = bound(**kwargs)
    except RateLimitError as exc:
        assert exc.provider == "alpha_vantage", f"{source}: wrong provider on RateLimitError"
        assert key not in str(exc), f"{source}: key leaked into RateLimitError"
        return None
    except EmptyDataError as exc:
        assert exc.provider == "alpha_vantage"
        return None

    assert_provenance_shape(result, expected_source=source)
    assert_no_secret_leak(result, secret=key)
    return result


# ---------------------------------------------------------------------------
# Representative verbs — real content asserts (frugal: ~5-6 calls max).
# ---------------------------------------------------------------------------


def test_search_apple_real_content() -> None:
    key = _key()
    result = _content_or_rate_limited(alpha_vantage_search, {"keywords": "apple"}, "alpha_vantage_search", key)
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert not df.empty
    assert df["symbol"].str.len().gt(0).any()
    assert df["name"].str.len().gt(0).any()
    # Theme-B: the bound key must not be in provenance params.
    assert "api_key" not in result.provenance.params


def test_quote_ibm_real_content() -> None:
    key = _key()
    result = _content_or_rate_limited(alpha_vantage_quote, {"symbol": "IBM"}, "alpha_vantage_quote", key)
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "IBM"
    assert df["price"].notna().any(), "price entirely NaN"


def test_daily_ibm_real_content() -> None:
    key = _key()
    result = _content_or_rate_limited(alpha_vantage_daily, {"symbol": "IBM"}, "alpha_vantage_daily", key)
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert not df.empty
    assert df.iloc[0]["symbol"] == "IBM", "symbol KEY not injected"
    assert df["close"].notna().any(), "close entirely NaN"
    assert df["volume"].notna().any(), "volume entirely NaN"


def test_fx_rate_usd_eur_real_content() -> None:
    key = _key()
    result = _content_or_rate_limited(
        alpha_vantage_fx_rate, {"from_currency": "USD", "to_currency": "EUR"}, "alpha_vantage_fx_rate", key
    )
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["from_currency"] == "USD"
    assert df["exchange_rate"].notna().any(), "exchange_rate entirely NaN"


def test_crypto_daily_btc_injects_symbol() -> None:
    key = _key()
    result = _content_or_rate_limited(
        alpha_vantage_crypto_daily, {"symbol": "BTC", "market": "USD"}, "alpha_vantage_crypto_daily", key
    )
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert not df.empty
    # Raw crypto rows carry no symbol field; KEY must be injected from the param.
    assert df.iloc[0]["symbol"] == "BTC", "symbol KEY not injected"
    assert df["close"].notna().any(), "close entirely NaN"


def test_enumerate_active_bounded_real_content() -> None:
    key = _key()
    result = _content_or_rate_limited(enumerate_alpha_vantage, {"state": "active"}, "enumerate_alpha_vantage", key)
    if result is None:
        pytest.skip("rate-limited (free 25/day cap) — documented outcome")
    df = result.data
    assert not df.empty
    # Bounded to a head slice; never assert a full count.
    assert len(df) <= 5000
    head = df.head(50)
    assert head["symbol"].str.len().gt(0).all(), "symbol empty in head slice"
    assert head["name"].str.len().gt(0).any(), "name empty across head slice"


# ---------------------------------------------------------------------------
# Premium-gated verb — accepts PaymentRequiredError OR RateLimitError.
# On a free key Alpha Vantage returns the ambiguous Information body, which the
# connector maps to RateLimitError (the body is byte-identical to the daily-cap
# notice). PaymentRequiredError would require a distinct premium-only notice.
# ---------------------------------------------------------------------------


def test_options_premium_gated() -> None:
    key = _key()
    bound = alpha_vantage_options.bind(api_key=key)
    try:
        result = bound(symbol="IBM")
    except (PaymentRequiredError, RateLimitError) as exc:
        assert exc.provider == "alpha_vantage"
        assert key not in str(exc), "key leaked into premium/rate-limit error"
        return
    except EmptyDataError as exc:
        assert exc.provider == "alpha_vantage"
        return

    # If a premium key is in use, assert real content.
    assert_provenance_shape(result, expected_source="alpha_vantage_options")
    data = result.data
    assert isinstance(data, pd.DataFrame) and not data.empty
    assert_no_secret_leak(result, secret=key)
