"""Alpha Vantage connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_alpha_vantage.connectors.connectors import (
    alpha_vantage_balance_sheet,
    alpha_vantage_cash_flow,
    alpha_vantage_crypto_daily,
    alpha_vantage_crypto_monthly,
    alpha_vantage_crypto_weekly,
    alpha_vantage_daily,
    alpha_vantage_earnings,
    alpha_vantage_earnings_calendar,
    alpha_vantage_econ,
    alpha_vantage_etf_profile,
    alpha_vantage_fx_daily,
    alpha_vantage_fx_monthly,
    alpha_vantage_fx_rate,
    alpha_vantage_fx_weekly,
    alpha_vantage_income_statement,
    alpha_vantage_intraday,
    alpha_vantage_ipo_calendar,
    alpha_vantage_metal_history,
    alpha_vantage_metal_spot,
    alpha_vantage_monthly,
    alpha_vantage_news,
    alpha_vantage_options,
    alpha_vantage_overview,
    alpha_vantage_quote,
    alpha_vantage_search,
    alpha_vantage_technical,
    alpha_vantage_top_movers,
    alpha_vantage_weekly,
    enumerate_alpha_vantage,
)

CONNECTORS = Connectors(
    [
        # Discovery
        alpha_vantage_search,
        # Market data
        alpha_vantage_quote,
        alpha_vantage_daily,
        alpha_vantage_weekly,
        alpha_vantage_monthly,
        alpha_vantage_intraday,
        # Company fundamentals
        alpha_vantage_overview,
        alpha_vantage_income_statement,
        alpha_vantage_balance_sheet,
        alpha_vantage_cash_flow,
        alpha_vantage_earnings,
        alpha_vantage_etf_profile,
        # Calendars
        alpha_vantage_earnings_calendar,
        alpha_vantage_ipo_calendar,
        # Forex
        alpha_vantage_fx_rate,
        alpha_vantage_fx_daily,
        alpha_vantage_fx_weekly,
        alpha_vantage_fx_monthly,
        # Crypto
        alpha_vantage_crypto_daily,
        alpha_vantage_crypto_weekly,
        alpha_vantage_crypto_monthly,
        # Economic indicators
        alpha_vantage_econ,
        # Precious metals (real-time spot — not available via FRED)
        alpha_vantage_metal_spot,
        alpha_vantage_metal_history,
        # Alpha intelligence
        alpha_vantage_news,
        alpha_vantage_top_movers,
        # Technical indicators
        alpha_vantage_technical,
        # Options
        alpha_vantage_options,
        # Enumeration
        enumerate_alpha_vantage,
    ]
)


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector."""
    return CONNECTORS.bind(api_key=api_key)


__all__ = ["CONNECTORS", "load"]
