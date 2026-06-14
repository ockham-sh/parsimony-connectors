"""Turnover Statistics family — aggregated turnover on Swedish FI/FX/IRD markets.

The Riksbank publishes aggregated turnover statistics (reported by market participants)
for three markets at two cadences. The JSON endpoint
``/markets/{market}/frequencies/{frequency}`` returns the full history since 1987 as a
tidy long table of ``{Period, Asset, Contract, Counterparty, Amount}`` records — a
faceted aggregate, not a single series. The fetchable unit is therefore the
``(market, frequency)`` dataset; the facets (asset/contract/counterparty) live inside it.

Markets and frequencies are small, stable enumerations (3 × 2 = 6 datasets), so the
catalog rows are a static registry rather than a live crawl.
"""

from __future__ import annotations

from typing import Any, Literal

TurnoverMarket = Literal["fi", "fx", "ird"]
TurnoverFrequency = Literal["daily", "monthly"]

MARKETS: frozenset[str] = frozenset({"fi", "fx", "ird"})
FREQUENCIES: frozenset[str] = frozenset({"daily", "monthly"})

CODE_PREFIX = "turnover"
_GROUP = "Turnover Statistics"
_PROVIDER = "Sveriges Riksbank"

_MARKET_LABEL: dict[str, str] = {
    "fi": "Fixed income",
    "fx": "Foreign exchange",
    "ird": "Interest rate derivatives",
}

_MARKET_DESC: dict[str, str] = {
    "fi": (
        "Aggregated turnover in the Swedish fixed-income market (government and covered "
        "bonds, treasury and other bills), broken down by asset, contract type and "
        "counterparty. Reported by participants to the Riksbank."
    ),
    "fx": (
        "Aggregated turnover in the Swedish foreign-exchange market (spot, forwards and "
        "swaps in SEK currency pairs), broken down by asset, contract type and "
        "counterparty. Reported by participants to the Riksbank."
    ),
    "ird": (
        "Aggregated turnover in Swedish interest-rate derivatives (FRAs, swaps and "
        "options), broken down by asset, contract type and counterparty. Reported by "
        "participants to the Riksbank."
    ),
}


def build_turnover_rows() -> list[dict[str, Any]]:
    """One catalog row per ``(market, frequency)`` dataset (6 total)."""
    rows: list[dict[str, Any]] = []
    for market in ("fi", "fx", "ird"):
        label = _MARKET_LABEL[market]
        for frequency in ("daily", "monthly"):
            rows.append(
                {
                    "code": f"{CODE_PREFIX}/{market}/{frequency}",
                    "title": f"Turnover — {label} ({frequency})",
                    "description": (
                        f"{_MARKET_DESC[market]} {frequency.capitalize()} frequency; full history "
                        "available since 1987."
                    ),
                    "source": "turnover",
                    "frequency": frequency.capitalize(),
                    "unit": "SEK million",
                    "group": _GROUP,
                    "provider": _PROVIDER,
                    "observation_min": "1987-01-01",
                    "observation_max": "",
                    "series_closed": False,
                }
            )
    return rows


def parse_turnover_rows(market: str, frequency: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a turnover response into ``{market, frequency, period, ...}`` rows.

    The payload is a flat JSON list of ``{Period, Asset, Contract, Counterparty,
    Amount}``. ``period``/``amount`` become the named DATA columns; the facet columns
    (asset/contract/counterparty) pass through so an analyst can pivot within a dataset.
    """
    items = payload if isinstance(payload, list) else []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        period = item.get("Period") or item.get("period")
        if period is None:
            continue
        amount = item.get("Amount")
        try:
            amount_val: float | None = float(amount) if amount is not None else None
        except (TypeError, ValueError):
            amount_val = None
        rows.append(
            {
                "market": market,
                "frequency": frequency,
                "period": period,
                "amount": amount_val,
                "asset": item.get("Asset"),
                "contract": item.get("Contract"),
                "counterparty": item.get("Counterparty"),
            }
        )
    return rows
