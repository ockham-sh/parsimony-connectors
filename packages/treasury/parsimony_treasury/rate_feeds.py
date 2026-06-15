"""Office of Debt Management rate-feed registry (archetype D — curated).

The famous daily interest-rate series — Daily Treasury Par Yield Curve, Bill Rates,
etc. — are **not** in Fiscal Data's metadata endpoint. They live on a separate Treasury
subdomain (``home.treasury.gov``) as OData/Atom XML feeds. There is no machine-readable
"list all feeds" endpoint, so the **feed set** is a curated registry — its authoritative
source is the dataset dropdown on the interest-rate-statistics page, which lists exactly
these 5 datasets (verified live 2026-06-09). The per-feed maturity columns are likewise
curated; ``scripts/harvest_rate_feeds.py`` cross-validates them against the live feed
columns (the archetype-D/C "freeze + committed reproduction" discipline).

Cataloguing these under the ``treasury`` namespace gives agents one search surface; the
``home/<feed>`` code prefix + the ``source="treasury_rates"`` column route a hit to
``treasury_rates_fetch`` (versioned ``v<n>/...`` codes route to ``treasury_fetch``).
"""

from __future__ import annotations

from typing import Any, Literal, get_args

TreasuryRateFeed = Literal[
    "daily_treasury_yield_curve",
    "daily_treasury_real_yield_curve",
    "daily_treasury_bill_rates",
    "daily_treasury_long_term_rate",
    "daily_treasury_real_long_term",
]

RATE_FEED_NAMES: frozenset[str] = frozenset(get_args(TreasuryRateFeed))

_TREASURY_RATE_DATASET_CATEGORY = "Office of Debt Management"


# One entry per maturity/column we catalog as a searchable benchmark. The columns are a
# curated subset of each feed's live columns: the rate-bearing benchmark maturities
# (``BC_*`` / ``TC_*`` / ``ROUND_B1_*``). The feeds also return secondary-market average
# columns (``CS_*``), CUSIPs, and maturity dates — those ride along as DATA on a fetch but
# are not catalogued as benchmarks (cardinality discipline).
#
# Field-list provenance (live-verified against the column union of the 2025 feed,
# 2026-06-09 — the registry matches the live current-year schema exactly):
#   * par curve: BC_1MONTH..BC_30YEAR incl. the **1.5-month** (``BC_1_5MONTH``), a CMT point
#     Treasury added in 2025 (alongside the 6-week bill). It is sparse in the feed (null on
#     older dates, which OData omits per-entry — so it is invisible in a first-entry column
#     read; only the full column union or a 2025 fetch reveals it).
#   * bills: 4/6/8/13/17/26/52-week CLOSE+YIELD — the 6-week (``*_6WK_2``) and 17-week were
#     likewise added recently (8-week ~2020, 17-week 2022, 6-week 2025).
# ``scripts/harvest_rate_feeds.py`` (and the live integration cross-check) diff this against
# the current-year feed so a future maturity addition/removal is caught.
_TREASURY_RATE_FEEDS: tuple[dict[str, Any], ...] = (
    {
        "feed": "daily_treasury_yield_curve",
        "dataset": "Daily Treasury Par Yield Curve Rates",
        "frequency": "Daily",
        "definition_template": (
            "{tenor} constant-maturity Treasury par yield curve rate, published daily by the "
            "U.S. Treasury Office of Debt Management. The par yield curve is derived from "
            "indicative bid-side prices on the most actively traded Treasury securities and is "
            "the canonical risk-free rate benchmark for that maturity."
        ),
        "fields": (
            ("BC_1MONTH", "1 Month"),
            ("BC_1_5MONTH", "1.5 Month"),
            ("BC_2MONTH", "2 Month"),
            ("BC_3MONTH", "3 Month"),
            ("BC_4MONTH", "4 Month"),
            ("BC_6MONTH", "6 Month"),
            ("BC_1YEAR", "1 Year"),
            ("BC_2YEAR", "2 Year"),
            ("BC_3YEAR", "3 Year"),
            ("BC_5YEAR", "5 Year"),
            ("BC_7YEAR", "7 Year"),
            ("BC_10YEAR", "10 Year"),
            ("BC_20YEAR", "20 Year"),
            ("BC_30YEAR", "30 Year"),
        ),
    },
    {
        "feed": "daily_treasury_real_yield_curve",
        "dataset": "Daily Treasury Real Yield Curve Rates",
        "frequency": "Daily",
        "definition_template": (
            "{tenor} real (TIPS-based) Treasury yield curve rate, published daily by the U.S. "
            "Treasury Office of Debt Management. Reflects the inflation-adjusted yield on "
            "Treasury Inflation-Protected Securities at the given constant maturity."
        ),
        "fields": (
            ("TC_5YEAR", "5 Year"),
            ("TC_7YEAR", "7 Year"),
            ("TC_10YEAR", "10 Year"),
            ("TC_20YEAR", "20 Year"),
            ("TC_30YEAR", "30 Year"),
        ),
    },
    {
        "feed": "daily_treasury_bill_rates",
        "dataset": "Daily Treasury Bill Rates",
        "frequency": "Daily",
        "definition_template": "{tenor} {kind}, published daily by the U.S. Treasury Office of Debt Management.",
        "fields": (
            ("ROUND_B1_CLOSE_4WK_2", "4-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_4WK_2", "4-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_6WK_2", "6-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_6WK_2", "6-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_8WK_2", "8-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_8WK_2", "8-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_13WK_2", "13-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_13WK_2", "13-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_17WK_2", "17-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_17WK_2", "17-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_26WK_2", "26-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_26WK_2", "26-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_52WK_2", "52-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_52WK_2", "52-Week Treasury Bill — Coupon Equivalent Yield"),
        ),
    },
    {
        "feed": "daily_treasury_long_term_rate",
        "dataset": "Daily Treasury Long-Term Rates",
        "frequency": "Daily",
        # Long format — the rate is in column ``RATE`` parameterised by ``RATE_TYPE``
        # (BC_20year / Over_10_Years / Real_Rate). One catalog row pointing at the feed is
        # the most useful surface; agents fetch and pivot on RATE_TYPE themselves.
        "definition_template": (
            "Daily Treasury long-term composite rates, published by the U.S. Treasury Office of "
            "Debt Management. The feed is in long format: each row carries a RATE_TYPE "
            "(e.g. BC_20year, Over_10_Years, Real_Rate) and a RATE value. Used to evaluate "
            "long-term Treasury yields when bonds with maturities of 10+ years are not available."
        ),
        "fields": (("RATE", "Long-Term Composite Rate"),),
    },
    {
        "feed": "daily_treasury_real_long_term",
        "dataset": "Daily Treasury Real Long-Term Rate Averages",
        "frequency": "Daily",
        "definition_template": (
            "Daily Treasury real long-term rate averages (TIPS-based), published by the U.S. "
            "Treasury Office of Debt Management."
        ),
        "fields": (("RATE", "Real Long-Term Rate Average"),),
    },
)


def build_treasury_rate_rows() -> list[dict[str, str]]:
    """One catalog row per (rate-feed, column) entry. Pure — the registry is static."""
    rows: list[dict[str, str]] = []
    for spec in _TREASURY_RATE_FEEDS:
        feed = spec["feed"]
        endpoint = f"home/{feed}"
        dataset = spec["dataset"]
        frequency = spec["frequency"]
        template: str = spec["definition_template"]
        for column_name, tenor in spec["fields"]:
            kind = "Closing Bank Discount Rate" if "_CLOSE_" in column_name else "Coupon Equivalent Yield"
            definition = template.format(tenor=tenor, kind=kind)
            rows.append(
                {
                    "code": f"{endpoint}#{column_name}",
                    "title": f"{tenor} — {dataset}",
                    "source": "treasury_rates",
                    "endpoint": endpoint,
                    "field": column_name,
                    "description": definition,
                    "data_type": "PERCENTAGE",
                    "dataset": dataset,
                    "category": _TREASURY_RATE_DATASET_CATEGORY,
                    "frequency": frequency,
                    "earliest_date": "",
                    "latest_date": "",
                }
            )
    return rows


def registry_columns(feed: str) -> set[str]:
    """The benchmark columns the registry catalogs for *feed* (for cross-validation)."""
    for spec in _TREASURY_RATE_FEEDS:
        if spec["feed"] == feed:
            return {col for col, _ in spec["fields"]}
    return set()


__all__ = [
    "TreasuryRateFeed",
    "RATE_FEED_NAMES",
    "build_treasury_rate_rows",
    "registry_columns",
]
