"""Live integration tests for parsimony-riksbank (all five products).

Hits the real public Riksbank gateway (``api.riksbank.se``). Every product is **open /
keyless**, so the fetch / enumerate / search tests need no env vars.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``. Run with::

    uv run pytest packages/riksbank -m integration

**Keyless rate limit is tight** (5 requests/minute per IP), so a ``_with_retry`` helper
backs off and retries on ``RateLimitError`` to keep the suite stable without a key. The
catalog-BUILD path is ⚠️-flagged: it cold-enumerates the whole universe and so requires
``RIKSBANK_API_KEY`` (not in the workspace ``.env``); it ``pytest.skip``s when absent.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.errors import RateLimitError
from parsimony.result import Result
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_riksbank import (
    RIKSBANK_ENUMERATE_OUTPUT,
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_holdings_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_swestr_fetch,
    riksbank_turnover_fetch,
)
from parsimony_riksbank.catalog_build import build_riksbank_catalog
from parsimony_riksbank.search import riksbank_search

pytestmark = pytest.mark.integration

_T = TypeVar("_T")


def _with_retry(fn: Callable[[], _T], *, attempts: int = 4, base_delay: float = 12.0) -> _T:
    """Retry a call on the keyless rate limit (HTTP 429 → RateLimitError).

    The unauthenticated Riksbank gateway throttles aggressively; without a key
    a burst hits 429 with a ~40-50s reset. We honour the provider's
    ``retry_after`` hint (capped) and back off, so the live suite stays green
    without a subscription key.
    """
    last: RateLimitError | None = None
    for i in range(attempts):
        try:
            return fn()
        except RateLimitError as exc:
            last = exc
            wait = min(float(exc.retry_after or base_delay), 60.0) if exc.retry_after else base_delay * (i + 1)
            time.sleep(wait)
    assert last is not None
    raise last


# ---------------------------------------------------------------------------
# SWEA + SWESTR (interest & exchange rates) — keyless
# ---------------------------------------------------------------------------


def test_riksbank_fetch_sekeur_live() -> None:
    """SEKEURPMI — EUR/SEK daily mid rate — is a stable, high-traffic series."""
    result = _with_retry(lambda: riksbank_fetch(series_id="SEKEURPMI", from_date="2026-01-01", to_date="2026-02-28"))

    assert_provenance_shape(result, expected_source="riksbank_fetch", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty
    assert set(df["series_id"]) == {"SEKEURPMI"}
    assert df["value"].notna().any()
    vals = df["value"].dropna()
    assert ((vals > 8.0) & (vals < 14.0)).all(), f"EUR/SEK out of plausible range: {vals.tolist()[:5]}"
    assert df["date"].dtype.kind == "M"


def test_riksbank_swestr_fetch_latest_rate_live() -> None:
    """The latest SWESTR overnight fixing carries a real numeric rate plus
    SWESTR's native trade metadata."""
    result = _with_retry(lambda: riksbank_swestr_fetch(series="SWESTR"))

    assert_provenance_shape(result, expected_source="riksbank_swestr_fetch", required_param_keys=["series"])
    df = result.data
    assert not df.empty
    assert set(df["series"]) == {"SWESTR"}
    vals = df["value"].dropna()
    assert ((vals > -2.0) & (vals < 15.0)).all(), f"SWESTR rate out of range: {vals.tolist()}"
    assert "numberOfTransactions" in df.columns


def test_riksbank_swestr_fetch_index_live() -> None:
    """The SWESTR index publishes ``value`` (an index level, not ``rate``);
    the connector normalises it onto the ``value`` column. This exercises a
    DIFFERENT URL family (/index) than the raw fixing — facet coverage."""
    result = _with_retry(lambda: riksbank_swestr_fetch(series="SWESTRINDEX"))
    df = result.data
    assert set(df["series"]) == {"SWESTRINDEX"}
    assert (df["value"].dropna() > 50.0).all()


# ---------------------------------------------------------------------------
# Monetary Policy Data (forecasts & outcomes) — keyless
# ---------------------------------------------------------------------------


def test_monetary_policy_fetch_single_round_live() -> None:
    """A single policy round returns exactly that series and vintage — the live proof the
    ``series`` + ``policy_round_name`` filters apply (the colon in the round name must be
    sent literally, or the gateway 404s and silently returns the whole universe)."""
    result = _with_retry(
        lambda: riksbank_monetary_policy_fetch(series="SEQGDPNAYSA", policy_round="2026:1")
    )
    assert_provenance_shape(
        result, expected_source="riksbank_monetary_policy_fetch", required_param_keys=["series"]
    )
    df = result.data
    assert not df.empty
    assert set(df["series"]) == {"SEQGDPNAYSA"}, "series filter did not apply (colon-encoding regression?)"
    assert set(df["policy_round"]) == {"2026:1"}, "policy_round filter did not apply"
    assert "GDP" in str(df.iloc[0]["title"])
    assert df["value"].notna().any()
    assert df["date"].dtype.kind == "M"


def test_monetary_policy_fetch_all_vintages_live() -> None:
    """Omitting the round returns many vintages — the policy_round column keeps them apart."""
    result = _with_retry(lambda: riksbank_monetary_policy_fetch(series="SEMCPIFNAYNA"))
    df = result.data
    assert set(df["series"]) == {"SEMCPIFNAYNA"}
    assert df["policy_round"].nunique() > 5, "expected multiple forecast vintages"


# ---------------------------------------------------------------------------
# Turnover Statistics (FI/FX/IRD market turnover) — keyless
# ---------------------------------------------------------------------------


def test_turnover_fetch_fx_monthly_live() -> None:
    result = _with_retry(lambda: riksbank_turnover_fetch(market="fx", frequency="monthly"))
    assert_provenance_shape(result, expected_source="riksbank_turnover_fetch", required_param_keys=["market"])
    df = result.data
    assert not df.empty
    assert set(df["market"]) == {"fx"}
    assert df["period"].dtype.kind == "M"
    assert df["amount"].notna().any()
    # The faceting columns carry the breakdown an analyst pivots on.
    assert {"asset", "contract", "counterparty"} <= set(df.columns)
    assert df["asset"].notna().any()


# ---------------------------------------------------------------------------
# Holdings (securities holdings, served as JSON despite the parquet metadata) — keyless
# ---------------------------------------------------------------------------


def test_holdings_fetch_aggregated_live() -> None:
    result = _with_retry(
        lambda: riksbank_holdings_fetch(dataset="swedish_securities_aggregated", start_date="2025-06-01")
    )
    assert_provenance_shape(result, expected_source="riksbank_holdings_fetch", required_param_keys=["dataset"])
    df = result.data
    assert not df.empty
    assert set(df["dataset"]) == {"swedish_securities_aggregated"}
    assert df["date"].dtype.kind == "M"
    assert df["balance_nominal_number"].notna().any()
    # The aggregated dataset breaks down by security group.
    assert df["security_group_name"].notna().any()


# ---------------------------------------------------------------------------
# enumerate_riksbank (live, keyless — bounded: 2 SWEA calls + 1 MP call)
# ---------------------------------------------------------------------------


def test_enumerate_riksbank_live() -> None:
    """Enumerate the live universe across all five products, asserting REAL content."""
    result = _with_retry(lambda: enumerate_riksbank())
    df = result.data

    assert list(df.columns) == [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]
    assert not df.empty
    # ~117 SWEA + 7 SWESTR + ~24 MP + 6 turnover + 2 holdings ≈ 156.
    assert len(df) > 120, f"implausibly few units enumerated: {len(df)}"

    by_source = df["source"].value_counts().to_dict()
    assert set(by_source) == {"swea", "swestr", "monetary_policy", "turnover", "holdings"}
    assert by_source["swea"] > 40, "too few SWEA series"
    assert by_source["swestr"] == 7
    assert by_source["monetary_policy"] > 15, "too few monetary-policy series"
    assert by_source["turnover"] == 6
    assert by_source["holdings"] == 2

    # Codes route each family.
    assert (df.loc[df["source"] == "monetary_policy", "code"].str.startswith("monetary_policy/")).all()
    assert (df.loc[df["source"] == "turnover", "code"].str.startswith("turnover/")).all()
    assert (df.loc[df["source"] == "holdings", "code"].str.startswith("holdings/")).all()

    # Real content in the indexed columns.
    assert df["code"].astype(str).str.len().gt(0).all()
    assert df["title"].astype(str).str.len().gt(0).all()
    assert df["description"].astype(str).str.len().gt(0).any()

    entities = Result(data=df, output_spec=RIKSBANK_ENUMERATE_OUTPUT).to_entities()
    assert len(entities) == len(df)
    assert entities[0].namespace == "riksbank"


# ---------------------------------------------------------------------------
# riksbank_search (over a locally-built fixture catalog — network-free)
# ---------------------------------------------------------------------------


def test_riksbank_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``riksbank_search`` end-to-end over a small, locally-built catalog that
    spans all five families, confirming routing-relevant ``code`` shapes round-trip."""
    cols = [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]

    def _row(code: str, title: str, description: str, source: str) -> dict[str, Any]:
        base: dict[str, Any] = dict.fromkeys(cols, "")
        base.update(code=code, title=title, description=description, source=source, series_closed=False)
        return base

    rows = [
        _row("SEKEURPMI", "EUR — euro mid rate against the Swedish krona",
             "Daily EUR/SEK mid exchange rate fixed at 16:15 CET.", "swea"),
        _row("SWESTR", "SWESTR — Swedish Krona Short-Term Rate",
             "The transaction-based overnight reference rate for Swedish kronor.", "swestr"),
        _row("monetary_policy/SEQGDPNAYSA", "GDP (Annual percentage change)",
             "Riksbank GDP forecast, annual percentage change, across policy rounds.", "monetary_policy"),
        _row("turnover/fx/monthly", "Turnover — Foreign exchange (monthly)",
             "Aggregated foreign-exchange market turnover, monthly, since 1987.", "turnover"),
        _row("holdings/swedish_securities_aggregated", "Holdings in Swedish securities (aggregated by group)",
             "The Riksbank's securities holdings aggregated by security group.", "holdings"),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = Result(data=df, output_spec=RIKSBANK_ENUMERATE_OUTPUT).to_entities()
    catalog = Catalog("riksbank", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "riksbank_catalog"
    catalog.save(out_dir)

    fx = riksbank_search(query="euro Swedish krona exchange rate", limit=5, catalog_url=str(out_dir))
    assert_provenance_shape(fx, expected_source="riksbank_search", required_param_keys=["query"])
    assert list(fx.data.columns) == ["code", "title", "source", "score"]
    assert fx.data.iloc[0]["code"] == "SEKEURPMI"

    # Ranking discriminates: each family's flagship query surfaces its own code.
    gdp = riksbank_search(query="GDP forecast monetary policy", limit=5, catalog_url=str(out_dir))
    assert gdp.data.iloc[0]["code"] == "monetary_policy/SEQGDPNAYSA"
    turn = riksbank_search(query="foreign exchange turnover market", limit=5, catalog_url=str(out_dir))
    assert turn.data.iloc[0]["code"] == "turnover/fx/monthly"


# ---------------------------------------------------------------------------
# catalog-build live path (⚠️ requires RIKSBANK_API_KEY — skips cleanly)
# ---------------------------------------------------------------------------


def test_build_riksbank_catalog_live() -> None:
    """Full live catalog build. ⚠️ The keyless quota cannot sustain a full
    ~117-series enumeration, so ``build_riksbank_catalog`` requires a key and
    this test skips cleanly when ``RIKSBANK_API_KEY`` is absent (it is not in
    the workspace ``.env``)."""
    creds = require_env("RIKSBANK_API_KEY")
    catalog = build_riksbank_catalog(api_key=creds["RIKSBANK_API_KEY"])
    assert catalog.name == "riksbank"
    assert len(catalog) > 120, f"live build produced implausibly few entries: {len(catalog)}"
