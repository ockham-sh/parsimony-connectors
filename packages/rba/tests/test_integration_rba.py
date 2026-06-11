"""Live integration tests for parsimony-rba (Reserve Bank of Australia).

Hits the real public RBA statistics site (``www.rba.gov.au``). RBA is
**keyless**, so these tests need no env vars and run without secrets — there is
no key to bind and no secret that could leak (no ``assert_no_secret_leak``
needed).

**Akamai / curl_cffi.** ``rba.gov.au`` TLS-fingerprint-blocks plain httpx (403);
the connector reaches the origin only via curl_cffi (Chrome impersonation).
curl_cffi is a hard dependency, but if it is somehow unavailable, or if Akamai
blocks *this* environment even with impersonation, the live tests skip with a
clear ⚠️ reason rather than weakening the assertions. The offline suite
(mocked curl_cffi + fixture Excel/CSV bytes) fully covers every verb regardless.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/rba -m integration

**Bounded crawls only.** A full ``enumerate_rba`` fetches ~216 CSVs + XLSX +
xls-hist (~250 requests). The live enumerate test monkeypatches the module-level
``_discover_csv_links`` seam down to ONE real CSV so the crawl fires a handful of
requests, and a request counter wrapped around ``Session.get`` asserts the
bound actually held. ``rba_search`` is covered against a locally-built fixture
catalog rather than the published snapshot, so it never triggers a cold full
enumerate + embed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw
from parsimony_test_support import assert_provenance_shape

import parsimony_rba
from parsimony_rba import RBA_ENUMERATE_OUTPUT, enumerate_rba, rba_fetch
from parsimony_rba.search import rba_search

pytestmark = pytest.mark.integration

# curl_cffi is a hard dep; if it is missing, skip live (offline still covers all).
curl_cffi = pytest.importorskip(
    "curl_cffi",
    reason="rba live skipped: curl_cffi unavailable (Akamai bypass impossible without it)",
)


def _akamai_reachable() -> bool:
    """Probe the RBA tables index once; False if Akamai blocks this environment."""
    from parsimony_rba import _make_session

    try:
        session = _make_session()
        try:
            r = session.get(
                "https://www.rba.gov.au/statistics/tables/",
                impersonate="chrome",
                timeout=30.0,
            )
            return r.status_code == 200 and "/statistics/tables/csv/" in r.text
        finally:
            session.close()
    except Exception:
        return False


def test_rba_fetch_cash_rate_live() -> None:
    """F1 is the RBA Cash Rate Target table — the canonical RBA dataset.

    Asserts REAL content: the cash-rate-target series (FIRMMCRTD) carries real
    numeric values in a plausible policy-rate band, not just a non-empty frame.
    """
    if not _akamai_reachable():
        pytest.skip("⚠️ rba live skipped: Akamai blocks this environment even with curl_cffi")

    result = rba_fetch(table_id="f1-data")

    assert_provenance_shape(result, expected_source="rba_fetch", required_param_keys=["table_id"])
    df = result.data
    assert not df.empty, "RBA fetch of f1-data returned an empty DataFrame"
    assert set(df["table_id"]) == {"f1-data"}

    # The cash-rate-target series must be present with real values.
    crt = df[df["series_key"] == "FIRMMCRTD"]
    assert not crt.empty, "f1-data did not include the cash-rate-target series FIRMMCRTD"
    assert crt["title"].astype(str).str.contains("Cash Rate", case=False).any()

    vals = crt["value"].dropna()
    assert len(vals) > 100, "implausibly few cash-rate observations"
    assert vals.nunique() > 1, "cash rate implausibly constant across history"
    # Australian cash rate has sat in roughly 0%–8% over the published window.
    assert ((vals >= 0.0) & (vals <= 8.0)).all(), f"cash rate out of plausible band: {vals.tolist()[:5]}"

    # Dates parse to real datetimes (declared dtype="datetime").
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "observation dates all NaT"


def test_enumerate_rba_bounded_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real CSV (f1-data) to verify the live RBA CSV shape without the
    full ~250-request fan-out. A request counter asserts the bound held."""
    if not _akamai_reachable():
        pytest.skip("⚠️ rba live skipped: Akamai blocks this environment even with curl_cffi")

    def _one_link(_session: Any) -> list[str]:
        return ["/statistics/tables/csv/f1-data.csv"]

    def _no_xlsx(_session: Any) -> set[str]:
        return set()

    def _no_xls_hist(_session: Any) -> list[str]:
        return []

    # Bound all three passes: one real CSV, and zero XLSX/xls-hist fetches.
    monkeypatch.setattr(parsimony_rba, "_discover_csv_links", _one_link)
    monkeypatch.setattr(parsimony_rba, "_discover_xlsx_stems", _no_xlsx)
    monkeypatch.setattr(parsimony_rba, "_discover_xls_hist_stems", _no_xls_hist)

    # Instrument curl_cffi Session.get so we can assert the bound held.
    from curl_cffi.requests import Session

    real_get = Session.get
    calls: list[str] = []

    def _counting_get(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(url)
        return real_get(self, url, *args, **kwargs)

    monkeypatch.setattr(Session, "get", _counting_get)

    result = enumerate_rba()
    df = result.data

    # The bound held: exactly the one real CSV — XLSX and xls-hist passes are
    # short-circuited to zero requests by the seam monkeypatches. Never ~250.
    assert len(calls) <= 2, f"bounded crawl fired {len(calls)} requests — bound did not hold"
    assert any("f1-data.csv" in c for c in calls), "the one bounded CSV was not fetched"

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in RBA_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"
    assert set(df["table_id"]) == {"f1-data"}

    # Real content in declared columns — not just column names.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert df["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert (df["source"] == "rba_csv").all(), "source metadata missing"
    assert df["unit"].astype(str).str.len().gt(0).any(), "no real unit prose"
    # The cash-rate-target series is in f1-data — its compound code must appear.
    assert any(c.startswith("f1-data#FIRMMCRTD") for c in df["code"]), "FIRMMCRTD not enumerated"

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = RBA_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "rba"


def test_rba_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``rba_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full ``build_rba_catalog()`` runs the expensive
    ~250-request enumerate + embeds thousands of rows. We build a 3-row catalog
    from real enumerator-shaped rows, persist it, and point ``catalog_url`` at
    it — so the search runs network-free and never triggers the expensive build.
    """
    cols = [c.name for c in RBA_ENUMERATE_OUTPUT.columns]

    def _row(code: str, title: str, description: str, **over: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(code=code, title=title, description=description, source="rba_csv")
        base.update(over)
        return base

    rows = [
        _row(
            "f1-data#FIRMMCRTD",
            "Cash Rate Target",
            "Official cash rate target set by the Reserve Bank of Australia Board.",
            table_id="f1-data",
            series_id="FIRMMCRTD",
        ),
        _row(
            "g1-data#FXRUSD",
            "Exchange rate — US dollar per Australian dollar",
            "Foreign exchange rate of the Australian dollar against the US dollar.",
            table_id="g1-data",
            series_id="FXRUSD",
        ),
        _row(
            "g3-data#GCPIAG",
            "Consumer Price Index — All groups",
            "Australian consumer price index, all groups, headline inflation.",
            table_id="g3-data",
            series_id="GCPIAG",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = entities_from_raw(df, RBA_ENUMERATE_OUTPUT)
    catalog = Catalog("rba", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "rba_catalog"
    catalog.save(out_dir)

    result = rba_search(query="cash rate target monetary policy", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="rba_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search exceeded the fixture catalog"
    # Real ranking: the cash-rate entry is the top hit; scores are populated.
    assert sdf.iloc[0]["code"] == "f1-data#FIRMMCRTD"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    fx = rba_search(query="US dollar exchange rate", limit=5, catalog_url=str(out_dir))
    assert fx.data.iloc[0]["code"] == "g1-data#FXRUSD"
