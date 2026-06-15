"""Live integration tests for parsimony-bde (Banco de España).

Hits the real public BdE sources — the BIEST JSON API
(``app.bde.es/bierest``) and the catalog CSV chapters
(``www.bde.es/.../csv``). BdE is **keyless**, so these tests need no env vars
and run in CI without secrets — there is no key to bind and no secret that could
leak (no ``assert_no_secret_leak`` needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/bde -m integration

**Bounded crawls only.** ``enumerate_bde`` normally pulls six catalog chapters
(the CF / Financial-Accounts chapter alone is several thousand rows) plus the
Bank Lending Survey ``pb.zip``; the live tests monkeypatch the chapter list down
to a single small source so the crawl stays cheap. ``bde_search`` is covered
against a locally-built 3-row fixture catalog rather than the published snapshot,
so it never triggers a cold full enumerate + embed.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw
from parsimony_test_support import assert_provenance_shape

from parsimony_bde.connectors import enumerate as enumerate_module
from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT
from parsimony_bde.search import bde_search

pytestmark = pytest.mark.integration

# A single small chapter — Interest Rates (``ti``) is ~50 series, the smallest
# of the seven. Used to bound the live enumerate crawl.
_BOUNDED_CHAPTERS = (("ti", "Interest Rates"),)


def test_bde_fetch_known_series_live() -> None:
    # D_1NBAF472 = One-year Euribor, a stable, high-traffic monthly BdE series.
    result = bde_fetch(key="D_1NBAF472")

    assert_provenance_shape(result, expected_source="bde_fetch", required_param_keys=["key"])
    df = result.data
    assert not df.empty, "BdE fetch returned an empty DataFrame"
    assert list(df["key"].unique()) == ["D_1NBAF472"]
    # Real content, not just shape: the title is real prose and values are real
    # numeric rates (not all-NaN, not a constant).
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    # Euribor is a small percentage rate — sanity-check the magnitude.
    vals = df["value"].dropna()
    assert ((vals > -2) & (vals < 25)).all(), f"rates out of plausible range: {vals.tolist()[:5]}"
    # Dates parse to real datetimes (declared dtype="datetime").
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "record dates all NaT"


def test_bde_fetch_multi_series_single_request_live() -> None:
    # Two real series of DIFFERENT frequencies (monthly Euribor + daily FX rate)
    # fetched in one comma-joined request. ``time_range`` is a year (the
    # universal form): BdE's ``30M``/``60M``/``MAX`` keywords are accepted only
    # for lower-frequency series and 412 on the daily FX series, but a year
    # works for both. See LESSONS — this is a real per-series provider rule.
    result = bde_fetch(key="D_1NBAF472,DTCCBCEUSDEUR.B", time_range="2024")

    df = result.data
    assert set(df["key"]) == {"D_1NBAF472", "DTCCBCEUSDEUR.B"}
    # Both series carry real values across the requested year.
    for serie in ("D_1NBAF472", "DTCCBCEUSDEUR.B"):
        sub = df[df["key"] == serie]
        assert sub["value"].notna().any(), f"{serie} has no real values"


def test_bde_fetch_recovered_pb_series_live() -> None:
    """A Bank Lending Survey series recovered from pb.zip (a real ``DPB…`` code,
    NOT the un-fetchable ``PB_1_1.1`` alias) must be fetchable via listaSeries.
    This is the whole point of the pb.zip recovery — the catalog key works."""
    result = bde_fetch(key="DPBCOCCNFNOAPTOPN.T.ES", time_range="MAX")

    df = result.data
    assert list(df["key"].unique()) == ["DPBCOCCNFNOAPTOPN.T.ES"]
    assert df["value"].notna().any(), "recovered BLS series has no real values"


def test_bde_fetch_pb_alias_is_rejected_as_invalid_param_live() -> None:
    """The raw catalog alias (``PB_1_1.1``) is NOT a fetchable code — BdE 412s
    it. We surface that as InvalidParameterError carrying BdE's own message,
    not a generic ProviderError."""
    from parsimony.errors import InvalidParameterError

    with pytest.raises(InvalidParameterError) as exc:
        bde_fetch(key="PB_1_1.1")
    assert "no existe" in str(exc.value).lower() or "PB_1_1.1" in str(exc.value)


def test_enumerate_bde_recovers_bank_lending_survey_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bound the crawl to ONLY pb.zip (no CSV chapters) and verify the survey is
    recovered as real fetchable ``DPB…`` codes — and that a sampled one fetches."""
    monkeypatch.setattr(enumerate_module, "CATALOG_CHAPTERS", ())

    df = enumerate_bde().data
    assert not df.empty, "pb.zip recovery returned no rows"
    assert (df["category"] == "Bank Lending Survey").all()
    # Real fetchable codes, not the un-fetchable PB_x_y.z aliases.
    assert df["key"].str.startswith("DPB").all(), "expected DPB-prefixed real codes"
    assert (~df["key"].str.startswith("PB_")).all(), "un-fetchable alias leaked into keys"
    # The alias is preserved as metadata for traceability.
    assert df["alias"].str.startswith("PB_").any(), "alias metadata missing"
    # Spot-check: a recovered code actually fetches.
    sample = df.iloc[0]["key"]
    fetched = bde_fetch(key=sample, time_range="MAX").data
    assert fetched["value"].notna().any(), f"recovered code {sample} not fetchable"


def test_enumerate_bde_bounded_single_chapter_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real chapter (Interest Rates ``ti``, ~50 series) to verify the
    live catalog CSV shape without pulling the other chapters or pb.zip."""
    monkeypatch.setattr(enumerate_module, "CATALOG_CHAPTERS", _BOUNDED_CHAPTERS)

    def _no_pb(_fetcher: object) -> list[dict[str, str]]:
        return []

    monkeypatch.setattr(enumerate_module, "_fetch_pb_survey", _no_pb)

    result = enumerate_bde()
    df = result.data

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in BDE_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"
    assert len(df) > 5, "implausibly few series in the ti chapter"

    # Real content in the declared columns — not just column names. These are
    # the metadata fields an agent/search relies on.
    assert df["key"].astype(str).str.len().gt(0).all(), "blank series code"
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert (df["source"] == "bde_biest").all(), "source routing metadata missing"
    assert (df["category"] == "Interest Rates").all(), "category not populated"
    # Frequency is translated from Spanish to English (LABORABLE -> Business Daily etc.).
    assert df["frequency"].astype(str).str.len().gt(0).any(), "frequency not populated"
    # Dates and observation counts ride along from the real CSV.
    assert df["start_date"].astype(str).str.len().gt(0).any(), "start_date not populated"
    assert df["n_obs"].astype(str).str.len().gt(0).any(), "n_obs not populated"

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = BDE_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "bde"


def test_bde_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``bde_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full ``build_bde_catalog()`` crawls all seven
    chapters and embeds thousands of rows. We build a 3-row catalog from real
    enumerator-shaped rows, persist it, and point ``catalog_url`` at it — so the
    search runs network-free and never triggers the expensive build.
    """
    rows = [
        {
            "key": "D_1NBAF472",
            "title": "One-year Euribor",
            "description": "One-year Euribor reference rate.",
            "source": "bde_biest",
            "alias": "TI_1_1.1",
            "dataset": "Interest rates › Euribor",
            "category": "Interest Rates",
            "frequency": "Monthly",
            "unit": "Percentage",
            "decimals": "3",
            "start_date": "ENE 1999",
            "end_date": "JUN 2026",
            "n_obs": "330",
            "source_org": "Banco de España",
        },
        {
            "key": "DTCCBCEUSDEUR.B",
            "title": "USD/EUR ECB spot rate",
            "description": "Exchange rate. US dollars per euro. Daily data.",
            "source": "bde_biest",
            "alias": "TC_1_1.1",
            "dataset": "Exchange rates › ECB spot rate",
            "category": "Exchange Rates",
            "frequency": "Business Daily",
            "unit": "US dollars per euro",
            "decimals": "4",
            "start_date": "ENE 1999",
            "end_date": "JUN 2026",
            "n_obs": "7000",
            "source_org": "European Central Bank",
        },
        {
            "key": "D_1KH90101",
            "title": "Consumer confidence index",
            "description": "Consumer confidence index for Spain.",
            "source": "bde_biest",
            "alias": "SI_1_1.2",
            "dataset": "General statistics › Opinion surveys",
            "category": "Financial Indicators",
            "frequency": "Monthly",
            "unit": "Net percentage",
            "decimals": "2",
            "start_date": "JUN 1986",
            "end_date": "MAR 2026",
            "n_obs": "478",
            "source_org": "Banco de España",
        },
    ]
    df = pd.DataFrame(rows, columns=[c.name for c in BDE_ENUMERATE_OUTPUT.columns])
    entries = entities_from_raw(df, BDE_ENUMERATE_OUTPUT)
    catalog = Catalog("bde", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "bde_catalog"
    catalog.save(out_dir)

    result = bde_search(query="one-year euribor", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="bde_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    # Bounded by the 3-row fixture — never the full catalog.
    assert len(sdf) <= 3
    # Real ranking: the Euribor entry is the top hit and scores are populated.
    assert sdf.iloc[0]["code"] == "D_1NBAF472"
    assert "Euribor" in sdf.iloc[0]["title"]
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    fx = bde_search(query="dollar euro exchange rate", limit=5, catalog_url=str(out_dir))
    assert fx.data.iloc[0]["code"] == "DTCCBCEUSDEUR.B"
