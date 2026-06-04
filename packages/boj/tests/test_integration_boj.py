"""Live integration tests for parsimony-boj (Bank of Japan).

Hits the real public BoJ Time-Series Data Search API
(``www.stat-search.boj.or.jp/api/v1``). BoJ is **keyless**, so these tests
need no env vars and run without secrets — there is no key to bind and no
secret that could leak (no ``assert_no_secret_leak`` needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/boj -m integration

**Bounded crawls only.** ``enumerate_boj`` normally fans out a ``getMetadata``
request to all 50 canonical BoJ databases behind Akamai (concurrency-capped,
browser UA, retries). The live enumerate test monkeypatches ``_BOJ_DATABASES``
down to a single small DB so the crawl fires exactly one request. The catalog
search tests build a tiny local fixture catalog and point ``catalog_root`` at
it, so they never trigger a cold full enumerate + embed.

**Akamai note.** From the dev/CI environment probed during the 0.7 sweep, the
BoJ endpoints returned HTTP 200 with both the default httpx UA and the browser
UA — no 403 observed. The connector still sends a browser UA on every request
as a safety measure; if a future network is Akamai-403'd even with it, that is
an environment limitation to flag, not a connector bug.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from parsimony.catalog.source import entities_from_raw
from parsimony_test_support import assert_provenance_shape

import parsimony_boj
from parsimony_boj import (
    BOJ_ENUMERATE_OUTPUT,
    boj_databases_search,
    boj_fetch,
    boj_series_search,
    enumerate_boj,
    fetch_boj_enumeration_rows_for_db,
)
from parsimony_boj.catalog_build import (
    build_databases_catalog,
    build_series_catalog,
    split_enumerated_entries,
)

pytestmark = pytest.mark.integration

# A single small DB bounds the live enumerate crawl to ONE getMetadata request.
# FM01 (Uncollateralized Overnight Call Rate) is ~6 metadata rows.
_BOUNDED_DB = ("FM01", "Financial Markets", "Uncollateralized Overnight Call Rate (Updated every business day)")


@pytest.mark.asyncio
async def test_boj_fetch_fx_rate_live() -> None:
    """FM08 / FXERD01 — BoJ USD/JPY spot rate — a stable public daily series."""
    result = await boj_fetch(db="FM08", code="FXERD01")

    assert_provenance_shape(result, expected_source="boj_fetch", required_param_keys=["code"])
    df = result.data
    assert not df.empty, "BoJ fetch of FM08/FXERD01 returned empty DataFrame"
    assert list(df["code"].unique()) == ["FXERD01"]
    # Real content, not just shape: title is real prose, values are real rates.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    # USD/JPY trades in the ~50-400 band over the series history.
    vals = df["value"].dropna()
    assert ((vals > 50) & (vals < 400)).all(), f"FX out of plausible range: {vals.tolist()[:5]}"
    # Survey dates parse to real datetimes (declared dtype="datetime").
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "record dates all NaT"


@pytest.mark.asyncio
async def test_boj_fetch_ranged_window_live() -> None:
    """A bounded period window (YYYYMM) returns real, recent observations.

    The daily FX series accepts a ``YYYYMM`` period range; a year-only or
    YYYYMMDD form is rejected by the provider as a period-format mismatch (a
    real per-series rule), so the universal bounded form here is YYYYMM.
    """
    result = await boj_fetch(db="FM08", code="FXERD01", start_date="202601", end_date="202606")

    df = result.data
    assert not df.empty, "ranged BoJ fetch returned empty"
    # Bounded window — far fewer rows than the full ~10k-row history.
    assert len(df) < 400, f"window not bounded: {len(df)} rows"
    assert df["value"].notna().any(), "no real values in the window"


@pytest.mark.asyncio
async def test_boj_fetch_multi_series_single_request_live() -> None:
    """Two real FX series fetched in one comma-joined request."""
    result = await boj_fetch(db="FM08", code="FXERD01,FXERD04")

    df = result.data
    assert {"FXERD01", "FXERD04"}.issubset(set(df["code"])), f"missing series: {set(df['code'])}"
    for serie in ("FXERD01", "FXERD04"):
        sub = df[df["code"] == serie]
        assert sub["value"].notna().any(), f"{serie} has no real values"


@pytest.mark.asyncio
async def test_boj_fetch_unknown_series_surfaces_provider_error_live() -> None:
    """BoJ returns HTTP 400 for an unknown series → typed ProviderError(400)."""
    from parsimony.errors import ProviderError

    with pytest.raises(ProviderError) as exc:
        await boj_fetch(db="FM08", code="NO_SUCH_SERIES_XYZ")
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_enumerate_boj_bounded_single_db_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real DB (FM01) to verify the live getMetadata shape + breadcrumb
    parsing without fanning out across all 50 databases."""
    monkeypatch.setattr(parsimony_boj, "_BOJ_DATABASES", (_BOUNDED_DB,))

    result = await enumerate_boj()
    df = result.data

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in BOJ_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1, "no series rows from the FM01 crawl"

    # Real content in declared metadata columns — not just column names.
    assert series_rows["code"].astype(str).str.len().gt(0).all(), "blank series code"
    assert series_rows["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert series_rows["description"].astype(str).str.len().gt(0).any(), "no description prose"
    assert (df["source"] == "stat_search").all(), "source routing metadata missing"
    assert series_rows["frequency"].astype(str).str.len().gt(0).any(), "frequency not populated"
    # Breadcrumb resolves to the section header NAME, never a bare integer ordinal.
    breadcrumbs = [str(b) for b in series_rows["breadcrumb"] if str(b).strip()]
    assert breadcrumbs, "no breadcrumb populated from real metadata"
    assert all(not b.strip().isdigit() for b in breadcrumbs), f"breadcrumb leaked an ordinal: {breadcrumbs[:3]}"

    # Exactly one synthetic db:<code> row for the single bounded DB.
    db_rows = df[df["entity_type"] == "db"]
    assert list(db_rows["code"]) == ["db:FM01"]

    # build_entities round-trips on the real slice.
    entities = BOJ_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert all(e.namespace == "boj" for e in entities)


@pytest.mark.asyncio
async def test_fetch_boj_enumeration_rows_for_db_live() -> None:
    """The single-DB metadata path returns a populated, schema-shaped frame."""
    df = await fetch_boj_enumeration_rows_for_db("FM01")

    assert list(df.columns) == [c.name for c in BOJ_ENUMERATE_OUTPUT.columns]
    assert not df.empty
    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1
    assert series_rows["title"].astype(str).str.len().gt(0).all()


async def _build_fixture_catalogs(tmp_path: Path) -> Path:
    """Build a tiny BoJ catalog from ONE real DB and persist the multi-bundle
    layout (``boj_databases`` + ``boj_series_fm01``) under ``tmp_path``.

    Bounded by design: one real getMetadata fetch (FM01), no full 50-DB sweep
    and no published-snapshot download.
    """
    df = await fetch_boj_enumeration_rows_for_db("FM01")
    entries = entities_from_raw(df, BOJ_ENUMERATE_OUTPUT)
    databases, series_by_db = split_enumerated_entries(entries)

    db_catalog = await build_databases_catalog(databases)
    await db_catalog.save(tmp_path / "boj_databases")

    series_entries = series_by_db.get("FM01") or []
    assert series_entries, "no FM01 series entries to build a series catalog"
    series_catalog = await build_series_catalog("FM01", series_entries)
    await series_catalog.save(tmp_path / "boj_series_fm01")
    return tmp_path


@pytest.mark.asyncio
async def test_boj_databases_search_over_fixture_catalog_live(tmp_path: Path) -> None:
    """Exercise ``boj_databases_search`` end-to-end over a small local catalog
    built from one real DB — never the published snapshot, never a full build."""
    root = await _build_fixture_catalogs(tmp_path)

    result = await boj_databases_search(query="overnight call rate", limit=5, catalog_root=str(root))

    assert_provenance_shape(result, expected_source="boj_databases_search", required_param_keys=["query"])
    df = result.data
    assert list(df.columns) == ["db", "title", "score", "category", "series_namespace"]
    assert not df.empty, "databases search returned nothing"
    # The single bounded DB is the top hit and dispatch hints are populated.
    assert df.iloc[0]["db"] == "FM01"
    assert df.iloc[0]["series_namespace"] == "boj_series_fm01"
    assert df["score"].notna().all(), "search scores not populated"


@pytest.mark.asyncio
async def test_boj_series_search_over_fixture_catalog_live(tmp_path: Path) -> None:
    """Exercise ``boj_series_search`` (step 2) over the same local fixture."""
    root = await _build_fixture_catalogs(tmp_path)

    result = await boj_series_search(
        query="uncollateralized overnight call rate", db="FM01", limit=5, catalog_root=str(root)
    )

    assert_provenance_shape(result, expected_source="boj_series_search", required_param_keys=["query"])
    df = result.data
    assert list(df.columns) == ["code", "title", "score", "db"]
    assert not df.empty, "series search returned nothing"
    assert (df["db"] == "FM01").all()
    # Codes returned are real BoJ series codes usable by boj_fetch.
    assert df["code"].astype(str).str.len().gt(0).all()
    assert df["score"].notna().all()
