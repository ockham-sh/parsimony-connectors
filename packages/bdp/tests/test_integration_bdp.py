"""Live integration tests for parsimony-bdp (Banco de Portugal).

Hits the real public BPstat JSON-stat API (``bpstat.bportugal.pt``). BdP is
**keyless**, so these tests need no env vars and run without secrets — there is
no key to bind and no secret that could leak.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/bdp -m integration

**Bounded crawls only.** A full ``enumerate_bdp`` walks ~7,200 dataset-detail
pages / ~72K series behind Akamai throttling. The live enumerate test
monkeypatches the module-level ``_list_domains`` down to a single tiny leaf
domain (id 48, "Coincident indicators": 1 dataset / 2 series) so the crawl
fires only a handful of requests and verifies the live JSON-stat shape without
the full fan-out. A request counter wrapped around the fetcher asserts the
bound held. ``bdp_search`` is covered against a locally-built 3-row fixture
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

import parsimony_bdp
from parsimony_bdp import BDP_ENUMERATE_OUTPUT, bdp_fetch, enumerate_bdp
from parsimony_bdp.search import bdp_search

pytestmark = pytest.mark.integration

# Smallest leaf domain on BPstat — "Coincident indicators": 1 dataset, 2 series.
_BOUNDED_DOMAIN_ID = 48
# The single dataset under domain 48 (stable content-addressed id).
_DOMAIN_48_DATASET = "aea9d7f70ddf9c6de29feaeba86a9456"


def _bounded_domains(fetcher: Any) -> list[dict[str, Any]]:
    """Return ONLY domain 48 — bounds the live enumerate to one tiny domain."""
    return [
        {
            "id": _BOUNDED_DOMAIN_ID,
            "label": "Coincident indicators",
            "description": "Coincident activity indicators",
            "has_series": True,
            "num_series": 2,
            "num_datasets": 1,
            "obs_updated_at": "",
        }
    ]


def test_bdp_fetch_known_dataset_live() -> None:
    """Fetch the real domain-48 dataset and assert real numeric content."""
    result = bdp_fetch(
        domain_id=_BOUNDED_DOMAIN_ID,
        dataset_id=_DOMAIN_48_DATASET,
        start_date="2020-01-01",
    )

    assert_provenance_shape(result, expected_source="bdp_fetch", required_param_keys=["dataset_id"])
    df = result.data
    assert not df.empty, "BdP fetch returned an empty DataFrame"
    # Two coincident-indicator series in this dataset.
    assert df["series_id"].nunique() >= 1
    # Real content, not just shape.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "record dates all NaT"


def test_bdp_fetch_series_filter_live() -> None:
    """The ``series_ids`` filter narrows the result to a single series live."""
    series_id = "12099329"  # Economic activity coincident indicator (yoy).
    result = bdp_fetch(
        domain_id=_BOUNDED_DOMAIN_ID,
        dataset_id=_DOMAIN_48_DATASET,
        series_ids=series_id,
        start_date="2024-01-01",
    )
    df = result.data
    assert set(df["series_id"]) == {series_id}, f"filter not honoured: {set(df['series_id'])}"
    assert df["value"].notna().any()


def test_enumerate_bdp_bounded_single_domain_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real tiny domain to verify the live JSON-stat shape without
    pulling the full ~7,200-page fan-out. A request counter asserts the bound."""
    monkeypatch.setattr(parsimony_bdp, "_list_domains", _bounded_domains)

    # Instrument the shared fetcher so we can assert the bound actually held —
    # one tiny domain (1 dataset, 2 series) should fire only a handful of GETs,
    # never thousands.
    from parsimony_shared import cb_enumerate

    real_get_json = cb_enumerate.ThrottledJsonFetcher.get_json
    calls: list[str] = []

    def _counting_get_json(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(url)
        return real_get_json(self, url, *args, **kwargs)

    monkeypatch.setattr(cb_enumerate.ThrottledJsonFetcher, "get_json", _counting_get_json)

    result = enumerate_bdp()
    df = result.data

    # The bound held: domain 48 → 1 dataset list + 1 detail page + 1 PT sweep.
    # Allow generous slack for pagination, but it must be a handful, not 7,200.
    assert len(calls) < 25, f"bounded crawl fired {len(calls)} requests — bound did not hold"

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in BDP_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"
    assert set(df["entity_type"]) >= {"domain", "dataset", "series"}

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1, "no series rows from the live crawl"
    # Real content in declared columns — not just column names.
    assert series_rows["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert series_rows["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert (df["source"] == "bpstat").all(), "source metadata missing"
    # KEY shape for series rows: "{domain}:{dataset}:{series}".
    assert series_rows["code"].str.startswith(f"{_BOUNDED_DOMAIN_ID}:").all()

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = BDP_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdp"


def test_bdp_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``bdp_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full ``build_bdp_catalog()`` crawls all 65 leaf
    domains and embeds ~72K rows. We build a 3-row catalog from real
    enumerator-shaped rows, persist it, and point ``catalog_url`` at it — so the
    search runs network-free and never triggers the expensive build.
    """
    cols = [c.name for c in BDP_ENUMERATE_OUTPUT.columns]

    def _row(code: str, title: str, description: str, **over: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(
            code=code,
            title=title,
            description=description,
            entity_type="series",
            source="bpstat",
        )
        base.update(over)
        return base

    rows = [
        _row(
            "48:ds1:12099329",
            "Economic activity coincident indicator",
            "Economic activity coincident indicator. Monthly. Banco de Portugal coincident indicators.",
            frequency="Monthly",
        ),
        _row(
            "12:ds2:55501",
            "Harmonised index of consumer prices",
            "Harmonised index of consumer prices (HICP) for Portugal. Inflation. Monthly.",
            frequency="Monthly",
        ),
        _row(
            "5:ds3:88812",
            "Current account balance",
            "Balance of payments. Current account balance. Quarterly external statistics.",
            frequency="Quarterly",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = entities_from_raw(df, BDP_ENUMERATE_OUTPUT)
    catalog = Catalog("bdp", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "bdp_catalog"
    catalog.save(out_dir)

    result = bdp_search(query="economic activity coincident indicator", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="bdp_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search exceeded the fixture catalog"
    # Real ranking: the coincident-indicator entry is the top hit; scores present.
    assert sdf.iloc[0]["code"] == "48:ds1:12099329"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    inflation = bdp_search(query="consumer price inflation HICP", limit=5, catalog_url=str(out_dir))
    assert inflation.data.iloc[0]["code"] == "12:ds2:55501"
