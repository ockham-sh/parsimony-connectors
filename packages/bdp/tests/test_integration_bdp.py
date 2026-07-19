"""Live integration tests for parsimony-bdp (Banco de Portugal).

Hits the real public BPstat JSON-stat API (``bpstat.bportugal.pt``). BdP is
**keyless**, so these tests need no env vars and run without secrets.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/bdp -m integration

**Bounded crawls only.** A full ``enumerate_bdp`` walks ~720 dataset pages /
~72 K series behind Akamai throttling. The live enumerate test monkeypatches the
module-level ``_list_domains`` down to a single tiny leaf domain (id 48,
"Coincident indicators": 1 dataset / 2 series) so the crawl fires only a handful
of requests, and a request counter asserts the bound held. ``bdp_search`` is
covered against a locally-built fixture catalog, never the published snapshot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result
from parsimony_test_support import assert_provenance_shape

from parsimony_bdp.connectors import enumerate as enum_mod
from parsimony_bdp.connectors.enumerate import enumerate_bdp
from parsimony_bdp.connectors.fetch import bdp_fetch
from parsimony_bdp.outputs import BDP_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS
from parsimony_bdp.search import bdp_search

pytestmark = pytest.mark.integration

# Smallest leaf domain on BPstat — "Coincident indicators": 1 dataset, 2 series.
_BOUNDED_DOMAIN_ID = 48
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
    df = result.raw
    assert not df.empty, "BdP fetch returned an empty DataFrame"
    assert df["series_id"].nunique() >= 1
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
    df = result.raw
    assert set(df["series_id"]) == {series_id}, f"filter not honoured: {set(df['series_id'])}"
    assert df["value"].notna().any()


def test_enumerate_bdp_bounded_single_domain_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real tiny domain to verify the live JSON-stat shape + two-level
    pagination without the full fan-out. A request counter asserts the bound."""
    monkeypatch.setattr(enum_mod, "_list_domains", _bounded_domains)

    from parsimony_shared import cb_enumerate

    real_get_json = cb_enumerate.ThrottledJsonFetcher.get_json
    calls: list[str] = []

    def _counting_get_json(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(url)
        return real_get_json(self, url, *args, **kwargs)

    monkeypatch.setattr(cb_enumerate.ThrottledJsonFetcher, "get_json", _counting_get_json)

    result = enumerate_bdp()
    df = result.raw

    # The bound held: domain 48 → 1 datasets-list page + 1 detail page. A
    # handful, never ~720.
    assert len(calls) < 25, f"bounded crawl fired {len(calls)} requests — bound did not hold"

    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert not df.empty, "bounded enumerate returned no rows"
    assert set(df["entity_type"]) >= {"domain", "dataset", "series"}

    series = df[df["entity_type"] == "series"]
    assert len(series) >= 1, "no series rows from the live crawl"
    assert series["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert series["description"].astype(str).str.contains("Banco de Portugal").any()
    assert (df["source"] == "bpstat").all()
    # KEY shape for series rows: "{domain}:{dataset}:{series}".
    assert series["code"].str.startswith(f"{_BOUNDED_DOMAIN_ID}:").all()

    entities = list(Result(raw=df, output_spec=BDP_ENUMERATE_OUTPUT).entities.values())
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdp"


def test_bdp_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``bdp_search`` end-to-end over a small, locally-built catalog
    (network-free — never triggers the expensive cold build)."""
    cols = list(ENUMERATE_COLUMNS)

    def _row(code: str, title: str, description: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(code=code, title=title, description=description, entity_type="series", source="bpstat")
        return base

    rows = [
        _row(
            "48:ds1:12099329",
            "Economic activity coincident indicator",
            "Economic activity coincident indicator. Coincident indicators - Portugal - Monthly. Banco de Portugal.",
        ),
        _row(
            "12:ds2:55501",
            "Harmonised index of consumer prices",
            "Harmonised index of consumer prices (HICP) for Portugal. Inflation. Monthly.",
        ),
        _row(
            "5:ds3:88812",
            "Current account balance",
            "Balance of payments. Current account balance. Quarterly external statistics.",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = list(Result(raw=df, output_spec=BDP_ENUMERATE_OUTPUT).entities.values())
    catalog = Catalog("bdp", indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "bdp_catalog"
    catalog.save(out_dir)

    result = bdp_search(query="economic activity coincident indicator", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="bdp_search", required_param_keys=["query"])
    sdf = result.raw
    assert list(sdf.columns) == ["code", "title", "coverage", "score", "matched"]
    assert not sdf.empty
    assert sdf.iloc[0]["code"] == "48:ds1:12099329"
    assert sdf["score"].notna().all()

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    inflation = bdp_search(query="consumer price inflation HICP", limit=5, catalog_url=str(out_dir))
    assert inflation.raw.iloc[0]["code"] == "12:ds2:55501"
