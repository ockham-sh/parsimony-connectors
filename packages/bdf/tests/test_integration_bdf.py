"""Live integration tests for parsimony-bdf (Banque de France Webstat).

Hits the real Webstat Opendatasoft API
(``webstat.banque-france.fr/api/explore/v2.1``), which **requires an API key**
(``Authorization: Apikey <KEY>``) supplied via ``BDF_API_KEY``. Live-verified
2026-06-08: the key, the ``observations`` fetch path, and the ``series`` /
``webstat-datasets`` export shapes all execute against the production API.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly (with the key exported) via::

    set -a; . /home/espinet/ockham/.env; set +a
    uv run pytest packages/bdf -m integration

**Bounded crawls only.** A full ``enumerate_bdf`` streams the whole ~41.6k-row
``series`` table. The live test monkeypatches the ``_list_all_series`` seam to a
single small dataset (``PAI``, 3 series) so it verifies the real export shape
without pulling the universe. ``bdf_search`` runs against a locally-built fixture
catalog so it never triggers a cold full enumerate + embed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_bdf._http import BASE_URL, SERIES_PATH, SERIES_SELECT
from parsimony_bdf.connectors import enumerate as enum_mod
from parsimony_bdf.connectors.enumerate import enumerate_bdf
from parsimony_bdf.connectors.fetch import bdf_fetch
from parsimony_bdf.outputs import BDF_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS
from parsimony_bdf.search import bdf_search

pytestmark = pytest.mark.integration

# A stable, high-traffic monthly BdF series — ECB USD/Euro reference rate.
_KNOWN_KEY = "EXR.M.USD.EUR.SP00.E"
# A tiny dataset to bound the live enumerate crawl (3 series).
_BOUNDED_DATASET = "PAI"


def test_bdf_fetch_known_series_live() -> None:
    creds = require_env("BDF_API_KEY")
    bound = bdf_fetch.bind(api_key=creds["BDF_API_KEY"])

    result = bound(key=_KNOWN_KEY, start_period="2020-01-01", end_period="2023-12-31")

    assert_provenance_shape(result, expected_source="bdf_fetch", required_param_keys=["key"])
    df = result.data
    assert not df.empty, "BdF fetch returned an empty DataFrame"
    assert list(df["key"].unique()) == [_KNOWN_KEY]
    # Real content, not just shape.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].dropna().nunique() > 1, "values implausibly constant"
    # FX rate magnitude sanity-check.
    vals = df["value"].dropna()
    assert ((vals > 0) & (vals < 10)).all(), f"FX rates out of plausible range: {vals.tolist()[:5]}"
    # Dates parse to real datetimes.
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "record dates all NaT"

    assert_no_secret_leak(result, secret=creds["BDF_API_KEY"])


def test_enumerate_bdf_bounded_single_dataset_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl ONE real dataset to verify the live series-export shape without
    streaming all ~41.6k series."""
    creds = require_env("BDF_API_KEY")

    def _bounded_series(fetcher: Any) -> Any:
        url = f"{BASE_URL}/{SERIES_PATH}"
        return fetcher.get_json(
            url,
            params={"select": SERIES_SELECT, "refine": f"dataset_id:{_BOUNDED_DATASET}"},
            label="series",
        )

    monkeypatch.setattr(enum_mod, "_list_all_series", _bounded_series)

    result = enumerate_bdf.bind(api_key=creds["BDF_API_KEY"])()
    df = result.data

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert not df.empty, "bounded enumerate returned no rows"
    # The real (unbounded) dataflow list still loads — 45 stubs.
    assert (df["entity_type"] == "dataset").sum() > 1, "dataflow stubs not emitted"
    series = df[df["entity_type"] == "series"]
    assert len(series) >= 1, "no series in the bounded dataset"

    # Real content in the declared columns — not just column names.
    assert series["code"].astype(str).str.len().gt(0).all(), "blank series code"
    assert series["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert series["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert series["frequency"].astype(str).str.len().gt(0).any(), "frequency not populated"
    assert series["path"].astype(str).str.len().gt(0).any(), "breadcrumb path not populated"

    # entities_from_raw round-trips on the real slice.
    entities = entities_from_raw(df, BDF_ENUMERATE_OUTPUT)
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdf"

    assert_no_secret_leak(result, secret=creds["BDF_API_KEY"])


def test_bdf_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``bdf_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full build streams ~41.6k rows and embeds them. We
    build a 3-row catalog from real enumerator-shaped rows, persist it, and point
    ``catalog_url`` at it — so the search runs network-free and never triggers the
    expensive build. (This test does not need the key, but stays in the
    integration suite alongside the keyed ones.)
    """
    require_env("BDF_API_KEY")  # keep the bounded-search test gated with the rest

    base = {name: "" for name in ENUMERATE_COLUMNS}
    rows = [
        {
            **base,
            "code": "EXR.M.USD.EUR.SP00.E",
            "title": "US dollar/Euro spot exchange rate",
            "description": "US dollar (USD)/Euro (EUR) spot exchange rate, monthly average.",
            "entity_type": "series",
            "dataset_id": "EXR",
            "frequency": "M",
        },
        {
            **base,
            "code": "ICP.M.FR.N.000000.4.ANR",
            "title": "France HICP all-items annual rate of change",
            "description": "Harmonised index of consumer prices, France, annual rate of change.",
            "entity_type": "series",
            "dataset_id": "ICP",
            "frequency": "M",
        },
        {
            **base,
            "code": "RPP.Q.FR.N.A.D.00.0.0.0",
            "title": "France residential property prices",
            "description": "Residential property price index for France, quarterly.",
            "entity_type": "series",
            "dataset_id": "RPP",
            "frequency": "Q",
        },
    ]
    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
    entries = entities_from_raw(df, BDF_ENUMERATE_OUTPUT)
    catalog = Catalog("bdf", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "bdf_catalog"
    catalog.save(out_dir)

    result = bdf_search(query="dollar euro exchange rate", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="bdf_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3
    assert sdf.iloc[0]["code"] == "EXR.M.USD.EUR.SP00.E"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking discriminates: a different query surfaces a different top hit.
    infl = bdf_search(query="consumer prices annual rate of change", limit=5, catalog_url=str(out_dir))
    assert infl.data.iloc[0]["code"] == "ICP.M.FR.N.000000.4.ANR"
