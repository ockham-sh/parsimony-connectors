"""Tests for the SNB catalog index configuration.

Pins the discovery-index policy: a BM25 ``code`` index for exact lookups and
adaptive ``title`` / ``description`` indexes, with ``title`` as the default search
field. (On a small sample the adaptive fields are Hybrid; the real catalog has
>1000 unique titles, so the live policy degrades title/description to BM25-only —
handled in queries.yaml.)
"""

from __future__ import annotations

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_snb.catalog_build import CATALOG_NAMESPACE
from parsimony_snb.outputs import _ENUMERATE_COLUMNS, SNB_ENUMERATE_OUTPUT


def _sample_entries() -> list:
    base = {name: "" for name in _ENUMERATE_COLUMNS}
    rows = [
        {
            **base,
            "code": "rendoblim#10J",
            "title": "10 years — Yields on bond issues",
            "description": "Interest rates and exchange rates. 10 years.",
            "source": "snb_data_portal",
            "cube_id": "rendoblim",
            "series_key": "10J",
            "category": "Interest rates and exchange rates",
            "frequency": "Monthly",
            "unit": "In percent",
        },
        {
            **base,
            "code": "BSTA@SNB.AUR_U.ODF#",
            "title": "Outstanding derivative financial instruments",
            "description": "Annual banking statistics. Outstanding derivative financial instruments.",
            "source": "snb_warehouse",
            "cube_id": "BSTA@SNB.AUR_U.ODF",
            "category": "Annual banking statistics",
            "frequency": "Unknown",
        },
    ]
    df = pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))
    return entities_from_raw(df, SNB_ENUMERATE_OUTPUT)


def test_discovery_indexes_for_snb_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes, default_field="title")

    assert catalog.name == "snb"
    assert catalog.default_field == "title"
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_dispatch_metadata() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "snb" for e in entries)
    wh = next(e for e in entries if e.code == "BSTA@SNB.AUR_U.ODF#")
    # the source dispatch column survives onto the entity for agent routing
    assert wh.metadata.get("source") == "snb_warehouse"
    assert wh.metadata.get("cube_id") == "BSTA@SNB.AUR_U.ODF"
