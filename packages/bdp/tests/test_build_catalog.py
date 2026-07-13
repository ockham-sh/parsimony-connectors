"""Tests for the BdP catalog index configuration.

Pins the discovery-index policy the catalog is built with: a BM25 ``code`` index
for exact lookups and hybrid (BM25 + vector) ``title`` / ``description`` indexes
for lexical + semantic recall, with ``title`` as the default search field.
"""

from __future__ import annotations

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result

from parsimony_bdp.catalog_build import CATALOG_NAMESPACE
from parsimony_bdp.outputs import BDP_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS


def _sample_entries() -> list:
    base = dict.fromkeys(ENUMERATE_COLUMNS, "")
    rows = [
        {
            **base,
            "code": "48:ds1:12099329",
            "title": "Economic activity coincident indicator",
            "description": "Coincident indicators - Economic activity - Portugal - Monthly.",
            "entity_type": "series",
            "domain_id": "48",
            "dataset_id": "ds1",
            "source": "bpstat",
        },
        {
            **base,
            "code": "dataset:48:ds1",
            "title": "Coincident indicators dataset",
            "description": "Banco de Portugal dataset under domain 'Coincident indicators'.",
            "entity_type": "dataset",
            "domain_id": "48",
            "dataset_id": "ds1",
            "source": "bpstat",
        },
    ]
    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
    return Result(data=df, output_spec=BDP_ENUMERATE_OUTPUT).to_entities()


def test_discovery_indexes_for_bdp_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes, default_field="title")

    assert catalog.name == "bdp"
    assert catalog.default_field == "title"
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_metadata() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "bdp" for e in entries)
    series = next(e for e in entries if e.code == "48:ds1:12099329")
    assert series.metadata.get("entity_type") == "series"
    assert series.metadata.get("domain_id") == "48"
