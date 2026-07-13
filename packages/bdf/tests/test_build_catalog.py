"""Tests for the BdF catalog index configuration.

Pins the discovery-index policy the catalog is built with: a BM25 ``code`` index
for exact lookups and hybrid (BM25 + vector) ``title`` / ``description`` indexes
for lexical + semantic recall, with ``title`` as the default search field.
"""

from __future__ import annotations

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result

from parsimony_bdf.catalog_build import CATALOG_NAMESPACE
from parsimony_bdf.outputs import BDF_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS


def _sample_entries() -> list:
    base = {name: "" for name in ENUMERATE_COLUMNS}
    rows = [
        {
            **base,
            "code": "EXR.M.USD.EUR.SP00.E",
            "title": "US dollar/Euro ECB reference exchange rate",
            "description": "US dollar (USD)/Euro (EUR) reference exchange rate, monthly.",
            "entity_type": "series",
            "dataset_id": "EXR",
            "frequency": "M",
        },
        {
            **base,
            "code": "dataset:EXR",
            "title": "Exchange rates",
            "description": "Euro foreign exchange reference rates.",
            "entity_type": "dataset",
            "dataset_id": "EXR",
        },
    ]
    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
    return Result(data=df, output_spec=BDF_ENUMERATE_OUTPUT).to_entities()


def test_discovery_indexes_for_bdf_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes, default_field="title")

    assert catalog.name == "bdf"
    assert catalog.default_field == "title"
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_metadata() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "bdf" for e in entries)
    series = next(e for e in entries if e.code == "EXR.M.USD.EUR.SP00.E")
    assert series.metadata.get("entity_type") == "series"
    assert series.metadata.get("frequency") == "M"
