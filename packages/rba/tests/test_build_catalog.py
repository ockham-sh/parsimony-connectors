"""Tests for the RBA catalog index configuration.

Pins the discovery-index policy: a BM25 ``code`` index for exact lookups and adaptive
``title`` / ``description`` indexes, with ``title`` as the default search field. (On a
small sample the adaptive fields are Hybrid; the real catalog has >1000 unique titles,
so the live policy degrades title/description to BM25-only — handled in queries.yaml.)
"""

from __future__ import annotations

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result

from parsimony_rba.catalog_build import CATALOG_NAMESPACE
from parsimony_rba.outputs import _ENUMERATE_COLUMNS, RBA_ENUMERATE_OUTPUT


def _sample_entries() -> list:
    base = {name: "" for name in _ENUMERATE_COLUMNS}
    rows = [
        {
            **base,
            "code": "f1-data#FIRMMCRTD",
            "title": "Cash Rate Target",
            "description": "Official cash rate target set by the RBA Board.",
            "source": "rba_csv",
            "table_id": "f1-data",
            "series_id": "FIRMMCRTD",
            "category": "Interest Rates and Yields",
            "frequency": "Daily",
        },
        {
            **base,
            "code": "a03/Bond Purchase Program#ALDBPPFVD",
            "title": "Bond Purchase Program — Face Value",
            "description": "Face value of bonds purchased under the Bond Purchase Program.",
            "source": "rba_xlsx",
            "table_id": "a03/Bond Purchase Program",
            "series_id": "ALDBPPFVD",
            "category": "Reserve Bank",
        },
    ]
    df = pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))
    return list(Result(raw=df, output_spec=RBA_ENUMERATE_OUTPUT).entities.values())


def test_discovery_indexes_for_rba_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes)

    assert catalog.name == "rba"
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_dispatch_metadata() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "rba" for e in entries)
    bpp = next(e for e in entries if e.code == "a03/Bond Purchase Program#ALDBPPFVD")
    # the source dispatch column survives onto the entity for agent routing
    assert bpp.metadata.get("source") == "rba_xlsx"
    assert bpp.metadata.get("table_id") == "a03/Bond Purchase Program"
