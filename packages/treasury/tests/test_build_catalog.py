"""Tests for the Treasury catalog index configuration.

Pins the discovery-index policy: a BM25 ``code`` index for exact lookups and adaptive
``title`` / ``description`` indexes, with ``title`` as the default search field. (On a small
sample the adaptive fields are Hybrid; the real catalog has >1000 unique titles, so the
live policy degrades title to BM25-only — handled in queries.yaml.)
"""

from __future__ import annotations

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result

from parsimony_treasury.catalog_build import CATALOG_NAMESPACE
from parsimony_treasury.outputs import _ENUMERATE_COLUMNS, TREASURY_ENUMERATE_OUTPUT


def _sample_entries() -> list:
    base = {name: "" for name in _ENUMERATE_COLUMNS}
    rows = [
        {
            **base,
            "code": "v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt",
            "title": "Total Public Debt Outstanding — Debt to the Penny",
            "description": "Total federal debt outstanding to the penny.",
            "source": "fiscal_data",
            "endpoint": "v2/accounting/od/debt_to_penny",
            "field": "tot_pub_debt_out_amt",
            "data_type": "CURRENCY",
            "dataset": "Debt to the Penny",
            "category": "Bureau of the Fiscal Service",
            "frequency": "Daily",
        },
        {
            **base,
            "code": "home/daily_treasury_yield_curve#BC_10YEAR",
            "title": "10 Year — Daily Treasury Par Yield Curve Rates",
            "description": "10 Year constant-maturity Treasury par yield curve rate.",
            "source": "treasury_rates",
            "endpoint": "home/daily_treasury_yield_curve",
            "field": "BC_10YEAR",
            "data_type": "PERCENTAGE",
            "dataset": "Daily Treasury Par Yield Curve Rates",
            "category": "Office of Debt Management",
            "frequency": "Daily",
        },
    ]
    df = pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))
    return list(Result(raw=df, output_spec=TREASURY_ENUMERATE_OUTPUT).entities.values())


def test_discovery_indexes_for_treasury_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes)

    assert catalog.name == "treasury"
    assert {"code", "title", "description"} <= set(indexes)
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_dispatch_metadata() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "treasury" for e in entries)
    odm = next(e for e in entries if e.code == "home/daily_treasury_yield_curve#BC_10YEAR")
    # the source dispatch column survives onto the entity for agent routing
    assert odm.metadata.get("source") == "treasury_rates"
    assert odm.metadata.get("endpoint") == "home/daily_treasury_yield_curve"
