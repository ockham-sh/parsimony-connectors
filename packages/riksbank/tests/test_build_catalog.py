"""Tests for the Riksbank catalog index configuration + cross-family routing metadata.

Pins the discovery-index policy (a BM25 ``code`` index for exact lookups, adaptive
``title`` / ``description`` indexes, ``title`` the default field) and checks that the
``source`` dispatch column survives onto each entity so an agent can route a search hit
back to the right fetch verb across all five families.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from parsimony.catalog import BM25Index, Catalog, HybridIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result

from parsimony_riksbank.catalog_build import CATALOG_NAMESPACE
from parsimony_riksbank.outputs import ENUMERATE_COLUMNS, RIKSBANK_ENUMERATE_OUTPUT


def _row(code: str, title: str, description: str, source: str) -> dict[str, Any]:
    base: dict[str, Any] = dict.fromkeys(ENUMERATE_COLUMNS, "")
    base.update(code=code, title=title, description=description, source=source, series_closed=False)
    return base


def _sample_entries() -> list:
    rows = [
        _row("SEKEURPMI", "EUR — euro mid rate", "Daily EUR/SEK mid exchange rate.", "swea"),
        _row("SWESTR", "SWESTR — Swedish Krona Short-Term Rate", "Overnight reference rate.", "swestr"),
        _row("monetary_policy/SEQGDPNAYSA", "GDP (Annual % change)", "GDP forecast vintages.", "monetary_policy"),
        _row("turnover/fx/monthly", "Turnover — Foreign exchange (monthly)", "FX market turnover.", "turnover"),
        _row("holdings/swedish_securities_aggregated", "Holdings aggregated", "Securities holdings.", "holdings"),
    ]
    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
    return list(Result(raw=df, output_spec=RIKSBANK_ENUMERATE_OUTPUT).entities.values())


def test_discovery_indexes_for_riksbank_sample() -> None:
    entries = _sample_entries()
    indexes = discovery_indexes(entries)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=indexes, default_field="title")

    assert catalog.name == "riksbank"
    assert catalog.default_field == "title"
    assert {"code", "title", "description"} <= set(indexes)
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_entities_carry_namespace_and_source_dispatch() -> None:
    entries = _sample_entries()
    assert all(e.namespace == "riksbank" for e in entries)
    by_code = {e.code: e for e in entries}
    assert by_code["monetary_policy/SEQGDPNAYSA"].metadata.get("source") == "monetary_policy"
    assert by_code["turnover/fx/monthly"].metadata.get("source") == "turnover"
    assert by_code["holdings/swedish_securities_aggregated"].metadata.get("source") == "holdings"
