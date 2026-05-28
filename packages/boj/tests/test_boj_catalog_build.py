"""Tests for BoJ multi-bundle catalog assembly helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from parsimony.catalog import BM25Index, Entity, HybridIndex

from parsimony_boj import catalog_build
from parsimony_boj.catalog_build import (
    DATABASES_NAMESPACE,
    series_indexes,
    series_namespace,
    split_enumerated_entries,
)
from parsimony_boj.catalog_policy import macro_discovery_indexes


def _flat_entries() -> list[Entity]:
    return [
        Entity(
            namespace="boj",
            code="db:FM08",
            title="Foreign Exchange Rates",
            metadata={"entity_type": "db", "db": "FM08", "category": "Financial Markets"},
        ),
        Entity(
            namespace="boj",
            code="FXERD01",
            title="JPY/USD Spot Rate",
            metadata={"entity_type": "series", "db": "FM08", "description": "Tokyo closing rate."},
        ),
        Entity(
            namespace="boj",
            code="db:IR01",
            title="Basic Discount Rate",
            metadata={"entity_type": "db", "db": "IR01", "category": "Interest Rates"},
        ),
    ]


def test_series_namespace_is_lowercase() -> None:
    assert series_namespace("FM08") == "boj_series_fm08"


def test_split_enumerated_entries_partitions_databases_and_series() -> None:
    databases, series_by_db = split_enumerated_entries(_flat_entries())

    assert len(databases) == 2
    assert databases[0].namespace == DATABASES_NAMESPACE
    assert databases[0].code == "FM08"
    assert databases[0].metadata["entity_type"] == "db"

    assert set(series_by_db) == {"FM08"}
    series = series_by_db["FM08"]
    assert len(series) == 1
    assert series[0].code == "FXERD01"
    assert series[0].namespace == "boj_series_fm08"


def test_databases_indexes_use_hybrid_title_and_description() -> None:
    databases, _ = split_enumerated_entries(_flat_entries())
    indexes = macro_discovery_indexes(databases, include_description=True)
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


def test_series_indexes_are_field_keyed_hybrids() -> None:
    _, series_by_db = split_enumerated_entries(_flat_entries())
    indexes = series_indexes(series_by_db["FM08"])
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)


@pytest.mark.asyncio
async def test_build_series_catalog_for_db_does_not_call_full_enumerate() -> None:
    # ``build_series_catalog`` is mocked: this test pins the per-db fetch
    # path, not catalog construction itself (which would pull a real
    # sentence-transformers embedder via the hybrid title/description
    # indexes). Catalog assembly is exercised by the other tests in this
    # module via the pure helpers, and end-to-end via ``catalog_tests/``.
    fake_catalog = SimpleNamespace(name="boj_series_fm08")
    with (
        patch("parsimony_boj.fetch_boj_enumeration_rows_for_db", new_callable=AsyncMock) as fetch_one,
        patch("parsimony_boj.enumerate_boj", new_callable=AsyncMock) as enumerate_all,
        patch.object(catalog_build, "build_series_catalog", new_callable=AsyncMock) as build_inner,
    ):
        fetch_one.return_value = pd.DataFrame(
            [
                {
                    "code": "FXERD01",
                    "title": "JPY/USD",
                    "description": "rate",
                    "db": "FM08",
                    "db_title": "FX",
                    "entity_type": "series",
                    "frequency": "",
                    "unit": "",
                    "category": "",
                    "breadcrumb": "",
                    "start_date": "",
                    "end_date": "",
                    "last_update": "",
                    "source": "stat_search",
                }
            ]
        )
        build_inner.return_value = fake_catalog

        catalog = await catalog_build.build_boj_series_catalog_for_db("FM08")

    enumerate_all.assert_not_called()
    fetch_one.assert_awaited_once_with("FM08")
    build_inner.assert_awaited_once()
    assert build_inner.await_args is not None
    db_arg, rows_arg = build_inner.await_args.args
    assert db_arg == "FM08"
    assert [row.code for row in rows_arg] == ["FXERD01"]
    assert catalog is fake_catalog
