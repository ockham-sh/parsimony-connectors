"""Tests for BoJ multi-bundle catalog assembly helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from parsimony.catalog import BM25Index, HybridIndex
from parsimony.result import Result

from parsimony_boj import BOJ_ENUMERATE_OUTPUT, catalog_build
from parsimony_boj.catalog_build import (
    DATABASES_NAMESPACE,
    entities_from_boj_enumeration,
    series_indexes,
    series_namespace,
    split_enumerated_entries,
)
from parsimony_boj.catalog_policy import discovery_indexes


def _flat_rows() -> pd.DataFrame:
    blank = {
        "description": "",
        "db_title": "",
        "frequency": "",
        "unit": "",
        "category": "",
        "breadcrumb": "",
        "start_date": "",
        "end_date": "",
        "last_update": "",
        "source": "stat_search",
    }
    return pd.DataFrame(
        [
            {"code": "db:FM08", "title": "Foreign Exchange Rates", "db": "FM08", "entity_type": "db", **blank},
            {
                "code": "FXERD01",
                "title": "JPY/USD Spot Rate",
                "db": "FM08",
                "entity_type": "series",
                **{**blank, "description": "Tokyo closing rate."},
            },
            {"code": "db:IR01", "title": "Basic Discount Rate", "db": "IR01", "entity_type": "db", **blank},
        ]
    )


def _flat_entries():
    return list(Result(raw=_flat_rows(), output_spec=BOJ_ENUMERATE_OUTPUT).entities.values())


def test_series_namespace_is_lowercase() -> None:
    assert series_namespace("FM08") == "boj_series_fm08"


def test_entities_from_boj_enumeration_dedupes_duplicate_codes_within_db() -> None:
    blank = {
        "title": "Shared series",
        "db_title": "",
        "frequency": "",
        "unit": "",
        "category": "",
        "breadcrumb": "",
        "start_date": "",
        "end_date": "",
        "last_update": "",
        "source": "stat_search",
    }
    df = pd.DataFrame(
        [
            {"code": "SAME", "db": "PR01", "entity_type": "series", "description": "first", **blank},
            {"code": "SAME", "db": "PR01", "entity_type": "series", "description": "second", **blank},
        ]
    )
    entries = entities_from_boj_enumeration(df)
    _, series_by_db = split_enumerated_entries(entries)
    assert len(series_by_db["PR01"]) == 1
    assert series_by_db["PR01"][0].metadata["description"] == "first"


def test_entities_from_boj_enumeration_allows_duplicate_codes_across_dbs() -> None:
    blank = {
        "title": "Shared series",
        "db_title": "",
        "frequency": "",
        "unit": "",
        "category": "",
        "breadcrumb": "",
        "start_date": "",
        "end_date": "",
        "last_update": "",
        "source": "stat_search",
    }
    df = pd.DataFrame(
        [
            {"code": "SAME", "db": "FM08", "entity_type": "series", "description": "FX context", **blank},
            {"code": "SAME", "db": "PR01", "entity_type": "series", "description": "Prices context", **blank},
        ]
    )
    entries = entities_from_boj_enumeration(df)
    _, series_by_db = split_enumerated_entries(entries)
    assert set(series_by_db) == {"FM08", "PR01"}
    assert series_by_db["FM08"][0].metadata["description"] == "FX context"
    assert series_by_db["PR01"][0].metadata["description"] == "Prices context"


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
    indexes = discovery_indexes(databases, include_description=True)
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


def test_build_series_catalog_for_db_does_not_call_full_enumerate() -> None:
    # ``build_series_catalog`` is mocked: this test pins the per-db fetch
    # path, not catalog construction itself (which would pull a real
    # sentence-transformers embedder via the hybrid title/description
    # indexes). Catalog assembly is exercised by the other tests in this
    # module via the pure helpers, and end-to-end via ``catalog_tests/``.
    fake_catalog = SimpleNamespace(name="boj_series_fm08")
    with (
        patch("parsimony_boj.fetch_boj_enumeration_rows_for_db", new_callable=MagicMock) as fetch_one,
        patch("parsimony_boj.enumerate_boj", new_callable=MagicMock) as enumerate_all,
        patch.object(catalog_build, "build_series_catalog", new_callable=MagicMock) as build_inner,
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

        catalog = catalog_build.build_boj_series_catalog_for_db("FM08")

    enumerate_all.assert_not_called()
    fetch_one.assert_called_once_with("FM08")
    build_inner.assert_called_once()
    assert build_inner.call_args is not None
    db_arg, rows_arg = build_inner.call_args.args
    assert db_arg == "FM08"
    assert [row.code for row in rows_arg] == ["FXERD01"]
    assert catalog is fake_catalog
