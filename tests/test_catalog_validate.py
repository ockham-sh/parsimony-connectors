"""Unit tests for maintainer catalog validation helpers (no HF required)."""

from __future__ import annotations

from pathlib import Path

import pytest
from parsimony.catalog import BM25Index, Catalog
from parsimony.entity import Entity

from catalog_validate.fixtures import load_queries_file, write_queries_file
from catalog_validate.probes import generate_probes, indexed_fields
from catalog_validate.runner import validate_catalog


def _tiny_catalog() -> Catalog:
    entries = [
        Entity(namespace="demo", code="A.1", title="Alpha macro indicator", metadata={"topic": "prices"}),
        Entity(namespace="demo", code="B.2", title="Beta unemployment rate", metadata={"topic": "labor"}),
    ]
    catalog = Catalog(
        "demo",
        indexes={
            "code": BM25Index(),
            "title": BM25Index(),
            "topic": BM25Index(),
        },
        default_field="title",
    )
    catalog.set_entities(entries)
    return catalog


def test_generate_probes_respects_index_shape(tmp_path: Path) -> None:
    catalog = _tiny_catalog()
    catalog.build()
    save = tmp_path / "demo"
    catalog.save(str(save), builder="test")

    loaded = Catalog.load(f"file://{save}")
    probes = generate_probes(loaded, catalog_url=f"file://{save}", sample_size=2, seed=0)
    modes = {p["mode"] for p in probes}
    assert "code" in modes
    assert "title_bm25" in modes
    assert "structured_field" in modes
    assert "title_bm25" in modes


def test_validate_curated_queries_local(tmp_path: Path) -> None:
    catalog = _tiny_catalog()
    catalog.build()
    save = tmp_path / "demo"
    catalog.save(str(save), builder="test")
    url = f"file://{save}"

    queries_path = tmp_path / "queries.yaml"
    write_queries_file(
        queries_path,
        {
            "catalog_url": url,
            "queries": [
                {
                    "id": "code_a1",
                    "query": "code: A.1",
                    "expected_code": "A.1",
                    "mode": "code",
                    "required": True,
                },
                {
                    "id": "topic_prices",
                    "query": "topic: prices",
                    "expected_code": "A.1",
                    "mode": "structured_field",
                    "required": True,
                },
            ],
            "thresholds": {"min_required_recall": 1.0},
        },
    )
    query_set = load_queries_file(queries_path)
    report = validate_catalog(url, query_set)
    assert report.ok
    assert report.required_recall == 1.0


def test_indexed_fields_from_meta() -> None:
    index_fields = {"code": "bm25", "title": "hybrid", "topic": "bm25"}
    assert indexed_fields(index_fields) == {"code": "bm25", "title": "hybrid", "topic": "bm25"}
