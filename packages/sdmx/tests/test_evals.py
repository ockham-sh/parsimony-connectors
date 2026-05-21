"""Eval harness for parsimony-sdmx keyword/semantic catalog search."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from parsimony_sdmx.connectors import enumerate_sdmx_datasets

_QUERIES_PATH = Path(__file__).parent / "evals" / "queries.yaml"
_BUNDLE_URL_TEMPLATE = os.environ.get("SDMX_BUNDLE_URL_TEMPLATE", "hf://parsimony-dev/sdmx/{namespace}")
_SLICES = ("dataset_title_nl", "series_title_nl", "series_dimension_label")


@pytest.fixture(scope="module")
def eval_set() -> dict[str, object]:
    data: dict[str, object] = yaml.safe_load(_QUERIES_PATH.read_text(encoding="utf-8"))
    return data


def test_eval_file_has_required_sections(eval_set: dict) -> None:
    for key in _SLICES:
        assert key in eval_set
    assert "thresholds" in eval_set


def test_dataset_queries_use_datasets_namespace(eval_set: dict) -> None:
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None
    datasets_ns = output_config.columns[0].namespace
    for q in eval_set["dataset_title_nl"]:
        assert q["namespace"] == datasets_ns


def test_series_slices_use_series_namespace_prefix(eval_set: dict) -> None:
    for slice in ("series_title_nl", "series_dimension_label"):
        for q in eval_set[slice]:
            assert str(q["namespace"]).startswith("sdmx_series_")


def test_query_ids_are_unique(eval_set: dict) -> None:
    ids = [q["id"] for slice in _SLICES for q in eval_set[slice]]
    assert len(ids) == len(set(ids))


async def _load_bundle_for(namespace: str):
    from parsimony.catalog import Catalog

    return await Catalog.load(_BUNDLE_URL_TEMPLATE.format(namespace=namespace))


async def _required_recall(eval_set: dict, slice: str) -> float:
    from parsimony.catalog import catalog_key

    queries = [q for q in eval_set[slice] if not q.get("optional")]
    if not queries:
        return 1.0

    hits = 0
    for q in queries:
        catalog = await _load_bundle_for(q["namespace"])
        assert await catalog.get(q["namespace"], q["expected"]) is not None
        matches = await catalog.search(q["query"], limit=10, namespaces=[q["namespace"]])
        codes = [catalog_key(m.namespace, m.code)[1] for m in matches]
        if q["expected"] in codes:
            hits += 1
        else:
            print(f"MISS [{slice}] {q['id']}: expected {q['expected']!r}")
            for i, m in enumerate(matches[:5], 1):
                print(f"  {i} {m.code}  {m.title[:80]}")
    return hits / len(queries)


@pytest.mark.skipif(os.environ.get("SDMX_RUN_EVALS") != "1", reason="set SDMX_RUN_EVALS=1")
@pytest.mark.asyncio
async def test_dataset_title_nl_required_recall(eval_set: dict) -> None:
    recall = await _required_recall(eval_set, "dataset_title_nl")
    assert recall >= eval_set["thresholds"]["min_dataset_title_nl_required"]


@pytest.mark.skipif(os.environ.get("SDMX_RUN_EVALS") != "1", reason="set SDMX_RUN_EVALS=1")
@pytest.mark.asyncio
async def test_series_title_nl_required_recall(eval_set: dict) -> None:
    recall = await _required_recall(eval_set, "series_title_nl")
    assert recall >= eval_set["thresholds"]["min_series_title_nl_required"]


@pytest.mark.skipif(os.environ.get("SDMX_RUN_EVALS") != "1", reason="set SDMX_RUN_EVALS=1")
@pytest.mark.asyncio
async def test_optional_probes_smoke(eval_set: dict) -> None:
    """Optional probes: print rankings for human review, no assertion."""
    from parsimony.catalog import catalog_key

    for slice in _SLICES:
        for q in eval_set[slice]:
            if not q.get("optional"):
                continue
            catalog = await _load_bundle_for(q["namespace"])
            matches = await catalog.search(q["query"], limit=10, namespaces=[q["namespace"]])
            codes = [catalog_key(m.namespace, m.code)[1] for m in matches]
            hit = q["expected"] in codes
            print(f"[optional {slice}] {q['id']}: hit={hit}")
            for i, m in enumerate(matches[:5], 1):
                mark = "*" if m.code == q["expected"] else " "
                print(f"  {mark}{i} {m.code}  {m.title[:70]}")
