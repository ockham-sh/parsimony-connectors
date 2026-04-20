"""Golden eval harness for parsimony-sdmx semantic search.

Unit-mode (always on): parses ``queries.yaml``, validates structure, and
confirms every query references a namespace the plugin is capable of
producing. This fails fast if someone drops a namespace that no
enumerator exposes.

Integration-mode (opt-in via ``SDMX_RUN_EVALS=1``): loads the built HF
bundles and runs each query through ``Catalog.search``, asserting the
expected code appears in top-k. Release-blocking for the publish
workflow; skipped locally unless the env var is set to avoid pulling
hundreds of MB of index on every test run.

Both halves use the SAME fixture file so a curated query that passes
the structural check here is the same one CI runs against the real
bundles.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from parsimony_sdmx.connectors import (
    enumerate_sdmx_datasets,
)
from parsimony_sdmx.connectors.enumerate_series import SERIES_NAMESPACE_TEMPLATE

_QUERIES_PATH = Path(__file__).parent / "evals" / "queries.yaml"


@pytest.fixture(scope="module")
def eval_set() -> dict:
    data: dict = yaml.safe_load(_QUERIES_PATH.read_text(encoding="utf-8"))
    return data


# ---------------------------------------------------------------------------
# Structural checks (always run)
# ---------------------------------------------------------------------------


def test_eval_file_has_required_sections(eval_set: dict) -> None:
    assert "dataset_queries" in eval_set
    assert "two_hop_queries" in eval_set
    assert "thresholds" in eval_set


def test_dataset_queries_use_datasets_namespace(eval_set: dict) -> None:
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None
    datasets_ns = output_config.columns[0].namespace
    for q in eval_set["dataset_queries"]:
        assert q["namespace"] == datasets_ns, (
            f"dataset query {q['id']} uses namespace {q['namespace']!r}; expected {datasets_ns!r}"
        )


def test_two_hop_series_namespaces_match_template(eval_set: dict) -> None:
    """Every two-hop query's series namespace must be a valid resolution of the template."""
    tmpl = SERIES_NAMESPACE_TEMPLATE
    prefix = tmpl.split("{", 1)[0]
    for q in eval_set["two_hop_queries"]:
        ns = q["series_step"]["namespace"]
        assert ns.startswith(prefix), f"{q['id']}: series namespace {ns!r} should start with {prefix!r}"


def test_thresholds_are_in_0_to_1_range(eval_set: dict) -> None:
    thresholds = eval_set["thresholds"]
    assert 0.0 <= thresholds["min_dataset_recall"] <= 1.0
    assert 0.0 <= thresholds["min_two_hop_joint_recall"] <= 1.0


def test_expected_codes_are_nonempty_strings(eval_set: dict) -> None:
    for q in eval_set["dataset_queries"]:
        assert isinstance(q["expected"], str) and q["expected"]
    for q in eval_set["two_hop_queries"]:
        assert isinstance(q["dataset_step"]["expected"], str)
        assert isinstance(q["series_step"]["expected"], str)


def test_query_ids_are_unique(eval_set: dict) -> None:
    ids = [q["id"] for q in eval_set["dataset_queries"]] + [
        q["id"] for q in eval_set["two_hop_queries"]
    ]
    assert len(ids) == len(set(ids)), "Duplicate query IDs"


# ---------------------------------------------------------------------------
# Integration: live search against built bundles (opt-in)
# ---------------------------------------------------------------------------


# Bundles ship one-per-namespace as ``hf://<org>/catalog-<namespace>``. The
# test loads the matching bundle per query rather than maintaining a combined
# catalog — one catalog per namespace is the new on-disk contract.
_BUNDLE_URL_TEMPLATE = os.environ.get("SDMX_BUNDLE_URL_TEMPLATE", "hf://ockham/catalog-{namespace}")


async def _load_bundle_for(namespace: str):
    """Load the standard Catalog for *namespace* from the configured HF org."""
    from parsimony._standard.catalog import Catalog

    return await Catalog.from_url(_BUNDLE_URL_TEMPLATE.format(namespace=namespace))


@pytest.mark.skipif(
    os.environ.get("SDMX_RUN_EVALS") != "1",
    reason="integration eval disabled; set SDMX_RUN_EVALS=1 to run against built bundles",
)
@pytest.mark.asyncio
async def test_dataset_queries_recall(eval_set: dict) -> None:
    """Single-hop: every dataset_query's expected code must appear in top-10."""
    from parsimony.catalog.models import catalog_key

    hits = misses = 0
    for q in eval_set["dataset_queries"]:
        catalog = await _load_bundle_for(q["namespace"])
        matches = await catalog.search(q["query"], limit=10, namespaces=[q["namespace"]])
        codes = [catalog_key(m.namespace, m.code)[1] for m in matches]
        if q["expected"] in codes:
            hits += 1
        else:
            misses += 1
            print(f"MISS: {q['id']} — expected {q['expected']!r}, top codes: {codes}")

    recall = hits / (hits + misses) if (hits + misses) else 0.0
    threshold = eval_set["thresholds"]["min_dataset_recall"]
    assert recall >= threshold, f"dataset recall {recall:.2f} below {threshold}"


@pytest.mark.skipif(
    os.environ.get("SDMX_RUN_EVALS") != "1",
    reason="integration eval disabled; set SDMX_RUN_EVALS=1 to run against built bundles",
)
@pytest.mark.asyncio
async def test_two_hop_joint_recall(eval_set: dict) -> None:
    """Two-hop: dataset_search → series_search — both must hit in top-10."""
    from parsimony.catalog.models import catalog_key

    joint_hits = total = 0
    for q in eval_set["two_hop_queries"]:
        total += 1
        ds_catalog = await _load_bundle_for(q["dataset_step"]["namespace"])
        ds_matches = await ds_catalog.search(
            q["query"], limit=10, namespaces=[q["dataset_step"]["namespace"]]
        )
        ds_codes = [catalog_key(m.namespace, m.code)[1] for m in ds_matches]
        if q["dataset_step"]["expected"] not in ds_codes:
            continue

        sr_catalog = await _load_bundle_for(q["series_step"]["namespace"])
        sr_matches = await sr_catalog.search(
            q["query"], limit=10, namespaces=[q["series_step"]["namespace"]]
        )
        sr_codes = [catalog_key(m.namespace, m.code)[1] for m in sr_matches]
        if q["series_step"]["expected"] in sr_codes:
            joint_hits += 1

    joint_recall = joint_hits / total if total else 0.0
    threshold = eval_set["thresholds"]["min_two_hop_joint_recall"]
    assert joint_recall >= threshold, f"two-hop joint recall {joint_recall:.2f} below {threshold}"
