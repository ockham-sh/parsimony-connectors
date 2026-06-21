"""Validate locally rebuilt v1 catalogs when present under /tmp/parsimony-catalogs-v1."""

from __future__ import annotations

from pathlib import Path

import pytest
from parsimony_test_support.catalog_remote import REPO_ROOT, import_catalog_validate

V1_ROOT = Path("/tmp/parsimony-catalogs-v1")


def _provider_ids() -> list[str]:
    _, specs, _, _ = import_catalog_validate()
    return sorted(specs)


@pytest.mark.parametrize("provider", _provider_ids())
def test_local_v1_catalog_probes(provider: str) -> None:
    load_queries_file, specs, _, validate_catalog = import_catalog_validate()
    spec = specs[provider]
    catalog_path = V1_ROOT / provider
    if not (catalog_path / "meta.json").is_file():
        pytest.skip(f"local v1 catalog not built: {catalog_path}")

    queries_path = REPO_ROOT / spec.queries_file
    query_set = load_queries_file(queries_path)
    report = validate_catalog(f"file://{catalog_path}", query_set)
    assert report.schema_ok, report.probe_results
    assert report.entry_count > 0
    min_recall = query_set.thresholds.get("min_required_recall", 1.0)
    assert report.required_recall >= min_recall, report.probe_results
