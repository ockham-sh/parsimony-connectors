"""Eval harness for parsimony-sdmx keyword/semantic catalog search."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

_QUERIES_PATH = Path(__file__).parent / "evals" / "queries.yaml"
_BUNDLE_URL_TEMPLATE = os.environ.get("SDMX_BUNDLE_URL_TEMPLATE", "hf://parsimony-dev/sdmx/{namespace}")
_SLICES = ("dataset_title_nl",)


@pytest.fixture(scope="module")
def eval_set() -> dict[str, object]:
    data: dict[str, object] = yaml.safe_load(_QUERIES_PATH.read_text(encoding="utf-8"))
    return data


def test_eval_file_has_required_sections(eval_set: dict) -> None:
    for key in _SLICES:
        assert key in eval_set
    assert "thresholds" in eval_set


def test_dataset_queries_use_datasets_namespace(eval_set: dict) -> None:
    from parsimony_sdmx.core.namespaces import is_datasets_namespace

    for q in eval_set["dataset_title_nl"]:
        assert is_datasets_namespace(q["namespace"])


def test_query_ids_are_unique(eval_set: dict) -> None:
    ids = [q["id"] for slice in _SLICES for q in eval_set[slice]]
    assert len(ids) == len(set(ids))
