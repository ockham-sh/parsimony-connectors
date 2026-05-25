"""YAML query fixture load/save for catalog validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class CatalogQuery:
    """One search probe against a catalog snapshot."""

    id: str
    query: str
    expected_code: str
    mode: str
    required: bool = True
    optional: bool = False
    why: str = ""
    catalog_url: str | None = None
    namespace: str | None = None
    limit: int = 10
    notes: str = ""


@dataclass(frozen=True, slots=True)
class CatalogQuerySet:
    """Curated probes for one catalog URL or SDMX bundle namespace."""

    catalog_url: str | None
    catalog_root: str | None
    queries: tuple[CatalogQuery, ...]
    thresholds: dict[str, float]


def _query_from_raw(raw: dict[str, Any]) -> CatalogQuery:
    required = bool(raw.get("required", True))
    optional = bool(raw.get("optional", False))
    if optional:
        required = False
    return CatalogQuery(
        id=str(raw["id"]),
        query=str(raw["query"]),
        expected_code=str(raw.get("expected_code") or raw.get("expected", "")),
        mode=str(raw.get("mode", "title_bm25")),
        required=required,
        optional=optional,
        why=str(raw.get("why", "")),
        catalog_url=raw.get("catalog_url"),
        namespace=raw.get("namespace"),
        limit=int(raw.get("limit", 10)),
        notes=str(raw.get("notes", "")),
    )


def load_queries_file(path: Path) -> CatalogQuerySet:
    """Load a curated probe file."""
    data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid queries file (expected mapping): {path}")

    raw_queries = data.get("queries")
    if raw_queries is None:
        # SDMX legacy slices: flatten dataset_title_nl, series_*, etc.
        slices = ("dataset_title_nl", "series_title_nl", "series_dimension_label")
        merged: list[dict[str, Any]] = []
        for key in slices:
            for item in data.get(key, []) or []:
                mode = "structured_field" if key == "series_dimension_label" else "hybrid_title"
                merged.append(
                    {
                        "id": item["id"],
                        "query": item["query"],
                        "expected_code": item["expected"],
                        "namespace": item.get("namespace"),
                        "mode": mode,
                        "required": not bool(item.get("optional")),
                        "optional": bool(item.get("optional")),
                        "why": item.get("notes", ""),
                    }
                )
        raw_queries = merged

    if not isinstance(raw_queries, list):
        raise ValueError(f"queries must be a list in {path}")

    queries = tuple(_query_from_raw(item) for item in raw_queries)
    thresholds = dict(data.get("thresholds") or {})
    return CatalogQuerySet(
        catalog_url=data.get("catalog_url"),
        catalog_root=data.get("catalog_root"),
        queries=queries,
        thresholds=thresholds,
    )


def probes_to_yaml(
    *,
    catalog_url: str,
    probes: list[dict[str, Any]],
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    return {
        "catalog_url": catalog_url,
        "queries": probes,
        "thresholds": thresholds or {"min_required_recall": 1.0},
    }


def write_queries_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
