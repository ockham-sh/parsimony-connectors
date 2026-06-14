"""JSON serialization for :class:`~parsimony_sdmx.core.models.StructureRecord`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from parsimony_sdmx.core.models import CodelistCode, CodelistRecord, DimensionStructure, StructureRecord


def structure_to_dict(record: StructureRecord) -> dict[str, Any]:
    return {
        "dataset_id": record.dataset_id,
        "agency_id": record.agency_id,
        "title": record.title,
        "dsd_order": list(record.dsd_order),
        "dimensions": [
            {
                "dimension_id": dim.dimension_id,
                "codelist_id": dim.codelist_id,
                "name": dim.name,
                "code_count": dim.code_count,
                "sample": [{"code": s.code, "label": s.label} for s in dim.sample],
            }
            for dim in record.dimensions
        ],
        "codelists": [
            {
                "codelist_id": cl.codelist_id,
                "codes": [{"code": c.code, "label": c.label} for c in cl.codes],
            }
            for cl in record.codelists
        ],
    }


def structure_from_dict(payload: dict[str, Any]) -> StructureRecord:
    dimensions = tuple(
        DimensionStructure(
            dimension_id=str(dim["dimension_id"]),
            codelist_id=dim.get("codelist_id"),
            name=dim.get("name"),
            code_count=int(dim.get("code_count", 0)),
            sample=tuple(CodelistCode(code=str(s["code"]), label=str(s["label"])) for s in dim.get("sample", [])),
        )
        for dim in payload.get("dimensions", [])
    )
    codelists = tuple(
        CodelistRecord(
            codelist_id=str(cl["codelist_id"]),
            codes=tuple(CodelistCode(code=str(c["code"]), label=str(c["label"])) for c in cl.get("codes", [])),
        )
        for cl in payload.get("codelists", [])
    )
    return StructureRecord(
        dataset_id=str(payload["dataset_id"]),
        agency_id=str(payload["agency_id"]),
        title=str(payload["title"]),
        dsd_order=tuple(str(d) for d in payload.get("dsd_order", [])),
        dimensions=dimensions,
        codelists=codelists,
    )


def write_structure(record: StructureRecord, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(structure_to_dict(record), separators=(",", ":")), encoding="utf-8")


def read_structure(path: Path) -> StructureRecord:
    return structure_from_dict(json.loads(path.read_text(encoding="utf-8")))
