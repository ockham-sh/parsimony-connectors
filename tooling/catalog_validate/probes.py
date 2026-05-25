"""Generate catalog search probes from snapshot index shape and sampled entries."""

from __future__ import annotations

import random
import re
from collections.abc import Sequence
from typing import Any

from parsimony.catalog import Catalog
from parsimony.entity import Entity
from parsimony.catalog.storage import SCHEMA_VERSION

from catalog_validate.snapshot_meta import snapshot_meta_for

_WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*")


def indexed_fields(index_fields: dict[str, str]) -> dict[str, str]:
    """Map catalog field -> index kind (``bm25``, ``hybrid``, ``vector``)."""
    return dict(index_fields)


def _sample_entries(entries: Sequence[Entity], size: int, seed: int) -> list[Entity]:
    if not entries:
        return []
    n = min(size, len(entries))
    rng = random.Random(seed)
    return rng.sample(list(entries), n)


def _title_lexical_query(title: str, *, max_words: int = 4) -> str:
    words = [w for w in _WORD_RE.findall(title) if len(w) > 2][:max_words]
    return " ".join(words) if words else title[:48]


def _title_semantic_query(title: str) -> str:
    cleaned = " ".join(_WORD_RE.findall(title))
    return cleaned[:120] if cleaned else title[:120]


def _metadata_value_for_field(entry: Entity, field: str) -> str | None:
    value = entry.metadata.get(field)
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            text = str(item).strip()
            if text:
                return text
        return None
    text = str(value).strip()
    return text or None


def generate_probes(
    catalog: Catalog,
    *,
    catalog_url: str,
    sample_size: int = 5,
    seed: int = 0,
) -> list[dict[str, Any]]:
    """Build draft probes aligned to the catalog's serialized index policy."""
    meta = snapshot_meta_for(catalog, catalog_url)
    if meta.schema_version != SCHEMA_VERSION:
        raise ValueError(f"Unsupported schema_version {meta.schema_version}; expected {SCHEMA_VERSION}")

    fields = indexed_fields(meta.index_fields)
    samples = _sample_entries(catalog.entities, sample_size, seed)
    probes: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def add_probe(
        probe_id: str,
        *,
        query: str,
        expected_code: str,
        mode: str,
        why: str,
        required: bool = True,
        optional: bool = False,
    ) -> None:
        if probe_id in seen_ids:
            return
        seen_ids.add(probe_id)
        probes.append(
            {
                "id": probe_id,
                "query": query,
                "expected_code": expected_code,
                "mode": mode,
                "required": required,
                "optional": optional,
                "why": why,
            }
        )

    for entry in samples:
        code = entry.code
        if "code" in fields:
            add_probe(
                f"code_{code[:32]}",
                query=f"code: {code}",
                expected_code=code,
                mode="code",
                why=f"code field indexed as {fields['code']}",
            )

        if "title" in fields:
            kind = fields["title"]
            if kind == "hybrid":
                add_probe(
                    f"title_lex_{code[:24]}",
                    query=_title_lexical_query(entry.title),
                    expected_code=code,
                    mode="title_bm25",
                    why="title field uses hybrid index; lexical slice exercises BM25 path",
                )
                add_probe(
                    f"title_sem_{code[:24]}",
                    query=_title_semantic_query(entry.title),
                    expected_code=code,
                    mode="hybrid_title",
                    optional=True,
                    why="title field uses hybrid index; longer phrase may rank variably",
                )
            else:
                add_probe(
                    f"title_{code[:24]}",
                    query=_title_lexical_query(entry.title),
                    expected_code=code,
                    mode="title_bm25",
                    why=f"title field indexed as {kind}; plain/BM25 query only",
                )

        for field, kind in sorted(fields.items()):
            if field in ("code", "title"):
                continue
            value = _metadata_value_for_field(entry, field)
            if value is None:
                continue
            add_probe(
                f"{field}_{code[:16]}",
                query=f"{field}: {value}",
                expected_code=code,
                mode="structured_field",
                why=f"metadata field {field!r} indexed as {kind}",
            )

    return probes


def inspect_snapshot(catalog: Catalog, *, catalog_url: str) -> dict[str, Any]:
    """Return a JSON-serializable inspection report."""
    meta = snapshot_meta_for(catalog, catalog_url)
    fields = indexed_fields(meta.index_fields)
    return {
        "catalog_url": catalog_url,
        "name": catalog.name,
        "entry_count": len(catalog),
        "schema_version": meta.schema_version,
        "default_field": meta.default_field,
        "indexed_fields": fields,
        "index_fields": dict(meta.index_fields),
        "sample_entries": [
            {"code": e.code, "title": e.title[:120], "metadata_keys": sorted(e.metadata)}
            for e in _sample_entries(catalog.entities, 3, seed=0)
        ],
    }
