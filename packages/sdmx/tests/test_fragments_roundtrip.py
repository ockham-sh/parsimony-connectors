"""Phase 2b — fragments round-trip across the SDMX subprocess boundary.

Covers the wire between ``parsimony-sdmx`` structural output and
``parsimony.Catalog`` compositional embedding:

* ``write_series`` → ``read_table`` — parquet ``list(string)`` column
  preserves the per-series fragment tuple.
* ``enumerate_sdmx_series`` DataFrame feeds ``entries_from_result``
  (from parsimony core) cleanly — fragments arrive on
  :class:`SeriesEntry` as ``list[str]`` (not ``np.ndarray``).
* The ``FragmentEmbeddingCache`` compose path returns one vector per
  series when given an SDMX-shaped DataFrame.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from parsimony.catalog import entries_from_result
from parsimony.embedder import EmbedderInfo, FragmentEmbeddingCache
from parsimony.result import Provenance, Result

from parsimony_sdmx.connectors.enumerate_series import ENUMERATE_SERIES_OUTPUT
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.io.parquet import SERIES_SCHEMA, write_series


class _CountingStubEmbedder:
    DIM = 8

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    @property
    def dimension(self) -> int:
        return self.DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            out.append([x / norm for x in raw])
        return out

    async def embed_query(self, query: str) -> list[float]:
        (vec,) = await self.embed_texts([query])
        return vec

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model="stub/hash-sha256",
            dim=self.DIM,
            normalize=True,
            package="test-stub",
        )


def test_write_series_persists_fragments(tmp_path: Path) -> None:
    rows = [
        SeriesRecord(
            id="A.U2",
            dataset_id="YC",
            title="A: Annual - U2: Euro area",
            fragments=("Annual", "Euro area"),
        ),
        SeriesRecord(
            id="M.U2",
            dataset_id="YC",
            title="M: Monthly - U2: Euro area",
            fragments=("Monthly", "Euro area"),
        ),
    ]
    path = write_series(rows, tmp_path, "ECB", "YC")
    table = pq.read_table(path)
    assert table.schema == SERIES_SCHEMA

    out_fragments = table.column("fragments").to_pylist()
    assert out_fragments == [["Annual", "Euro area"], ["Monthly", "Euro area"]]


def test_write_series_accepts_empty_fragments(tmp_path: Path) -> None:
    """Legacy SeriesRecord construction (no fragments arg) still round-trips."""
    rows = [SeriesRecord(id="S1", dataset_id="YC", title="t")]
    path = write_series(rows, tmp_path, "ECB", "YC")
    table = pq.read_table(path)
    assert table.column("fragments").to_pylist() == [[]]


def test_entries_from_sdmx_result_receives_list_str_fragments() -> None:
    """Enumerator DataFrame → entries_from_result preserves fragments as list[str].

    Guards against the pyarrow-groupby silent ``np.ndarray`` conversion
    that would otherwise leak into the embedder input.
    """
    df = pd.DataFrame(
        {
            "code": ["A.1", "B.2"],
            "title": ["title A", "title B"],
            "fragments": [["Monthly", "Spain"], ["Quarterly", "Germany"]],
            "agency": ["ECB", "ECB"],
            "dataset_id": ["FLOW", "FLOW"],
        }
    )
    result = Result(
        data=df,
        provenance=Provenance(source="sdmx"),
        output_schema=ENUMERATE_SERIES_OUTPUT,
    )
    entries = entries_from_result(result, namespace="sdmx_series_ecb_flow")

    by_code = {e.code: e for e in entries}
    assert by_code["A.1"].fragments == ["Monthly", "Spain"]
    assert by_code["B.2"].fragments == ["Quarterly", "Germany"]
    # Explicit type assertion — pandas groupby pyarrow list columns can
    # silently yield np.str_ instead of str. The str() cast in
    # entries_from_result locks this contract in.
    assert all(type(f) is str for f in by_code["A.1"].fragments)


def test_fragment_cache_composes_over_sdmx_batch() -> None:
    """End-to-end: project → DataFrame → entries → cache.compose_many."""
    df = pd.DataFrame(
        {
            "code": ["M.ES", "M.DE"],
            "title": ["Spain monthly", "Germany monthly"],
            "fragments": [["Monthly", "Spain"], ["Monthly", "Germany"]],
            "agency": ["ECB", "ECB"],
            "dataset_id": ["FLOW", "FLOW"],
        }
    )
    result = Result(
        data=df,
        provenance=Provenance(source="sdmx"),
        output_schema=ENUMERATE_SERIES_OUTPUT,
    )
    entries = entries_from_result(result, namespace="sdmx_series_ecb_flow")

    emb = _CountingStubEmbedder()
    cache = FragmentEmbeddingCache(emb)
    fragments_per_item = [e.fragments or [] for e in entries]
    vectors = asyncio.run(cache.compose_many(fragments_per_item))

    assert len(vectors) == 2
    # Base embedder was called once, with exactly the unique fragments.
    assert len(emb.calls) == 1
    assert sorted(emb.calls[0]) == ["Germany", "Monthly", "Spain"]


def test_enumerator_output_declares_fragments_column() -> None:
    """The OutputConfig is the contract publish_provider reads —
    FRAGMENTS must be present for Phase 2 wiring to activate."""
    roles = {c.name: c.role for c in ENUMERATE_SERIES_OUTPUT.columns}
    assert roles.get("fragments") is not None
    assert roles["fragments"].value == "fragments"
