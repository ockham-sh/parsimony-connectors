"""Ad-hoc SDMX catalog search reranking experiment.

Example:

    uv run --with sentence-transformers --with accelerate python packages/sdmx/scripts/rerank_search.py \
        --prepare-catalogs \
        --include-optional \
        --candidate-limit 50

    # Faster CPU inference (pre-optimized ONNX on the Hub):
    uv run --with 'sentence-transformers[onnx]' python packages/sdmx/scripts/rerank_search.py \
        --model cross-encoder/ms-marco-MiniLM-L-6-v2 \
        --backend onnx --onnx-file onnx/model_O3.onnx \
        --candidate-text minimal --rerank-cap 30

``zeroentropy/zerank-1`` is a ~4B-parameter causal reranker; plan for a GPU or
16GB+ RAM. On smaller machines use ``--model cross-encoder/ms-marco-MiniLM-L-6-v2``
to exercise the pipeline, or ``--search-only`` for baseline metrics.

The script keeps the existing catalog search as the candidate generator, then
reranks those candidates with a sentence-transformers CrossEncoder. It is meant
for quick offline assessment, not as a connector runtime dependency.

This script defaults to building/loading schema v1 catalogs under
``/tmp/parsimony-sdmx-rerank-eval``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import math
import os
import sys
import time
import types
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import yaml
from parsimony.catalog import Catalog
from parsimony.catalog.models import CatalogMatch
from parsimony.catalog.storage import META_FILENAME
from parsimony.ranking import Ranking, ranking_from_scores
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import is_datasets_namespace, parse_datasets_namespace
from parsimony_sdmx.connectors.search import DEFAULT_CATALOG_ROOT, PARSIMONY_SDMX_CATALOG_URL_ENV

DEFAULT_MODEL = "zeroentropy/zerank-1"
DEFAULT_QUERIES = Path(__file__).resolve().parents[1] / "tests" / "evals" / "queries.yaml"
DEFAULT_LOCAL_CATALOG_ROOT = Path("/tmp/parsimony-sdmx-rerank-eval")
SCHEMA_VERSION = 1

CandidateTextMode = Literal["minimal", "full"]
RerankBackend = Literal["torch", "onnx"]


@dataclass(frozen=True)
class Query:
    id: str
    slice: str
    query: str
    namespace: str
    expected: str
    optional: bool


@dataclass(frozen=True)
class RankRow:
    rank: int
    code: str
    title: str
    score: float
    expected: bool


@dataclass(frozen=True)
class QueryResult:
    query: Query
    baseline_rows: tuple[RankRow, ...]
    reranked_rows: tuple[RankRow, ...]
    baseline_top1: bool
    reranked_top1: bool
    baseline_hit_at_10: bool
    reranked_hit_at_10: bool
    baseline_rank: int | None
    reranked_rank: int | None
    search_result_count: int
    rerank_candidate_count: int
    load_seconds: float
    search_seconds: float
    rerank_seconds: float

    @property
    def total_seconds(self) -> float:
        return self.load_seconds + self.search_seconds + self.rerank_seconds


class CatalogReranker(Protocol):
    """Rerank resolved catalog matches for a user query."""

    def rerank(self, query: str, matches: Sequence[CatalogMatch], *, limit: int) -> Ranking:
        """Return a reranked :class:`Ranking` for the given candidates."""
        ...


def _load_queries(path: Path, *, include_optional: bool) -> list[Query]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    queries: list[Query] = []
    for slice_name, items in raw.items():
        if slice_name == "thresholds":
            continue
        if not items:
            continue
        for item in items:
            optional = bool(item.get("optional", False))
            if optional and not include_optional:
                continue
            queries.append(
                Query(
                    id=str(item["id"]),
                    slice=str(slice_name),
                    query=str(item["query"]),
                    namespace=str(item["namespace"]),
                    expected=str(item["expected"]),
                    optional=optional,
                )
            )
    return queries


def _namespaces_for_queries(path: Path, *, include_optional: bool) -> set[str]:
    return {query.namespace for query in _load_queries(path, include_optional=include_optional)}


def _all_namespaces_in_file(path: Path) -> set[str]:
    return _namespaces_for_queries(path, include_optional=True)


def _recall_limit_for_queries(path: Path, *, candidate_limit: int) -> int:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    thresholds = raw.get("thresholds") or {}
    probe_limit = int(thresholds.get("recall_limit", candidate_limit))
    return max(probe_limit, candidate_limit)


async def _validate_queries_recallable(
    *,
    queries: list[Query],
    root: str,
    recall_limit: int,
    catalog_cache: dict[str, Catalog] | None = None,
) -> None:
    cache = catalog_cache if catalog_cache is not None else {}
    missing: list[str] = []
    for query in queries:
        catalog, _ = await _load_catalog_cached(root=root, namespace=query.namespace, cache=cache)
        matches, _ = await catalog.search(query.query, limit=recall_limit, namespaces=[query.namespace])
        codes = [match.code for match in matches]
        if query.expected not in codes:
            missing.append(
                f"  {query.id} ({query.slice}): expected {query.expected!r} not in top-{recall_limit} "
                f"for query {query.query!r} [{len(matches)} results]"
            )
    if missing:
        raise RuntimeError(
            "Eval queries must return expected within the recall limit; "
            "move retrieval-gap probes to queries_negative.yaml or pass --allow-unretrievable.\n"
            + "\n".join(missing)
        )


def _catalog_url(root: str, namespace: str) -> str:
    return f"{root.rstrip('/')}/{namespace}"


def _resolve_catalog_root(explicit: str | None) -> str:
    if explicit:
        return explicit
    env = os.environ.get(PARSIMONY_SDMX_CATALOG_URL_ENV, "").strip()
    if env:
        return env
    if DEFAULT_LOCAL_CATALOG_ROOT.is_dir() and any(DEFAULT_LOCAL_CATALOG_ROOT.iterdir()):
        return f"file://{DEFAULT_LOCAL_CATALOG_ROOT}"
    return DEFAULT_CATALOG_ROOT


def _local_catalog_root(root: str) -> Path | None:
    if root.startswith("file://"):
        return Path(root.removeprefix("file://"))
    if root.startswith("/"):
        return Path(root)
    return None


def _snapshot_schema_version(path: Path) -> int | None:
    meta_path = path / META_FILENAME
    if not meta_path.is_file():
        return None
    return int(json.loads(meta_path.read_text(encoding="utf-8")).get("schema_version", 0))


def _missing_or_stale_namespaces(root: str, namespaces: set[str]) -> list[str]:
    local_root = _local_catalog_root(root)
    if local_root is None:
        return sorted(namespaces)
    missing: list[str] = []
    for namespace in sorted(namespaces):
        schema = _snapshot_schema_version(local_root / namespace)
        if schema != SCHEMA_VERSION:
            missing.append(namespace)
    return missing


def _parse_series_namespace(namespace: str) -> tuple[AgencyId, str]:
    prefix = "sdmx_series_"
    if not namespace.startswith(prefix):
        raise ValueError(f"Unsupported series namespace {namespace!r}")
    body = namespace.removeprefix(prefix)
    for agency in AgencyId:
        token = f"{agency.value.lower()}_"
        if body.startswith(token):
            return agency, body.removeprefix(token).upper()
    raise ValueError(f"Could not parse agency/dataset from namespace {namespace!r}")


def _load_build_catalog_module() -> Any:
    build_path = Path(__file__).resolve().parent / "build_catalog.py"
    spec = importlib.util.spec_from_file_location("sdmx_build_catalog", build_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load build helper from {build_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def _prepare_local_catalogs(save_root: Path, namespaces: set[str]) -> None:
    build_mod = _load_build_catalog_module()
    save_root.mkdir(parents=True, exist_ok=True)
    save = str(save_root)

    series_targets = {
        namespace: _parse_series_namespace(namespace)
        for namespace in namespaces
        if not is_datasets_namespace(namespace)
    }
    dataset_targets = {
        namespace: parse_datasets_namespace(namespace)
        for namespace in namespaces
        if is_datasets_namespace(namespace)
    }

    for namespace, agency in sorted(dataset_targets.items()):
        if _snapshot_schema_version(save_root / namespace) == SCHEMA_VERSION:
            print(f"Skipping {namespace}; schema v{SCHEMA_VERSION} snapshot already present", flush=True)
            continue
        print(f"Building {namespace} ({agency.value}) -> {save_root}", flush=True)
        catalog = await build_mod.build_datasets(agency=agency, save_root=save)
        await build_mod._publish_datasets_catalog(catalog, save_root=save, push=None, push_root=None)
        print(f"  {len(catalog)} entries", flush=True)

    for namespace, (agency, dataset_id) in sorted(series_targets.items()):
        if _snapshot_schema_version(save_root / namespace) == SCHEMA_VERSION:
            print(f"Skipping {namespace}; schema v{SCHEMA_VERSION} snapshot already present", flush=True)
            continue
        print(f"Building {namespace} ({agency.value}/{dataset_id}) -> {save_root}", flush=True)
        build = await build_mod.build_series(agency, dataset_id, fetch_timeout_s=900.0)
        await build_mod._publish(build.catalog, save_root=save, push=None, push_root=None)
        print(f"  {len(build.catalog)} entries", flush=True)
        if namespace in dataset_targets:
            from parsimony_sdmx.catalog_build import build_agency_dataset_entities, build_datasets_catalog
            from parsimony_sdmx.connectors.enumerate_datasets import datasets_namespace
            from parsimony_sdmx.core.models import DatasetRecord

            records = [
                DatasetRecord(dataset_id=dataset_id, agency_id=agency.value, title=dataset_id),
            ]
            entries = await build_agency_dataset_entities(records, {build.dataset_code: build.manifest})
            ds_catalog = await build_datasets_catalog(
                entries,
                agency=agency,
                existing_path=str(save_root / datasets_namespace(agency)),
            )
            await build_mod._publish_datasets_catalog(ds_catalog, save_root=save, push=None, push_root=None)


def _rank_of(expected: str, codes: list[str]) -> int | None:
    try:
        return codes.index(expected) + 1
    except ValueError:
        return None


def _mrr(rank: int | None) -> float:
    return 0.0 if rank is None else 1.0 / rank


def _rows_from_matches(matches: list[CatalogMatch], *, expected: str, limit: int) -> tuple[RankRow, ...]:
    rows: list[RankRow] = []
    for idx, match in enumerate(matches[:limit], start=1):
        rows.append(
            RankRow(
                rank=idx,
                code=match.code,
                title=match.title,
                score=float(match.score),
                expected=match.code == expected,
            )
        )
    return tuple(rows)


def _stringify_metadata(value: Any, *, max_chars: int = 280) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, dict):
        parts = [f"{key}: {_stringify_metadata(item, max_chars=80)}" for key, item in value.items()]
        text = "; ".join(part for part in parts if part)
    elif isinstance(value, (list, tuple, set)):
        parts = [_stringify_metadata(item, max_chars=80) for item in value]
        text = "; ".join(part for part in parts if part)
    else:
        text = str(value)
    text = " ".join(text.split())
    return text[:max_chars]


def _candidate_text(match: CatalogMatch, *, mode: CandidateTextMode) -> str:
    if mode == "minimal":
        return f"{match.title}\n{match.code}"
    metadata = []
    for key, value in sorted(match.metadata.items()):
        rendered = _stringify_metadata(value)
        if rendered:
            metadata.append(f"{key}: {rendered}")
    fields = [
        f"title: {match.title}",
        f"code: {match.code}",
        f"namespace: {match.namespace}",
        *metadata[:12],
    ]
    return "\n".join(fields)


def _match_with_score(match: CatalogMatch, score: float) -> CatalogMatch:
    return CatalogMatch(
        namespace=match.namespace,
        code=match.code,
        title=match.title,
        score=score,
        metadata=match.metadata,
    )


def _ranking_to_matches(ranking: Ranking, candidates: Sequence[CatalogMatch]) -> list[CatalogMatch]:
    by_key = {(match.namespace, match.code): match for match in candidates}
    matches: list[CatalogMatch] = []
    for item in ranking.items:
        original = by_key.get((item.namespace, item.code))
        if original is not None:
            matches.append(_match_with_score(original, item.score))
    return matches


def _ensure_zerank_patch() -> Any:
    from huggingface_hub import hf_hub_download

    patch_path = hf_hub_download("zeroentropy/zerank-1", "modeling_zeranker.py")
    spec = importlib.util.spec_from_file_location("zerank_modeling_patch", patch_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load zerank remote code from {patch_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _patch_zerank_low_memory_loader(module)
    return module


def _patch_zerank_low_memory_loader(module: Any) -> None:
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    offload_dir = Path("/tmp/zerank-offload")
    offload_dir.mkdir(parents=True, exist_ok=True)

    def load_model(device: torch.device | None = None) -> tuple[Any, Any]:
        if device is None:
            device = torch.device("cpu")
        module.global_device = device
        model = AutoModelForCausalLM.from_pretrained(
            module.MODEL_PATH,
            torch_dtype=torch.float16,
            device_map="auto",
            max_memory={"cpu": "4GiB"},
            offload_folder=str(offload_dir),
            trust_remote_code=True,
        )
        model.eval()
        tokenizer = AutoTokenizer.from_pretrained(module.MODEL_PATH, padding_side="right")
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        return tokenizer, model

    module.load_model = load_model
    module.global_device = torch.device("cpu")


@dataclass(frozen=True)
class IdentityReranker:
    """No-op reranker used for baseline/search-only evaluation."""

    def rerank(self, query: str, matches: Sequence[CatalogMatch], *, limit: int) -> Ranking:
        del query
        selected = matches if limit <= 0 else matches[:limit]
        return ranking_from_scores(
            [(match.namespace, match.code, float(match.score)) for match in selected],
            limit=len(selected),
        )


@dataclass
class CrossEncoderCatalogReranker:
    """Sentence-Transformers cross-encoder adapter for catalog matches."""

    model: Any
    batch_size: int
    candidate_text: CandidateTextMode
    zerank_module: Any | None = None

    def rerank(self, query: str, matches: Sequence[CatalogMatch], *, limit: int) -> Ranking:
        selected = list(matches if limit <= 0 else matches[:limit])
        if not selected:
            return Ranking.empty()

        pairs = [(query, _candidate_text(match, mode=self.candidate_text)) for match in selected]
        scores = self._predict(pairs, batch_size=self.batch_size)

        scored = [
            (match, float(score), idx)
            for idx, (match, score) in enumerate(zip(selected, scores, strict=True))
            if math.isfinite(float(score))
        ]
        scored.sort(key=lambda row: (-row[1], -row[0].score, row[2]))
        return ranking_from_scores(
            [(match.namespace, match.code, score) for match, score, _idx in scored],
            limit=len(scored),
        )

    def _predict(self, pairs: list[tuple[str, str]], *, batch_size: int) -> Sequence[float]:
        if self.zerank_module is not None:
            return cast(
                Sequence[float],
                self.zerank_module.predict(
                    self.model,
                    sentences=pairs,
                    batch_size=batch_size,
                    convert_to_numpy=False,
                    show_progress_bar=False,
                ),
            )
        return cast(
            Sequence[float],
            self.model.predict(
                pairs,
                batch_size=batch_size,
                convert_to_numpy=False,
                show_progress_bar=False,
            ),
        )


def _load_cross_encoder_reranker(
    model_name: str,
    *,
    device: str,
    backend: RerankBackend,
    onnx_file: str | None,
    batch_size: int,
    candidate_text: CandidateTextMode,
) -> CrossEncoderCatalogReranker:
    if os.environ.get("CUDA_VISIBLE_DEVICES") is None and device == "cpu":
        os.environ["CUDA_VISIBLE_DEVICES"] = ""

    if model_name == DEFAULT_MODEL:
        if backend != "torch":
            raise ValueError(f"{DEFAULT_MODEL} only supports --backend torch")
        return CrossEncoderCatalogReranker(
            model=types.SimpleNamespace(),
            batch_size=batch_size,
            candidate_text=candidate_text,
            zerank_module=_ensure_zerank_patch(),
        )

    from sentence_transformers import CrossEncoder

    model_kwargs: dict[str, Any] = {}
    if onnx_file:
        model_kwargs["file_name"] = onnx_file
    if backend == "onnx" and device == "cuda":
        model_kwargs["provider"] = "CUDAExecutionProvider"

    if backend == "torch":
        model = CrossEncoder(model_name, trust_remote_code=True, device=device, backend=backend)
    else:
        model = CrossEncoder(
            model_name,
            trust_remote_code=True,
            backend=backend,
            model_kwargs=model_kwargs or None,
        )
    return CrossEncoderCatalogReranker(model=model, batch_size=batch_size, candidate_text=candidate_text)


def _load_reranker(
    model_name: str,
    *,
    device: str,
    backend: RerankBackend,
    onnx_file: str | None,
    search_only: bool,
    batch_size: int,
    candidate_text: CandidateTextMode,
) -> tuple[CatalogReranker, float]:
    if search_only:
        return IdentityReranker(), 0.0

    t0 = time.perf_counter()
    reranker = _load_cross_encoder_reranker(
        model_name,
        device=device,
        backend=backend,
        onnx_file=onnx_file,
        batch_size=batch_size,
        candidate_text=candidate_text,
    )
    return reranker, time.perf_counter() - t0


async def _load_catalog_cached(
    *,
    root: str,
    namespace: str,
    cache: dict[str, Catalog],
) -> tuple[Catalog, float]:
    cached = cache.get(namespace)
    if cached is not None:
        return cached, 0.0

    url = _catalog_url(root, namespace)
    print(f"Loading {namespace}: {url}", flush=True)
    t_load = time.perf_counter()
    try:
        catalog = await Catalog.load(url)
    except Exception as exc:
        local_root = _local_catalog_root(root)
        hint = (
            f"Catalog load failed for {url}: {exc}\n"
            f"Published HF SDMX snapshots are schema v1; rebuild local schema v1 catalogs with:\n"
            f"  uv run python packages/sdmx/scripts/rerank_search.py --prepare-catalogs --include-optional"
        )
        if local_root is not None:
            hint += f"\nOr point --catalog-root file://{local_root}"
        raise RuntimeError(hint) from exc
    load_seconds = time.perf_counter() - t_load
    cache[namespace] = catalog
    print(f"  {len(catalog)} entries ({load_seconds:.2f}s load)", flush=True)
    return catalog, load_seconds


async def _evaluate(
    *,
    queries: list[Query],
    root: str,
    reranker: CatalogReranker,
    candidate_limit: int,
    rerank_cap: int | None,
    report_limit: int,
    catalog_cache: dict[str, Catalog] | None = None,
) -> list[QueryResult]:
    results: list[QueryResult] = []
    cache = catalog_cache if catalog_cache is not None else {}

    for query in queries:
        catalog, load_seconds = await _load_catalog_cached(root=root, namespace=query.namespace, cache=cache)
        if load_seconds == 0.0:
            print(f"Using cached {query.namespace} ({len(catalog)} entries)", flush=True)

        t0 = time.perf_counter()
        matches, _ = await catalog.search(query.query, limit=candidate_limit, namespaces=[query.namespace])
        search_seconds = time.perf_counter() - t0
        search_result_count = len(matches)

        cap = rerank_cap if rerank_cap is not None else candidate_limit
        rerank_candidates = matches if cap <= 0 else matches[:cap]
        t_rerank = time.perf_counter()
        reranked_ranking = reranker.rerank(query.query, matches, limit=cap)
        rerank_seconds = time.perf_counter() - t_rerank
        if isinstance(reranker, IdentityReranker):
            rerank_seconds = 0.0
        reranked_matches = _ranking_to_matches(reranked_ranking, rerank_candidates)
        rerank_candidate_count = len(rerank_candidates)

        baseline_rows = _rows_from_matches(matches, expected=query.expected, limit=report_limit)
        reranked_rows = _rows_from_matches(reranked_matches, expected=query.expected, limit=report_limit)
        baseline_codes = [row.code for row in baseline_rows]
        reranked_codes = [row.code for row in reranked_rows]

        results.append(
            QueryResult(
                query=query,
                baseline_rows=baseline_rows,
                reranked_rows=reranked_rows,
                baseline_top1=bool(baseline_codes) and baseline_codes[0] == query.expected,
                reranked_top1=bool(reranked_codes) and reranked_codes[0] == query.expected,
                baseline_hit_at_10=query.expected in baseline_codes[:10],
                reranked_hit_at_10=query.expected in reranked_codes[:10],
                baseline_rank=_rank_of(query.expected, baseline_codes),
                reranked_rank=_rank_of(query.expected, reranked_codes),
                search_result_count=search_result_count,
                rerank_candidate_count=rerank_candidate_count,
                load_seconds=load_seconds,
                search_seconds=search_seconds,
                rerank_seconds=rerank_seconds,
            )
        )

    return results


def _format_rows(rows: tuple[RankRow, ...], *, limit: int = 5) -> list[str]:
    out: list[str] = []
    for row in rows[:limit]:
        mark = "*" if row.expected else " "
        out.append(f"{mark}{row.rank:>2} {row.code}  score={row.score:7.3f}  {row.title[:72]}")
    return out


def _summarize(results: list[QueryResult], *, include_optional: bool, show_top: int) -> dict[str, Any]:
    by_slice: dict[str, list[QueryResult]] = defaultdict(list)
    for result in results:
        by_slice[result.query.slice].append(result)

    print("")
    print("=== Summary ===")
    summary: dict[str, Any] = {"slices": {}, "aggregate": {}, "queries": []}
    for label, subset in [*sorted(by_slice.items()), ("AGGREGATE", results)]:
        n = len(subset)
        if n == 0:
            continue
        base_hit = sum(result.baseline_hit_at_10 for result in subset) / n
        rerank_hit = sum(result.reranked_hit_at_10 for result in subset) / n
        base_top1 = sum(result.baseline_top1 for result in subset) / n
        rerank_top1 = sum(result.reranked_top1 for result in subset) / n
        base_mrr = sum(_mrr(result.baseline_rank) for result in subset) / n
        rerank_mrr = sum(_mrr(result.reranked_rank) for result in subset) / n
        avg_load = sum(result.load_seconds for result in subset) / n
        avg_search = sum(result.search_seconds for result in subset) / n
        avg_rerank = sum(result.rerank_seconds for result in subset) / n
        avg_total = sum(result.total_seconds for result in subset) / n
        print(
            f"{label:<24} n={n:<3} "
            f"hit@10 {base_hit:.3f}->{rerank_hit:.3f} ({rerank_hit - base_hit:+.3f})  "
            f"top1 {base_top1:.3f}->{rerank_top1:.3f} ({rerank_top1 - base_top1:+.3f})  "
            f"MRR {base_mrr:.3f}->{rerank_mrr:.3f} ({rerank_mrr - base_mrr:+.3f})  "
            f"total={avg_total:.2f}s (load {avg_load:.2f}+search {avg_search:.2f}+rerank {avg_rerank:.2f})"
        )
        bucket = {
            "count": n,
            "hit_at_10": {"baseline": base_hit, "reranked": rerank_hit, "delta": rerank_hit - base_hit},
            "top1": {"baseline": base_top1, "reranked": rerank_top1, "delta": rerank_top1 - base_top1},
            "mrr": {"baseline": base_mrr, "reranked": rerank_mrr, "delta": rerank_mrr - base_mrr},
            "avg_load_seconds": avg_load,
            "avg_search_seconds": avg_search,
            "avg_rerank_seconds": avg_rerank,
            "avg_total_seconds": avg_total,
        }
        if label == "AGGREGATE":
            summary["aggregate"] = bucket
        else:
            summary["slices"][label] = bucket

    print("")
    print("=== Per Query (timing + reranked top results) ===")
    for result in results:
        marker = "optional" if result.query.optional else "required"
        movement = "same"
        if _mrr(result.reranked_rank) > _mrr(result.baseline_rank):
            movement = "improved"
        elif _mrr(result.reranked_rank) < _mrr(result.baseline_rank):
            movement = "regressed"
        if result.search_result_count != result.rerank_candidate_count:
            candidate_note = f"search={result.search_result_count} rerank={result.rerank_candidate_count}"
        else:
            candidate_note = f"candidates={result.rerank_candidate_count}"
        print(
            f"{result.query.id:<26} {marker:<8} {movement:<9} "
            f"rank {result.baseline_rank or '-'}->{result.reranked_rank or '-'}  "
            f"{candidate_note:<22}  "
            f"TOTAL {result.total_seconds:.2f}s (load {result.load_seconds:.2f} "
            f"+ search {result.search_seconds:.2f} + rerank {result.rerank_seconds:.2f})"
        )
        print(f"  query: {result.query.query}")
        print(f"  expected: {result.query.expected}")
        print(f"  reranked top-{show_top}:")
        for line in _format_rows(result.reranked_rows, limit=show_top):
            print(f"    {line}")

        summary["queries"].append(
            {
                "id": result.query.id,
                "slice": result.query.slice,
                "optional": result.query.optional,
                "movement": movement,
                "baseline_rank": result.baseline_rank,
                "reranked_rank": result.reranked_rank,
                "search_result_count": result.search_result_count,
                "rerank_candidate_count": result.rerank_candidate_count,
                "load_seconds": result.load_seconds,
                "search_seconds": result.search_seconds,
                "rerank_seconds": result.rerank_seconds,
                "total_seconds": result.total_seconds,
                "baseline_rows": [asdict(row) for row in result.baseline_rows],
                "reranked_rows": [asdict(row) for row in result.reranked_rows],
            }
        )

    if not include_optional:
        print("")
        print("Optional probes were skipped. Pass --include-optional to include the harder ECB YC/HICP probes.")

    return summary


async def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", type=Path, default=DEFAULT_QUERIES, help="Eval YAML path.")
    parser.add_argument(
        "--catalog-root",
        default=None,
        help=(
            "Catalog root URL containing namespace subfolders. "
            f"Defaults to {DEFAULT_LOCAL_CATALOG_ROOT} when present, else {DEFAULT_CATALOG_ROOT}."
        ),
    )
    parser.add_argument(
        "--prepare-catalogs",
        action="store_true",
        help=f"Build missing schema v1 catalogs under {DEFAULT_LOCAL_CATALOG_ROOT}.",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="sentence-transformers CrossEncoder model.")
    parser.add_argument("--candidate-limit", type=int, default=50, help="Baseline candidate pool size to rerank.")
    parser.add_argument(
        "--rerank-cap",
        type=int,
        default=None,
        help="Hard cap on candidates sent to the reranker (default: --candidate-limit). Use 0 for no cap.",
    )
    parser.add_argument("--report-limit", type=int, default=10, help="Metric/report cutoff after reranking.")
    parser.add_argument("--batch-size", type=int, default=16, help="CrossEncoder batch size.")
    parser.add_argument(
        "--backend",
        default="torch",
        choices=("torch", "onnx"),
        help="CrossEncoder inference backend (onnx requires sentence-transformers[onnx]).",
    )
    parser.add_argument(
        "--onnx-file",
        default=None,
        help='ONNX model path inside the Hub repo (e.g. "onnx/model_O3.onnx").',
    )
    parser.add_argument(
        "--candidate-text",
        default="minimal",
        choices=("full", "minimal"),
        help="Document text fed to the reranker: full metadata or title+code only.",
    )
    parser.add_argument("--device", default="cpu", choices=("cpu", "cuda"), help="Reranker device.")
    parser.add_argument("--search-only", action="store_true", help="Skip reranking; report baseline search metrics.")
    parser.add_argument("--include-optional", action="store_true", help="Include optional/harder probes.")
    parser.add_argument(
        "--allow-unretrievable",
        action="store_true",
        help="Skip recall validation (for queries_negative.yaml diagnostics).",
    )
    parser.add_argument("--show-top", type=int, default=10, help="Number of reranked rows to print per query.")
    parser.add_argument("--report-json", type=Path, help="Optional JSON report output path.")
    args = parser.parse_args(argv)

    rerank_cap = args.candidate_limit if args.rerank_cap is None else args.rerank_cap
    if args.report_limit > args.candidate_limit:
        parser.error("--report-limit must be <= --candidate-limit")
    if rerank_cap > 0 and args.report_limit > rerank_cap:
        parser.error("--report-limit must be <= --rerank-cap")

    queries = _load_queries(args.queries, include_optional=args.include_optional)
    if not queries:
        parser.error("No queries selected")

    namespaces = {query.namespace for query in queries}
    catalog_root = _resolve_catalog_root(args.catalog_root)
    local_root = _local_catalog_root(catalog_root) or DEFAULT_LOCAL_CATALOG_ROOT
    if args.prepare_catalogs:
        local_root.mkdir(parents=True, exist_ok=True)
        catalog_root = f"file://{local_root}"

    prepare_namespaces = _all_namespaces_in_file(args.queries) if args.prepare_catalogs else namespaces
    missing = _missing_or_stale_namespaces(catalog_root, prepare_namespaces if args.prepare_catalogs else namespaces)
    if args.prepare_catalogs:
        if missing:
            print(f"Preparing schema v1 catalogs under {local_root}: {', '.join(missing)}", flush=True)
            await _prepare_local_catalogs(local_root, prepare_namespaces)
        else:
            print(f"All required catalogs already present under {local_root}", flush=True)
    elif missing:
        parser.error(
            "Missing schema v1 catalogs: "
            f"{', '.join(missing)}. Re-run with --prepare-catalogs or point --catalog-root at local snapshots."
        )

    recall_limit = _recall_limit_for_queries(args.queries, candidate_limit=args.candidate_limit)
    if recall_limit > args.candidate_limit:
        parser.error(
            f"--candidate-limit {args.candidate_limit} is below recall_limit {recall_limit} "
            f"declared in {args.queries.name}"
        )

    catalog_cache: dict[str, Catalog] = {}
    if not args.allow_unretrievable:
        print(f"Validating recall (expected in top-{recall_limit})...", flush=True)
        await _validate_queries_recallable(
            queries=queries,
            root=catalog_root,
            recall_limit=recall_limit,
            catalog_cache=catalog_cache,
        )
        print(f"  all {len(queries)} probes retrievable", flush=True)

    extra = "[onnx]" if args.backend == "onnx" else ""
    if not args.search_only:
        try:
            import sentence_transformers  # noqa: F401
        except ImportError:
            print(
                "Missing dependency: sentence-transformers (and accelerate for zeroentropy/zerank-1). "
                "Run with:\n"
                f"  uv run --with 'sentence-transformers{extra}' --with accelerate "
                "python packages/sdmx/scripts/rerank_search.py ...",
                file=sys.stderr,
            )
            return 2
        if args.backend == "onnx":
            try:
                import onnxruntime  # noqa: F401
            except ImportError:
                print(
                    "Missing dependency for --backend onnx. Run with:\n"
                    "  uv run --with 'sentence-transformers[onnx]' "
                    "python packages/sdmx/scripts/rerank_search.py ...",
                    file=sys.stderr,
                )
                return 2

    print(f"Loaded {len(queries)} queries from {args.queries}", flush=True)
    print(f"Catalog root: {catalog_root}", flush=True)
    if args.search_only:
        print("Reranker: disabled (--search-only)", flush=True)
    else:
        print(
            f"Loading reranker: {args.model} (backend={args.backend}, device={args.device})",
            flush=True,
        )
    reranker, model_load_seconds = _load_reranker(
        args.model,
        device=args.device,
        backend=args.backend,
        onnx_file=args.onnx_file,
        search_only=args.search_only,
        batch_size=args.batch_size,
        candidate_text=args.candidate_text,
    )
    if not args.search_only:
        print(f"  model ready ({model_load_seconds:.2f}s)", flush=True)

    t0 = time.perf_counter()
    results = await _evaluate(
        queries=queries,
        root=catalog_root,
        reranker=reranker,
        candidate_limit=args.candidate_limit,
        rerank_cap=rerank_cap,
        report_limit=args.report_limit,
        catalog_cache=catalog_cache,
    )
    eval_seconds = time.perf_counter() - t0
    total_seconds = model_load_seconds + eval_seconds
    print(
        f"\nTotal wall-clock: {total_seconds:.2f}s "
        f"(model {model_load_seconds:.2f}s + eval {eval_seconds:.2f}s)",
        flush=True,
    )
    summary = _summarize(results, include_optional=args.include_optional, show_top=args.show_top)
    summary["model"] = args.model
    summary["backend"] = args.backend
    summary["onnx_file"] = args.onnx_file
    summary["candidate_text"] = args.candidate_text
    summary["catalog_root"] = catalog_root
    summary["candidate_limit"] = args.candidate_limit
    summary["recall_limit"] = recall_limit
    summary["rerank_cap"] = rerank_cap
    summary["model_load_seconds"] = model_load_seconds
    summary["eval_seconds"] = eval_seconds
    summary["total_seconds"] = total_seconds

    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"\nReport written: {args.report_json}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main(sys.argv[1:])))
