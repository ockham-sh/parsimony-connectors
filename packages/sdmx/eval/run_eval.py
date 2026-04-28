"""Phase 0b eval harness: stratified quality evaluation for SDMX catalogs.

Runs a YAML-defined query set against a :class:`parsimony.Catalog`, reports
``recall@10``, ``NDCG@10``, and ``top-1 hit rate`` **per slice and in
aggregate**. Phase 3's ship gate evaluates each slice independently because
aggregate numbers can mask a 15-point regression on a failure-mode slice
(§0b in the plan).

Supports two modes:

* **Single** — ``run_eval.py --catalog <url> --queries <path>``. Reports
  one catalog's quality against the query set. Use this to capture the
  pre-Phase-2 baseline today.
* **Compare** — ``run_eval.py --catalog <url> --baseline <url> --queries <path>``.
  Runs both catalogs; reports deltas and ``top-1 agreement`` (fraction of
  queries where both catalogs returned the same top-1 code).

Typical usage::

    # Capture baseline against the locally-published HICP catalog
    uv run python eval/run_eval.py \\
        --catalog file:///home/espinet/ockham/catalogs/sdmx/repo/sdmx_series_ecb_hicp \\
        --queries eval/queries.yaml \\
        --report eval/reports/baseline_hicp.json

    # After Phase 2 ships, compare:
    uv run python eval/run_eval.py \\
        --catalog file:///.../phase2_hicp \\
        --baseline file:///.../baseline_hicp \\
        --queries eval/queries.yaml \\
        --report eval/reports/phase2_vs_baseline.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from parsimony.catalog import Catalog

SLICES = (
    "codelist_exact",
    "colloquial_controlled",
    "ambiguous_single",
    "long_compound",
    "code_level",
)


@dataclass(frozen=True)
class Query:
    """One eval query with ground-truth relevance.

    ``relevant_codes`` holds the 2-3 series keys curated as known-relevant.
    Empty list means "curation pending" — the harness still runs the
    query for observation but skips it from aggregate metrics.
    """

    id: str
    slice: str
    query: str
    relevant_codes: list[str]
    notes: str = ""


@dataclass
class QueryResult:
    query: Query
    top_codes: list[str]  # top-10 result codes in rank order
    recall_at_10: float
    ndcg_at_10: float
    top1_hit: bool  # top-1 is a relevant code
    curation_skipped: bool  # True when relevant_codes is empty — excluded from agg


def load_queries(path: Path) -> list[Query]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if "queries" not in raw:
        raise ValueError(f"{path}: expected top-level 'queries' key")
    out: list[Query] = []
    seen: set[str] = set()
    for item in raw["queries"]:
        qid = item["id"]
        if qid in seen:
            raise ValueError(f"Duplicate query id: {qid!r}")
        seen.add(qid)
        slice_ = item["slice"]
        if slice_ not in SLICES:
            raise ValueError(f"{qid}: unknown slice {slice_!r}; expected one of {SLICES}")
        out.append(
            Query(
                id=qid,
                slice=slice_,
                query=item["query"],
                relevant_codes=list(item.get("relevant_codes", [])),
                notes=item.get("notes", ""),
            )
        )
    return out


def recall_at_k(relevant: list[str], top_k: list[str]) -> float:
    if not relevant:
        return 0.0
    hits = sum(1 for code in relevant if code in top_k)
    return hits / len(relevant)


def ndcg_at_k(relevant: list[str], top_k: list[str]) -> float:
    """NDCG with binary relevance (0/1), cap at len(top_k).

    DCG = sum over i where top_k[i] is relevant of 1 / log2(i + 2).
    Ideal = sum over i in [0, min(|relevant|, len(top_k))) of 1 / log2(i + 2).
    NDCG = DCG / ideal.
    """
    if not relevant:
        return 0.0
    rel = set(relevant)
    dcg = sum(1.0 / math.log2(i + 2) for i, code in enumerate(top_k) if code in rel)
    ideal_n = min(len(rel), len(top_k))
    ideal = sum(1.0 / math.log2(i + 2) for i in range(ideal_n))
    return dcg / ideal if ideal > 0 else 0.0


async def _run_queries(catalog: Catalog, queries: list[Query]) -> list[QueryResult]:
    results: list[QueryResult] = []
    for q in queries:
        hits = await catalog.search(q.query, limit=10)
        top_codes = [h.code for h in hits]
        skipped = not q.relevant_codes
        if skipped:
            results.append(
                QueryResult(
                    query=q,
                    top_codes=top_codes,
                    recall_at_10=0.0,
                    ndcg_at_10=0.0,
                    top1_hit=False,
                    curation_skipped=True,
                )
            )
            continue
        rel = set(q.relevant_codes)
        results.append(
            QueryResult(
                query=q,
                top_codes=top_codes,
                recall_at_10=recall_at_k(q.relevant_codes, top_codes),
                ndcg_at_10=ndcg_at_k(q.relevant_codes, top_codes),
                top1_hit=bool(top_codes) and top_codes[0] in rel,
                curation_skipped=False,
            )
        )
    return results


@dataclass
class SliceStats:
    count: int = 0
    curated_count: int = 0
    recall_at_10: float = 0.0
    ndcg_at_10: float = 0.0
    top1_hit_rate: float = 0.0
    query_ids: list[str] = field(default_factory=list)


def _slice_stats(results: list[QueryResult], slice_: str | None) -> SliceStats:
    subset = [r for r in results if slice_ is None or r.query.slice == slice_]
    curated = [r for r in subset if not r.curation_skipped]
    stats = SliceStats(
        count=len(subset),
        curated_count=len(curated),
        query_ids=[r.query.id for r in subset],
    )
    if curated:
        n = len(curated)
        stats.recall_at_10 = sum(r.recall_at_10 for r in curated) / n
        stats.ndcg_at_10 = sum(r.ndcg_at_10 for r in curated) / n
        stats.top1_hit_rate = sum(1 for r in curated if r.top1_hit) / n
    return stats


def _format_stats_row(label: str, s: SliceStats) -> str:
    if s.curated_count == 0:
        return f"{label:<24} {s.count:>5}  (no curated ground-truth — skipped)"
    return (
        f"{label:<24} {s.count:>5} ({s.curated_count}q)  "
        f"R@10={s.recall_at_10:6.3f}  NDCG@10={s.ndcg_at_10:6.3f}  "
        f"top1={s.top1_hit_rate:6.3f}"
    )


def _print_summary(results: list[QueryResult], label: str) -> None:
    print("", flush=True)
    print(f"=== {label} — per-slice ===", flush=True)
    for slice_ in SLICES:
        print(_format_stats_row(slice_, _slice_stats(results, slice_)), flush=True)
    print("-" * 72, flush=True)
    print(_format_stats_row("AGGREGATE", _slice_stats(results, None)), flush=True)


def _build_report(
    catalog_url: str,
    baseline_url: str | None,
    results: list[QueryResult],
    baseline_results: list[QueryResult] | None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "catalog_url": catalog_url,
        "baseline_url": baseline_url,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "per_slice": {},
        "aggregate": {},
        "queries": [],
    }

    for slice_ in SLICES:
        s = _slice_stats(results, slice_)
        report["per_slice"][slice_] = {
            "count": s.count,
            "curated_count": s.curated_count,
            "recall_at_10": s.recall_at_10,
            "ndcg_at_10": s.ndcg_at_10,
            "top1_hit_rate": s.top1_hit_rate,
        }
    agg = _slice_stats(results, None)
    report["aggregate"] = {
        "count": agg.count,
        "curated_count": agg.curated_count,
        "recall_at_10": agg.recall_at_10,
        "ndcg_at_10": agg.ndcg_at_10,
        "top1_hit_rate": agg.top1_hit_rate,
    }

    # Per-query detail so diffs can be diagnosed without re-running
    for r in results:
        entry = {
            "id": r.query.id,
            "slice": r.query.slice,
            "query": r.query.query,
            "relevant": r.query.relevant_codes,
            "top_codes": r.top_codes,
            "recall_at_10": r.recall_at_10,
            "ndcg_at_10": r.ndcg_at_10,
            "top1_hit": r.top1_hit,
            "curation_skipped": r.curation_skipped,
        }
        report["queries"].append(entry)

    if baseline_results is not None:
        # Top-1 agreement: identical top-1 code (regardless of relevance)
        by_id = {r.query.id: r for r in baseline_results}
        agreements = 0
        compared = 0
        per_slice_agreement: dict[str, tuple[int, int]] = {s: (0, 0) for s in SLICES}
        for r in results:
            if r.query.id not in by_id:
                continue
            other = by_id[r.query.id]
            if not r.top_codes or not other.top_codes:
                continue
            compared += 1
            compared_slice, matches_slice = per_slice_agreement[r.query.slice]
            per_slice_agreement[r.query.slice] = (compared_slice + 1, matches_slice)
            if r.top_codes[0] == other.top_codes[0]:
                agreements += 1
                c, m = per_slice_agreement[r.query.slice]
                per_slice_agreement[r.query.slice] = (c, m + 1)

        report["top1_agreement"] = {
            "overall": agreements / compared if compared else 0.0,
            "per_slice": {
                slice_: (matches / count if count else 0.0)
                for slice_, (count, matches) in per_slice_agreement.items()
            },
        }

    return report


def _print_comparison(
    results: list[QueryResult],
    baseline_results: list[QueryResult],
) -> None:
    print("", flush=True)
    print("=== COMPARISON (deltas: current vs baseline) ===", flush=True)
    for slice_ in SLICES:
        cur = _slice_stats(results, slice_)
        base = _slice_stats(baseline_results, slice_)
        if cur.curated_count == 0:
            print(f"{slice_:<24} (no curated ground-truth)", flush=True)
            continue
        d_recall = cur.recall_at_10 - base.recall_at_10
        d_ndcg = cur.ndcg_at_10 - base.ndcg_at_10
        d_top1 = cur.top1_hit_rate - base.top1_hit_rate
        print(
            f"{slice_:<24}  ΔR@10={d_recall:+.3f}  ΔNDCG@10={d_ndcg:+.3f}  Δtop1={d_top1:+.3f}",
            flush=True,
        )
    print("-" * 72, flush=True)
    cur_agg = _slice_stats(results, None)
    base_agg = _slice_stats(baseline_results, None)
    if cur_agg.curated_count > 0:
        print(
            f"{'AGGREGATE':<24}  "
            f"ΔR@10={cur_agg.recall_at_10 - base_agg.recall_at_10:+.3f}  "
            f"ΔNDCG@10={cur_agg.ndcg_at_10 - base_agg.ndcg_at_10:+.3f}  "
            f"Δtop1={cur_agg.top1_hit_rate - base_agg.top1_hit_rate:+.3f}",
            flush=True,
        )


async def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Catalog URL (file:// or hf://)")
    parser.add_argument("--baseline", default=None, help="Optional baseline catalog URL for comparison")
    parser.add_argument("--queries", required=True, type=Path, help="YAML path for query set")
    parser.add_argument("--report", default=None, type=Path, help="Optional JSON report output path")
    args = parser.parse_args(argv)

    queries = load_queries(args.queries)
    print(f"Loaded {len(queries)} queries from {args.queries}", flush=True)

    print(f"Loading catalog: {args.catalog}", flush=True)
    catalog = await Catalog.from_url(args.catalog)
    print(f"  {len(catalog)} entries", flush=True)

    t0 = time.perf_counter()
    results = await _run_queries(catalog, queries)
    print(f"  eval wall-clock: {time.perf_counter() - t0:.2f}s", flush=True)
    _print_summary(results, f"CATALOG: {args.catalog}")

    baseline_results: list[QueryResult] | None = None
    if args.baseline:
        print("", flush=True)
        print(f"Loading baseline: {args.baseline}", flush=True)
        baseline_catalog = await Catalog.from_url(args.baseline)
        print(f"  {len(baseline_catalog)} entries", flush=True)
        t0 = time.perf_counter()
        baseline_results = await _run_queries(baseline_catalog, queries)
        print(f"  eval wall-clock: {time.perf_counter() - t0:.2f}s", flush=True)
        _print_summary(baseline_results, f"BASELINE: {args.baseline}")
        _print_comparison(results, baseline_results)

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        report = _build_report(args.catalog, args.baseline, results, baseline_results)
        args.report.write_text(json.dumps(report, indent=2, default=str))
        print("", flush=True)
        print(f"Report written: {args.report}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main(sys.argv[1:])))
