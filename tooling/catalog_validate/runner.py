"""Run catalog compatibility and search-probe validation."""

from __future__ import annotations

from dataclasses import dataclass, field

from parsimony.catalog import Catalog, entity_key
from parsimony.catalog.storage import SCHEMA_VERSION

from catalog_validate.fixtures import CatalogQuery, CatalogQuerySet
from catalog_validate.probes import indexed_fields
from catalog_validate.snapshot_meta import snapshot_meta_for


@dataclass
class ProbeResult:
    query: CatalogQuery
    hit: bool
    top_codes: list[str]
    error: str | None = None


@dataclass
class ValidationReport:
    catalog_url: str
    skipped: bool = False
    skip_reason: str = ""
    schema_ok: bool = False
    entry_count: int = 0
    indexed_fields: dict[str, str] = field(default_factory=dict)
    probe_results: list[ProbeResult] = field(default_factory=list)

    @property
    def required_recall(self) -> float:
        required = [r for r in self.probe_results if r.query.required and not r.query.optional]
        if not required:
            return 1.0
        hits = sum(1 for r in required if r.hit)
        return hits / len(required)

    @property
    def ok(self) -> bool:
        if self.skipped:
            return True
        if not self.schema_ok:
            return False
        return all(
            r.hit or r.query.optional or not r.query.required or (r.error or "").startswith("skipped:")
            for r in self.probe_results
        )


def _resolve_catalog_url(
    query: CatalogQuery,
    default_url: str,
    query_set: CatalogQuerySet,
    *,
    catalog_root: str | None = None,
) -> str:
    if query.catalog_url:
        return query.catalog_url
    root = catalog_root or query_set.catalog_root
    if query.namespace and root:
        return f"{root.rstrip('/')}/{query.namespace}"
    # CLI --catalog-url wins over queries.yaml canonical URL (local vs remote validation).
    if default_url:
        return default_url
    if query_set.catalog_url:
        return query_set.catalog_url
    raise ValueError(f"Cannot resolve catalog URL for probe {query.id!r}")


def _run_probe(catalog: Catalog, query: CatalogQuery) -> ProbeResult:
    try:
        matches = catalog.search(query.query, limit=query.limit)
        codes = [entity_key(m.namespace, m.code)[1] for m in matches]
        hit = query.expected_code in codes
        return ProbeResult(query=query, hit=hit, top_codes=codes[:5])
    except Exception as exc:  # noqa: BLE001 - validation reports probe failures
        return ProbeResult(query=query, hit=False, top_codes=[], error=f"{type(exc).__name__}: {exc}")


def validate_catalog(
    catalog_url: str,
    query_set: CatalogQuerySet | None = None,
    *,
    allow_missing: bool = False,
    catalog_root: str | None = None,
) -> ValidationReport:
    """Load *catalog_url* and run compatibility + optional curated probes."""
    report = ValidationReport(catalog_url=catalog_url)
    try:
        catalog = Catalog.load(catalog_url)
    except Exception as exc:  # noqa: BLE001
        if allow_missing:
            report.skipped = True
            report.skip_reason = f"{type(exc).__name__}: {exc}"
            return report
        raise

    meta = snapshot_meta_for(catalog, catalog_url)
    report.entry_count = len(catalog)
    report.schema_ok = meta.schema_version == SCHEMA_VERSION
    report.indexed_fields = indexed_fields(meta.index_fields)

    if query_set is None:
        return report

    # Group probes by resolved URL so SDMX multi-bundle files work.
    buckets: dict[str, list[CatalogQuery]] = {}
    for q in query_set.queries:
        url = _resolve_catalog_url(q, catalog_url, query_set, catalog_root=catalog_root)
        buckets.setdefault(url, []).append(q)

    for url, queries in buckets.items():
        try:
            bucket_catalog = catalog if url == catalog_url else Catalog.load(url)
        except FileNotFoundError as exc:
            for q in queries:
                if q.optional or not q.required:
                    report.probe_results.append(
                        ProbeResult(query=q, hit=False, top_codes=[], error=f"skipped: {exc}")
                    )
                else:
                    raise
            continue
        for q in queries:
            report.probe_results.append(_run_probe(bucket_catalog, q))

    return report


def format_report(report: ValidationReport) -> str:
    lines: list[str] = []
    lines.append(f"catalog_url: {report.catalog_url}")
    if report.skipped:
        lines.append(f"status: SKIPPED ({report.skip_reason})")
        return "\n".join(lines)
    lines.append(f"schema_ok: {report.schema_ok}")
    lines.append(f"entry_count: {report.entry_count}")
    lines.append(f"indexed_fields: {report.indexed_fields}")
    if report.probe_results:
        lines.append(f"required_recall: {report.required_recall:.2f}")
        for result in report.probe_results:
            status = "HIT" if result.hit else "MISS"
            opt = " optional" if result.query.optional else ""
            lines.append(f"  [{status}{opt}] {result.query.id}: {result.query.query!r}")
            if not result.hit:
                if result.error:
                    lines.append(f"    error: {result.error}")
                elif result.top_codes:
                    lines.append(f"    top: {result.top_codes}")
    lines.append(f"overall: {'OK' if report.ok else 'FAIL'}")
    return "\n".join(lines)
