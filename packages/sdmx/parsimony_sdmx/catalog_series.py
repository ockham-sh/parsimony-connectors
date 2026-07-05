"""Per-flow SDMX series catalog: CSV fetch, columnar parquet, parsimony indexes."""

from __future__ import annotations

import csv
import json
import logging
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from parsimony.catalog import BM25Index, Catalog, Entity, HybridIndex, VectorIndex
from parsimony.catalog.contracts import CatalogBackendConfig
from parsimony.catalog.indexes import CatalogIndex
from parsimony.catalog.storage import read_meta
from parsimony.embedder import SentenceTransformerEmbedder
from parsimony.ranking import ZScoreFusion

from parsimony_sdmx.catalog_policy import HYBRID_BM25_WEIGHT, HYBRID_VECTOR_WEIGHT
from parsimony_sdmx.connectors._agencies import AgencyId, to_namespace_token
from parsimony_sdmx.core.models import StructureRecord
from parsimony_sdmx.core.titles import compose_series_title
from parsimony_sdmx.io.http import HttpConfig, build_session
from parsimony_sdmx.io.structure_json import read_structure
from parsimony_sdmx.series_fields import META_FILENAME, SERIES_PARQUET, dim_code_field, dim_label_field

logger = logging.getLogger(__name__)

PARQUET_BATCH_ROWS = 10_000
SKIP_COLUMNS = frozenset({"DATAFLOW", "LAST UPDATE", "TIME_PERIOD", "KEY"})
CATALOG_KIND = "sdmx_series_catalog_v1"
TITLE_INDEX_MAX_VALUES = 100_000

AGENCY_CSV: dict[AgencyId, tuple[str, str]] = {
    AgencyId.ESTAT: (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
        "application/vnd.sdmx.data+csv;version=1.0.0",
    ),
    AgencyId.ECB: (
        "https://data-api.ecb.europa.eu/service",
        "text/csv",
    ),
    AgencyId.IMF_DATA: (
        "https://api.imf.org/external/sdmx/2.1",
        "application/vnd.sdmx.data+csv;version=1.0.0",
    ),
}

SERIES_AGENCIES: frozenset[AgencyId] = frozenset({AgencyId.ESTAT, AgencyId.ECB, AgencyId.IMF_DATA})


def series_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    agency_token = to_namespace_token(agency)
    return f"sdmx_series_{agency_token}_{dataset_id.lower()}"


def structure_marker_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    agency_token = to_namespace_token(agency)
    return f"sdmx_structure_{agency_token}_{dataset_id.lower()}"


def labels_from_structure(record: StructureRecord) -> dict[str, dict[str, str]]:
    cl_map = {cl.codelist_id: {c.code: c.label for c in cl.codes} for cl in record.codelists}
    out: dict[str, dict[str, str]] = {}
    for dim in record.dimensions:
        cl_id = dim.codelist_id
        if cl_id and cl_id in cl_map:
            out[dim.dimension_id] = cl_map[cl_id]
        else:
            out[dim.dimension_id] = {}
    return out


def _series_csv_url(agency: AgencyId, dataset_id: str) -> tuple[str, str]:
    base, accept = AGENCY_CSV[agency]
    # ECB and IMF require uppercase flow IDs; ESTAT is case-insensitive but upper is safe.
    flow_id = dataset_id.upper() if agency in (AgencyId.ECB, AgencyId.IMF_DATA) else dataset_id
    url = f"{base}/data/{flow_id}?detail=serieskeysonly"
    return url, accept


def _parse_csv_header(header: list[str], dsd_order: Sequence[str]) -> tuple[list[int], list[str]]:
    """Return column indices and dimension ids aligned to CSV columns."""
    col_dims: list[tuple[int, str]] = []
    for idx, name in enumerate(header):
        col = name.strip()
        if col in SKIP_COLUMNS:
            continue
        col_dims.append((idx, col))
    dim_ids = [d for _, d in col_dims]
    if set(dim_ids) >= set(dsd_order):
        indices = []
        ordered_dims = []
        for dim_id in dsd_order:
            for idx, name in col_dims:
                if name == dim_id:
                    indices.append(idx)
                    ordered_dims.append(dim_id)
                    break
        return indices, ordered_dims
    return [idx for idx, _ in col_dims], dim_ids


def _strip_flow_prefix(key: str, dataset_id: str) -> str:
    """Strip a redundant leading ``<dataset_id>.`` flow prefix from a raw SDMX-CSV ``KEY`` value.

    Some agencies' SDMX-CSV export (observed on ECB) prefixes the ``KEY`` column with the
    dataflow id (e.g. ``"YC.B.U2.EUR..."``), which is not the bare key ``sdmx_fetch``'s
    ``series_ref`` expects — passing it straight through duplicates the flow id in the request
    URL and 400s at the provider. Case-insensitive since ECB/IMF request the flow uppercased but
    the export isn't guaranteed to echo that same case back.
    """
    prefix, sep, rest = key.partition(".")
    return rest if sep and prefix.upper() == dataset_id.upper() else key


def _series_row_dict(
    *,
    row: list[str],
    col_indices: list[int],
    dim_ids: list[str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
    key_idx: int | None,
    dataset_id: str,
) -> dict[str, str] | None:
    dim_values: dict[str, str] = {}
    for col_idx, dim_id in zip(col_indices, dim_ids, strict=True):
        if col_idx >= len(row):
            continue
        code = row[col_idx].strip()
        if code:
            dim_values[dim_id] = code
    if not dim_values:
        return None
    if key_idx is not None and key_idx < len(row):
        key = row[key_idx].strip()
    else:
        key = ".".join(dim_values.get(d, "") for d in dsd_order)
    key = _strip_flow_prefix(key.strip("."), dataset_id)
    if not key:
        return None
    title = compose_series_title(dim_values, dsd_order, labels)
    if not title.strip():
        title = key

    out: dict[str, str] = {"key": key, "title": title}
    for dim_id in dsd_order:
        code = dim_values.get(dim_id, "")
        label = labels.get(dim_id, {}).get(code, code) if code else ""
        out[dim_code_field(dim_id)] = code
        out[dim_label_field(dim_id)] = label
    return out


def series_parquet_schema(dsd_order: Sequence[str]) -> pa.Schema:
    fields = [("key", pa.string()), ("title", pa.string())]
    for dim_id in dsd_order:
        fields.append((dim_code_field(dim_id), pa.string()))
        fields.append((dim_label_field(dim_id), pa.string()))
    return pa.schema(fields)


def build_series_parquet_columns(
    agency: AgencyId,
    dataset_id: str,
    *,
    structure: StructureRecord,
    out_parquet: Path,
    session: requests.Session | None = None,
    http_config: HttpConfig | None = None,
) -> int:
    """Stream CSV serieskeysonly into columnar ``series.parquet``. Returns series count."""
    url, accept = _series_csv_url(agency, dataset_id)
    cfg = http_config or HttpConfig(read_timeout=600.0, max_response_bytes=800 * 1024 * 1024)
    own_session = session is None
    sess = session or build_session(cfg)
    labels = labels_from_structure(structure)
    dsd_order = list(structure.dsd_order)
    schema = series_parquet_schema(dsd_order)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_parquet.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()

    count = 0
    batch: list[dict[str, str]] = []
    writer: pq.ParquetWriter | None = None
    response: requests.Response | None = None
    try:
        response = sess.get(
            url,
            timeout=cfg.timeout,
            stream=True,
            headers={"Accept": accept, "Accept-Encoding": "gzip, deflate"},
        )
        response.raise_for_status()
        response.raw.decode_content = True
        header_row: str | None = None
        col_indices: list[int] = []
        dim_ids: list[str] = []
        key_idx: int | None = None

        for raw_line in response.iter_lines(decode_unicode=False):
            if not raw_line:
                continue
            line = raw_line.decode("utf-8", errors="replace")
            if header_row is None:
                header_row = line
                header = next(csv.reader([header_row]))
                key_idx = header.index("KEY") if "KEY" in header else None
                col_indices, dim_ids = _parse_csv_header(header, dsd_order)
                continue
            row = next(csv.reader([line]))
            record = _series_row_dict(
                row=row,
                col_indices=col_indices,
                dim_ids=dim_ids,
                dsd_order=dsd_order,
                labels=labels,
                key_idx=key_idx,
                dataset_id=dataset_id,
            )
            if record is None:
                continue
            batch.append(record)
            count += 1
            if len(batch) >= PARQUET_BATCH_ROWS:
                table = pa.Table.from_pylist(batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(tmp, schema, compression="zstd")
            writer.write_table(table)
            batch.clear()
        if header_row is None:
            raise ValueError(f"Empty CSV from {url}")
    finally:
        if writer is not None:
            writer.close()
        if response is not None:
            response.close()
        if own_session:
            sess.close()

    if count == 0:
        if tmp.exists():
            tmp.unlink()
        raise ValueError(f"No series parsed for {agency.value}/{dataset_id}")
    tmp.replace(out_parquet)
    return count


def build_series_parquet(
    agency: AgencyId,
    dataset_id: str,
    *,
    structure: StructureRecord,
    out_parquet: Path,
    session: requests.Session | None = None,
    http_config: HttpConfig | None = None,
) -> int:
    """Fetch CSV into staging parquet (columnar schema)."""
    return build_series_parquet_columns(
        agency,
        dataset_id,
        structure=structure,
        out_parquet=out_parquet,
        session=session,
        http_config=http_config,
    )


def collect_distinct_from_columnar(
    series_parquet: Path,
    dsd_order: tuple[str, ...],
) -> dict[str, dict[str, str]]:
    """Single-pass distinct collection over an already-columnar series parquet."""
    cols = []
    for dim in dsd_order:
        cols.extend([dim_code_field(dim), dim_label_field(dim)])
    distinct: dict[str, dict[str, str]] = {dim: {} for dim in dsd_order}
    for batch in pq.ParquetFile(series_parquet).iter_batches(batch_size=PARQUET_BATCH_ROWS, columns=cols):
        for dim in dsd_order:
            code_col = dim_code_field(dim)
            label_col = dim_label_field(dim)
            dim_seen = distinct[dim]
            for row in batch.select([code_col, label_col]).to_pylist():
                code = str(row.get(code_col, "")).strip()
                if code and code not in dim_seen:
                    dim_seen[code] = str(row.get(label_col, code)).strip() or code
    return distinct


def _dim_label_index(embedder: SentenceTransformerEmbedder | None) -> HybridIndex:
    return HybridIndex(
        components=[BM25Index(), VectorIndex(embedder=embedder or SentenceTransformerEmbedder())],
        fusion=ZScoreFusion(weights={"bm25": HYBRID_BM25_WEIGHT, "vector": HYBRID_VECTOR_WEIGHT}),
    )


def _collect_distinct_titles(series_parquet: Path, *, limit: int = TITLE_INDEX_MAX_VALUES) -> list[str]:
    """Collect up to *limit* distinct series titles from the parquet in one pass."""
    seen: set[str] = set()
    titles: list[str] = []
    for batch in pq.ParquetFile(series_parquet).iter_batches(batch_size=PARQUET_BATCH_ROWS, columns=["title"]):
        for title in batch.column("title").to_pylist():
            t = str(title).strip() if title is not None else ""
            if t and t not in seen:
                seen.add(t)
                titles.append(t)
                if len(titles) >= limit:
                    return titles
    return titles


def _title_entities_for_parquet(series_parquet: Path, namespace: str) -> list[Entity]:
    """Create one index Entity per distinct series title in *series_parquet*."""
    return [
        Entity(namespace=namespace, code=f"__title__{i}", title=title, metadata={"title": title})
        for i, title in enumerate(_collect_distinct_titles(series_parquet))
    ]


def _index_entities_for_distinct(
    namespace: str,
    dsd_order: Sequence[str],
    distinct: Mapping[str, dict[str, str]],
) -> list[Entity]:
    """Dimension-value index entities.

    ``title`` is set to the composite code (e.g. ``"FREQ:M"``) rather than the
    human-readable label (e.g. ``"Monthly"``), so these entities do not pollute
    the ``title`` BM25 index with labels that would trigger the exact-match
    shortcut and mask actual series titles (e.g. "Monthly, Germany") on broad
    title searches.  Only the ``{dim}_label`` and ``{dim}_code`` metadata fields
    are used for the per-dimension indexes.
    """
    entities: list[Entity] = []
    for dim_id in dsd_order:
        label_field = dim_label_field(dim_id)
        code_field = dim_code_field(dim_id)
        for code, label in distinct.get(dim_id, {}).items():
            entity_code = f"{dim_id}:{code}"
            entities.append(
                Entity(
                    namespace=namespace,
                    code=entity_code,
                    title=entity_code,  # not a human-readable label; keeps entity.title non-empty
                    metadata={label_field: label, code_field: code},
                )
            )
    return entities


def _series_catalog_indexes(
    dsd_order: Sequence[str],
    *,
    embedder: SentenceTransformerEmbedder | None,
) -> dict[str, CatalogIndex]:
    indexes: dict[str, CatalogIndex] = {"title": BM25Index()}
    for dim_id in dsd_order:
        indexes[dim_code_field(dim_id)] = BM25Index()
        indexes[dim_label_field(dim_id)] = _dim_label_index(embedder)
    return indexes


def build_series_catalog(
    series_parquet: Path,
    *,
    namespace: str,
    dsd_order: Sequence[str],
    distinct: Mapping[str, dict[str, str]] | None = None,
    embedder: SentenceTransformerEmbedder | None = None,
) -> Catalog:
    """Build a parquet-backed parsimony catalog for one SDMX series flow.

    Index entities are the union of dimension-value entities (for per-dim soft
    search) and title entities (one per distinct series title, for broad title
    search via the parquet backend).
    """
    if distinct is None:
        distinct = collect_distinct_from_columnar(series_parquet, tuple(dsd_order))
    dim_entities = _index_entities_for_distinct(namespace, dsd_order, distinct)
    title_entities = _title_entities_for_parquet(series_parquet, namespace)
    entities = dim_entities + title_entities
    field_links = {dim_label_field(dim_id): dim_code_field(dim_id) for dim_id in dsd_order}
    indexes = _series_catalog_indexes(dsd_order, embedder=embedder)
    catalog = Catalog(
        namespace,
        indexes=indexes,
        default_field="title",
        field_links=field_links,
    )
    catalog.set_entities(entities)
    catalog.build()
    backend = CatalogBackendConfig(
        kind="parquet",
        rows_path=SERIES_PARQUET,
        namespace=namespace,
        code_column="key",
        title_column="title",
        field_links=field_links,
    )
    catalog.attach_parquet_rows(series_parquet, config=backend)
    return catalog


def is_series_catalog(path: Path) -> bool:
    """Return True if *path* is a built parquet-backed SDMX series catalog."""
    meta_path = path / META_FILENAME
    if not meta_path.is_file() or not (path / SERIES_PARQUET).is_file():
        return False
    try:
        meta = read_meta(path)
    except (OSError, ValueError):
        return False
    return meta.backend.kind == "parquet" and (path / "indexes").is_dir()


def _atomic_write_dir(partial: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    partial.rename(target)


@dataclass(frozen=True, slots=True)
class CatalogBuildResult:
    namespace: str
    series_count: int
    catalog_kind: str = CATALOG_KIND


def build_flow_catalog(
    *,
    series_parquet: Path,
    namespace: str,
    agency: AgencyId,
    flow_id: str,
    structure: StructureRecord,
    catalogs_dir: Path,
    staging_dir: Path,
    builder: str = "packages/sdmx/scripts/sdmx_catalog_worker.py",
    embedder: SentenceTransformerEmbedder | None = None,
    distinct: dict[str, dict[str, str]] | None = None,
) -> CatalogBuildResult:
    """Assemble a parquet-backed parsimony catalog for one SDMX flow."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    partial = staging_dir / f"{namespace}.partial"
    if partial.exists():
        shutil.rmtree(partial)
    partial.mkdir(parents=True)

    series_count = pq.read_metadata(series_parquet).num_rows
    shutil.copy2(series_parquet, partial / SERIES_PARQUET)

    catalog = build_series_catalog(
        partial / SERIES_PARQUET,
        namespace=namespace,
        dsd_order=structure.dsd_order,
        distinct=distinct,
        embedder=embedder,
    )
    catalog._save_to_path(partial)  # noqa: SLF001

    raw_meta = json.loads((partial / META_FILENAME).read_text(encoding="utf-8"))
    raw_meta["sdmx"] = {
        "catalog_kind": CATALOG_KIND,
        "agency": agency.value,
        "flow_id": flow_id,
        "dsd_order": list(structure.dsd_order),
        "series_count": series_count,
    }
    from parsimony.catalog.storage import CatalogMeta
    from parsimony.catalog.validation import compute_manifest_contract_sha256

    meta_obj = CatalogMeta.model_validate(raw_meta)
    raw_meta["build"]["manifest_contract_sha256"] = compute_manifest_contract_sha256(meta_obj)
    (partial / META_FILENAME).write_text(json.dumps(raw_meta, indent=2), encoding="utf-8")

    target = catalogs_dir / namespace
    _atomic_write_dir(partial, target)
    return CatalogBuildResult(namespace=namespace, series_count=series_count, catalog_kind=CATALOG_KIND)


def load_structure_marker(catalogs_dir: Path, agency: AgencyId, dataset_id: str) -> StructureRecord:
    marker_ns = structure_marker_namespace(agency, dataset_id)
    path = catalogs_dir / marker_ns / "structure.json"
    if not path.is_file():
        raise FileNotFoundError(f"Structure marker missing: {path}")
    return read_structure(path)


__all__ = [
    "CATALOG_KIND",
    "CatalogBuildResult",
    "PARQUET_BATCH_ROWS",
    "SERIES_AGENCIES",
    "build_flow_catalog",
    "build_series_catalog",
    "build_series_parquet",
    "build_series_parquet_columns",
    "collect_distinct_from_columnar",
    "is_series_catalog",
    "labels_from_structure",
    "load_structure_marker",
    "series_namespace",
    "series_parquet_schema",
    "structure_marker_namespace",
]
