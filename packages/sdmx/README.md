# parsimony-sdmx

SDMX connector plugin for the [parsimony](https://parsimony.dev) framework. Harvests dataflow listings and per-dataset series keys from statistical agencies (ECB, Eurostat, IMF, World Bank), composes human-readable titles from the DSD + codelists, and publishes one parquet + FAISS bundle per catalog via `parsimony publish`.

No separate builder CLI, no intermediate on-disk cache — every call hits the live agency endpoint inside a spawned subprocess.

## Supported agencies

| Agency ID  | Source                                           |
|------------|--------------------------------------------------|
| `ECB`      | European Central Bank SDMX 2.1                   |
| `ESTAT`    | Eurostat SDMX 2.1                                |
| `IMF_DATA` | IMF SDMX 3 (``sdmx.imf.org``)                    |
| `WB_WDI`   | World Bank SDMX 2.1 (custom path × decade sweep) |

## Install

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
# From the monorepo root:
uv sync --package parsimony-sdmx --extra dev
uv pip install "parsimony-core[standard] @ file://$PWD/../parsimony"
```

The `standard` extra on `parsimony-core` pulls FAISS, BM25, and sentence-transformers — the default embedder stack used at publish time.

## Publish a single catalog

The plugin exposes two kinds of namespace:

- ``sdmx_datasets`` — one cross-agency catalog of every dataflow.
- ``sdmx_series_<agency>_<dataset_id>`` — one per-dataset catalog of series keys, e.g. ``sdmx_series_ecb_yc``.

Publish by name with ``--only`` (pure string lookup, no listing walk):

```bash
parsimony publish \
  --provider sdmx \
  --target "file:///tmp/parsimony-smoke/{namespace}" \
  --only sdmx_series_ecb_yc
```

The ``{namespace}`` placeholder is substituted before the push. The target scheme is what decides where the bundle lands:

| Scheme              | Destination                               | Extra required |
|---------------------|-------------------------------------------|----------------|
| ``file://<path>``   | Local filesystem                          | —              |
| ``hf://<repo>``     | Hugging Face dataset repo                 | ``standard``   |
| ``s3://<bucket>``   | S3 bucket                                 | ``s3``         |

A local publish produces:

```
/tmp/parsimony-smoke/sdmx_series_ecb_yc/
├── entries.parquet   # rows: (namespace, code, title, description, tags, metadata, embedding)
├── embeddings.faiss  # FAISS index aligned with entries.parquet
└── meta.json         # catalog metadata + embedder fingerprint
```

## Publish everything

Omit ``--only`` to walk every agency listing and publish one bundle per discovered ``(agency, dataset_id)`` pair plus the static ``sdmx_datasets`` catalog. Expect a long run — ESTAT alone has 8 k+ dataflows:

```bash
parsimony publish \
  --provider sdmx \
  --target "file:///tmp/parsimony-smoke/{namespace}"
```

An agency that fails listing is skipped with a warning; the run continues for the others.

## Search a published bundle

```python
import asyncio
from parsimony.catalog import Catalog

async def main():
    cat = await Catalog.from_url("file:///tmp/parsimony-smoke/sdmx_series_ecb_yc")
    for hit in await cat.search("10 year yield", 3):
        print(f"{hit.similarity:.3f}  {hit.code}  {hit.title[:80]}")

asyncio.run(main())
```

The same `Catalog.from_url(...)` works against `hf://`, `s3://`, and `file://` URLs — FAISS + BM25 are combined via RRF at query time.

## Plugin contract

The package implements the standard parsimony plugin contract, exported at the top level of ``parsimony_sdmx``:

| Export               | Role                                                                          |
|----------------------|-------------------------------------------------------------------------------|
| ``CATALOGS``         | Async generator — yields every catalog this plugin can publish.               |
| ``RESOLVE_CATALOG``  | ``namespace -> Callable \| None`` — cheap reverse lookup for ``--only``.      |
| ``CONNECTORS``       | Enumerators + live-fetch connector discovered via ``parsimony list``.         |
| ``ENV_VARS``         | Required environment variables (empty — SDMX endpoints are public).           |
| ``PROVIDER_METADATA``| Static provider facts (agency list, namespace pattern, plugin version).       |

## Architecture

```
parsimony_sdmx/
├── core/         pure domain logic: record dataclasses, title composition,
│                 codelist resolution, outcome types, domain exceptions
├── io/           boundary effects: atomic parquet writers, hardened lxml
│                 iterparse, HTTPS-only bounded session, path safety
├── providers/    per-agency adapters behind a narrow `CatalogProvider`
│                 protocol; ECB/ESTAT/IMF share a common sdmx1 flow helper,
│                 WB diverges with a path × decade sweep
├── connectors/   parsimony `@enumerator` surface + ``sdmx_fetch`` live
│                 observation connector
└── _isolation/   subprocess-spawning boundary for every sdmx1 call
```

### Why subprocess isolation

``sdmx1`` caches parsed structure messages (DSDs, codelists, dataflows) at module scope with no public invalidation hook. A long-lived Python process that imports it accumulates cache monotonically until OOM. Process death is the only working way to flush that cache.

Every sdmx1-touching call (``list_datasets`` for listings, ``fetch_series`` for per-dataset sweeps) runs inside a freshly spawned process that is discarded after the call — never pooled. A ``ProcessPoolExecutor`` would retain sdmx1 in each worker across tasks and defeat the invariant.

The two entry points in ``_isolation`` handle payload size differently:

- ``list_datasets`` returns up to ~8 k dataflow tuples through an ``mp.Queue`` that the parent drains *before* ``proc.join()`` — the feeder thread blocks on the OS pipe buffer once pickled bytes exceed ~64 KB, so join-before-read deadlocks. Regression-guarded by ``test_listing.py::test_large_payload_does_not_deadlock``.
- ``fetch_series`` writes the series parquet to a caller-supplied tmpdir inside the child and returns only a small ``DatasetOutcome`` envelope. The parent reads the parquet back and the tmpdir is discarded. Disk is the transport.

Under load (ESTAT with ~8 k dataflows, ECB YC with ~2 k series) the parent process stays sdmx1-free — verified by ``test_listing.py::test_plugin_surface_import_does_not_pull_sdmx``.

## Development

```bash
# Fast tier (306 tests, ~3 s) — excludes slow + integration markers
uv run --package parsimony-sdmx pytest packages/sdmx/tests -q

# Subprocess regression tier (2 tests, ~2 s) — real mp.Process children
uv run --package parsimony-sdmx pytest packages/sdmx/tests -m slow -v

# Lint + type check
uv run --package parsimony-sdmx ruff check packages/sdmx/
uv run --package parsimony-sdmx mypy packages/sdmx/parsimony_sdmx/
```

Hardening defaults: HTTPS-only bounded HTTP session, hardened `lxml.iterparse` (no entity resolution, no DTD load, no network), path traversal guards on every on-disk write.
