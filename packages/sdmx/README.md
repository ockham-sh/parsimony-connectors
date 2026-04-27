# parsimony-sdmx

SDMX connector plugin for parsimony. Harvests dataflow listings and per-dataset series keys from statistical agencies (ECB, Eurostat, IMF, World Bank), composes human-readable titles from the DSD + codelists, and publishes one parquet + FAISS bundle per catalog via `parsimony publish`.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-sdmx`.

No separate builder CLI, no intermediate on-disk cache — every call hits the live agency endpoint inside a spawned subprocess.

## Supported agencies

| Agency ID  | Source                                           |
|------------|--------------------------------------------------|
| `ECB`      | European Central Bank SDMX 2.1                   |
| `ESTAT`    | Eurostat SDMX 2.1                                |
| `IMF_DATA` | IMF SDMX 3 (``sdmx.imf.org``)                    |
| `WB_WDI`   | World Bank SDMX 2.1 (custom path × decade sweep) |

## Connectors

| Name | Kind | Description |
|---|---|---|
| `enumerate_sdmx_datasets` | enumerator | One row per dataflow across every supported agency. Drives the `sdmx_datasets` catalog. |
| `enumerate_sdmx_series` | enumerator | One row per series key for a single `(agency, dataset_id)`. Drives one `sdmx_series_<agency>_<dataset_id>` catalog per dataset. |
| `sdmx_fetch` | connector | Live observation fetch for a series key against the agency endpoint. |

## Install

```bash
pip install parsimony-sdmx
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. For local publishing you also want the `standard` extra on parsimony-core, which adds FAISS, BM25, and sentence-transformers (the default embedder stack):

```bash
pip install "parsimony-core[standard]"
```

Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
import asyncio
from parsimony_sdmx import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["sdmx_fetch"](
        agency="ECB",
        dataset_id="YC",
        key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
    )
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Catalog publishing

This plugin's namespaces are dynamic — one per `(agency, dataset_id)` pair discovered at publish time, plus one static cross-agency catalog:

- ``sdmx_datasets`` — one cross-agency catalog of every dataflow.
- ``sdmx_series_<agency>_<dataset_id>`` — one per-dataset catalog of series keys, e.g. ``sdmx_series_ecb_yc``.

The plugin exports ``CATALOGS`` as an **async generator function**: yielding the static `sdmx_datasets` namespace first, then one `sdmx_series_<agency>_<dataset_id>` namespace per dataflow returned by live agency listing. `RESOLVE_CATALOG(namespace)` provides the cheap reverse lookup used by `--only`, parsing namespace strings back into `(agency, dataset_id)` without enumerating the full listing.

### Publish a single catalog

Publish by name with ``--only`` (pure string lookup, no listing walk — `RESOLVE_CATALOG` fast-path):

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

### Publish everything

Omit ``--only`` to drive the async generator through every agency listing, publishing one bundle per discovered ``(agency, dataset_id)`` pair plus the static ``sdmx_datasets`` catalog. Expect a long run — ESTAT alone has 8 k+ dataflows:

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
| ``CONNECTORS``       | ``Connectors`` collection — two enumerators + the ``sdmx_fetch`` connector.   |
| ``CATALOGS``         | Async generator function — yields every catalog this plugin can publish.      |
| ``RESOLVE_CATALOG``  | ``namespace -> Callable \| None`` — cheap reverse lookup for ``--only``.      |

SDMX endpoints are public; no environment variables are required.

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

### Title composition

Each series row's `title` is built per DSD:

- **ECB** — uses the `TITLE` / `TITLE_COMPL` natural-language attributes fetched via the portal side-channel. Titles like `"All euro area yield curve - 10-year spot rate"`. Short, semantic, directly embedder-friendly.
- **ESTAT / IMF_DATA / WB_WDI** — no natural-language attributes exposed; falls back to `compose_series_title()` which concatenates `"DIM: label - DIM: label - …"` across every dimension in DSD order. Longer (80-150 tokens) but still searchable.

The codelist-composed form used to be prefixed onto ECB titles as well (`"base | TITLE - TITLE_COMPL"`), but it duplicated content the natural language already expresses and inflated embedding cost quadratically (BERT attention is O(N²)). It now serves only as a fallback when TITLE_COMPL is absent. The raw SDMX series key is always available in the `code` column, so keyword-exact queries are unaffected.

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

## Provider

- SDMX standard: <https://sdmx.org>
- ECB SDMX: <https://data.ecb.europa.eu/help/api/overview>
- Eurostat SDMX: <https://wikis.ec.europa.eu/display/EUROSTATHELP/API+SDMX+2.1>
- IMF SDMX: <https://datahelp.imf.org/knowledgebase/articles/667681-using-sdmx-to-query-imf-data>
- World Bank SDMX: <https://datahelpdesk.worldbank.org/knowledgebase/articles/889398-developer-information-overview>

## License

See [LICENSE](./LICENSE).
