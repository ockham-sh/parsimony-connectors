# parsimony-sdmx

SDMX connector plugin for parsimony. Harvests dataflow listings and per-dataset series keys from statistical agencies (ECB, Eurostat, IMF, World Bank), composes human-readable titles from the DSD + codelists, and exposes lazy `Catalog` declarations that maintainers can build and push directly.

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

Pulls in `parsimony-core>=0.7,<0.8` automatically. Local catalog publishing uses the core catalog stack (hybrid BM25+vector or BM25-only per field):

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
    connectors = CONNECTORS
    result = await connectors["sdmx_fetch"](
        dataset_key="ECB-YC",
        series_key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
    )
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog building

Catalog building is an operator workflow in `scripts/build_catalog.py`. The user-facing plugin exports connectors for search/fetch; the script owns live enumeration, batching, indexing policy, and optional push. Indexing policy lives in `parsimony_sdmx/catalog_policy.py`: each searchable field gets a hybrid index when its unique text count is below 100k, otherwise BM25-only (`code` on `sdmx_datasets` stays BM25).

Namespaces are dynamic — one per `(agency, dataset_id)` pair discovered at build time, plus one static cross-agency catalog:

- ``sdmx_datasets`` — one cross-agency catalog of every dataflow.
- ``sdmx_series_<agency>_<dataset_id>`` — one per-dataset catalog of series keys, e.g. ``sdmx_series_ecb_yc``.

### Build and push a single catalog

```bash
uv run python scripts/build_catalog.py --catalog datasets --push hf://parsimony-dev/sdmx/sdmx_datasets
uv run python scripts/build_catalog.py --catalog series --agency ECB --dataset-id YC --push hf://parsimony-dev/sdmx/sdmx_series_ecb_yc
```

Use `--save-root /tmp/sdmx` to write local snapshots under namespace subdirectories. Use `--push <url>` for one explicit catalog URL or `--push-root <root>` for namespace subdirectories.

A local build produces:

```
/tmp/parsimony-smoke/sdmx_series_ecb_yc/
├── entries.parquet
├── indexes/
└── meta.json
```

### Build an agency batch

```bash
uv run python scripts/build_catalog.py --catalog agency --agency ECB --push-root hf://parsimony-dev/sdmx
uv run python scripts/build_catalog.py --catalog agency --agency ESTAT --max-catalogs 30 --save-root /tmp/sdmx
```

An agency that fails listing raises before building; individual `$DV_*` derived views are skipped because they are not fetchable series catalogs.

## Search a published bundle

```python
import asyncio
from parsimony.catalog import Catalog

async def main():
    cat = await Catalog.load("file:///tmp/parsimony-smoke/sdmx_series_ecb_yc")

    # 1. Structured search (preferred: explicit dimension filters)
    print("--- Structured Search ---")
    for hit in await cat.search("REF_AREA: Spain && FREQ: Monthly", 3):
        print(f"{hit.score:.3f}  {hit.code}  {hit.title[:80]}")

asyncio.run(main())
```

The same `Catalog.load(...)` works against `hf://` and `file://` URLs. Structured queries intersect candidates across fields; plain text without field syntax falls back to the title index.

## Plugin contract

The package implements the standard parsimony plugin contract, exported at the top level of ``parsimony_sdmx``:

| Export               | Role                                                                          |
|----------------------|-------------------------------------------------------------------------------|
| ``CONNECTORS``       | ``Connectors`` collection — two enumerators + the ``sdmx_fetch`` connector.   |

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

The codelist-composed form is used only as a fallback when `TITLE_COMPL` is absent — duplicating it onto natural-language titles inflates embedding cost quadratically (BERT attention is O(N²)) without adding signal. The raw SDMX series key is always available in the `code` column, so keyword-exact queries are unaffected.

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
