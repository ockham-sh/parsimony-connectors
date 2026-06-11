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
| `enumerate_sdmx_datasets` | enumerator | One row per dataflow per agency (`sdmx_datasets_<agency>` namespaces). |
| `enumerate_sdmx_series` | connector (dynamic schema) | One row per series key for a single `(agency, dataset_id)`. Drives one `sdmx_series_<agency>_<dataset_id>` catalog per dataset. |
| `sdmx_fetch` | connector | Live observation fetch for a series key against the agency endpoint. |
| `sdmx_datasets_search` | connector | Structured search over per-agency dataset catalogs. |
| `sdmx_series_search` | connector | Structured search over per-dataset series catalogs. |

Five registered connectors total (1 enumerator + 1 dynamic-schema connector + 1 fetch + 2 search).

### Dynamic schema: `enumerate_sdmx_series`

Per-dataset series enumeration returns a wide DataFrame whose columns depend
on the SDMX datastructure definition for that flow. The output schema is
therefore **dynamic per call** — it cannot be declared statically on
``@enumerator``. The connector stays a plain ``@connector`` that returns raw
``pd.DataFrame`` rows; catalog builders project entities with
``entities_from_connector`` after the framework applies the per-call schema.

## Install

```bash
pip install parsimony-sdmx
```

Pulls in `parsimony-core[catalog]>=0.7,<0.8` automatically (includes the hybrid BM25+vector catalog stack).

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

Catalog building is an operator workflow in `scripts/build_catalog.py`. Indexing policy lives in `parsimony_sdmx/catalog_policy.py`: hybrid BM25+vector per field when unique text count is below **1,000**, otherwise BM25-only.

Namespaces:

- ``sdmx_datasets_<agency>`` — one dataset catalog per agency (e.g. ``sdmx_datasets_ecb``).
- ``sdmx_series_<agency>_<dataset_id>`` — per-flow series catalogs for selected macro/finance flows.

### Build and push

```bash
# One agency: full dataset index + selected series catalogs
uv run python scripts/build_catalog.py --catalog agency --agency ECB \
  --save-root /tmp/parsimony-catalogs/sdmx --push-root hf://parsimony-dev/sdmx

# Full portfolio (all agencies)
uv run python scripts/build_catalog.py --catalog portfolio \
  --save-root /tmp/parsimony-catalogs/sdmx --push-root hf://parsimony-dev/sdmx \
  --parallel 2 --keep-going
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

## Expected search workflow (agents and maintainers)

SDMX catalogs are built for **structured field search first**, not open-ended semantic Q&A.

1. **`sdmx_datasets_search(agency='ECB', query=...)`** on ``sdmx_datasets_ecb`` — structured ``code: ECB|YC`` or title text.
2. Read the returned **`dimensions`** manifest (present only when a series catalog exists for that flow).
3. **`sdmx_series_search(flow_id='ECB/YC', ...)`** — structured dimension clauses.
4. **`sdmx_fetch`** with the series key from search results.

High-cardinality fields (especially `title` on large series catalogs) may be BM25-only when unique value count reaches **1,000** or more. Prefer structured `FIELD: value` clauses over long natural-language probes on those catalogs.

Override the catalog root for local dev: `PARSIMONY_SDMX_CATALOG_URL=file:///tmp/sdmx` (default publish target: `hf://parsimony-dev/sdmx`).

## Search a published bundle

```python
import asyncio
from parsimony.catalog import Catalog

async def main():
    datasets = await Catalog.load("hf://parsimony-dev/sdmx/sdmx_datasets_ecb")
    flows, _ = await datasets.search("code: ECB|YC", limit=3)
    print("datasets", flows[0].code, flows[0].title[:80])

    series = await Catalog.load("hf://parsimony-dev/sdmx/sdmx_series_ecb_yc")
    hits, _ = await series.search("REF_AREA: Spain && FREQ: Monthly", limit=3)
    for hit in hits:
        print(f"{hit.score:.3f}  {hit.code}  {hit.title[:80]}")

asyncio.run(main())
```

The same `Catalog.load(...)` works against `hf://` and `file://` URLs. Structured queries intersect candidates across fields; plain text without field syntax falls back to the title index only.

Validate a built or published snapshot:

```bash
uv run python scripts/validate_catalog.py --catalog-url file:///tmp/parsimony-catalogs/sdmx/sdmx_series_ecb_yc
uv run python scripts/validate_catalog.py \
  --catalog-url file:///tmp/parsimony-catalogs/sdmx/sdmx_datasets_ecb \
  --catalog-root file:///tmp/parsimony-catalogs/sdmx \
  --queries-file packages/sdmx/catalog_tests/queries.yaml
```

## Plugin contract

The package implements the standard parsimony plugin contract, exported at the top level of ``parsimony_sdmx``:

| Export               | Role                                                                          |
|----------------------|-------------------------------------------------------------------------------|
| ``CONNECTORS``       | ``Connectors`` collection — one enumerator, one dynamic-schema connector, ``sdmx_fetch``, and two search connectors.   |

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
