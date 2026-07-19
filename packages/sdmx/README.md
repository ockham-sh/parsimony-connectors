# parsimony-sdmx

SDMX connector plugin for parsimony. Harvests dataflow listings, DSD structure (dimensions + codelists), and populated series keys from statistical agencies (ECB, Eurostat, IMF, World Bank), composes human-readable titles from codelists, and exposes searchable local catalog bundles for agent workflows.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-sdmx`.

Live observation fetches hit the agency endpoint inside a spawned subprocess. Maintainer catalog builds are explicit operator workflows under `scripts/`.

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
| `sdmx_datasets_search` | connector | Structured search over per-agency dataset catalogs (agency optional — fans out across all agencies). |
| `sdmx_series_search` | connector | Search populated per-flow series catalogs with dimension filters and title search. |
| `sdmx_dimension_search` | connector | Search or enumerate one flow dimension's values (`code`, `label`) from its catalog. |
| `sdmx_fetch` | connector | Live observation fetch for a series key against the agency endpoint. |

Four registered connectors total. Only published flows are searchable; an unpublished flow hard-errors (there is no live fallback).

## Install

```bash
pip install parsimony-sdmx
```

Pulls in a compatible `parsimony-core[catalog]` automatically (includes the hybrid BM25+vector catalog stack).

Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
from parsimony_sdmx import CONNECTORS

result = CONNECTORS["sdmx_fetch"](
    dataset_ref="ECB-YC",
    series_ref="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
)
print(result.raw.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog building

Catalog building is an operator workflow. Use `scripts/build_catalog.py` for individual dataset snapshots and `scripts/build_all_catalogs.py` for full SDMX release roots. Indexing policy lives in `parsimony_sdmx/catalog_policy.py`:

- **Dataset catalogs** — BM25 `code` (the composite `{agency}|{dataset_id}` key) plus a hybrid BM25+vector `title`. No description index: DSD-vocabulary text matches flows that break down *by* a subject, not flows *about* it.
- **Series catalogs** — per-flow parquet-backed catalogs with a title index plus code/label indexes for each DSD dimension. Dimension codes/labels are indexed here, per flow — there are no standalone codelist catalogs.

Namespaces:

- ``sdmx_datasets_<agency>`` — one dataset catalog per agency (e.g. ``sdmx_datasets_ecb``). Each entity carries a summarized **DSD** in metadata (dimension order, codelist refs, sample codes).
- ``sdmx_series_<agency>_<flow>`` — one populated-series catalog per supported flow (e.g. ``sdmx_series_estat_prc_hicp_manr``). Rows are stored in `series.parquet`; indexes resolve titles and dimension labels/codes.

### Build and push

```bash
# One agency: dataset index (+ per-flow series catalogs)
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
/tmp/parsimony-catalogs/sdmx/
├── sdmx_datasets_ecb/
│   ├── entries.parquet
│   ├── indexes/
│   └── meta.json
├── sdmx_series_ecb_yc/
│   ├── series.parquet
│   ├── indexes/
│   └── meta.json
└── ...
```

### Build an agency batch

```bash
uv run python scripts/build_catalog.py --catalog agency --agency ECB --push-root hf://parsimony-dev/sdmx
uv run python scripts/build_catalog.py --catalog agency --agency ESTAT --save-root /tmp/sdmx
```

Structure fetches are bounded (~2–15 s per flow) and fully parallelizable. Series catalog builds stream keys to parquet and then index distinct dimension values.

## Expected agent workflow (dataset → series → fetch)

Agents usually navigate in three steps:

1. **`sdmx_datasets_search(query=..., agency=...)`** — find the right dataflow. *Agency is optional*; omit it to search across all agency dataset catalogs. Read the returned **`dimensions`** list (the axes the flow breaks down by, in key order).
2. **`sdmx_series_search(agency=..., dataset_id=..., query=...)`** — search populated series keys. Use `{dimension}_label` for semantic resolution, `{dimension}_code` for exact filters, and `&&` to combine clauses. Need a dimension's valid codes? **`sdmx_dimension_search(agency=..., dataset_id=..., dimension=...)`** searches or enumerates them.
3. **`sdmx_fetch(dataset_ref=..., series_ref=...)`** — live observation fetch. On empty/too-broad results, loop back to step 2 with more filters.

Only published flows are searchable. A flow with no series catalog hard-errors ("not published; ask the maintainers to build it") — there is no live fallback.

### Cookbook: German monthly unemployment rate (Eurostat)

```python
from parsimony_sdmx import load

c = load()

# 1. Find the dataset — inspect the candidates, then take the top match
ds = c["sdmx_datasets_search"](query="unemployment rate monthly", agency="ESTAT", limit=5)
print(ds.raw[["dataset_id", "title", "score"]])
row = ds.raw.iloc[0]
dataset_id = row["dataset_id"]      # thread this into steps 2 and 3
dimensions = row["dimensions"]      # axes this flow breaks down by, in key order

# 2. Search populated combinations using DSD field names
series = c["sdmx_series_search"](
    agency="ESTAT",
    dataset_id=dataset_id,
    query="geo_label: Germany && freq_code: M",
    limit=10,
)
print(series.raw[["key", "title"]].head())

# 3. Fetch observations for the chosen series (paste the key straight in)
obs = c["sdmx_fetch"](dataset_ref=f"ESTAT-{dataset_id}", series_ref=series.raw.iloc[0]["key"])
print(obs.raw.head())
```

Override the catalog root for local dev: `PARSIMONY_SDMX_CATALOG_URL=file:///tmp/parsimony-catalogs/sdmx` (default publish target: `hf://parsimony-dev/sdmx`).

## Search a published bundle

```python
from parsimony.catalog import Catalog

datasets = Catalog.load("hf://parsimony-dev/sdmx/sdmx_datasets_ecb")
flows = datasets.search("code: ECB|YC", limit=3)
print("datasets", flows[0].code, flows[0].title[:80])

series = Catalog.load("hf://parsimony-dev/sdmx/sdmx_series_ecb_yc")
hits = series.search("10-year spot rate", limit=3)
for hit in hits:
    print(f"{hit.score:.3f}  {hit.code}  {hit.title[:80]}")
```

The same `Catalog.load(...)` works against `hf://` and `file://` URLs.

Validate a built or published snapshot:

```bash
uv run python scripts/validate_catalog_build.py --root /tmp/parsimony-catalogs
```

## Plugin contract

The package implements the standard parsimony plugin contract, exported at the top level of ``parsimony_sdmx``:

| Export               | Role                                                                          |
|----------------------|-------------------------------------------------------------------------------|
| ``CONNECTORS``       | ``Connectors`` collection — three catalog-search connectors and ``sdmx_fetch``.   |

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
├── connectors/   parsimony connector surface: dataset/series/dimension
│                 catalog search + ``sdmx_fetch`` live observation connector
└── _isolation/   subprocess-spawning boundary for every sdmx1 call
```

### Title composition

Each series row's `title` is built per DSD:

- **ECB** — uses the `TITLE` / `TITLE_COMPL` natural-language attributes fetched via the portal side-channel.
- **ESTAT / IMF_DATA / WB_WDI** — falls back to `compose_series_title()` which concatenates dimension labels in DSD order.

### Why subprocess isolation

``sdmx1`` caches parsed structure messages at module scope with no public invalidation hook. Every sdmx1-touching call runs inside a freshly spawned process that is discarded after the call.

Under load the parent process stays sdmx1-free — verified by ``test_listing.py::test_plugin_surface_import_does_not_pull_sdmx``.

## Development

```bash
# Fast tier — excludes slow + integration markers
make verify PKG=sdmx

# Integration (live agency endpoints)
uv run --package parsimony-sdmx pytest packages/sdmx/tests -m integration -v
```

Hardening defaults: HTTPS-only bounded HTTP session, hardened `lxml.iterparse`, path traversal guards on every on-disk write.

## Provider

- SDMX standard: <https://sdmx.org>
- ECB SDMX: <https://data.ecb.europa.eu/help/api/overview>
- Eurostat SDMX: <https://wikis.ec.europa.eu/display/EUROSTATHELP/API+SDMX+2.1>
- IMF SDMX: <https://datahelp.imf.org/knowledgebase/articles/667681-using-sdmx-to-query-imf-data>
- World Bank SDMX: <https://datahelpdesk.worldbank.org/knowledgebase/articles/889398-developer-information-overview>

## License

See [LICENSE](./LICENSE).
