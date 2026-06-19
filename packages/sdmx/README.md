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
| `enumerate_sdmx_datasets` | enumerator | One row per dataflow per agency (`sdmx_datasets_<agency>` namespaces). |
| `enumerate_sdmx_series` | connector (dynamic schema) | Scoped keys-only discovery for one `(agency, dataset_id, key_pattern)` — returns matching series with labeled dimensions, no observations. |
| `sdmx_fetch` | connector | Live observation fetch for a series key against the agency endpoint. |
| `sdmx_datasets_search` | connector | Structured search over per-agency dataset catalogs (agency optional — fans out across all agencies). |
| `sdmx_codelist_search` | connector | Semantic search over deduplicated per-agency codelist catalogs. |
| `sdmx_series_search` | connector | Search populated per-flow series catalogs with dimension filters, title search, and refine facets. |

Six registered connectors total (1 enumerator + 1 dynamic-schema connector + 1 fetch + 3 search).

### Dynamic schema: `enumerate_sdmx_series`

Scoped discovery returns a wide DataFrame whose columns depend on the SDMX datastructure definition for that flow. The output schema is **dynamic per call** — it cannot be declared statically on ``@enumerator``. The connector stays a plain ``@connector`` that returns raw ``pd.DataFrame`` rows.

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
from parsimony_sdmx import CONNECTORS

result = CONNECTORS["sdmx_fetch"](
    dataset_key="ECB-YC",
    series_key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
)
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog building

Catalog building is an operator workflow. Use `scripts/build_catalog.py` for individual dataset/codelist snapshots and `scripts/build_all_catalogs.py` for full SDMX release roots. Indexing policy lives in `parsimony_sdmx/catalog_policy.py`:

- **Dataset catalogs** — hybrid BM25+vector on `title`/`description` when unique text count is below **1,000**, otherwise BM25-only.
- **Codelist catalogs** — BM25 on `code`, hybrid on `label` (always hybrid for semantic concept→code resolution).
- **Series catalogs** — per-flow parquet-backed catalogs with a title index plus code/label indexes for each DSD dimension.

Namespaces:

- ``sdmx_datasets_<agency>`` — one dataset catalog per agency (e.g. ``sdmx_datasets_ecb``). Each entity carries a summarized **DSD** in metadata (dimension order, codelist refs, sample codes).
- ``sdmx_codelist_<agency>_<codelist_id>`` — deduplicated codelist catalogs (e.g. ``sdmx_codelist_ecb_cl_freq``). Entities are `{code, label}` pairs.
- ``sdmx_series_<agency>_<flow>`` — one populated-series catalog per supported flow (e.g. ``sdmx_series_estat_prc_hicp_manr``). Rows are stored in `series.parquet`; indexes resolve titles and dimension labels/codes.

### Build and push

```bash
# One agency: dataset index + deduplicated codelist catalogs
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
├── sdmx_codelist_ecb_cl_freq/
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

1. **`sdmx_datasets_search(query=..., agency=...)`** — find the right dataflow. *Agency is optional*; omit it to search across all agency dataset catalogs. Read the returned **`dsd`** summary (dimension order + codelist refs).
2. **`sdmx_series_search(agency=..., dataset_id=..., query=...)`** — search populated series keys. Use `{dimension}_label` for semantic resolution, `{dimension}_code` for exact filters, and `&&` to combine clauses. The `refine` column returns facet JSON for unpinned dimensions.
3. **`sdmx_fetch(dataset_key=..., series_key=...)`** — live observation fetch. On empty/too-broad results, loop back to step 2 with more filters.

`sdmx_codelist_search` and `enumerate_sdmx_series` remain available for narrower workflows or live fallback, but `sdmx_series_search` is the preferred path when the release catalog is installed.

### Cookbook: German monthly unemployment rate (Eurostat)

```python
from parsimony_sdmx import load

c = load()

# 1. Find the dataset
ds = c["sdmx_datasets_search"](query="unemployment rate monthly", agency="ESTAT", limit=3)
row = ds.data.iloc[0]
print(row["code"], row["title"])
dsd = row["dsd"]  # dimension order + codelist refs

# 2. Search populated combinations using DSD field names
series = c["sdmx_series_search"](
    agency="ESTAT",
    dataset_id="UNE_RT_M",
    query="geo_label: Germany && freq_code:M",
    limit=10,
)
print(series.data[["key", "title", "refine"]].head())

# 3. Fetch observations for the chosen series
obs = c["sdmx_fetch"](dataset_ref="ESTAT-UNE_RT_M", series_ref=series.data.iloc[0]["key"])
print(obs.data.head())
```

Override the catalog root for local dev: `PARSIMONY_SDMX_CATALOG_URL=file:///tmp/parsimony-catalogs/sdmx` (default publish target: `hf://parsimony-dev/sdmx`).

## Search a published bundle

```python
from parsimony.catalog import Catalog

datasets = Catalog.load("hf://parsimony-dev/sdmx/sdmx_datasets_ecb")
flows = datasets.search("code: ECB|YC", limit=3)
print("datasets", flows[0].code, flows[0].title[:80])

codelists = Catalog.load("hf://parsimony-dev/sdmx/sdmx_codelist_ecb_cl_freq")
hits = codelists.search("monthly", limit=3)
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
| ``CONNECTORS``       | ``Connectors`` collection — one enumerator, one dynamic-schema connector, ``sdmx_fetch``, and three search connectors.   |

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
│                 observation connector + dataset/codelist/series search connectors
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
