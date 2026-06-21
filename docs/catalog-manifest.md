# Catalog manifest

Operator reference for catalog bundles: which connectors ship hosted snapshots, how
runtime load order works, and how to pre-warm locally before publishing to Hugging Face.

## Load order (runtime)

1. In-memory `CatalogLRU` hit (per search connector process).
2. Configured catalog URL (`PARSIMONY_<PROVIDER>_CATALOG_URL` or package default `hf://…`).
3. Lazy disk cache under `parsimony.cache.connectors_dir(provider)`.
4. Provider `build_catalog` callable → save to lazy cache.

Point env vars at local trees for zero cold-start during testing:

```bash
export PARSIMONY_TREASURY_CATALOG_URL=file:///tmp/parsimony-catalogs-v1/treasury
export PARSIMONY_SDMX_CATALOG_URL=file:///tmp/parsimony-catalogs-v1/sdmx
export PARSIMONY_BOJ_CATALOG_URL=file:///tmp/parsimony-catalogs-v1/boj
```

## Catalog-backed connectors (11)

All flat macros, BoJ, and SDMX publish `schema_version: 1` snapshots. Rebuild and
republish before release — stale `parsimony-dev/*` artifacts from earlier dev builds are
not loadable.

| Provider | Default HF root | Local build |
|----------|-----------------|-------------|
| treasury | `hf://parsimony-dev/treasury` | `packages/treasury/scripts/build_catalog.py` |
| bdp | `hf://parsimony-dev/bdp` | `packages/bdp/scripts/build_catalog.py` |
| snb | `hf://parsimony-dev/snb` | `packages/snb/scripts/build_catalog.py` |
| boc | `hf://parsimony-dev/boc` | `packages/boc/scripts/build_catalog.py` |
| bde | `hf://parsimony-dev/bde` | `packages/bde/scripts/build_catalog.py` |
| rba | `hf://parsimony-dev/rba` | `packages/rba/scripts/build_catalog.py` |
| destatis | `hf://parsimony-dev/destatis` | `packages/destatis/scripts/build_catalog.py` |
| riksbank | `hf://parsimony-dev/riksbank` | `packages/riksbank/scripts/build_catalog.py` |
| bdf | `hf://parsimony-dev/bdf` | `packages/bdf/scripts/build_catalog.py` (needs `BDF_API_KEY`) |
| boj | `hf://parsimony-dev/boj` | `packages/boj/scripts/build_catalog.py` (multi-bundle) |
| sdmx | `hf://parsimony-dev/sdmx` | `packages/sdmx/scripts/build_catalog.py` |

### SDMX agency footprint

| Agency | Datasets catalog | Codelist catalogs |
|--------|------------------|-------------------|
| ECB | all non-derived flows (~103) | deduplicated from DSD structure (~hundreds) |
| ESTAT | all recall-fixed macro flows (~3,467) | deduplicated from DSD structure |
| IMF_DATA | all flows (193) | deduplicated from DSD structure |
| WB_WDI | single flow | deduplicated from DSD structure |

Series discovery uses prebuilt ``sdmx_series_*`` catalogs searched via
``sdmx_series_search`` (built by ``packages/sdmx/scripts/build_all_catalogs.py``).
Structure markers (``sdmx_structure_*``) are build-time only and must not be published.

### BoJ bundles

- `boj_databases` — database discovery
- `boj_series_<db>` — per-database series catalogs (lazy on first search if not prebuilt)

## Indexing policy

Flat macros: `parsimony.catalog.policy.discovery_indexes()` (code BM25; title/description
hybrid when unique values < 1,000). SDMX/BoJ use provider-specific policies with the same
threshold for high-cardinality fields.

## Summary script

```bash
uv run python scripts/catalog_manifest_summary.py --catalog-root /tmp/parsimony-catalogs-v1 --audit
```

See [catalog-operations.md](catalog-operations.md) for validation and HF publish steps.
