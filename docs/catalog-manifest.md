# Catalog manifest (Wave 1 pre-warm + lazy build)

Operator reference for which catalog bundles are **pre-warmed** under
`/tmp/parsimony-catalogs/` versus **lazy-built** on first search. Pre-warming is
optional; search connectors can build missing snapshots into
`~/.cache/parsimony/connectors/<provider>/catalogs/<namespace>` when the
configured URL (HF or `file://`) is absent.

## Load order (runtime)

1. In-memory `CatalogLRU` hit (per search connector process).
2. Configured catalog URL (`PARSIMONY_<PROVIDER>_CATALOG_URL` or package default).
3. Lazy disk cache under `parsimony.cache.connectors_dir(provider)`.
4. Provider `build_catalog` callable → save to lazy cache.

Point env vars at pre-warmed trees for zero cold-start during agent testing:

```bash
export PARSIMONY_TREASURY_CATALOG_URL=file:///tmp/parsimony-catalogs/treasury
export PARSIMONY_SDMX_CATALOG_URL=file:///tmp/parsimony-catalogs/sdmx
export PARSIMONY_BOJ_CATALOG_URL=file:///tmp/parsimony-catalogs/boj
```

## Wave 1 — pre-warm locally (`/tmp/parsimony-catalogs/`)

| Provider | Bundle / path | Build command |
|----------|---------------|---------------|
| treasury | `treasury/` | `cd packages/treasury && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/treasury` |
| bdp | `bdp/` | `cd packages/bdp && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/bdp` |
| snb | `snb/` | `cd packages/snb && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/snb` |
| boc | `boc/` | `cd packages/boc && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/boc` |
| bde | `bde/` | `cd packages/bde && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/bde` |
| rba | `rba/` | `cd packages/rba && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/rba` |
| destatis | `destatis/` | `cd packages/destatis && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/destatis` |
| riksbank | `riksbank/` | `cd packages/riksbank && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/riksbank` |
| bdf | `bdf/` (requires `BDF_API_KEY`) | `cd packages/bdf && uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/bdf` |
| boj | `boj/boj_databases/` | `cd packages/boj && uv run python scripts/build_catalog.py --catalog databases --save-root /tmp/parsimony-catalogs/boj` |
| sdmx | `sdmx/sdmx_datasets_ecb/` | `cd packages/sdmx && uv run python scripts/build_catalog.py --catalog agency --agency ECB --save-root /tmp/parsimony-catalogs/sdmx` |
| sdmx | `sdmx/sdmx_series_ecb_<flow>/` (13 flows) | See ECB curated flows below |

### ECB curated series flows (Wave 1)

Pre-warm one series bundle per flow (namespace `sdmx_series_ecb_<flow_lower>`):

`EXR`, `ICP`, `BSI`, `FM`, `IRS`, `YC`, `MIR`, `BLS`, `BOP`, `GFS`, `STS`, `RPP`, `CISS`

```bash
cd packages/sdmx
for flow in EXR ICP BSI FM IRS YC MIR BLS BOP GFS STS RPP CISS; do
  uv run python scripts/build_catalog.py --catalog series \
    --agency ECB --dataset-id "$flow" \
    --save-root /tmp/parsimony-catalogs/sdmx
done
```

## Lazy by default (no Wave 1 pre-warm)

| Provider | Behavior |
|----------|----------|
| boj series | Per-DB `boj_series_<db>` builds on first `boj_series_search` for that DB |
| sdmx ESTAT | Dataset + series catalogs build on first search (large; avoid bulk pre-warm) |
| sdmx IMF_DATA / WB_WDI | Wave 2 optional pre-warm; lazy until then |
| Any flat macro | Missing HF snapshot → lazy build via `build_<provider>_catalog()` |

## Indexing policy

Flat macros: `parsimony.catalog.policy.discovery_indexes()` (code BM25; title/description
hybrid when unique values &lt; 1,000). SDMX/BoJ keep provider-specific policies with the
same threshold for dimension/structured fields.

## Summary script

```bash
uv run python scripts/catalog_manifest_summary.py
uv run python scripts/catalog_manifest_summary.py --catalog-root /tmp/parsimony-catalogs
```

See [catalog-operations.md](catalog-operations.md) for validation and HF push steps.
