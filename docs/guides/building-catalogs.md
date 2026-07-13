# Building catalogs

This is the operator-facing entry point for catalog work in `parsimony-connectors`.
It explains **when** a provider gets a built catalog, **why**, and the full
lifecycle from enumeration to a hosted snapshot that connectors load lazily at
runtime. It links into two deeper references:

- [`../catalog-operations.md`](../catalog-operations.md) — the step-by-step maintainer
  runbook for building, validating, and publishing Hugging Face snapshots
  (probe modes, multi-bundle layouts, HF auth, the Dataset Viewer card).
- [`../catalog-manifest.md`](../catalog-manifest.md) — the per-provider snapshot manifest:
  which connectors ship hosted snapshots, the runtime load order, and the
  indexing policy.

For the search/discovery model itself (how `code`, `title`, and `description`
indexes are built and queried), see [`../concepts/discovery-and-catalogs.md`](../concepts/discovery-and-catalogs.md).

## When a provider gets a built catalog vs native search

A connector package falls into one of two discovery models. The choice is per
provider, not per release.

**Built catalog (13 providers).** The provider has no usable native search API,
or its universe is large enough that an agent needs a local, indexed snapshot to
find the right series before fetch. We enumerate the provider's addressable units
once, index them, and publish the result as a Hugging Face dataset. The
catalog-backed providers are: `bde`, `bdf`, `bdp`, `bls`, `boc`, `boj`,
`destatis`, `eia`, `rba`, `riksbank`, `sdmx`, `snb`, `treasury`.

> Both `eia` and `bls` are catalog-backed. Older notes that describe them as
> "enumerate only" are stale.

**Native search (9 providers).** The provider already exposes a first-class
search endpoint, so the agent queries the provider directly and there is nothing
to pre-build. These are: `alpha_vantage`, `coingecko`, `eodhd`, `finnhub`, `fmp`,
`fred`, `polymarket`, `sec_edgar`, `tiingo`. Do **not** build catalogs for these;
agents use the provider's own search tools.

Catalog-backed packages depend on `parsimony-core[catalog]`. Native-search
packages depend on plain `parsimony-core`.

## Where the build code lives

A built catalog has two layers, both maintainer tooling:

| Layer | File | Responsibility |
|---|---|---|
| Builder helper | `packages/<p>/parsimony_<p>/catalog_build.py` (`build_<p>_catalog`) | Enumerate, index, return a `Catalog` |
| Operator CLI | `packages/<p>/scripts/build_catalog.py` | Drive the builder, then `--save` / `--push` |

Build scripts are **not part of the plugin contract.** The user-facing surface of
a package is `CONNECTORS`. Nothing enumerates or downloads at import time; the
runtime only ever loads an already-published snapshot (or rebuilds one lazily
into the local cache). For the flat-macro reference, see
[`packages/treasury/parsimony_treasury/catalog_build.py`](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/treasury/parsimony_treasury/catalog_build.py)
and [`packages/treasury/scripts/build_catalog.py`](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/treasury/scripts/build_catalog.py).

## Lifecycle

```
enumerate  →  build  →  validate  →  publish to HF  →  lazy load at runtime
```

1. **Enumerate.** An `@enumerator` connector returns one row per addressable
   unit (KEY = code + namespace, TITLE, METADATA). It walks the provider's API
   or files; it runs only during a build, never at import.
2. **Build.** `build_<p>_catalog()` turns that result into a snapshot:

   ```python
   result  = enumerate_<p>()
   entries = result.to_entities()
   catalog = Catalog(namespace, indexes=discovery_indexes(entries), default_field="title")
   catalog.set_entities(entries)
   catalog.build()
   ```

   `discovery_indexes` indexes `code` as BM25 (exact lookup) and `title` +
   `description` adaptively — Hybrid BM25 + vector when a field has fewer than
   1,000 unique values, BM25-only otherwise.
3. **Validate.** Run the curated recall probes against the local snapshot
   (see below). Required probes must all pass before publishing.
4. **Publish to HF.** `catalog.save("hf://parsimony-dev/<p>")` writes the
   snapshot to the provider's hosted home. Needs `HF_TOKEN`.
5. **Lazy load.** At runtime the connector loads the snapshot from the
   configured URL, caching it locally. The load order is in
   [`../catalog-manifest.md`](../catalog-manifest.md).

A snapshot is `entries.parquet` + `indexes/<field>/` + `meta.json` (carrying
`content_sha256` and `schema_version`). Snapshot URLs use `file://` (or a bare
path) and `hf://<org>/<repo>[/<sub>]`. The hosted home for each provider is
`hf://parsimony-dev/<provider>`.

## Build → validate → publish commands

The connectors are synchronous plain `def`s, and so is this workflow. Run from
the package directory:

```bash
cd packages/<p>

# 1. Build locally
uv run python scripts/build_catalog.py --save file:///tmp/<p>

# 2. Validate the local snapshot (expect: overall OK, required_recall 1.00)
uv run python ../../tooling/validate_catalog.py --provider <p> \
    --catalog-url file:///tmp/<p>

# 3. Publish to the hosted home (needs HF_TOKEN)
uv run python scripts/build_catalog.py --push hf://parsimony-dev/<p>
```

Keyed builds take `--api-key`, which falls back to `<PROVIDER>_API_KEY`:

```bash
# bdf (falls back to BDF_API_KEY)
uv run python scripts/build_catalog.py --save file:///tmp/bdf --api-key "$BDF_API_KEY"

# eia (falls back to EIA_API_KEY)
uv run python scripts/build_catalog.py --save file:///tmp/eia --api-key "$EIA_API_KEY"
```

`build_catalog.py` exposes `--save` (local snapshot) and `--push` (catalog URL).
The multi-bundle providers (`boj`, `sdmx`) use `--save-root` / `--push-root` and
extra selectors; see [`../catalog-operations.md`](../catalog-operations.md) for those.

## Recall probes and `required_recall 1.00`

Every catalog-backed package carries a curated probe file at
`packages/<p>/catalog_tests/queries.yaml`. Probes assert that known series stay
findable through the snapshot's indexes. There are three kinds:

- **Required `code:` probes** — exact lookups on the BM25 `code` index. These are
  the contract: a catalogued unit must be retrievable by its code.
- **Lexical `title_bm25` probes** — a short slice of a distinctive title.
- **Optional semantic `hybrid_title` probes** — longer phrases, only meaningful
  when the title field built a hybrid index. Marked `optional: true` because
  ranking can vary.

The file ends with a threshold:

```yaml
thresholds:
  min_required_recall: 1.0
```

`required_recall 1.00` means **every `required` probe found its expected code.**
Anything less is a hard failure: a series that was supposed to be catalogued is
no longer retrievable, so the build is not publishable. Optional probes do not
affect the gate; they document intent and catch ranking regressions.

`validate_catalog.py` can draft a probe file from the snapshot's index shape
with `--write-queries`; you then curate it by hand. The probe modes and the
multi-bundle `catalog_root` form are documented in
[`../catalog-operations.md`](../catalog-operations.md).

## When to rebuild

Rebuild and re-push a snapshot when any of the following is true:

- **Kernel schema bump.** The catalog `SCHEMA_VERSION` is `1`. A snapshot whose
  `meta.json` `schema_version` does not match the kernel's is a **hard load gate**
  — it will not load until rebuilt and re-pushed. When the kernel bumps the
  schema, every hosted snapshot must be rebuilt.
- **Provider added or removed series.** The enumeration is a point-in-time
  snapshot. New series the provider published since the last build are invisible
  until you re-enumerate.
- **Indexing policy change.** If `discovery_indexes` or a provider-specific policy
  changes which fields are indexed or how, rebuild so probes test the live shape.

After any publish, **diff the emitted row count against the previous run.** A
quietly shrunk snapshot (an enumeration that lost a branch, a truncated fetch)
will still validate against probes that happen to land in the surviving rows. The
row-count diff is the cheap guard against silent loss.

## Cache and inspection

At runtime, snapshots are cached on disk so cold starts stay fast. The default
cache layout is:

| Platform | Default catalog cache |
|---|---|
| Linux | `~/.cache/parsimony/catalogs/<provider>/<namespace>/` |
| macOS | `~/Library/Caches/parsimony/catalogs/<provider>/<namespace>/` |
| Windows | `%LOCALAPPDATA%/parsimony/Cache/catalogs/<provider>/<namespace>/` |

Override the entire cache root with `PARSIMONY_CACHE_DIR` (useful on HF runners,
alternate disks, or CI):

```bash
PARSIMONY_CACHE_DIR=/data/parsimony \
    uv run python scripts/build_catalog.py --save file:///data/parsimony/<p>
```

Inspect occupancy and resolve cache paths with the kernel CLI:

```bash
uv run parsimony cache info
```

Point a connector at a local tree (zero cold-start during testing) with the
per-provider env override:

```bash
export PARSIMONY_TREASURY_CATALOG_URL=file:///tmp/treasury
```

The full runtime load order — in-memory cache, configured URL, lazy disk cache,
then a lazy rebuild from the package's `build_catalog` callable — is in
[`../catalog-manifest.md`](../catalog-manifest.md).

## Repo dev workflow

```bash
make sync                  # bootstrap the uv workspace
make verify PKG=<name>     # ruff + mypy + pytest + plugin listing for one package
make verify-all            # the same, across every package
make readme-roster         # refresh the connector roster table
```

`make verify` mirrors the CI pipeline; if it passes locally, CI passes too.
