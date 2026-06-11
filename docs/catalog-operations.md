# Catalog operations (maintainer runbook)

Catalog build/push scripts are **operator tooling**. They are not part of the
connector plugin contract and are **not** documented in `parsimony-core`
(`parsimony/docs/contract.md`). Each published connector only needs:

- a default catalog URL (often `hf://parsimony-dev/<provider>`), and
- an optional env override (`PARSIMONY_<PROVIDER>_CATALOG_URL`).

How **official** connectors in this monorepo shape catalogs (hierarchy, indexing,
search tool shapes) is an internal authoring standard — not part of the published
`parsimony-core` contract. Third-party plugin authors may ignore it.

### Catalog authoring rules (official connectors)

1. **Follow the upstream API hierarchy** when choosing catalog topology (e.g.
   BoJ database + series split; flat catalog when fetch needs a single key).
2. **Search must expose fetch dispatch fields** when fetch needs more than the
   returned key alone (`make_local_search_connector` is fine when the key suffices).
3. **Indexing policy**: hybrid BM25+vector when a field has fewer than **1,000**
   unique non-empty values; BM25-only at or above that limit. Flat macro builders
   use `parsimony.catalog.policy.discovery_indexes()` (re-exported from
   `parsimony.catalog.policy`); SDMX and BoJ
   use provider-specific policies with the same threshold.
4. **Lazy load-or-build**: search connectors try the configured catalog URL, then
   `~/.cache/parsimony/connectors/<provider>/catalogs/<namespace>`, then the
   package `build_<provider>_catalog()` helper. Pre-warm under
   `/tmp/parsimony-catalogs/` is optional — see [catalog-manifest.md](catalog-manifest.md).
5. **Macro-only scope**: build catalogs for public macro/statistical providers in
   `tooling/catalog_validate/registry.py`. Do **not** build catalogs for commercial
   connectors with built-in upstream search (FMP, Alpha Vantage, Tiingo, etc.) —
   agents use those providers' native search tools instead.
6. **Credentials**: never hardcode API keys in build scripts. Pass `--api-key` and/or
   read from env (e.g. `BDF_API_KEY`, `RIKSBANK_API_KEY`); required keys must fail
   fast with a clear message when missing.
7. **Multi-bundle HF repos** when per-part snapshots are large (SDMX, BoJ).
8. **Validation**: curated probes under `packages/<provider>/catalog_tests/queries.yaml`.

Reference patterns: `packages/sdmx/` (multi-bundle + structured search),
`packages/boj/` (database/series split), `packages/treasury/` / `packages/bde/`
(flat catalogs).

The scripts under `packages/*/scripts/build_catalog.py` are one implementation
that produces those remote artifacts. The plugin runtime never imports them.

## Philosophy

| Layer | Responsibility |
|-------|----------------|
| **Plugin** (`parsimony_<provider>`) | Search/fetch connectors; default catalog URL; env override |
| **Build script** (`packages/*/scripts/build_catalog.py`) | Enumerate source data, choose indexes, write/push snapshots |
| **Validation** (`scripts/validate_catalog.py`) | Inspect snapshots; generate/run search probes |

HF endpoints are always parameterized (`--push`, `--push-root`, `--catalog-url`).
Defaults may point at `parsimony-dev` repos; use staging repos while iterating.

## v1 bulk publish (gated)

After local schema-v1 snapshots exist under `/tmp/parsimony-catalogs-v1/`,
maintainers with `HF_TOKEN` (or `hf auth login`) run:

```bash
cd parsimony-connectors
export HF_TOKEN=...   # never commit; load from ockham/.env locally
./scripts/publish_v1_catalogs.sh
PARSIMONY_RUN_REMOTE_CATALOGS=1 uv run pytest tests/test_remote_catalogs.py -m remote_catalog
```

`bdf` is skipped unless `BDF_API_KEY` was used for a local build. SDMX and BoJ
push every namespace subdirectory under their multi-bundle roots.

## One catalog at a time

For each catalog:

1. **Build** locally (`--save` or `--save-root`)
2. **Inspect** indexes + sample entries
3. **Generate** draft probes (`--write-queries`)
4. **Curate** `packages/<provider>/catalog_tests/queries.yaml`
5. **Validate** locally against the snapshot
6. **Push** to staging HF (optional)
7. **Validate** staging URL
8. **Push** to canonical HF
9. **Validate** canonical URL (or run remote pytest suite)

Do not advance to the next catalog until the current one is green.

## Validation tooling

```bash
cd parsimony-connectors

# Schema + index inspection (no curated probes)
uv run python scripts/validate_catalog.py --catalog-url file:///tmp/parsimony-catalogs/riksbank

# Generate draft probes from index shape + sampled entries
uv run python scripts/validate_catalog.py --provider riksbank \
  --catalog-url file:///tmp/parsimony-catalogs/riksbank \
  --write-queries packages/riksbank/catalog_tests/queries.generated.yaml

# Run curated probes
uv run python scripts/validate_catalog.py --provider riksbank \
  --catalog-url file:///tmp/parsimony-catalogs/riksbank \
  --queries-file packages/riksbank/catalog_tests/queries.yaml

# Remote canonical (may fail until rebuilt for current schema v1 snapshots)
uv run python scripts/validate_catalog.py --provider riksbank --allow-missing-remote
```

### Probe modes (match indexes, not wishful semantics)

| Mode | When to use | Example |
|------|-------------|---------|
| `code` | `code` field has BM25 index | `code: SEKEURPMI` |
| `title_bm25` | title indexed (BM25 or hybrid) | short lexical slice of title |
| `hybrid_title` | title uses **hybrid** index | longer phrase; often `optional: true` |
| `structured_field` | metadata/dimension field indexed | `agency: ECB`, `REF_AREA: France` |

Do **not** add semantic-style probes when the build policy chose BM25-only
(high cardinality). SDMX series catalogs often use structured dimension probes only.

### Curated probe file format

`packages/<provider>/catalog_tests/queries.yaml`:

```yaml
catalog_url: hf://parsimony-dev/riksbank
queries:
  - id: code_example
    query: "code: EXAMPLE"
    expected_code: EXAMPLE
    mode: code
    required: true
    why: "code field is indexed"
thresholds:
  min_required_recall: 1.0
```

SDMX multi-bundle files use `catalog_root` + per-query `namespace` instead of
a single `catalog_url`.

## Remote pytest suite

Opt-in (excluded from default `pytest`):

```bash
PARSIMONY_RUN_REMOTE_CATALOGS=1 \
  uv run pytest tests/test_remote_catalogs.py -m remote_catalog

# Staging override
PARSIMONY_CATALOG_URL=hf://parsimony-dev-staging/riksbank \
  PARSIMONY_RUN_REMOTE_CATALOGS=1 \
  uv run pytest tests/test_remote_catalogs.py -m remote_catalog

# Skip when canonical not rebuilt yet
PARSIMONY_ALLOW_MISSING_REMOTE_CATALOG=1 \
  PARSIMONY_RUN_REMOTE_CATALOGS=1 \
  uv run pytest tests/test_remote_catalogs.py -m remote_catalog
```

## Provider rollout order

Recommended sequence (smallest / simplest first):

1. `riksbank` — pilot for validation harness
2. `treasury`, `snb`, `rba`
3. `boc`, `bde`, `bdf`, `bdp`, `destatis`, `boj`
4. `sdmx` — see below

### Per-provider build commands

From `packages/<provider>/`:

```bash
# Example: riksbank (optional --api-key or RIKSBANK_API_KEY for higher rate limits)
uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/riksbank

# Example: bdf (requires BDF_API_KEY or --api-key)
uv run python scripts/build_catalog.py --save /tmp/parsimony-catalogs/bdf --api-key "$BDF_API_KEY"
uv run python scripts/validate_catalog.py --provider riksbank \
  --catalog-url file:///tmp/parsimony-catalogs/riksbank --write-queries \
  catalog_tests/queries.generated.yaml
# Curate catalog_tests/queries.yaml, then:
uv run python scripts/validate_catalog.py --provider riksbank \
  --catalog-url file:///tmp/parsimony-catalogs/riksbank
# Push with modern `hf` CLI (preferred; replaces deprecated huggingface-cli)
chmod +x ../../scripts/push_catalog.sh
../../scripts/push_catalog.sh hf://parsimony-dev-staging/riksbank /tmp/parsimony-catalogs/riksbank  # optional
../../scripts/push_catalog.sh hf://parsimony-dev/riksbank /tmp/parsimony-catalogs/riksbank
# Or directly:
# uv tool run hf repos create parsimony-dev/riksbank --repo-type dataset --exist-ok
# uv tool run hf upload parsimony-dev/riksbank /tmp/parsimony-catalogs/riksbank --repo-type dataset
```

Repeat with `--provider <name>` and paths from `tooling/catalog_validate/registry.py`.

**Layout note:** publish snapshots so `meta.json` lives at the catalog URL root
(`hf://parsimony-dev/riksbank`), not nested (`riksbank/meta.json`). Some legacy
HF repos used nested layouts; rebuilds should normalize to root-level snapshots
expected by `Catalog.load("hf://parsimony-dev/<name>")`.

## BoJ (multi-bundle)

Root: `hf://parsimony-dev/boj` (override: `PARSIMONY_BOJ_CATALOG_URL`).

Expected agent path: **structured search first** — `boj_databases_search` →
`boj_series_search(db=...)` → `boj_fetch`. See
`packages/boj/catalog_tests/queries.yaml` and the catalog authoring rules in this document.

Per-DB series bundles keep embedding memory bounded (avoids OOM on flat builds).

```bash
cd packages/boj

# All bundles (long-running; enumerates 50 DBs from live API)
uv run python scripts/build_catalog.py --catalog all \
  --save-root /tmp/parsimony-catalogs/boj

# Pilot one DB
uv run python scripts/build_catalog.py --catalog series --db FM08 \
  --save-root /tmp/parsimony-catalogs/boj

# Validate databases bundle + curated probes
uv run python ../../scripts/validate_catalog.py --provider boj \
  --catalog-url file:///tmp/parsimony-catalogs/boj/boj_databases \
  --catalog-root file:///tmp/parsimony-catalogs/boj \
  --queries-file catalog_tests/queries.yaml
```

## SDMX (multi-bundle, per-agency)

Root: `hf://parsimony-dev/sdmx` (override: `PARSIMONY_SDMX_CATALOG_URL`).

Layout: ``sdmx_datasets_<agency>`` + ``sdmx_series_<agency>_<flow>`` for selected macro flows.
``sdmx_datasets_search`` requires ``agency`` (no cross-agency catalog).

```bash
cd packages/sdmx

# Full portfolio (all agencies, selected series per agency heuristics)
uv run python scripts/build_catalog.py --catalog portfolio \
  --save-root /tmp/parsimony-catalogs/sdmx \
  --push-root hf://parsimony-dev/sdmx \
  --parallel 2 --keep-going --resume

# Single agency
uv run python scripts/build_catalog.py --catalog agency --agency ECB \
  --save-root /tmp/parsimony-catalogs/sdmx \
  --push-root hf://parsimony-dev/sdmx --parallel 2 --keep-going
```

## Current remote state (May 2026)

Canonical `parsimony-dev/*` datasets may still carry pre-v1 or legacy index
layouts. Rebuild + push with the current `parsimony-core` catalog schema (v1,
value-deduplicated field indexes) is required before remote catalog tests pass
without `PARSIMONY_ALLOW_MISSING_REMOTE_CATALOG=1`.

## HF Dataset Viewer (catalog browsing)

Catalog repos on Hugging Face can expose `entries.parquet` in the web Dataset Viewer
without re-uploading FAISS/BM25 index artifacts. Configure this via YAML frontmatter
in the repo root `README.md` (`configs` + `data_files`; use split name `train`).

**Flat catalog** (one bundle at repo root, e.g. riksbank):

```yaml
configs:
  - config_name: default
    data_files:
      - split: train
        path: entries.parquet
```

**Multi-bundle repo** (boj, sdmx): one HF config/subset per bundle subdirectory:

```yaml
configs:
  - config_name: sdmx_datasets_ecb
    data_files:
      - split: train
        path: sdmx_datasets_ecb/entries.parquet
  - config_name: sdmx_series_ecb_yc
    data_files:
      - split: train
        path: sdmx_series_ecb_yc/entries.parquet
```

Publish or refresh the dataset card (README only — no snapshot rebuild):

```bash
# Flat pilot (verified on parsimony-dev/riksbank)
uv run python scripts/publish_catalog_dataset_card.py --repo-id parsimony-dev/riksbank

# All providers with local trees under /tmp/parsimony-catalogs
uv run python scripts/publish_catalog_dataset_card.py --all

# Multi-bundle
uv run python scripts/publish_catalog_dataset_card.py --provider sdmx --catalog-root /tmp/parsimony-catalogs
```

`scripts/push_catalog.sh` refreshes the dataset card after repo-root uploads. For multi-bundle
subpath uploads, set `PARSIMONY_UPDATE_DATASET_CARD=1` and `PARSIMONY_CATALOG_ROOT` to the save root.

Verify viewer indexing:

```bash
curl "https://datasets-server.huggingface.co/splits?dataset=parsimony-dev/riksbank"
curl "https://datasets-server.huggingface.co/first-rows?dataset=parsimony-dev/riksbank&config=default&split=train"
```

Helper module: `scripts/catalog_dataset_card.py` (generation + local tree discovery).

## HF auth and CLI

Use the **`hf`** CLI (replaces deprecated `huggingface-cli`). Install once:

```bash
uv tool install 'huggingface_hub[cli]'   # installs `hf`; run via `uv tool run hf` or add to PATH
hf auth whoami                           # or: uv tool run hf auth whoami
```

Auth options (pick one):

- **`HF_TOKEN`** env var (recommended in `ockham/.env`) — passed through by `scripts/push_catalog.sh`
- **`hf auth login`** — interactive token from https://huggingface.co/settings/tokens

Common commands:

| Task | Command |
|------|---------|
| Whoami | `hf auth whoami` |
| Create dataset repo | `hf repos create parsimony-dev/riksbank --repo-type dataset --exist-ok` |
| Upload snapshot (root) | `hf upload parsimony-dev/riksbank /tmp/parsimony-catalogs/riksbank --repo-type dataset` |
| Upload SDMX sub-bundle | `hf upload parsimony-dev/sdmx /tmp/.../sdmx_datasets sdmx_datasets --repo-type dataset` |
| Publish dataset card (viewer) | `uv run python scripts/publish_catalog_dataset_card.py --repo-id parsimony-dev/riksbank` |
| Dataset info | `hf datasets info parsimony-dev/riksbank` |

Validation read access uses public dataset repos when available.
