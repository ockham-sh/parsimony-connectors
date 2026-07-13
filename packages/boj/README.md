# parsimony-boj

Bank of Japan connector — Japanese interest rates, FX, money & deposits, flow of
funds, TANKAN, prices (CGPI/SPPI), balance of payments and BIS-related statistics,
as numeric time series via the **BOJ Time-Series Data Search** API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boj`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boj_databases_search` | connector | **Step 1.** Search the 50 statistics databases; returns a `db` code (e.g. `FM08`) and the `series_namespace` for step 2. |
| `boj_series_search` | connector | **Step 2.** Search series within one database (`db=…`); returns `code` + `db` for `boj_fetch`. |
| `boj_fetch` | connector | Fetch observations by database + series code(s) (e.g. `db=FM08, code=FXERD01,FXERD04`). Max 250 codes; large requests are paginated transparently. |
| `enumerate_boj` | enumerator | Catalog feed: one row per series (~326k) and one per database (50), from a per-DB `getMetadata` fan-out. |

Discovery chain: `boj_databases_search` → `boj_series_search(db=...)` → `boj_fetch`.

## Discovery model

BoJ has **no native keyword search**, so discovery is a built catalog. The API
also exposes **no way to list its databases** (`getMetadata` requires a `db`),
so the universe is enumerated two ways (archetype **C + B**):

- **A frozen 50-database registry** (the *C* part). The list is transcribed from
  the official API manual and cross-validated against the machine-readable
  `api_tool.xlsx` `DB_Name` sheet — both agree exactly.
  `scripts/harvest_databases.py` regenerates it (`--diff` checks for drift).
- **A live per-database `getMetadata` fan-out** (the *B* part). `getMetadata` is
  **uncapped** — one call returns *every* series in a database (the `CO`/TANKAN
  database alone is 166,513 series) — so each per-DB series catalog is complete.

The published catalog is **multi-bundle**: a `boj_databases` bundle (the 50 DBs)
plus one `boj_series_<db>` bundle per database, built lazily and LRU-cached. This
two-tier shape (like `parsimony-bls`) keeps each namespace tractable for the
326k-series universe. **Every series is fetchable by `(db, code)`** regardless of
catalog coverage — the boundary is discovery, not access.

## Install

```bash
pip install parsimony-boj
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the BOJ API is open and unauthenticated.

The search connectors read published catalog snapshots (default root
`hf://parsimony-dev/boj`). Override with `PARSIMONY_BOJ_CATALOG_URL` or
`catalog_root=` at call time; a missing snapshot is built on demand from the live
metadata and cached in an LRU.

> **Request limits.** `boj_fetch` accepts up to **250 series codes** per call,
> all of the **same frequency** (a BoJ rule). The API also caps each response at
> **60,000 data points** `(series × periods)`; `boj_fetch` paginates over that
> limit automatically (via the API's `NEXTPOSITION` cursor), so a large
> multi-series request returns its full result rather than a silent truncation.

## Quick start

```python
from parsimony_boj import CONNECTORS

# step 1: find the database
dbs = CONNECTORS["boj_databases_search"](query="foreign exchange rates")
db = dbs.raw.iloc[0]["db"]                  # e.g. "FM08"
# step 2: find a series within it
hits = CONNECTORS["boj_series_search"](query="US dollar spot", db=db)
code = hits.raw.iloc[0]["code"]             # e.g. "FXERD01"
# fetch observations
result = CONNECTORS["boj_fetch"](db=db, code=code, start_date="202401")
print(result.raw.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog building

`scripts/build_catalog.py` builds the multi-bundle catalog from the live
metadata and saves/pushes snapshots:

```bash
uv run python packages/boj/scripts/build_catalog.py \
  --catalog all --save-root file:///tmp/parsimony-catalogs/boj --push-root hf://parsimony-dev/boj
```

## Provider

- Homepage: https://www.boj.or.jp
- API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
- Terms: https://www.stat-search.boj.or.jp/info/api_notice_en.pdf — public reuse
  with **attribution to the Bank of Japan**. This connector and its catalog are a
  derived index of series identifiers and titles.

## License

See [LICENSE](./LICENSE).
