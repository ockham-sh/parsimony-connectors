# parsimony-boj

Bank of Japan (BoJ) statistics connector — fetches time series from the BoJ Time-Series Data Search.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boj`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boj_fetch` | fetch | Fetch BoJ time series by database (`db`) and series `code`. Max 250 codes per request. |
| `enumerate_boj` | enumerator | Enumerate BoJ series across the 50 canonical databases (catalog indexing). |
| `boj_databases_search` | search | Step 1: search statistics databases (`db` codes). |
| `boj_series_search` | search | Step 2: search series within one database; returns `code` + `db` for `boj_fetch`. |

Catalog discovery chain: `boj_databases_search` → `boj_series_search(db=...)` → `boj_fetch`.
Published snapshots use a multi-bundle layout under `hf://parsimony-dev/boj/` (see
connectors-repo [catalog-operations.md](../../docs/catalog-operations.md) — internal
maintainer standard, not part of `parsimony-core`).

## Install

```bash
pip install parsimony-boj
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No environment variables required. The BoJ API is unauthenticated.

## Quick start

```python
from parsimony_boj import CONNECTORS

result = CONNECTORS["boj_fetch"](db="FM08", code="FXERD01")
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.boj.or.jp
- API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf

## License

See [LICENSE](./LICENSE).
