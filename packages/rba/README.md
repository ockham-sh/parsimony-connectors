# parsimony-rba

Reserve Bank of Australia source for parsimony: statistical tables fetch and catalog enumeration.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-rba`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `rba_fetch` | connector | Fetch a published RBA statistical table by `table_id` (e.g. `f1-data`, `g1-data`). Resolves the live tables index, downloads the CSV, and returns a tidy long-format DataFrame. |
| `enumerate_rba` | enumerator | Discover available series by scraping the tables index and parsing each CSV's (and XLSX/xls-hist) metadata header rows. Drives the `rba` catalog. |
| `rba_search` | search | Semantic search over the published RBA catalog. Pass the `table_id` portion (before `#`) of a returned code to `rba_fetch(table_id=...)`. |

## Install

```bash
pip install parsimony-rba
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No API key required — the RBA statistics site is public and keyless.

**`curl_cffi` is a required runtime dependency** (installed automatically). The
RBA site (`rba.gov.au`) is fronted by Akamai bot-mitigation that
TLS-fingerprint-blocks stock python-httpx (every request returns HTTP 403).
`parsimony-rba` reaches the origin only via `curl_cffi`, which presents a real
Chrome TLS handshake. Without it the connector is non-functional — this is why
`curl_cffi` ships as a hard dependency rather than an optional extra.

`rba_search` reads its catalog snapshot from `hf://parsimony-dev/rba` by
default; override with the `PARSIMONY_RBA_CATALOG_URL` env var or
`load(catalog_url=...)`.

## Quick start

```python
import asyncio
from parsimony_rba import CONNECTORS

async def main():
    connectors = CONNECTORS
    result = await connectors["rba_fetch"](table_id="f1-data")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

`rba_search` runs semantic search over a published catalog snapshot
(`hf://parsimony-dev/rba` by default; override with the
`PARSIMONY_RBA_CATALOG_URL` env var or `load(catalog_url=...)`). The snapshot is
built from `enumerate_rba` via `scripts/build_catalog.py`. No API key is
required — RBA is a public, keyless data source.

## Provider

- Homepage: <https://www.rba.gov.au>
- Statistical tables: <https://www.rba.gov.au/statistics/tables/>

## License

See [LICENSE](./LICENSE).
