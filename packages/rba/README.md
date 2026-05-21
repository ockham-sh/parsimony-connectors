# parsimony-rba

Reserve Bank of Australia source for parsimony: statistical tables fetch and catalog enumeration.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-rba`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `rba_fetch` | connector | Fetch a published RBA statistical table by `table_id` (e.g. `f1-data`, `g1-data`). Resolves the live tables index, downloads the CSV, and returns a tidy long-format DataFrame. |
| `enumerate_rba` | enumerator | Discover available series by scraping the tables index and parsing each CSV's metadata header rows. |

## Install

```bash
pip install parsimony-rba
```

Pulls in `parsimony-core>=0.5,<0.6` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No environment variables required — the RBA statistics site is public. The optional `curl_cffi` extra improves reliability against the Akamai CDN's TLS-fingerprinting checks; the connector falls back to `httpx` automatically.

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

This plugin currently exposes connectors only. If a catalog is added, it should be a lazy `Catalog` declaration that maintainers build and push directly.

## Provider

- Homepage: <https://www.rba.gov.au>
- Statistical tables: <https://www.rba.gov.au/statistics/tables/>

## License

See [LICENSE](./LICENSE).
