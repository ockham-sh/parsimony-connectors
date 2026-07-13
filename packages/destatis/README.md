# parsimony-destatis

Destatis (German Federal Statistical Office) connector — fetches and enumerates tables from the public GENESIS-Online REST API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-destatis`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `destatis_fetch` | fetch | Fetch a GENESIS table by code (e.g. `61111-0001`), with optional `start_year` / `end_year`. JSON-stat 2.0 is parsed into long-format rows; German month/quarter labels are normalized to ISO dates. |
| `enumerate_destatis` | enumerator | Enumerate GENESIS statistics and their tables (catalog indexing) by crawling `/statistics`, `/statistics/{code}/information`, and `/statistics/{code}/tables`. |
| `destatis_search` | tool | Semantic-search the published Destatis catalog and map a natural-language query to a table code (feed it to `destatis_fetch(name=...)`). |

## Coverage

The keyless GENESIS-Online REST API exposes **3,009 predefined tables across 331 statistics**
(prices, population, national accounts, labour, foreign trade, production, transport, health,
environment, earnings, …). `enumerate_destatis` walks the authoritative path — `/statistics`
then a per-statistic `/tables` fan-out — which is lossless (every statistic's table list comes
back in full, no pagination cap, no table shared across statistics), so the catalog contains
every fetchable table. Each table is itself multi-dimensional: `destatis_fetch` returns one row
per (classification combination × period), with the classification dimensions preserved as
columns. Time is parsed from the JSON-stat period keys (annual, monthly, quarterly, semester,
reference-date, school-year — all normalized to ISO dates).

The multidimensional **cube / custom-table** power-user surface of classic GENESIS is **not**
part of this keyless API (it belonged to the retired registration-gated webservice) and is out
of scope.

## Install

```bash
pip install parsimony-destatis
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

**No credentials required.** GENESIS-Online allows anonymous access — there is no API key, username, or password to set.

The only optional setting is the catalog-snapshot URL used by `destatis_search`:

```bash
export PARSIMONY_DESTATIS_CATALOG_URL="hf://your-org/destatis"   # optional; overrides the default snapshot
```

## Quick start

```python
from parsimony_destatis import CONNECTORS

result = CONNECTORS["destatis_fetch"](name="61111-0001")
print(result.raw.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.destatis.de
- GENESIS-Online API: https://genesis.destatis.de/genesis/api/rest

## License

See [LICENSE](./LICENSE).
