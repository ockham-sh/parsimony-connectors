# parsimony-treasury

US Treasury connector plugin for parsimony — federal fiscal datasets (debt, revenue, spending, securities) from the Bureau of the Fiscal Service, plus the Office of Debt Management daily rate feeds (par yield curve, bill rates). **Keyless** — no API credentials required.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-treasury`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `treasury_fetch` | connector | Fetch any Fiscal Data API endpoint (e.g. `v2/accounting/od/debt_to_penny`) as a DataFrame, with optional `filter`, `sort`, and `page_size`. |
| `treasury_rates_fetch` | connector | Fetch one Office of Debt Management rate feed (par yield curve, real yield curve, bill rates, long-term, real long-term) for a calendar year from the home.treasury.gov OData/Atom XML feed. |
| `enumerate_treasury` | enumerator | Enumerate every addressable Fiscal Data time-series measure plus the ODM rate-feed benchmarks for catalog indexing (drives the `treasury` catalog). |
| `treasury_search` | connector | Semantic-search the published `treasury` catalog and return matching `code` + `title` + `score` rows. Routing: `home/<feed>` → `treasury_rates_fetch`; `v<n>/<endpoint>#<field>` → `treasury_fetch`. |

## Install

```bash
pip install parsimony-treasury
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
from parsimony_treasury import CONNECTORS

result = CONNECTORS["treasury_fetch"](
    endpoint="v2/accounting/od/debt_to_penny",
    sort="-record_date",
    page_size=10,
)
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

This plugin ships a `treasury` catalog driven by `enumerate_treasury`. `treasury_search` loads a published snapshot (overridable via the `PARSIMONY_TREASURY_CATALOG_URL` env var) and falls back to building one in-process when no snapshot is reachable. Maintainers build and push the snapshot with `scripts/build_catalog.py`.

## Provider

- Homepage: <https://fiscaldata.treasury.gov>
- API docs: <https://fiscaldata.treasury.gov/api-documentation/>

## License

See [LICENSE](./LICENSE).
