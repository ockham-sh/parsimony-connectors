# parsimony-snb

Swiss National Bank connector plugin for parsimony — monetary, exchange-rate, balance-of-payments, and price series from the SNB data portal (https://data.snb.ch).

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-snb`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `snb_fetch` | connector | Fetch an SNB cube as a long-format DataFrame by `cube_id`, with optional date window, dimension selection, and language. |
| `enumerate_snb` | enumerator | Enumerate the curated SNB cube list as per-series rows with inferred category and frequency (drives the `snb` catalog). |
| `snb_search` | search | Semantic search over the published SNB catalog. Pass the `cube_id` portion (before `#`) of a returned code to `snb_fetch(cube_id=...)`. |

## Install

```bash
pip install parsimony-snb
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
from parsimony_snb import CONNECTORS

result = CONNECTORS["snb_fetch"](cube_id="rendoblim")
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

`snb_search` runs semantic search over a published catalog snapshot
(`hf://parsimony-dev/snb` by default; override with the
`PARSIMONY_SNB_CATALOG_URL` env var or `load(catalog_url=...)`). The snapshot
is built from `enumerate_snb` via `scripts/build_catalog.py`. No API key is
required — SNB is a public, keyless data portal.

## Provider

- Homepage: <https://www.snb.ch>
- Data portal: <https://data.snb.ch>

## License

See [LICENSE](./LICENSE).
