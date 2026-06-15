# parsimony-snb

Swiss National Bank connector plugin for parsimony — monetary, exchange-rate, balance-of-payments, and price series from the SNB data portal (https://data.snb.ch).

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-snb`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `snb_fetch` | connector | Fetch an SNB cube as a long-format DataFrame by `cube_id` — both publication cubes (`rendoblim`) and data-warehouse cubes (`BSTA@SNB.AUR_U.ODF`), routed automatically. Optional date window, dimension selection, and language. |
| `enumerate_snb` | enumerator | Enumerate every SNB cube discovered live from the portal sitemap (publication + warehouse) as catalog rows (drives the `snb` catalog). |
| `snb_search` | search | Semantic search over the published SNB catalog. Pass the `cube_id` portion (before `#`) of a returned code to `snb_fetch(cube_id=...)`. |

## Coverage

The cube universe is discovered live from the SNB portal's published XML sitemap
(`https://data.snb.ch/sitemap`), so the catalog self-tracks new cubes:

- **237 publication cubes** (`/topics/{topic}/cube/{id}`) — catalogued at series
  granularity (compound `cube_id#series_key` codes).
- **912 data-warehouse cubes** (`/warehouse/{group}/cube/{sdmx_id}`, groups BSTA /
  ZAST / ZAHL / DDUM / KRED / SNB1A / WKI) — catalogued at cube level; the individual
  dimension-leaf series stay fetchable via `dim_sel`.

Every catalogued cube (1,149 total) is fetchable through `snb_fetch`. Cube titles
come from the portal's metadata API on a best-effort basis. SNB is a public, keyless
data portal — no API key.

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
