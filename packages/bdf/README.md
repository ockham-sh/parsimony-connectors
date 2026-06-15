# parsimony-bdf

Banque de France connector — French macroeconomic, monetary, and financial time
series via the Webstat (Opendatasoft) API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bdf`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bdf_fetch` | connector | Fetch a Banque de France time series by SDMX key (e.g. `EXR.M.USD.EUR.SP00.E`), optionally bounded by `start_period`/`end_period`. |
| `enumerate_bdf` | enumerator | Stream the full BdF series universe (~41.6k series across 45 dataflows) for catalog discovery. |
| `bdf_search` | connector | Semantic-search the published BdF catalog snapshot; returns ranked series codes. |

## Install

```bash
pip install parsimony-bdf
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

The Webstat API requires a free API key. Register at
https://developer.webstat.banque-france.fr/, then set:

```bash
export BDF_API_KEY="<your-key>"
```

The key is sent in the `Authorization: Apikey <KEY>` header (the literal word
`Apikey`, **not** `Bearer`). It is declared as a secret (stripped from
provenance) and never appears in request logs. Supply it via the env var above,
or bind it explicitly:

```python
from parsimony_bdf import load
connectors = load(api_key="<your-key>")   # binds the key across the bundle
```

A missing key fails fast with `UnauthorizedError` naming `BDF_API_KEY`.

`bdf_search` reads a published catalog snapshot (default `hf://parsimony-dev/bdf`).
Override the snapshot location with the `PARSIMONY_BDF_CATALOG_URL` environment
variable, or pass `catalog_url=` at call time.

## Quick start

```python
from parsimony_bdf import load

connectors = load(api_key="<your-key>")
result = connectors["bdf_fetch"](key="EXR.M.USD.EUR.SP00.E")
print(result.data.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

The Webstat `series` dataset is a single flat queryable table, so `enumerate_bdf`
discovers the **entire** universe in two requests: one `series/exports/json`
export (~41.6k series) plus one `webstat-datasets` call for the 45 dataflow stub
rows. Each series row carries English + French titles and a topic breadcrumb,
folded into the catalog `description` for cross-language recall. Maintainers
build a `Catalog` snapshot from it (`scripts/build_catalog.py`) and push it to
the snapshot URL `bdf_search` reads — the build runs offline as a publish job,
never at query time. Quota: 10,000 requests/day.

## Provider

- Homepage: https://www.banque-france.fr
- Webstat portal: https://webstat.banque-france.fr
- Developer portal: https://developer.webstat.banque-france.fr/

## License

See [LICENSE](./LICENSE).
