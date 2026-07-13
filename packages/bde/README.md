# parsimony-bde

Banco de España connector — Spanish macroeconomic, monetary, and financial time series via the BIEST REST API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bde`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bde_fetch` | connector | Fetch one or more BdE time series by series code (comma-separated). |
| `enumerate_bde` | enumerator | Discover BdE series from six catalog CSV chapters plus the Bank Lending Survey (recovered from `pb.zip`). |
| `bde_search` | connector | Semantic-search the published BdE catalog snapshot; returns ranked series codes. |

`bde_fetch`'s `time_range` is frequency-dependent (BdE validates it): monthly /
quarterly series take `30M`/`60M`/`MAX`, daily series take `3M`/`12M`/`36M` (not
`MAX`), and any frequency accepts a 4-digit year (e.g. `2024`). A range that
doesn't fit a series' frequency is reported as an `InvalidParameterError`.

## Install

```bash
pip install parsimony-bde
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No API key required — the BdE BIEST API is open and unauthenticated.

`bde_search` reads a published catalog snapshot (default `hf://parsimony-dev/bde`).
Override the snapshot location with the `PARSIMONY_BDE_CATALOG_URL` environment
variable, or pass `catalog_url=` at call time.

## Quick start

```python
from parsimony_bde import CONNECTORS

result = CONNECTORS["bde_fetch"](key="D_1NBAF472")
print(result.raw.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

`enumerate_bde` discovers the full BdE series catalog — ~15.5k unique series
across six published CSV chapters (`be`, `cf`, `ie`, `si`, `tc`, `ti`), plus the
Bank Lending Survey. The survey's CSV chapter lists only un-fetchable family
aliases (`PB_1_1.1`), so its real fetchable codes (`DPB…`) are recovered from the
bulk `pb.zip`. Rows are de-duplicated by series code (a series can appear under
more than one thematic chapter). Maintainers build a `Catalog` snapshot from the
enumerator (see `scripts/build_catalog.py`) and push it to the snapshot URL that
`bde_search` reads. The crawl is expensive (the CF/Financial-Accounts chapter
alone is several thousand series), so it runs offline as a publish job — never at
query time.

A small tail (~1% of catalog codes, mostly dollar-denominated `…$…` financial
accounts variants and a handful of external-sector series) is listed in the CSV
catalog but not served by the web service; fetching such a code returns a clear
`InvalidParameterError` rather than data.

## Provider

- Homepage: https://www.bde.es
- API docs: https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html
- Series browser: https://app.bde.es/bie_www/bie_wwwias/xml/Arranque.html (BIEST)

## License

See [LICENSE](./LICENSE).
