# parsimony-rba

Reserve Bank of Australia source for parsimony: statistical tables fetch and catalog enumeration.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-rba`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `rba_fetch` | connector | Fetch a published RBA statistical table/series by `table_id`. Resolves across all three publication formats — a CSV stem (`f1-data`), a current XLSX-exclusive sheet (`a03/Bond Purchase Program`), or a legacy xls-hist workbook (`b03hist`) — and returns a tidy long-format DataFrame. |
| `enumerate_rba` | enumerator | Discover series via the 3-pass HTML scrape (CSV index + current XLSX-exclusive sheets + legacy xls-hist), parsing each table's metadata header rows. Drives the `rba` catalog. |
| `rba_search` | search | Semantic search over the published RBA catalog. Pass the `table_id` portion (before `#`) of a returned code to `rba_fetch(table_id=...)`. |

## Coverage

The catalog indexes **~4,672 series** across RBA's nine statistical categories
(Reserve Bank, banking & finance, credit cards, monetary aggregates, household &
business finance, interest rates, exchange rates, economic activity, balance of
payments), drawn from three publication formats RBA exposes as static files (it
has **no JSON/REST API**):

- **CSV index** (`/statistics/tables/`) — ~3,958 current series (the bulk).
- **Current XLSX-exclusive sheets** — series published only in a workbook, never
  re-exported as CSV (today: the Bond Purchase Program). Detected by dynamic
  exclusivity, so a future XLSX-only sheet is picked up automatically.
- **Legacy xls-hist binaries** (`/statistics/historical-data.html`) — discontinued
  series that left the live CSVs.

The catalog code is compound (`{table_id}#{series_id}`) because RBA reuses series
ids across related tables. **Every catalogued series is fetchable** — `rba_fetch`
resolves the `table_id` to whichever format published it. The redundant
`*hist.xlsx` long-history workbooks and the period-range archives are deliberately
skipped (an audit confirmed they carry the same series ids as the current CSVs).
Data is CC BY 4.0 — cite as "Source: Reserve Bank of Australia".

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
from parsimony_rba import CONNECTORS

result = CONNECTORS["rba_fetch"](table_id="f1-data")
print(result.data.head())
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
