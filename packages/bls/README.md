# parsimony-bls

US Bureau of Labor Statistics connector — CPI, PPI, employment (CES/CPS/QCEW),
unemployment (LAUS), JOLTS, ECI, productivity, import/export prices, and more, as
numeric time series.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bls`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bls_fetch` | connector | Fetch observations for any `series_id` via the BLS Public Data API. Reaches the **entire** universe by id. |
| `enumerate_bls_surveys` | enumerator | Tier-1 feed: one row per BLS survey (program). |
| `enumerate_bls_series` | connector | Tier-2 feed: one row per series in one survey, from its authoritative `.series` flat file. |
| `bls_surveys_search` | connector | Discover surveys and read their dimension manifests. |
| `bls_series_search` | connector | Search one survey's series (literal title `query=` and/or exact `filter=`). |

## Why two tiers

BLS's full universe is **far too large to embed** — the per-survey `.series` flat
files total ~15.6 GB (tens of millions of series; the injury/illness demographic
microdata surveys alone are ~12 GB). So discovery is **two-tier, mirroring
`parsimony-sdmx`** (survey ≈ SDMX dataflow; a survey's dimension code tables ≈ a
DSD's codelists; a BLS `series_id` ≈ a composed series key):

- **Tier 1 — `bls_surveys`** (always built, complete): one entity per survey, with
  a compact `dimensions` manifest (each dimension's codes + labels) for the
  surveys that have a series catalog.
- **Tier 2 — `bls_series_<survey>`** (built for the headline surveys, lazy-buildable
  for any indexable survey): one entity per series with a resolved title and
  per-dimension columns for exact `filter=` (and label fields for discovery).

Every series stays **fetchable by id** via `bls_fetch` regardless of catalog
coverage — the boundary is discovery, not access. The GB-scale microdata tail is
reachable by constructing an id from the tier-1 manifest and fetching it.

> **Note on dimension labels.** Each series carries its dimension codes plus a
> resolved label. For most surveys every label resolves (CU/CE/JT/SM = 100%), but a
> few have irregular code-table naming where some codes fall back to the raw code
> (e.g. LA ≈ 60%, WP ≈ 70%). Literal title `query=` and `bls_fetch` are unaffected;
> exact `filter=` on those dimensions still matches the stored code.

## Install

```bash
pip install parsimony-bls
```

Pulls in a compatible `parsimony` and `curl_cffi` automatically. `curl_cffi` is a
hard dependency: the bulk flat-file host (`download.bls.gov`) is Akamai
bot-managed and only a real Chrome TLS handshake passes — the data API host
(`api.bls.gov`) uses plain HTTPS.

## Configuration

No key required. An optional `registrationkey` raises the daily quota (25 → 500
queries/day) and request size; set it via the `BLS_API_KEY` environment variable
or bind it: `load(api_key=...)`.

`bls_surveys_search` / `bls_series_search` read published catalog snapshots
(default root `hf://parsimony-dev/bls`). Override with `PARSIMONY_BLS_CATALOG_URL`
or `catalog_root=` at call time; missing snapshots are built on demand from the
live flat files and cached in an LRU.

## Quick start

```python
from parsimony_bls import CONNECTORS

# 1. find the survey + read its dimension manifest
surveys = CONNECTORS["bls_surveys_search"](query="consumer price index")
# 2. search that survey's series — query ranks titles; filter= pins exact codes
hits = CONNECTORS["bls_series_search"](
    survey="CU",
    query="gasoline all types",
    filter={"item_code": "SETB01"},  # from the surveys_search dimensions manifest
)
series_id = hits.raw.iloc[0]["series_id"]  # e.g. "CUUR0000SETB01"
# 3. fetch observations (years optional — defaults to recent window within BLS cap)
result = CONNECTORS["bls_fetch"](series_id=series_id)
print(result.raw.head())
```


For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover

connectors = discover.load_all()
```

## Catalog building

`scripts/build_catalog.py` builds tier-2 series catalogs for the headline surveys,
collects their dimension manifests, then builds the tier-1 surveys catalog with
those manifests attached:

```bash
# headline allowlist → local + remote, per-namespace subdirs
uv run python packages/bls/scripts/build_catalog.py \
  --save-root /tmp/parsimony-catalogs/bls --push-root hf://parsimony-dev/bls

# one survey only
uv run python packages/bls/scripts/build_catalog.py --survey CU --save-root /tmp/bls
```

## Provider

- Homepage: https://www.bls.gov
- API docs: https://www.bls.gov/developers/
- Bulk flat files: https://download.bls.gov/pub/time.series/

## License

See [LICENSE](./LICENSE).
