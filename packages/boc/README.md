# parsimony-boc

Bank of Canada connector — Canadian exchange rates, interest/bond yields, money
& credit aggregates, CPI, commodity price indices, and the data behind BoC
publications, as numeric time series via the **Valet** API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boc`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boc_fetch` | connector | Fetch one or more series by name (e.g. `FXUSDCAD,FXEURCAD`) or a whole panel by group (e.g. `group:FX_RATES_DAILY`). Reaches the **entire** universe by name. |
| `enumerate_boc` | enumerator | Catalog feed: one row per series (~15.6k) and one per live group (~2.4k), from Valet's list endpoints plus a per-group membership fan-out. |
| `boc_search` | connector | Search the published BoC catalog (lexical title or `code:`/structured). Pass returned codes to `boc_fetch(series_name=...)`. |

## Discovery model

The Valet API has **no native keyword search**, so discovery is a built catalog
(archetype A — one live full-index call lists the whole universe):

- `GET /lists/series/json` is the **authoritative enumeration path** (15,638
  series, live 2026-06-09). It self-tracks BoC additions, so the catalog stays
  current on each rebuild.
- **Groups are first-class catalog entities.** Each named panel (e.g.
  `FX_RATES_DAILY`) gets its own row keyed `group:NAME` — the exact string
  `boc_fetch` accepts — because group descriptions carry retrieval signal no
  individual series has ("Month-end, Millions of dollars"). A fetch on a
  `group:` code returns the whole panel in one request.
- The per-group membership fan-out doubles as a **liveness probe**: ~29 retired
  one-off panels that 404 on every fetch path are pruned, so the catalog never
  offers a panel you cannot fetch.

Every series stays fetchable by name regardless of catalog coverage — the
boundary is discovery, not access.

## Install

```bash
pip install parsimony-boc
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the Valet API is open and unauthenticated.

`boc_search` reads published catalog snapshots (default root
`hf://parsimony-dev/boc`). Override with `PARSIMONY_BOC_CATALOG_URL` or
`catalog_url=` at call time; a missing snapshot is built on demand from the live
list endpoints and cached in an LRU.

> **Observations request limit.** Valet caps the `/observations` request URL near
> **4096 bytes** (roughly 100–160 comma-joined series, depending on name length).
> `boc_fetch` guards this pre-call and asks you to split the request or fetch a
> whole panel with `group:NAME`. The limit is on URL length, not series count.

## Quick start

```python
from parsimony_boc import CONNECTORS

# find a series (or a whole panel) in the catalog
hits = CONNECTORS["boc_search"](query="US dollar Canadian dollar exchange rate")
code = hits.raw.iloc[0]["code"]            # e.g. "FXUSDCAD" or "group:FX_RATES_DAILY"
# fetch observations
result = CONNECTORS["boc_fetch"](series_name=code, start_date="2024-01-01")
print(result.raw.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog building

`scripts/build_catalog.py` builds the single `boc` catalog from the live list
endpoints and saves/pushes a snapshot:

```bash
uv run python packages/boc/scripts/build_catalog.py \
  --save file:///tmp/parsimony-catalogs/boc --push hf://parsimony-dev/boc
```

## Provider

- Homepage: https://www.bankofcanada.ca
- Valet API docs: https://www.bankofcanada.ca/valet/docs
- Terms: https://www.bankofcanada.ca/terms/ — free reuse **with attribution to
  the Bank of Canada**. Data is © Bank of Canada; this connector and its catalog
  are a derived index of series identifiers and titles.

## License

See [LICENSE](./LICENSE).
