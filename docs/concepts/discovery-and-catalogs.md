# Discovery and catalogs

A connector exposes a provider's data through two separate jobs:

- **Discovery** answers *"what data exists?"* — given a query, return the codes (series IDs,
  table IDs, dataset IDs) you can address, plus enough metadata to dispatch a fetch.
- **Fetch** answers *"give me the values for this code"* — given a code, return a `Result`
  with `result.frame`, `result.data`, and `result.provenance`.

These are always different connectors. Discovery never returns observations; fetch never
guesses a code. The agent loop is: search to find a code, then fetch that code.

```python
from parsimony import discover

connectors = discover.load("fred")

# 1. discover: find the code
hits = connectors["fred_search"](search_text="US unemployment rate")
code = hits.frame.iloc[0]["id"]          # e.g. "UNRATE"

# 2. fetch: pull the values for that code
series = connectors["fred_fetch"](series_id=code)
print(series.frame.head())
```

The two discovery shapes below differ only in *where the search runs*. The fetch side is the
same everywhere.

## Two discovery shapes

### Native-search providers

Nine providers ship a usable search/screener endpoint of their own, so the connector simply
wraps it. No catalog is built; the query goes straight to the provider's API and the live
response is normalized into search rows.

| Provider | Search connector | Notes |
| --- | --- | --- |
| `alpha_vantage` | `alpha_vantage_search` | symbol/keyword search |
| `coingecko` | `coingecko_search` | coin/market search |
| `eodhd` | `eodhd_search` | symbol search |
| `finnhub` | `finnhub_search` | symbol search |
| `fmp` | `fmp_search` | symbol/screener search |
| `fred` | `fred_search` | series keyword search |
| `polymarket` | `polymarket_markets`, `polymarket_events` | live enumerators (no search verb) |
| `sec_edgar` | `sec_edgar_full_text_search`, `sec_edgar_find_company` | full-text search + lookups |
| `tiingo` | `tiingo_search` | symbol search |

`polymarket` and `sec_edgar` are slightly different in mechanism (`polymarket` enumerates
markets and events; `sec_edgar` wraps EDGAR full-text search plus ticker/CIK lookups) but
they belong here: no catalog is built, discovery hits the provider live.

The search connector is a normal `@connector` that calls the provider endpoint. For example,
`fred_search` (see
[fred/parsimony_fred/__init__.py](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/fred/parsimony_fred/__init__.py))
posts the query to FRED's `series/search` and returns the series metadata FRED sends back.

### Catalog-backed providers

Thirteen providers have no usable native search (their APIs only fetch by exact code, or the
search they expose is too narrow to enumerate the universe). For these the maintainers
**build a catalog**: a searchable index of every addressable unit, snapshotted and published.

| Provider | Search connector | Catalog covers |
| --- | --- | --- |
| `bde` | `bde_search` | Banco de España series |
| `bdf` | `bdf_search` | Banque de France series |
| `bdp` | `bdp_search` | Banco de Portugal series |
| `bls` | `bls_surveys_search`, `bls_series_search` | BLS surveys + series (two-tier) |
| `boc` | `boc_search` | Bank of Canada series |
| `boj` | `boj_databases_search`, `boj_series_search` | Bank of Japan databases + series (two-tier) |
| `destatis` | `destatis_search` | GENESIS predefined tables |
| `eia` | `eia_search` | EIA datasets |
| `rba` | `rba_search` | Reserve Bank of Australia series |
| `riksbank` | `riksbank_search` | Sveriges Riksbank series across five products |
| `sdmx` | `sdmx_datasets_search`, `sdmx_series_search`, `sdmx_dimension_search` | SDMX datasets, series + dimension values |
| `snb` | `snb_search` | Swiss National Bank series + warehouse cubes |
| `treasury` | `treasury_search` | US Treasury Fiscal Data + ODM rate feeds |

The agent calls `<provider>_search(query="...")` and gets back rows of
`{code, title, score, ...dispatch metadata}`, then passes a `code` to a fetch connector. The
search runs against the **local catalog snapshot**, not the provider, so it is fast, offline-capable,
and does not consume provider quota.

For the per-provider discovery method and exact code shapes, see
[../reference/providers.md](../reference/providers.md).

## How a catalog is structured

A catalog is built once per provider (operators run the build; see
[../guides/building-catalogs.md](../guides/building-catalogs.md)) and the resulting snapshot is
what search reads at runtime. Conceptually the build pipeline is:

1. An `@enumerator` connector emits **one row per addressable unit**: a `KEY` (the code plus
   its namespace), a `TITLE`, and any `METADATA` columns.
2. `result.to_entities()` converts that frame into entities, reading roles off
   `result.output_spec`.
3. `Catalog(namespace, indexes=discovery_indexes(entries), default_field="title")` wraps the
   entities with the index policy.
4. `catalog.build()` constructs the indexes; `catalog.save(url)` writes the snapshot.

The snapshot on disk (or on the Hub) is three things:

- `entries.parquet` — the rows themselves.
- `indexes/<field>/` — the built index for each indexed field.
- `meta.json` — manifest, including a `content_sha256` integrity digest and a
  `schema_version` (currently `1`).

### What gets indexed

The index layout comes from `discovery_indexes` in the kernel's
`parsimony.catalog.policy`. It builds:

- a **`code`** index — BM25, for exact code lookup;
- a **`title`** index — adaptive;
- a **`description`** index — adaptive (when a description field is present).

"Adaptive" means: if the field has **fewer than 1000 unique values**, the index is a
**Hybrid BM25 + vector** index (lexical match fused with semantic similarity). At or above
1000 unique values, it falls back to **BM25-only**.

The consequence is the single most important thing to understand about catalog recall:

> **Recall is driven by catalog content — the title and description text — not by how many
> metadata columns you attach.**

Adding more metadata columns does not improve search; only the indexed `code`, `title`, and
`description` text is matched against a query. Above 1000 unique titles the title index loses
its semantic component, so natural-language title queries degrade to lexical matching: exact
`code:` lookups stay reliable, but fuzzy title matches may miss. Catalogs whose universe is
large get the most recall benefit from rich, descriptive `title`/`description` text.

### Structured queries

`default_field="title"` means a bare query string searches titles, so `query="par yield"`
does broad title search. You can also issue **structured `FIELD: value` queries** against any
indexed field:

```python
connectors["fred_search"](search_text="UNRATE")            # broad title search
connectors["treasury_search"](query="code: AVMAT")          # exact code lookup
connectors["treasury_search"](query="description: par yield")  # description field search
```

Combine clauses with `&&` (AND) and `,` (OR):

```text
code: UNRATE, code: GDPC1          # OR — either code
description: yield && title: par   # AND — both must match
```

## The search connector and catalog resolution

Catalog-backed search connectors are created declaratively with `make_local_search_connector`
(from `parsimony.catalog.search`). The treasury connector is a representative example (see
[treasury/parsimony_treasury/search.py](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/treasury/parsimony_treasury/search.py)):

```python
treasury_search = make_local_search_connector(
    provider="treasury",
    default_url="hf://parsimony-dev/treasury",
    catalog_url_env_var="PARSIMONY_TREASURY_CATALOG_URL",
    build_catalog=build_treasury_catalog,
    tags=["macro", "us", "tool"],
    description="Semantic-search the US Treasury catalog ...",
    output_columns=TREASURY_SEARCH_OUTPUT.columns,
    metadata_columns=("source", "endpoint", "field"),
)
```

Key arguments:

- **`provider`** — names the connector (`<provider>_search`) and the cache namespace.
- **`default_url`** — the hosted snapshot, conventionally `hf://parsimony-dev/<provider>`.
- **`catalog_url_env_var`** — the override env var, conventionally
  `PARSIMONY_<PROVIDER>_CATALOG_URL`.
- **`build_catalog`** — the build function used for a cold rebuild when no snapshot is found.
- **`tags`** — free-form labels for organizing and filtering connectors.
- **`output_columns`** — the result schema (a `KEY` `code`, a `TITLE` `title`, a `DATA`
  `score`, plus the metadata columns).
- **`metadata_columns`** — the **dispatch payload** returned with each hit.

### `metadata_columns` is the dispatch payload

`metadata_columns` are the extra columns echoed onto each search hit so the agent knows
*which fetch verb to call and with what arguments* — without parsing the code string.
Treasury returns `source`, `endpoint`, and `field`; its description tells the agent how to
route them (`source=fiscal_data → treasury_fetch(endpoint=endpoint)`,
`source=treasury_rates → treasury_rates_fetch(feed=endpoint)`). Riksbank returns only
`source` and routes by the shape of the `code` itself (see
[riksbank/parsimony_riksbank/search.py](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/riksbank/parsimony_riksbank/search.py)).

These columns are **dispatch metadata, not recall** — they are not indexed and do not affect
which rows match a query.

### Catalog resolution order

When a search connector needs its catalog, it resolves the URL in this order:

1. a `catalog_url` parameter passed to the search call (explicit override);
2. the `PARSIMONY_<PROVIDER>_CATALOG_URL` environment variable;
3. the `default_url` — the hosted Hub snapshot;
4. the on-disk lazy cache;
5. a cold rebuild via `build_catalog`, written into the cache.

This is why search works unchanged both in production (it pulls the hosted snapshot) and in a
fresh clone (it falls through to a local rebuild). No code change is needed to move between
them.

## Snapshots, the Hub home, and the cache

A built catalog persists as `entries.parquet` + `indexes/<field>/` + `meta.json`. The default
Hub home is `hf://parsimony-dev/<provider>`. At runtime, catalogs are cached under:

```text
~/.cache/parsimony/catalogs/<provider>/<namespace>/
```

Override the entire cache root with `PARSIMONY_CACHE_DIR`, and inspect what is cached with:

```bash
parsimony cache info
```

(See [../reference/cli.md](../reference/cli.md) for the full CLI surface.)

`meta.json` carries a `schema_version` (currently `1`). A version mismatch between a snapshot
and the running kernel is a **hard gate** — the catalog will not load. When the kernel bumps
the schema, the catalog must be rebuilt and re-pushed; see
[../guides/building-catalogs.md](../guides/building-catalogs.md).

## See also

- [../guides/building-catalogs.md](../guides/building-catalogs.md) — the operator build and
  publish workflow.
- [../guides/using-connectors.md](../guides/using-connectors.md) — the search→fetch loop in
  practice.
- [./connectors.md](./connectors.md) — the connector and `Result` model.
- [../reference/providers.md](../reference/providers.md) — per-provider discovery method and
  code shapes.
