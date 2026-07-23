# Getting started

This guide takes you from an empty environment to a working fetch in a few
minutes. You will install the kernel plus one or two provider packages, load
their connectors, and run a search-then-fetch against a real data source.

A connector is a synchronous Python function that calls a data provider and
returns raw data. The framework wraps that return value into a `Result` with
provenance attached. You never construct a `Result` yourself; you call a
connector and read what comes back. See [the connector contract](concepts/connectors.md)
for the full model.

## Install

You always install two things: the kernel (`parsimony-core`) and at least one
provider package. Each provider ships as its own PyPI distribution named
`parsimony-<name>`.

```bash
pip install parsimony-core parsimony-fred
```

Catalog-backed providers (the ones that ship a searchable catalog snapshot,
such as `parsimony-treasury` and `parsimony-sdmx`) declare a dependency on
`parsimony-core[catalog]`, so the catalog runtime is pulled in automatically
when you install them. You do not need to request the `[catalog]` extra
yourself:

```bash
pip install parsimony-core parsimony-treasury parsimony-sdmx
```

Install as many providers as you need; each one registers itself with the
kernel under the `parsimony.providers` entry-point group, and the loader
discovers them automatically.

Requirements: Python 3.11 or newer.

## Load connectors

You do not import provider modules directly. Instead you ask the kernel's
`discover` module to load installed providers and hand you a `Connectors`
collection.

There are two entry points. `load_all()` is forgiving: it loads every installed
provider and skips any that fail to import (logging a warning).

```python
from parsimony import discover

connectors = discover.load_all()
print(connectors.names())
# e.g. ['fred_fetch', 'fred_search', 'treasury_fetch', 'treasury_rates_fetch',
#       'treasury_search', ...]
```

`load(*names)` is strict and named: it loads only the providers you ask for and
raises `LookupError` if one is not installed. Prefer it when you know exactly
which providers you want.

```python
from parsimony import discover

connectors = discover.load("fred", "treasury")
```

Both return the same immutable `Connectors` collection. You index it by
connector name:

```python
fred_fetch = connectors["fred_fetch"]
```

Indexing is by name, not position; `connectors[0]` raises. The collection also
supports `len(connectors)`, iteration, merging two collections with `+`, and
`connectors.names()`. The [using-connectors guide](guides/using-connectors.md)
covers the full collection API.

## First fetch: a native-search provider (FRED)

FRED is a native-search provider: it wraps the provider's own search endpoint
as a `fred_search` connector. There is no catalog. The pattern is to search for
a series, then fetch it by id.

FRED requires an API key. The connector reads `FRED_API_KEY` from the
environment as a fallback, so the simplest path is to export it:

```bash
export FRED_API_KEY="your-key-here"
```

```python
from parsimony import discover

connectors = discover.load("fred")

# 1. Search for a series. Calling a connector returns a Result.
hits = connectors["fred_search"](query="US unemployment rate")
print(hits.frame[["id", "title"]].head())
# id      title
# UNRATE  Unemployment Rate
# ...

# 2. Fetch observations by series_id.
result = connectors["fred_fetch"](series_id="UNRATE")
print(result.frame.head())
#   series_id              title units_short  ... date        value
# 0    UNRATE  Unemployment Rate     Percent  ... 1948-01-01    3.4
```

`result.frame` is a pandas DataFrame (it raises `TypeError` if the payload isn't tabular).
`result.provenance` records where the data came from:

```python
print(result.provenance.source)       # 'fred_fetch'
print(result.provenance.fetched_at)   # datetime, UTC
print(result.provenance.params)       # {'series_id': 'UNRATE', ...}
```

The API key never appears in provenance. It is declared as a secret on the
connector and stripped before the `Result` is built.

`fred_fetch` also accepts an optional observation window:

```python
result = connectors["fred_fetch"](
    series_id="UNRATE",
    observation_start="2020-01-01",
    observation_end="2020-12-31",
)
```

## First fetch: a catalog-backed provider (Treasury)

The US Treasury provider is keyless and catalog-backed. Instead of a live
search endpoint, it ships a prebuilt searchable catalog (a snapshot hosted on
Hugging Face). The catalog loads lazily on first search and caches under
`~/.cache/parsimony/catalogs/`, so the first call may take a moment while the
snapshot downloads.

A first search pays two one-time costs: the catalog download, and — for semantic
search — loading the embedding model, which also downloads once per machine
(~90 MB) and is the slower of the two on a fresh install. Both are cached
afterwards, and neither is a hang; retrying only starts them over.

Each logs a start and a completion line, with size and elapsed time, on the
`parsimony` logger — so a start with no matching finish is still running rather
than stuck. Parsimony installs no log handler of its own, so turn them on with
`logging.basicConfig(level=logging.INFO)`, or run the CLI with `--verbose`, if
you would rather watch the work than wait it out.

The flow is the same shape as native search, but `treasury_search` returns a
`code` column you feed back into a fetch connector:

```python
from parsimony import discover

connectors = discover.load("treasury")

# 1. Search the catalog. No API key needed.
hits = connectors["treasury_search"](query="federal debt outstanding")
print(hits.frame[["code", "title", "endpoint"]].head())

# 2. Fetch by endpoint, using a code/endpoint surfaced by the search.
result = connectors["treasury_fetch"](
    endpoint="v2/accounting/od/debt_to_penny",
)
print(result.frame.head())
```

Because Treasury is keyless, there is nothing to configure. The catalog
download is the only one-time cost.

For details on how catalogs are built, snapshotted, and cached, see
[discovery and catalogs](concepts/discovery-and-catalogs.md).

## Providing an API key

Keyed providers (such as FRED) accept an `api_key=""` parameter and fall back
to a `<PROVIDER>_API_KEY` environment variable. You have three ways to supply a
key.

**1. Environment variable (simplest for local work):**

```bash
export FRED_API_KEY="your-key-here"
```

The connector picks it up automatically; no code change is needed.

**2. Bind the key onto the collection.** This fixes the value so it is never
exposed on the connector's call surface. This is the recommended pattern when
an operator wants to keep the key away from an agent:

```python
from parsimony import discover

connectors = discover.load("fred").bind(api_key="your-key-here")

# api_key is now bound; callers (or an agent) never see or pass it.
result = connectors["fred_fetch"](series_id="UNRATE")
```

`bind` returns a new collection (the originals are immutable). It binds the key
on every connector in the collection that accepts an `api_key` parameter.

**3. Per-package `load(*, api_key=...)` helper.** Each keyed provider package
exports a small `load` convenience that binds the key for you:

```python
from parsimony_fred import load

fred = load(api_key="your-key-here")
result = fred["fred_fetch"](series_id="UNRATE")
```

A missing key fails fast with `UnauthorizedError` naming the expected
environment variable, before any network call. See
[credentials](concepts/credentials.md) for the full treatment, including how
operators pre-bind keys for agent surfaces.

## Next steps

- [The connector contract](concepts/connectors.md) — what a connector is, why
  they are synchronous, and what a `Result` carries.
- [Discovery and catalogs](concepts/discovery-and-catalogs.md) — native search
  vs catalog-backed providers, and how snapshots are cached.
- [Credentials](concepts/credentials.md) — keyed vs keyless providers, env vars,
  and binding for operator use.
- [Errors](concepts/errors.md) — the typed exceptions a connector can raise.
- [Using connectors](guides/using-connectors.md) — practical patterns: selecting
  subsets, the collection API, reading a `Result`, and handling errors.
- [Provider reference](reference/providers.md) — every installed provider, its
  connectors, and credential requirements.
