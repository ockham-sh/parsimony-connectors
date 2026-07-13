# Authoring a connector

This is the deep, example-driven guide to **building** a parsimony connector. It
complements [CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md),
which covers the policy side (acceptance, stewardship, the PR checklist, the merge
gate). Read that for the *what*; read this for the *how*.

If you only want to *use* connectors, see
[../guides/using-connectors.md](../guides/using-connectors.md). The conceptual model
behind everything below lives in [../concepts/connectors.md](../concepts/connectors.md),
[../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md),
[../concepts/credentials.md](../concepts/credentials.md), and
[../concepts/errors.md](../concepts/errors.md). This guide stays practical and assumes
you have skimmed those.

> A deeper internal authoring manual exists under
> [`docs/_guidebook/`](https://github.com/ockham-sh/parsimony-connectors/tree/main/docs/_guidebook).
> It is useful for the long tail of enumeration tactics, but it **predates the move to
> synchronous connectors** and still shows `async def` examples in places. When the
> guidebook and this page disagree, this page wins: **connectors are plain `def`, never
> `async def`.**

---

## 1. The mental model: discovery vs fetch

Every provider exposes its data through two distinct jobs, and they are **always
different connectors**:

- **Discovery** answers *"what data exists?"* Given a query, it returns the addressable
  codes (series ids, dataset codes, table ids) plus enough metadata to dispatch a fetch.
  Discovery never returns observations.
- **Fetch** answers *"give me the values for this code."* Given a code, it returns the
  rows. Fetch never guesses a code.

The agent loop is: **search to find a code, then fetch that code.** Build both halves.

A connector is a small **synchronous** Python function plus a decorator. The function
calls an HTTP API, parses the body, and returns **raw data** — a DataFrame, Series,
scalar, or dict. The framework wraps that return value into a `Result` and attaches a
`Provenance`. You never construct a `Result` or a `Provenance`, and you never return a
`(data, provenance)` tuple.

The three decorators (`@connector`, `@enumerator`, `@loader`) all decorate a plain `def`.
They **reject `async def` with a `TypeError` at import time.** There is no `async`/`await`
anywhere in a connector.

---

## 2. Decide: native-search or build-a-catalog

The fetch side is the same everywhere. The discovery side has two shapes, and which one
you build determines the package layout.

**Native-search provider.** The provider ships a usable search or screener endpoint of
its own. Your search connector is just a thin `@connector` that calls it; the live
response is normalized into search rows. No catalog. Examples: `fred`, `finnhub`,
`coingecko`, `sec_edgar`.

**Catalog-backed provider.** The provider has no usable native search — its API only
fetches by exact code, or its search is too narrow to enumerate the universe. You build a
**catalog**: an `@enumerator` lists every addressable unit, the build pipeline indexes
them into a snapshot, and a generated search connector queries that local snapshot
offline. Examples: `treasury`, `boc`, `destatis`, `sdmx`.

Decision rule:

> Can a single provider endpoint take a free-text query and return the codes a user would
> search for, covering the whole universe? If yes, native-search. If the provider can only
> fetch by exact code, build a catalog.

See [../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md) for the
full taxonomy and the current per-provider split.

---

## 3. The three names rule

A connector package carries the same name in three forms, and they must stay in lockstep:

| Form | Example | Where |
|---|---|---|
| PyPI distribution (hyphen) | `parsimony-treasury` | `[project] name` in `pyproject.toml` |
| Python import (underscore) | `parsimony_treasury` | the package directory + every `import` |
| Repo directory | `packages/treasury/` | the monorepo layout |

The PR checklist enforces all three. Pick the provider's short name once and apply it
everywhere.

---

## 4. The package skeleton

The fastest way to start is to copy the nearest exemplar and rename. There are two
layouts.

### 4a. Native-search layout (copy `fred`)

A single-module package. Everything lives in `parsimony_<name>/__init__.py`.

```text
packages/foo/
├── pyproject.toml
├── README.md
├── LICENSE
├── CHANGELOG.md
├── parsimony_foo/
│   ├── __init__.py        # connectors + CONNECTORS + load
│   └── py.typed
└── tests/
    ├── test_conformance.py
    └── test_foo_connectors.py
```

Exemplar:
[`packages/fred/parsimony_fred/__init__.py`](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/fred/parsimony_fred/__init__.py).

### 4b. Catalog-backed layout (copy `treasury`)

A modular package, because the catalog build, the search verb, and the fetch verbs are
distinct concerns.

```text
packages/foo/
├── pyproject.toml
├── README.md
├── LICENSE
├── CHANGELOG.md
├── parsimony_foo/
│   ├── __init__.py        # thin facade: re-export CONNECTORS + load
│   ├── _http.py           # transport clients (base URLs, key resolution)
│   ├── outputs.py         # OutputSpec schemas for every verb
│   ├── parsing.py         # response parsing (optional)
│   ├── catalog_build.py   # build_foo_catalog() -> Catalog
│   ├── search.py          # make_local_search_connector(...)
│   ├── connectors/
│   │   ├── __init__.py     # defines CONNECTORS + load, imports the verbs
│   │   ├── fetch.py        # the @connector fetch verb(s)
│   │   └── enumerate.py    # the @enumerator
│   └── py.typed
├── scripts/
│   └── build_catalog.py   # operator CLI: --save / --push
├── catalog_tests/
│   └── queries.yaml       # curated recall probes
└── tests/
    ├── test_conformance.py
    └── test_foo_connectors.py
```

Exemplar: the whole
[`packages/treasury/`](https://github.com/ockham-sh/parsimony-connectors/tree/main/packages/treasury)
tree.

### The `pyproject.toml`

Mirror an exemplar. The load-bearing lines:

```toml
[project]
name = "parsimony-foo"
requires-python = ">=3.11"
dependencies = [
    "parsimony-core>=0.7,<0.8",          # catalog-backed: parsimony-core[catalog]>=0.7,<0.8
    "pydantic>=2.11.1,<3",
    "pandas>=2.3.0,<3",
]

[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"

[tool.hatch.build.targets.wheel]
packages = ["parsimony_foo"]
```

The `parsimony-core` pin is `>=0.7,<0.8` — a compatibility range, not a floor; a
catalog-backed package depends on the `[catalog]` extra. The entry-point value is a
**bare module path** — see [§9](#9-the-entry-point).

---

## 5. Writing a fetch `@connector`

A fetch connector is a synchronous `def` with **flat top-level parameters**, an output
schema, and a body that returns **raw data**. Here is `fred_fetch`, trimmed:

```python
from typing import Annotated

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Column, ColumnRole, OutputSpec
from parsimony.transport.helpers import fetch_json, make_http_client, require_key

FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units_short", role=ColumnRole.METADATA),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


@connector(output=FETCH_OUTPUT, tags=["macro"], secrets=("api_key",))
def fred_fetch(
    series_id: Annotated[str, Namespace("fred")],
    observation_start: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch FRED time series observations by series_id.

    Returns date + value rows enriched with series metadata (title, units,
    frequency). Optional observation_start bounds the window (YYYY-MM-DD).
    """
    sid = series_id.strip()
    if not sid:
        raise InvalidParameterError("fred", "series_id must be non-empty")

    http = make_http_client(_BASE_URL, provider="fred", query_params={"api_key": require_key(api_key, env_var="FRED_API_KEY", provider="fred")})
    body = fetch_json(http, path="series/observations", params={"series_id": sid}, op_name="series/observations")

    observations = body.get("observations")
    if observations is None:
        raise ParseError("fred", "FRED response missing 'observations'")
    if not observations:
        raise EmptyDataError("fred", query_params={"series_id": sid})

    df = pd.DataFrame(observations)
    df["series_id"] = sid
    # ... enrich with metadata ...
    # OutputSpec never coerces dtypes — do it here, explicitly.
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[[c.name for c in FETCH_OUTPUT.columns]]
```

The rules this demonstrates:

- **Sync `def`, flat params.** Parameters are top-level scalars (`str`, `int`,
  `str | None`, …). There is no public `params: BaseModel` surface — the framework builds
  the call envelope from the bare signature. The conformance gate rejects a bundled
  Pydantic params model on the public surface.
- **`Annotated[str, Namespace("fred")]`** on the identity parameter scopes the code to the
  provider's namespace. Use it on the parameter that takes a code (`series_id`,
  `endpoint`).
- **Declare an `output=` schema** with `Column`/`ColumnRole` (`KEY`/`TITLE`/`METADATA`/
  `DATA`; at most one `KEY`, at most one `TITLE`; `namespace=` only on `KEY`). It is a
  **passive declaration** — the framework attaches it to the result unchanged and never
  inspects, coerces, or filters the DataFrame you return. On a plain `@connector`, `output=`
  is optional; any column you return that the schema doesn't name simply exists on
  `result.data`, undeclared (useful when a provider's data columns vary by endpoint —
  `treasury_fetch` relies on this). When you want only the declared columns on
  `result.data`, slice the frame yourself to `[c.name for c in OUTPUT.columns]` before
  returning, as `fred_fetch` does, so stray provider columns don't ride along.
- **Cast dtypes yourself.** `Column` has no `dtype=` field; `fred_fetch` parses `date` with
  `pd.to_datetime` and `value` with `pd.to_numeric` before returning, rather than declaring
  it on the schema.
- **Return raw data.** A DataFrame here; a Series, scalar, or dict elsewhere. Never a
  `Result`, never a tuple.
- **The docstring is the description.** 20–800 characters. Lead with a clear first
  sentence stating what the call returns — an agent reads it to decide whether to call.
- **Validate inputs and fail with typed errors** (see [§8](#8-typed-errors)).

For the full `Column`/`ColumnRole` reference, see
[../concepts/connectors.md](../concepts/connectors.md).

---

## 6. Writing an `@enumerator` and the catalog pipeline

For catalog-backed providers, the `@enumerator` is the discovery seam: it lists **one row
per addressable unit** so the build pipeline can index them.

### The enumerator

```python
import pandas as pd
from parsimony.connector import enumerator
from parsimony.result import Column, ColumnRole, OutputSpec

TREASURY_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="endpoint", role=ColumnRole.METADATA),
        # ... more METADATA columns ...
    ]
)
_ENUMERATE_COLUMNS = tuple(c.name for c in TREASURY_ENUMERATE_OUTPUT.columns)


@enumerator(output=TREASURY_ENUMERATE_OUTPUT, tags=["macro", "us"])
def enumerate_treasury() -> pd.DataFrame:
    """Enumerate Treasury Fiscal Data measures and ODM rate-feed benchmarks.

    One row per addressable measure across every dataset's endpoints, plus the
    static rate-feed series, for catalog indexing.
    """
    rows = _build_rows()  # your fan-out / live-index logic
    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
```

Enumerator-specific constraints (the conformance gate checks them):

- **Schema shape:** exactly **one namespaced `KEY`** plus **one or more `TITLE`**
  columns, and **no `DATA`** columns. `namespace=` on the `KEY` is **mandatory** — the
  catalog derives entity identity from it. This shape is checked at decoration time.
- **Annotate `-> pd.DataFrame`.** It is required, not cosmetic; also checked at decoration
  time.
- **Return the exact declared columns anyway, even though nothing enforces it at call
  time.** `OutputSpec` is passive — the decorator does not inspect your returned frame's
  columns. But `catalog_build.py` calls `result.to_entities()` right after, and *that*
  raises `ValueError` if a declared column is missing from the data — so build the frame
  with exactly the declared columns in order (the `pd.DataFrame(rows, columns=columns)`
  idiom above) to catch drift early and keep the catalog free of stray columns.
- Recall is driven by **`title`/`description` text content**, not by how many `METADATA`
  columns you attach. Name the descriptive field `description` so the index picks it up
  (a column named `definition` is never searched). Metadata columns are the search hit's
  **dispatch payload**, not a recall lever.

### `catalog_build.py`

The build function turns the enumerator output into a `Catalog`. The pipeline has four
steps:

```python
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_treasury.connectors.enumerate import enumerate_treasury

CATALOG_NAMESPACE = "treasury"


def build_treasury_catalog() -> Catalog:
    result = enumerate_treasury()
    entries = result.to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog
```

- **`result.to_entities()`** converts the enumerator's `Result` into catalog entities,
  reading roles off `result.output_spec` (the same `OutputSpec` the `@enumerator` decorator
  was given). (This is the current API. Older notes may say `entities_from_raw`,
  `entries_from_result`, or `entities_from_result` — all are stale; see
  [../concepts/connectors.md](../concepts/connectors.md).)
- **`discovery_indexes(entries)`** builds the index policy: a `code` index (BM25, exact
  lookup), and `title` + `description` indexes that are adaptive — **Hybrid BM25 + vector**
  when a field has fewer than 1000 unique values, **BM25-only** at or above 1000.
- **`Catalog(namespace, indexes=..., default_field="title")`** wraps the entities. A bare
  query string searches `default_field`.
- **`catalog.build()`** constructs the indexes; the operator script calls **`catalog.save(url)`**
  to persist.

### The search verb (`search.py`)

The search connector is created **declaratively** with `make_local_search_connector` — you
do not write a function body:

```python
from parsimony.catalog.search import make_local_search_connector

from parsimony_treasury.catalog_build import build_treasury_catalog
from parsimony_treasury.outputs import TREASURY_SEARCH_OUTPUT

treasury_search = make_local_search_connector(
    provider="treasury",
    default_url="hf://parsimony-dev/treasury",
    catalog_url_env_var="PARSIMONY_TREASURY_CATALOG_URL",
    build_catalog=build_treasury_catalog,
    tags=["macro", "us", "tool"],
    description=(
        "Semantic-search the US Treasury catalog (Fiscal Data + ODM rate feeds). "
        "Dispatch: source=treasury_rates → treasury_rates_fetch(feed=endpoint); "
        "source=fiscal_data → treasury_fetch(endpoint=endpoint)."
    ),
    output_columns=TREASURY_SEARCH_OUTPUT.columns,
    metadata_columns=("source", "endpoint", "field"),
)
```

Key arguments:

- **`provider`** names the connector (`<provider>_search`) and the cache namespace.
- **`default_url`** is the hosted snapshot, conventionally `hf://parsimony-dev/<provider>`.
- **`catalog_url_env_var`** is the override env var, conventionally
  `PARSIMONY_<PROVIDER>_CATALOG_URL`.
- **`build_catalog`** is used for a cold rebuild when no snapshot is found, so search works
  unchanged in a fresh clone.
- **`tags`** are free-form labels for organizing and filtering connectors.
- **`metadata_columns`** are the **dispatch payload** echoed onto each hit so the agent
  knows which fetch verb to call and with what arguments — without parsing the code string.
  These are *not* indexed and do not affect recall.

### The operator CLI (`scripts/build_catalog.py`)

A thin `argparse` driver that builds and persists the snapshot. Maintainers run it; it is
**not** part of the plugin contract or imported at runtime.

```python
import argparse
from parsimony_treasury.catalog_build import build_treasury_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", help="Local directory to write a snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/treasury.")
    args = parser.parse_args()
    catalog = build_treasury_catalog()
    if args.save:
        catalog.save(args.save, builder="packages/treasury/scripts/build_catalog.py")
    if args.push:
        catalog.save(args.push, builder="packages/treasury/scripts/build_catalog.py")
```

The full operator publish workflow (including `--api-key` for the Hub upload) is in
[../guides/building-catalogs.md](../guides/building-catalogs.md).

### Wiring the bundle (`connectors/__init__.py`)

The catalog-backed `connectors/__init__.py` imports the verbs and assembles `CONNECTORS`:

```python
from parsimony.connector import Connectors

from parsimony_treasury.connectors.enumerate import enumerate_treasury
from parsimony_treasury.connectors.fetch import treasury_fetch, treasury_rates_fetch
from parsimony_treasury.search import treasury_search

CONNECTORS = Connectors([treasury_fetch, treasury_rates_fetch, enumerate_treasury, treasury_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)
```

The top-level `parsimony_foo/__init__.py` is then a thin facade that re-exports
`CONNECTORS` and `load` (`__all__ = ["CONNECTORS", "load"]`).

---

## 7. The six enumeration archetypes

When you build a catalog, the hard part is listing the provider's **full** universe. There
are six recurring patterns, in rough order of preference. Pick the highest one the provider
supports.

| # | Archetype | What it is | Exemplars |
|---|---|---|---|
| A | Live full-index endpoint | One call returns the whole catalog; self-tracking. The best option. | `boc`, `destatis`, `snb`, `treasury` |
| B | Crawl-the-hierarchy fan-out | Walk every level, paginating each. | `bdp`, `boj` |
| C | Curated/hardcoded registry | A frozen list. A liability — commit a re-harvest script and floor/shape tests. | `bde`, `boj`, `treasury`, `riksbank` |
| D | Hybrid live + static merge | A live index plus a curated supplement. | `treasury` (live Fiscal Data + curated rate feeds) |
| E | Scrape publication indexes | Parse HTML/CSV publication listings. | `rba` |
| F | Brute-force Cartesian | Enumerate the product of facet values. A lower bound; last resort. | `sdmx` (World Bank) |

Prefer A; it self-tracks new entities with zero maintenance. Drop to a curated registry (C)
only when nothing live enumerates the universe, and when you do, **commit the harvester
script** that regenerates it plus tests that assert a floor count and the row shape — a
frozen registry silently drifts otherwise.

Shared fan-out helpers live in **`parsimony-shared`** (`ThrottledJsonFetcher`,
`MetadataCrawlConfig`, `truncate_description`, …) for the crawl-heavy archetypes.

---

## 8. Credentials

A keyless connector has **none** of the machinery below — no `secrets=`, no `.bind`, no
fast-fail. Call it directly. For keyed connectors, two **independent** mechanisms protect
the key, and a keyed connector uses both.

- **`secrets=("api_key",)`** on the decorator strips that parameter from
  `provenance.params`, so the key never lands in a stored receipt. It governs **provenance
  only**, not logs.
- **`Connector.bind(api_key=...)`** fixes the value and removes the parameter from the
  agent-facing signature, so an operator wires a key in once and the agent never sees it.

The standard per-package operator helper binds across the whole collection:

```python
def load(*, api_key: str) -> Connectors:
    return CONNECTORS.bind(api_key=api_key)
```

Inside the connector, resolve the key from the bound value with an env fallback and
fast-fail before any network call:

```python
from parsimony.transport.helpers import require_key

key = require_key(api_key, env_var="FOO_API_KEY", provider="foo")  # raises UnauthorizedError if absent
```

Carry the key the way the provider expects, **preferring a header or POST body over a query
string**. The HTTP client redacts query-param values only when the param *name* is in a
sensitive set (`api_key`, `token`, `*_token`, `secret`, …); a key sent under a name outside
that set leaks into logs. When a query param is unavoidable, name it so it falls inside the
redaction set. See [../concepts/credentials.md](../concepts/credentials.md) for the full
auth-shape taxonomy and the redaction set.

---

## 9. Typed errors

Map every upstream failure onto a typed error from `parsimony.errors`. The `provider`
string is the **first positional argument** on every subclass.

| Raise | When |
|---|---|
| `UnauthorizedError(provider, env_var=...)` | Missing/invalid key (fast-fail before the call). |
| `PaymentRequiredError(provider)` | The plan does not cover this call. |
| `RateLimitError(provider, retry_after, *, quota_exhausted=False)` | 429; set `quota_exhausted` when the quota is spent, not merely throttled. |
| `ProviderError(provider, status_code)` | A generic upstream HTTP failure. |
| `EmptyDataError(provider, query_params=...)` | A valid call that returned no rows. |
| `ParseError(provider, msg=None)` | The response shape was not what you expected. |
| `InvalidParameterError(provider, message)` | A caller-supplied argument is invalid (validate before the call). |
| `CatalogNotFoundError(msg)` | A catalog snapshot could not be resolved. |

A key-bearing connector must never let the key appear in `str(exception)`. See
[../concepts/errors.md](../concepts/errors.md) for the full hierarchy and HTTP-status
mapping.

---

## 10. The entry point

The **only** registration step is one stanza in `pyproject.toml`:

```toml
[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

The value is a **bare module path** — `parsimony_foo`, **not** `parsimony_foo:CONNECTORS`.
The kernel imports the module and reads its top-level `CONNECTORS = Connectors([...])`.
There is no central registry file to edit and no other place to register a provider.

---

## 11. Testing

Two gates must pass, plus the offline behavioural tests.

### Conformance (the merge gate)

`parsimony.testing.assert_plugin_valid(module)` runs the six checks every official plugin
must pass:

1. `CONNECTORS` is exported and non-empty.
2. Every description is 20–800 characters.
3. An enumerator is built with `@enumerator` (not `@connector(tags=["enumerator"])`).
4. An enumerator declares `output=` and annotates `-> pd.DataFrame`.
5. The public surface uses flat params (no bundled `params: BaseModel`).
6. Every credential-shaped parameter is declared in `secrets=`.

Wire it in one file:

```python
from parsimony.testing import assert_plugin_valid

import parsimony_foo


def test_conforms_to_parsimony_plugin_contract() -> None:
    assert_plugin_valid(parsimony_foo)
```

`parsimony list --strict` runs the same checks across every installed provider.

### Offline behavioural tests (`tests/test_foo_connectors.py`)

For every connector in `CONNECTORS`, write a happy-path test that mocks upstream HTTP with
`respx`, binds deps via `connector.bind(...)`, and asserts on the **public `Result`
surface**: `isinstance(result, Result)`, the expected columns, and `result.provenance.source`.
For an `@enumerator`, assert a non-empty `pd.DataFrame` with the expected columns and
schema roles.

Key-bearing connectors also need a **401 → `UnauthorizedError`** test and a
**429 → `RateLimitError`** test, and must assert the api-key value does **not** appear in
`str(raised_exception)`.

Do not assert on httpx internals, full DataFrame equality, or timing. No real network I/O
in happy-path tests. All respx mocks are **hand-authored** from upstream API docs — no
recorded cassettes (the `.gitignore` enforces this). Mark live-API tests
`@pytest.mark.integration`.

### Catalog recall probes (`catalog_tests/queries.yaml`)

A catalog-backed package ships a curated set of search probes that pin recall. Each probe
declares a query, an `expected_code`, a `mode`, and whether it is `required` (exact `code:`
lookups) or `optional` (fuzzy title probes that may reorder on a BM25-only index above 1000
titles). A `thresholds.min_required_recall` (typically `1.0`) is the bar. See
[`packages/treasury/catalog_tests/queries.yaml`](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/treasury/catalog_tests/queries.yaml).

---

## 12. The local gate

Before opening a PR, run the one command that mirrors CI for your package:

```bash
make verify PKG=foo
```

It runs `ruff` + `mypy` + `pytest` + the strict plugin listing. Run `make verify-all`
before a release. All of it must be green; the conformance suite — not founder judgement —
is the merge gate.

---

## See also

- [CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md)
  — acceptance criteria, the PR checklist, and stewardship policy.
- [../concepts/connectors.md](../concepts/connectors.md) — the connector and `Result`
  model.
- [../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md) — discovery
  shapes, the catalog index policy, and recall.
- [../concepts/credentials.md](../concepts/credentials.md) — auth shapes and key handling.
- [../concepts/errors.md](../concepts/errors.md) — the typed-error model.
- [../guides/building-catalogs.md](../guides/building-catalogs.md) — the operator build and
  publish workflow.
- [../reference/providers.md](../reference/providers.md) — per-provider discovery method,
  auth, and code shapes.
- [../reference/cli.md](../reference/cli.md) — the `parsimony` CLI surface.
