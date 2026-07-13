# The connector contract

A connector is a small **synchronous** Python function plus metadata. The
function does the work — call an HTTP API, parse the body, return raw data — and
a decorator from `parsimony` attaches the agent-facing contract: a description,
an output schema, tags, and declared secrets. The framework wraps every return
value into a [`Result`](#result-and-provenance) with provenance attached.

Connectors are plain `def` functions. The three decorators reject `async def`
with a `TypeError` at import time: there is no `async`/`await` anywhere in a
connector.

## Anatomy

```python
from typing import Annotated

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Column, ColumnRole, OutputSpec

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

    Returns date + value rows enriched with series metadata (title, units).
    Optional observation_start bounds the window (YYYY-MM-DD).
    """
    sid = series_id.strip()
    if not sid:
        raise InvalidParameterError("fred", "series_id must be non-empty")
    # ... call the API, build a DataFrame ...
    df["date"] = pd.to_datetime(df["date"])          # OutputSpec never coerces —
    df["value"] = pd.to_numeric(df["value"])          # cast in the connector body.
    return df  # raw DataFrame, never a Result
```

Three facts make this a connector:

- **It is a sync `def`** whose parameters are flat, top-level scalars. There is
  no public `params: BaseModel` surface — the framework builds the envelope from
  the bare function signature.
- **It returns raw data** — a DataFrame, Series, scalar, or dict — never a
  `Result`, never a `(data, provenance)` tuple. The framework constructs the
  `Result`.
- **It carries a description** (the docstring here, or `description=` on the
  decorator). The description is the agent-facing capability statement.

## The three decorators

All three live in `parsimony` and decorate a synchronous `def`. Pick by what the
function produces.

### `@connector`

The general-purpose decorator. Use it for almost everything: time-series
fetches, native-search wrappers, dict/scalar lookups, anything that does not fit
the stricter loader or enumerator shapes.

```python
@connector(output=..., tags=[...], secrets=(...))
def provider_fetch(...) -> pd.DataFrame: ...
```

`output=` is optional. When present, it is attached to the result as
`result.output_spec` **unchanged** — the framework never inspects, reorders, or
filters your returned DataFrame against it. Any column you return that the
schema doesn't name simply exists on `result.data`, undeclared. When `output=`
is absent, the framework still wraps the return value; the result simply has
no column governance.

### `@enumerator`

For **catalog discovery** — a function that lists the entities a provider offers
(series ids, dataset codes, table ids) so they can be indexed into a catalog.

```python
@enumerator(output=...)
def provider_enumerate(...) -> pd.DataFrame: ...
```

The output schema must be **exactly one namespaced `KEY` column plus one or more
`TITLE` columns, and no `DATA` columns** — checked at decoration time. The
function must also be annotated `-> pd.DataFrame`. The decorator does **not**
check the returned frame's actual columns against the schema at call time —
`OutputSpec` is passive everywhere. That check happens later, when something
calls `result.to_entities()` (or reads `result.entities`) on the enumerator's
output: a declared column missing from the data raises `ValueError` there.

### `@loader`

For functions that feed a writable data store. A loader's output must be
**exactly one namespaced `KEY` column plus one or more `DATA` columns, and no
`TITLE` or `METADATA` columns**.

```python
@loader(output=...)
def provider_load(...) -> pd.DataFrame: ...
```

In practice almost every "fetch" is a plain `@connector`, not a `@loader`,
because a real fetch carries a human-readable `TITLE` column (the series name,
the dataset label), which a loader schema forbids. Reach for `@loader` only when
the output is pure keyed data with no titling.

See [discovery and catalogs](discovery-and-catalogs.md) for how enumerators feed
catalogs, and [building catalogs](../guides/building-catalogs.md) for the
end-to-end build.

## The output schema

The schema is declarative and **passive**: you describe the *roles* of columns,
and that's the whole job. The framework never applies it to the data — no dtype
coercion, no renaming, no reordering, no dropping. It only labels columns so a
later consumer (the LLM card, a catalog's entity projection, a data store) knows
what each one means.

```python
from parsimony.result import OutputSpec, Column, ColumnRole

OutputSpec(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title",     role=ColumnRole.TITLE),
    Column(name="units",     role=ColumnRole.METADATA),
    Column(name="date",      role=ColumnRole.DATA),
    Column(name="value",     role=ColumnRole.DATA),
])
```

### Column roles

| Role | Meaning | Multiplicity |
|---|---|---|
| `KEY` | The entity identity — series id, dataset code, table id. | At most one per schema. |
| `TITLE` | The human-readable name of the entity. | At most one per schema. |
| `METADATA` | Descriptive attributes (units, frequency, dates). | Any number. |
| `DATA` | The observations themselves (a date, a value). | Any number. |

There is no minimum-column requirement at declaration time — an `OutputSpec`
with zero columns is valid. The stricter decorators (`@loader`, `@enumerator`)
layer their own multiplicity requirements on top; see [The three decorators](#the-three-decorators).

### Column options

- `namespace=` is allowed **only** on a `KEY` column, and only needs to be
  **non-empty when set**. It may be omitted at declaration time (e.g. a
  per-call dynamic namespace); it becomes mandatory only when something
  actually projects entities from the result (`result.entities` /
  `result.to_entities()`) — or, eagerly, at decoration time for `@loader` and
  `@enumerator`, since their whole point is to feed a store/catalog.
- There is no `dtype=` — `Column` has no dtype field. Cast values yourself, in
  the connector body, before you return (`pd.to_datetime`, `pd.to_numeric`,
  `.astype(...)`).
- `Column(name="*")` is a wildcard, meaningful only at entity-projection time:
  it claims every DataFrame column not otherwise taken by an explicit KEY/
  TITLE/DATA/METADATA entry.
- `exclude_from_llm_view=True` keeps a column in the data payload but out of the
  agent-facing view. It is rejected on `DATA` and `TITLE` columns (those must
  always be visible).

## `Result` and provenance

The framework wraps every connector return value in a `parsimony.result.Result`.
Connectors never construct one. A `Result` carries:

- `data` — the raw payload (DataFrame, scalar, dict, …), exactly as returned —
  never copied, coerced, renamed, or reordered.
- `output_spec` — the `OutputSpec`, when one was declared, attached unchanged.
- `provenance` — a `Provenance` recording `source` (the connector name),
  `source_description`, `fetched_at` (UTC), and `params` (the call arguments,
  minus anything declared in `secrets=` and minus bound arguments). Oversized
  provenance fields are replaced with a structured marker, never a truncated
  prefix that could leak the head of an unredacted secret.

### Accessors

| Accessor | Returns |
|---|---|
| `result.frame` | The DataFrame; raises `TypeError` if the payload is not tabular. |
| `result.data` | The raw payload (used for dict/scalar results). |
| `result.is_tabular` | Whether `data` is a DataFrame. |
| `result.text` | The payload as a string. |
| `result.columns` | The declared `Column` list (empty when no schema). |
| `result.entities` / `result.to_entities()` | Lazy `(namespace, code)`-keyed projection / `list[Entity]`, built from `output_spec` against `data` — see [discovery and catalogs](discovery-and-catalogs.md). |
| `result.to_llm(max_rows=..., max_chars=...)` | A bounded, governed string view for agent context — honest row/column counts, hidden columns dropped, first N rows. |

## Descriptions and tags

A **description is required**: 20–800 characters after stripping. It comes from
the docstring or `description=`. It is the agent-facing capability statement — an
agent decides whether to call the connector based on it. Lead with a clear first
sentence that states what the call returns.

`tags=` are free-form labels (`"macro"`, `"us"`, ...) used to organize connectors
and to filter a collection (`connectors.filter(tags=["macro"])`). They carry no
special meaning to the framework.

The `loader` and `enumerator` decorators inject a `"loader"` / `"enumerator"`
tag automatically; do not add those by hand.

## Conformance: the merge gate

`parsimony.testing.assert_plugin_valid(module)` runs the six checks every
official plugin must pass. `parsimony list --strict` runs them across all
installed plugins, and they are the gate for merging a connector.

1. **CONNECTORS exported** — the module exposes `CONNECTORS: Connectors`, a
   non-empty collection.
2. **Descriptions in bounds** — every connector's description is 20–800 chars.
3. **Enumerator uses the real decorator** — an `enumerator`-tagged connector
   must be built with `@enumerator`, not `@connector(..., tags=["enumerator"])`.
4. **Enumerator return type** — enumerators declare `output=` and annotate a
   `pd.DataFrame` return.
5. **Flat public params** — no bundled `params: BaseModel` on the public
   surface.
6. **Secrets declared** — any credential-shaped parameter (`api_key`, `token`,
   `*_key`, …) must appear in `secrets=`.

Wire these into a plugin's tests by subclassing `ProviderTestSuite` and setting
`module` or `module_path`; pytest then runs `assert_plugin_valid` for you.

See [errors](errors.md) for the typed-error model a connector raises, and
[using connectors](../guides/using-connectors.md) for the consumer side.
