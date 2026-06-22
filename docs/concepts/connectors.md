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
from parsimony.result import Column, ColumnRole, OutputConfig

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units_short", role=ColumnRole.METADATA),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
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

`output=` is optional. When present, declared columns are mapped by role and any
returned column the schema does not name folds in as a `DATA` column. When
absent, the framework still wraps the return value; the result simply has no
column governance.

### `@enumerator`

For **catalog discovery** — a function that lists the entities a provider offers
(series ids, dataset codes, table ids) so they can be indexed into a catalog.

```python
@enumerator(output=...)
def provider_enumerate(...) -> pd.DataFrame: ...
```

The output schema must be **exactly one namespaced `KEY` column plus one or more
`TITLE` columns, and no `DATA` columns**. The function must be annotated
`-> pd.DataFrame`, and the returned frame must match the declared columns
**exactly**: unmapped columns are dropped, then an exact-match check runs at call
time. A frame that is missing a declared column, or carries an undeclared one,
raises `ValueError`.

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

The schema is declarative. You describe the *roles* of columns and the framework
applies them — coercing dtypes, dropping hidden columns from the agent view, and
deriving entity identity for catalogs.

```python
from parsimony.result import OutputConfig, Column, ColumnRole

OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title",     role=ColumnRole.TITLE),
    Column(name="units",     role=ColumnRole.METADATA),
    Column(name="date",      role=ColumnRole.DATA, dtype="datetime"),
    Column(name="value",     role=ColumnRole.DATA, dtype="numeric"),
])
```

### Column roles

| Role | Meaning | Multiplicity |
|---|---|---|
| `KEY` | The entity identity — series id, dataset code, table id. | At most one per schema. |
| `TITLE` | The human-readable name of the entity. | At most one per schema. |
| `METADATA` | Descriptive attributes (units, frequency, dates). | Any number. |
| `DATA` | The observations themselves (a date, a value). | Any number. |

A schema must define **at least one of `KEY` / `TITLE` / `DATA`**. A bare
`@connector` only needs enough to describe its payload; the stricter decorators
add the constraints listed above.

### Column options

- `namespace=` is allowed **only** on a `KEY` or `METADATA` column, and is
  **mandatory** on a loader's or enumerator's `KEY` (the catalog and store derive
  entity identity from it). On a `KEY` it scopes the entity codes; on
  `METADATA` it is a lightweight annotation Parsimony does not enforce.
- `dtype=` coerces the column. Values: `auto` (default, no coercion),
  `datetime`, `timestamp`, `date`, `numeric`, `bool`, or any pandas dtype
  string. A `numeric`/`timestamp` column that coerces entirely to NaN/NaT raises
  rather than emitting a silently empty column.
- `Column(name="*")` is a wildcard catch-all that claims every otherwise
  unmapped column.
- `exclude_from_llm_view=True` keeps a column in the data payload but out of the
  agent-facing view. It is rejected on `DATA` and `TITLE` columns (those must
  always be visible).

## `Result` and provenance

The framework wraps every connector return value in a `parsimony.result.Result`.
Connectors never construct one. A `Result` carries:

- `data` — the raw payload (DataFrame, scalar, dict, …).
- `output_schema` — the resolved `OutputConfig`, when one was declared.
- `provenance` — a `Provenance` recording `source` (the connector name),
  `source_description`, `fetched_at` (UTC), and `params` (the call arguments,
  minus anything declared in `secrets=` and minus bound arguments). Oversized
  provenance fields are replaced with a structured marker, never a truncated
  prefix that could leak the head of an unredacted secret.

### Accessors

| Accessor | Returns |
|---|---|
| `result.df` / `result.frame` | The DataFrame; raises `TypeError` if the payload is not tabular. |
| `result.data` | The raw payload (used for dict/scalar results). |
| `result.is_tabular` | Whether `data` is a DataFrame. |
| `result.text` | The payload as a string. |
| `result.columns` | The declared `Column` list (empty when no schema). |
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
