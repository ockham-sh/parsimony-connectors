# Using connectors

This guide collects the practical patterns for working with connectors in
application or notebook code: choosing which providers to load, navigating the
`Connectors` collection, running the search-then-fetch loop, reading a `Result`,
binding credentials, pointing at a custom catalog, and handling typed errors.

For the underlying model see [the connector contract](../concepts/connectors.md)
and [discovery and catalogs](../concepts/discovery-and-catalogs.md). For the list
of installed providers and their connectors, see
[the provider reference](../reference/providers.md).

## Selecting and loading providers

Loading is done through the kernel's `discover` module. You rarely import a
provider package directly.

Load everything installed (forgiving — skips and logs any provider that fails
to import):

```python
from parsimony import discover

connectors = discover.load_all()
```

Load a named subset (strict — raises `LookupError` if a name is not installed):

```python
from parsimony import discover

connectors = discover.load("fred", "treasury", "riksbank")
```

Use the strict form when you depend on specific providers and want a clear
failure if one is missing from the environment. Use `load_all()` when you want
whatever the deployment happens to have installed.

You can also enumerate what is installed without importing any provider module:

```python
from parsimony import discover

for provider in discover.iter_providers():
    print(provider.name, provider.version, provider.homepage)
```

## The `Connectors` collection API

`discover.load` and `discover.load_all` both return an immutable `Connectors`
collection. Every mutating-looking operation returns a new collection.

**Index by name.** Indexing is name-keyed, not positional:

```python
fred_fetch = connectors["fred_fetch"]    # a Connector
connectors[0]                            # raises — not positional
```

`connectors.get("fred_fetch")` returns the connector or `None` instead of
raising. `"fred_fetch" in connectors` tests membership.

**List names, length, iterate:**

```python
connectors.names()        # sorted list of connector names
len(connectors)           # number of connectors
for c in connectors:      # iterate over Connector objects
    print(c.name)
```

**Merge two collections** with `+`:

```python
from parsimony import discover

macro = discover.load("fred")
markets = discover.load("treasury")
both = macro + markets
```

**Bind parameter values** across the collection (see
[Binding credentials](#binding-credentials-for-operator-use) below):

```python
fred = discover.load("fred").bind(api_key="...")
```

**Filter by tag.** Connectors carry free-form tags (such as domain labels like
`macro`):

```python
macro = connectors.filter(tags=["macro"])
```

## Search then fetch

Both discovery models follow the same two-step shape — find an identifier, then
fetch by it — but they differ in where the search runs.

### Catalog-backed providers

Catalog-backed providers (such as `treasury`, `riksbank`, `sdmx`, `bls`) ship a
prebuilt searchable catalog. The search connector is named `<provider>_search`
and takes `query`, an optional `limit`, and an optional `catalog_url`. It
returns a `code` (KEY) column you pass to the fetch connector.

```python
from parsimony import discover

connectors = discover.load("riksbank")

hits = connectors["riksbank_search"](query="EUR SEK exchange rate", limit=5)
print(hits.df[["code", "title", "score"]])

# Take a code from the search results and fetch it.
code = hits.df.iloc[0]["code"]
result = connectors["riksbank_fetch"](series_id=code)
print(result.df.head())
```

The catalog snapshot loads lazily on first search and is cached under
`~/.cache/parsimony/catalogs/`. Subsequent searches reuse the cache.

### Native-search providers

Native-search providers (such as `fred`, `finnhub`, `fmp`, `sec_edgar`) wrap the
provider's own search endpoint instead of shipping a catalog. There is no
snapshot to download; each search is a live call. The search connector is still
named `<provider>_search`, but its parameter is provider-specific (for FRED it
is `search_text`), and the identifier column it returns is provider-specific
too (for FRED it is `id`).

```python
from parsimony import discover

connectors = discover.load("fred")

hits = connectors["fred_search"](search_text="US unemployment rate")
series_id = hits.df.iloc[0]["id"]
result = connectors["fred_fetch"](series_id=series_id)
```

Consult [the provider reference](../reference/providers.md) for each provider's
search parameter and identifier column.

## Reading a `Result`

Calling any connector returns a `Result`. How you read it depends on the
payload.

**Tabular payloads** (most connectors) expose a pandas DataFrame:

```python
result = connectors["fred_fetch"](series_id="UNRATE")

result.df          # the DataFrame
result.frame       # the same DataFrame (alias)
result.data        # the raw payload (here, the same DataFrame)
result.is_tabular  # True
```

**Dict or scalar payloads** are read through `result.data`:

```python
# A connector that returns a dict or a scalar:
payload = result.data
```

**Provenance** is attached to every result and is framework-built:

```python
result.provenance.source        # connector name, e.g. 'fred_fetch'
result.provenance.fetched_at    # datetime in UTC
result.provenance.params        # the call-time parameters (secrets stripped)
```

**A bounded view for LLM context.** `to_llm()` renders a governed,
size-bounded string view — honest row/column counts, a per-column schema, and
the first rows of a table (or a structural preview for opaque payloads). This is
what an agent sees, and it is useful for logging too:

```python
print(result.to_llm())
# Result (table): 900 rows × 7 columns
# Columns:
# - series_id: object (KEY ns:fred)
# - title: object (TITLE)
# ...
# Rows (showing 10 of 900):
# series_id,title,...
```

## Binding credentials for operator use

Keyed providers accept an `api_key=""` parameter and fall back to a
`<PROVIDER>_API_KEY` environment variable. For local work the environment
variable is enough. When an operator wants the key fixed and kept off the
connector's call surface — for example before handing connectors to an agent —
bind it:

```python
from parsimony import discover

connectors = discover.load("fred").bind(api_key="your-key-here")

# api_key is bound; it is no longer an exposed parameter and is never
# recorded in provenance.
result = connectors["fred_fetch"](series_id="UNRATE")
```

`bind` returns a new collection and only touches connectors that actually accept
the named parameter, so binding `api_key` across a mixed collection leaves
keyless connectors untouched. Each keyed provider package also exports a
`load(*, api_key=...)` helper that does the bind for you:

```python
from parsimony_fred import load

fred = load(api_key="your-key-here")
```

See [credentials](../concepts/credentials.md) for the full model, including why
binding keeps the key away from an agent surface.

## Pointing at a custom catalog

Each catalog-backed provider reads its catalog snapshot from a default Hugging
Face URL, but you can override it with the environment variable
`PARSIMONY_<PROVIDER>_CATALOG_URL`. This is useful for pinning to a mirrored or
self-hosted snapshot, or for testing against a locally built catalog.

```bash
export PARSIMONY_TREASURY_CATALOG_URL="hf://your-org/treasury-mirror"
```

You can also override per call with the search connector's `catalog_url`
parameter, which takes precedence over the environment variable:

```python
result = connectors["treasury_search"](
    query="federal debt outstanding",
    catalog_url="hf://your-org/treasury-mirror",
)
```

A keyless catalog-backed package's `load` helper accepts the same override:

```python
from parsimony_treasury import load

treasury = load(catalog_url="hf://your-org/treasury-mirror")
```

The resolution order is: explicit `catalog_url` argument, then the
`PARSIMONY_<PROVIDER>_CATALOG_URL` environment variable, then the published
default.

## Handling typed errors

Connectors raise typed exceptions from `parsimony.errors` rather than returning
error sentinels. Every exception carries a `provider` attribute, and the message
is an agent-facing string. Catch the base class `ConnectorError` to handle any
connector failure, or catch specific subclasses for tailored handling.

```python
from parsimony import discover
from parsimony.errors import (
    UnauthorizedError,
    RateLimitError,
    EmptyDataError,
    ConnectorError,
)

connectors = discover.load("fred")

try:
    result = connectors["fred_fetch"](series_id="UNRATE")
except UnauthorizedError as exc:
    # Missing or invalid key — exc names the expected env var.
    print(f"auth failed for {exc.provider}: {exc}")
except RateLimitError:
    # Back off and retry later.
    print("rate limited; retry after a delay")
except EmptyDataError:
    # The parameters were valid but returned no rows.
    print("no data for those parameters")
except ConnectorError as exc:
    # Any other connector failure (ProviderError, ParseError,
    # InvalidParameterError, CatalogNotFoundError, ...).
    print(f"{exc.provider} error: {exc}")
```

The full set is `UnauthorizedError`, `PaymentRequiredError`, `RateLimitError`,
`ProviderError`, `EmptyDataError`, `ParseError`, `InvalidParameterError`, and
`CatalogNotFoundError`. See [errors](../concepts/errors.md) for what each one means
and when a connector raises it.

## See also

- [The connector contract](../concepts/connectors.md)
- [Discovery and catalogs](../concepts/discovery-and-catalogs.md)
- [Credentials](../concepts/credentials.md)
- [Errors](../concepts/errors.md)
- [Provider reference](../reference/providers.md)
