# Credentials

Some providers need an API key; many do not. This page covers the two questions that fix a
connector's credential shape, the auth shapes across the 22 providers, the two independent
mechanisms for keeping a key out of logs and out of an agent's view, and how you actually
supply a key as a user.

## Two questions

A connector's credential shape is fixed by two independent decorator declarations, each
answering a different question:

- **`secrets=("api_key",)` — must this value be *hidden*?** It names *parameters* whose
  call-time values are stripped from `provenance.params`. It says nothing about whether the
  connector works without the value.
- **`requires=("FRED_API_KEY",)` — must this value *exist*?** It names *environment variables*
  that must resolve for a call to succeed. A name in `requires=` is exactly the env var that
  `UnauthorizedError` names when the connector is called with nothing configured. It is a static
  declaration — the kernel never reads `os.environ` from it; the connector body does that (via
  `require_key`) and fast-fails.

The two axes are orthogonal, and the four **auth shapes** below are their four combinations. A
package's `keyless` flag in the manifest is computed as `not requires` — a connector is keyless
exactly when it declares no required env var. `requires=` also drives what `parsimony list
--strict` reports and the `(needs FRED_API_KEY)` suffix on the LLM card.

## Auth shapes

Every connector falls into one of four shapes.

### Required key

Declares both `secrets=("api_key",)` and `requires=("<PROVIDER>_API_KEY",)`. The connector
**fast-fails before any network call** if no key is present, raising
`UnauthorizedError(provider, env_var="<PROVIDER>_API_KEY")` so the message names the env var
to set — the same name it declares in `requires=`.

<!-- credentials:required-key:start -->
```text
alpha_vantage  bdf  coingecko  eia  eodhd  finnhub  fmp  fred  tiingo
```
<!-- credentials:required-key:end -->

The fast-fail is done by `require_key` (see how `fred` wires it in
[fred/parsimony_fred/__init__.py](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/fred/parsimony_fred/__init__.py)):
it resolves the key from the bound value or the env var, and raises if neither is present —
before httpx is ever touched.

### Optional key

<!-- credentials:optional-key:start -->
```text
bls  riksbank
```
<!-- credentials:optional-key:end -->

These work **without** a key and never fast-fail. A key only raises the rate-limit quota
(and for `bls`, also enriches the output). Set the env var to lift the quota — `BLS_API_KEY`
for `bls`, `RIKSBANK_API_KEY` for `riksbank` — or leave it unset to run keyless at the lower
limit. They declare `secrets=("api_key",)` but **`requires=()`** — the call succeeds
unconfigured, so the env var stays out of `requires=` and lives only in this prose and the
connector docstring.

### Keyless

<!-- credentials:keyless:start -->
```text
bde  bdp  boc  boj  destatis  polymarket  rba  sdmx  snb  treasury
```
<!-- credentials:keyless:end -->

No key, no `secrets=`, no `requires=`, no `.bind`, no fast-fail. Call them directly. (`keyless`
in the manifest is computed as `not requires`, so an empty `requires=` is what marks them.)

### Keyless but header-required

<!-- credentials:header-required:start -->
```text
sec_edgar
```
<!-- credentials:header-required:end -->

`sec_edgar` needs no secret, but SEC's fair-access policy requires every request to carry a
`User-Agent` header identifying the requester (name + email). It is read from
`SEC_EDGAR_USER_AGENT` and resolved before any network call; a missing value fast-fails with
`UnauthorizedError`. This is modeled as an **env-resolved header**, deliberately **not** via
`secrets=` or `.bind` — a `User-Agent` is required infrastructure, not a logged-and-redacted
secret. So every `sec_edgar` verb declares `requires=("SEC_EDGAR_USER_AGENT",)` with **no**
`secrets=`: it must *exist*, but there is nothing to *hide*. This is the fourth combination —
a required env var with no secret parameter at all. See
[sec_edgar/parsimony_sec_edgar/_http.py](https://github.com/ockham-sh/parsimony-connectors/blob/main/packages/sec_edgar/parsimony_sec_edgar/_http.py).

For the full per-provider auth table, see [../reference/providers.md](../reference/providers.md).

## Two mechanisms for handling a key

For a keyed connector, two independent mechanisms protect the key. They solve different
problems, and a keyed connector uses **both**. (These are separate again from `requires=`, which
does not *protect* the key but *declares* that the call needs it — see [Two
questions](#two-questions) above. A required-key connector declares all three:
`secrets=`, `requires=`, and a `.bind`-able parameter.)

### `secrets=` — keep the key out of provenance

The `@connector(..., secrets=("api_key",))` decorator argument strips the named parameter
from `provenance.params`, so the key never lands in a stored receipt. `fred_fetch` and
`fred_search` both declare `secrets=("api_key",)`:

```python
@connector(output=FETCH_OUTPUT, tags=["macro"], secrets=("api_key",))
def fred_fetch(series_id, ..., api_key: str = "") -> pd.DataFrame:
    ...
```

`secrets=` governs **provenance only**. It does not touch logs (see the redaction note
below).

### `.bind` — fix the value and hide it from agents

`Connector.bind(api_key=...)` returns a new connector with the value fixed. The bound
parameter is removed from the exposed signature and from the agent-facing `describe()` /
`to_llm()` cards. This is how an operator wires a key in once and then hands the connector to
an agent without the key appearing anywhere the agent can see or set it.

The standard per-package operator helper does exactly this:

```python
def load(*, api_key: str) -> Connectors:
    return CONNECTORS.bind(api_key=api_key)
```

## Supplying a key as a user

You have two equivalent options.

**Option A — set the environment variable** and pass nothing. Every required/optional-key
connector falls back to `<PROVIDER>_API_KEY`:

```bash
export FRED_API_KEY=your-key-here
```

```python
from parsimony import discover

connectors = discover.load("fred")
series = connectors["fred_fetch"](series_id="UNRATE")   # key resolved from env
```

**Option B — bind the key explicitly** via the package `load` helper (or `.bind` directly).
This is the right choice when you are wiring a connector for an agent, because the key is then
hidden from the agent's view:

```python
from parsimony_fred import load

connectors = load(api_key="your-key-here")
series = connectors["fred_fetch"](series_id="UNRATE")
```

Either way, no key is required to construct the collection; required-key connectors only fail
when called without a resolvable key.

## Headers over query strings

**Carry the key the way the provider expects, preferring a header or POST body over a query
string.**

The kernel's HTTP client auto-redacts query-param **values** whose **name** is in a sensitive
set — `api_key`, `apikey`, `token`, `*_token`, `access_token`, `secret`, `client_secret`,
`password`, `authorization`, `registrationkey`, and similar. A key sent as a query param
under a name **outside** that set is **not** redacted and can leak into logs.

Two things follow:

- Prefer a header or POST body so the key never appears in a logged URL at all.
- `secrets=()` protects **provenance**, not logs. The two are separate: a key can be absent
  from `provenance.params` (via `secrets=`) yet still leak into a log line if it rides in an
  un-redacted query param. Use the right transport, and the right param name when a query
  param is unavoidable.

When the provider only accepts the key as a query param, name it so it falls inside the
redaction set (e.g. `api_key`) — `fred`, for instance, sends `api_key` as a default query
param and the transport redacts it from logs.

## See also

- [./connectors.md](./connectors.md) — the connector, `Connectors` collection, and `Result`
  model.
- [../guides/using-connectors.md](../guides/using-connectors.md) — loading and binding in
  practice.
- [../reference/providers.md](../reference/providers.md) — per-provider auth requirements and
  env vars.
