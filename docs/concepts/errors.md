# The typed-error model

When a connector fails to fetch, it raises a **typed error** from
`parsimony.errors`. Each typed error carries a `provider` attribute and a
default message written for the agent that called the connector. An agent can
branch on the error's *type and attributes* to decide what to do next, and
render the message verbatim with a single `str(exc)`.

## Operational errors vs programmer errors

The typed errors model **operational failures** — the things that go wrong when
you talk to a remote provider: a bad key, a rate limit, an empty result, a
server outage. These are normal outcomes a running agent must handle.

**Programmer errors stay native.** A bad argument shape, a type mismatch, a
schema-validation failure inside your own code — raise `TypeError`,
`ValueError`, or let a Pydantic `ValidationError` propagate. Those are bugs, not
operational states, and dressing them as connector errors hides them.

The one boundary case is a *call-time* argument check before any network call —
an empty `series_id`, an out-of-range `page_size`. That is operational (the agent
can fix it and retry) so it uses `InvalidParameterError`, not a bare
`ValueError`.

## The error table

All import from `parsimony.errors`. The base is
`ConnectorError(message, *, provider)`, but **every subclass flips the argument
order so `provider` is the first positional** — see the gotcha below.

| Error | Raise for | Notes |
|---|---|---|
| `UnauthorizedError(provider, env_var=...)` | 401/403 from bad or missing credentials | `env_var` is keyword-only and names the variable the agent should set. |
| `PaymentRequiredError(provider)` | 402, or a plan-tier restriction | Use even when the upstream status is a 401/403 whose *body* says the plan is the problem. |
| `RateLimitError(provider, retry_after, *, quota_exhausted=False)` | 429 | `retry_after` is seconds-until-retry; `quota_exhausted=True` means the billing-period quota is gone. |
| `ProviderError(provider, status_code)` | 5xx, 404, or timeout (408) | Carries the `status_code`. |
| `EmptyDataError(provider, query_params=...)` | HTTP 200 with zero rows | A valid outcome — recovery is to adjust params, so no "do not retry" directive. |
| `ParseError(provider, msg=None)` | HTTP 200 that cannot be parsed, or schema drift | The honest mapping for a 200 carrying an error body. |
| `InvalidParameterError(provider, message)` | A bad call-time argument, before any network call | `message` is required. |
| `CatalogNotFoundError(msg)` | A catalog bundle missing or unreachable | Takes the message first; `provider` defaults to `"catalog"`. |

### `retry_after` must be a duration

`RateLimitError` rejects a `retry_after` greater than 86,400 seconds — that
looks like a Unix epoch timestamp, not a delay. Pass seconds-until-retry
(`60.0`), never an absolute timestamp.

## The agent-facing-message principle

For every typed subclass, the kernel-built default message **is** the canonical
agent-facing string. Each default embeds the right semantics and the right
agent-loop directive — `DO NOT retry`, `pick a different connector`, `adjust
parameters` — so a consumer renders it with one `str(exc)` call and the agent
knows what to do.

`UnauthorizedError("fred", env_var="FRED_API_KEY")` produces, roughly:

> fred: API credentials missing or invalid — set the FRED_API_KEY env var (and
> ensure it is exported). DO NOT retry with different arguments.

You rarely pass `message=`. It is an escape hatch for context the kernel cannot
construct (an upstream `error_code`, say). If you override, you own the
agent-facing text: keep it free of URLs, tokens, and upstream-derived prose that
could carry credentials or a prompt-injection payload.

## Branch on attributes, never on strings

The message is for the agent to *read*; it is not a stable contract for code to
*parse*. Branch on the error type and its attributes — `provider`,
`status_code`, `retry_after`, `quota_exhausted`, `env_var`, `query_params` —
never on substrings of the message. Messages are tuned for agents and will
change; attributes are the contract.

```python
from parsimony.errors import ProviderError

try:
    result = some_connector(...)
except ProviderError as exc:
    if exc.status_code == 408:
        ...  # upstream timeout — back off
    elif 500 <= exc.status_code <= 599:
        ...  # transient server error
```

## The flipped-argument-order gotcha

`ConnectorError`, the base, takes the message first and `provider` as a
keyword-only argument:

```python
ConnectorError("something failed", provider="fred")
```

But **every subclass you actually raise puts `provider` first**, positionally:

```python
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

UnauthorizedError("fred", env_var="FRED_API_KEY")
PaymentRequiredError("fred")
RateLimitError("fred", 60.0)                       # retry_after seconds
RateLimitError("fred", 0.0, quota_exhausted=True)
ProviderError("fred", 503)                         # status_code
EmptyDataError("fred", query_params={"series_id": sid})
ParseError("fred", "response missing 'observations'")
InvalidParameterError("fred", "series_id must be non-empty")
```

Writing `EmptyDataError("no data", provider="fred")` — base-style — puts the
message where `provider` is expected and is wrong. The first positional argument
to any subclass is the provider name.

## What an agent does with each

The directive baked into each message tells the agent its next move. Two
representative cases:

```python
from parsimony.errors import EmptyDataError, RateLimitError

try:
    result = provider_fetch(series_id="GDPC1")
except RateLimitError as exc:
    if exc.quota_exhausted:
        ...  # billing quota gone: switch connectors or wait for the next cycle
    else:
        ...  # burst limit: do not retry immediately; try elsewhere or wait
             #                exc.retry_after seconds
except EmptyDataError as exc:
    ...  # a clean 200 with no rows: adjust exc.query_params (a wider date
         # window, a different id) and retry, or try a different connector
```

- **`RateLimitError`** — distinguish the two cases by `quota_exhausted`. A burst
  limit (`False`) is recoverable after `retry_after` seconds; a quota
  (`True`) is not recoverable this billing period, so the agent should pivot.
- **`EmptyDataError`** — not a failure of the connector, a failure of the query.
  The agent should adjust parameters using `query_params` for context, or move
  on. There is deliberately no "do not retry" directive.

See [the connector contract](connectors.md) for where these errors are raised,
and [using connectors](../guides/using-connectors.md) for handling them on the
consumer side.
