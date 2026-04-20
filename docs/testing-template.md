# Connector testing template

Every `parsimony-<name>` package must satisfy two gates: the kernel conformance
suite (`parsimony.testing.assert_plugin_valid`) and a per-connector happy-path
test. This document specifies the happy-path shape.

**Reference implementation:** [`packages/fred/tests/test_fred_connectors.py`](../packages/fred/tests/test_fred_connectors.py).

---

## 1. Files per package

Each `packages/<name>/tests/` directory contains exactly two test files:

- `test_conformance.py` — one-liner subclass/call of `parsimony.testing.assert_plugin_valid`.
- `test_<name>_connectors.py` — the happy-path + error-mapping tests specified below.

Do **not** add a `tests/__init__.py`. Tests are discovered by pytest's rootdir
layout. Sharing fixtures inside one package uses `conftest.py`; do NOT create a
workspace-level shared testing helper — every package's upstream API shape is
different and shared fixtures leak across packages.

---

## 2. Happy-path test: one per `@connector` / `@enumerator`

For every connector exported in `CONNECTORS`, write one async test that:

1. Mocks the upstream HTTP response with `respx` at the full upstream URL.
2. Binds keyword-only deps via `connector.bind_deps(api_key="test-key", ...)`.
3. Awaits the bound callable with a valid `Params` instance.
4. Asserts on the **public `Result` surface only** — never on internal helpers,
   request headers, or full DataFrame equality.

Minimum assertions, required on every happy-path test:

- `isinstance(result, Result)` — the `@enumerator` decorator is expected to
  wrap the returned `pd.DataFrame` into a `Result`; this assertion is what
  catches silent decorator regressions.
- `result.provenance.source == "<provider-name>"` — matches the
  `ENV_VARS` / entry-point key.
- `result.data` has the expected columns (by name, not by full content).
- For `@enumerator`: row count > 0 on the happy-path fixture.

---

## 3. Provenance envelope

Every `Provenance(...)` constructed inside a connector must populate at least:

| Field | Type | Required | Notes |
|---|---|---|---|
| `source` | `str` | yes | Provider identifier (matches entry-point key). |
| `params` | `dict[str, Any]` | yes | The parameter payload that produced the result. Never include the `api_key`. |
| `properties["retrieved_at"]` | `str` (ISO-8601 UTC) | yes | `datetime.now(timezone.utc).isoformat()`. |
| `properties["upstream_endpoint"]` | `str` | yes | The request path (e.g. `/series/observations`) — **not** the full URL with query string (which may contain the API key). |
| `properties["as_of"]` | `str` | when upstream exposes it | Upstream vintage / version / `last_updated` date. Omit if the provider has no concept of vintage. |

Everything else goes under `properties[...]` or is elided. Do not write an
authenticated URL into any provenance field — downstream LLM tools read this.

The happy-path test asserts on `source`; the error-mapping test asserts the
api-key value does **not** appear in `str(raised_exception)` (see §4).

---

## 4. Error-mapping tests: required for key-bearing connectors

For every `@connector` with a keyword-only `api_key` / `token` dep, add two
additional async tests:

### 4a. 401 → `UnauthorizedError`

```
@respx.mock
@pytest.mark.asyncio
async def test_<name>_maps_401_to_unauthorized() -> None:
    respx.get("<upstream-url>").mock(return_value=httpx.Response(401, json={...}))
    bound = <connector>.bind_deps(api_key="live-looking-key-123")
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(<Params>(...))
    assert "live-looking-key-123" not in str(exc_info.value)
```

### 4b. 429 → `RateLimitError`

Same shape, returning 429, asserting `RateLimitError` and the api_key string
is absent from the exception message.

If the connector does not map 401/429 today, fix the mapping — the exception
hierarchy is the LLM-facing control-flow signal. Generic `httpx.HTTPStatusError`
leaking through is a contract violation.

---

## 5. Tool-description contract for `tool`-tagged connectors

For every `@connector(tags=[..., "tool"], ...)`, the first line of the
docstring must be a single-sentence **action contract**:

```
<verb> <noun> [by <identifier>] [— when to use vs sibling connectors].
```

- Verb: `fetch`, `search`, `list`, `enumerate`, `resolve`.
- Noun: what the connector returns (plain English, vendor term in parens).
- Identifier: what the caller must know to pick the right tool.
- Optional disambiguator if the package has siblings an agent might confuse.

Examples:
- ✅ `Fetch FRED time series observations by series_id.`
- ✅ `Search FRED economic time series by keyword — use for discovering unknown series.`
- ❌ `FRED API.` (no verb, no noun)
- ❌ `This connector returns data.` (what kind?)

The conformance suite enforces first-line length ≥ 40 chars; this document
raises the quality bar to "an LLM router can pick correctly."

---

## 6. Do NOT assert on

- `httpx.AsyncClient` internals, request headers, or query-string construction.
- Full DataFrame equality via `pd.testing.assert_frame_equal` on multi-row payloads.
- Kernel-private fields (`_impl`, `_deps`, etc.).
- Timing, retry counts, backoff duration.
- `sys.modules` state (unless the specific test is auditing import cost).

Those are implementation-detail tests. They break on every kernel refactor and
provide false confidence.

---

## 7. Forbidden in the happy-path test

- No `time.sleep` / `asyncio.sleep > 0`.
- No real network I/O — respx must cover every `httpx` call path.
- No cassette-style recorded fixtures with real headers. Hand-author the JSON
  response from the provider's API docs.
- No `@pytest.mark.integration` markers on happy-path tests — integration tests
  are excluded from the default pytest run via workspace `addopts = "-m 'not integration'"`.

---

## 8. Runtime expectation

A complete happy-path test must run in under 100 ms on a warm venv. Slow tests
erode trust in the four-gate bar and compound across the 23-package matrix.
