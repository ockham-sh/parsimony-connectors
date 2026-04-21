# Changelog — parsimony-coingecko

## 0.2.0

### Non-breaking changes

- Decomposed the single-file package into four internal modules
  (`_http.py`, `params.py`, `outputs.py`, `__init__.py`). All public symbols
  remain importable from `parsimony_coingecko` at the same names.
- Unified HTTP error mapping: 401/403 → `UnauthorizedError` (with plan-
  restriction `error_code` inspection escalating to `PaymentRequiredError`),
  402 → `PaymentRequiredError`, 429 → `RateLimitError`, else → `ProviderError`.
- `RateLimitError` now reads `Retry-After` from the response header with a
  bounds-checked parser (falls back to 60s).
- Per-request 15s HTTP timeout; timeouts raise `ProviderError(status_code=408)`
  via the unified mapper. The enumerator keeps its own 60s client for the
  ~15k-row `/coins/list` payload.
- Error messages never echo the API key (auth rides in the
  `x-cg-demo-api-key` request header, not the URL).

## 0.1.0

- Initial scaffold.
