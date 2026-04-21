# Changelog — parsimony-tiingo

## 0.2.0

### Non-breaking changes

- Decomposed the single-file package into four internal modules
  (`_http.py`, `params.py`, `outputs.py`, `__init__.py`). All public symbols
  remain importable from `parsimony_tiingo` at the same names.
- Unified HTTP error mapping: 401/403 → `UnauthorizedError`, 402 →
  `PaymentRequiredError`, 429 → `RateLimitError`, else → `ProviderError`.
- `RateLimitError` now reads `Retry-After` from the response header when
  present (falls back to 60s).
- HTTP timeouts raise `ProviderError` (status 408) via the unified mapper.

## 0.1.0

- Initial scaffold.
