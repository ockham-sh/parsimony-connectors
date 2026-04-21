# Changelog — parsimony-finnhub

## 0.2.0

### Non-breaking changes

- Decompose the single-file package into four internal modules
  (`_http.py`, `params.py`, `outputs.py`, `__init__.py`). All public symbols
  remain importable from `parsimony_finnhub` at the same names.
- Unify HTTP error mapping: 401 → `UnauthorizedError`, 403 →
  `PaymentRequiredError`, 429 → `RateLimitError`, else → `ProviderError`.
- `RateLimitError` reads `Retry-After` from the response header when
  present, falls back to `X-Ratelimit-Reset` (unix-epoch seconds),
  then to 60s.
- Apply a per-request 15s HTTP timeout; timeouts raise `ProviderError`
  via the unified mapper.

## 0.1.0

- Initial scaffold.
