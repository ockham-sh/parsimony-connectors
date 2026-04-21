# Changelog — parsimony-eodhd

## 0.2.0

### Non-breaking changes

- Decomposed single-file package into four internal modules
  (`_http.py`, `params.py`, `outputs.py`, `__init__.py`). All public symbols
  remain importable from `parsimony_eodhd` at the same names.
- Unified HTTP error mapping: 401/403 → `UnauthorizedError`, 402 →
  `PaymentRequiredError`, 429 → `RateLimitError`, else → `ProviderError`.
- URL redaction (`?api_token=<key>` → `?api_token=***`) as defence-in-depth
  for exception text and logs.
- `RateLimitError` now reads `Retry-After` from the response header when
  present (falls back to 60s).
- 15s per-request HTTP timeout by default; bulk endpoints override explicitly.
  `httpx.TimeoutException` is mapped to `ProviderError(status_code=408)`.

## 0.1.0

- Initial scaffold.
