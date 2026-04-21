# Changelog — parsimony-alpha-vantage

## 0.2.0

### Non-breaking changes

- Decomposed the single-file package into four internal modules
  (`_http.py`, `params.py`, `outputs.py`, `__init__.py`). All public symbols
  remain importable from `parsimony_alpha_vantage` at the same names.
- Unified HTTP error mapping: 401/403 → `UnauthorizedError`, 402 →
  `PaymentRequiredError`, 429 → `RateLimitError`, else → `ProviderError`.
  In-body error envelopes (`Error Message`, `Note`, `Information`) keep their
  existing semantic mapping.
- Added URL redaction (`?apikey=<key>` → `?apikey=***`) as defence-in-depth
  for exception text and logs.
- `RateLimitError` now reads `Retry-After` from the response header when
  present (falls back to 60s).
- Added a per-request 20s HTTP timeout; timeouts raise `ProviderError`
  via the unified mapper.

## 0.1.0

- Initial scaffold.
