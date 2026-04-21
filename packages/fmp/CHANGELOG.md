# Changelog — parsimony-fmp

## 0.2.0

### Breaking changes

- **Merged `parsimony-fmp-screener` into `parsimony-fmp`.** The `fmp_screener`
  connector now ships as part of this package under the `fmp` entry-point in
  the `parsimony.providers` group. The separate `parsimony-fmp-screener`
  distribution and its `fmp_screener` entry-point are removed. Callers should
  `pip uninstall parsimony-fmp-screener` and update imports from
  `parsimony_fmp_screener` to `parsimony_fmp`.
- **Screener 402 responses now raise `PaymentRequiredError`.** Previously the
  screener package mapped HTTP 402 to `ProviderError`; all 19 FMP connectors
  now share the canonical mapping of 402 → `PaymentRequiredError`. Callers
  that caught `ProviderError` specifically for screener plan-gating failures
  should widen to `PaymentRequiredError`.
- **429 responses raise `RateLimitError` across all connectors.** Previously
  only the screener treated 429 specially (as `ProviderError`). Callers that
  caught `ProviderError` on 429 should catch `RateLimitError` — the new
  exception carries `retry_after` for programmatic backoff.

### Non-breaking changes

- Decomposed the single-file package into five internal modules
  (`_http.py`, `params.py`, `outputs.py`, `_screener.py`, `__init__.py`). All
  public symbols remain importable from `parsimony_fmp` at the same names.
- Unified HTTP error mapping in one function; every connector routes through
  the same 401/402/429/other taxonomy.
- Added URL redaction (`?apikey=<key>` → `?apikey=***`) in every error path
  as defence-in-depth for logs and exception traces.
- Set a 15s per-request HTTP timeout on the FMP transport (matches the
  Tiingo connector's precedent); timeouts raise `ProviderError` via the
  unified mapper.
- Collapsed the screener's two per-endpoint semaphores into one shared
  semaphore (`DEFAULT_ENRICHMENT_CONCURRENCY = 10`); previously the two
  semaphores overstated effective concurrency by 2×.

## 0.1.0

- Initial scaffold.
