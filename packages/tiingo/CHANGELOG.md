# Changelog — parsimony-tiingo

All notable changes to `parsimony-tiingo` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Every connector now declares `requires=("TIINGO_API_KEY",)` alongside its
  existing `secrets=("api_key",)` — the env var the fast-fail `UnauthorizedError`
  names when a verb is called with nothing configured.
- Credential-declaration conformance tests
  (`tests/test_credential_declaration_tiingo.py`): one
  `CredentialDeclarationSuite` subclass per key-carrying verb, proving the
  `requires` declaration matches runtime (fast-fail names the env var; an env- or
  bind-supplied key reaches the outgoing request via the `Authorization: Token`
  header). `enumerate_tiingo` is covered by a dedicated fast-fail test instead:
  it uses the key only as a gate and then downloads a public, unauthenticated CDN
  snapshot, so the request-canary checks do not apply to it.

## [0.5.0] — 2026-05-06

### Changed

- Adapted to `parsimony-core==0.5`. Connector code no longer constructs `Provenance` directly; the framework authors all provenance fields in `Connector._wrap_result`. Source-specific extras (where present) move to `Result.with_properties(**kwargs)`. Drops the `provenance=` and `params=` kwargs from `OutputConfig.build_table_result` / `Result.from_dataframe` call sites.
- Bump `parsimony-core` pin from `>=0.4.0,<0.5` to `>=0.5.0,<0.6` (and `[standard-onnx]` extra accordingly on catalog-publishing packages).
## [0.4.0] — 2026-04-24

Part of the first coordinated release of the
[`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors)
monorepo under `parsimony-core==0.4`.

### Changed

- Connector rewritten against the kernel's `parsimony.discover` surface
  (`iter_providers`, `load`, `load_all`) and the `@connector(env=...)`
  decorator-level env-var declaration that replaced module-level
  `ENV_VARS`.
- Pin bumped to `parsimony-core>=0.4,<0.5`.
