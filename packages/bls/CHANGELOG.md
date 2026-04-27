# Changelog — parsimony-bls

All notable changes to `parsimony-bls` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
