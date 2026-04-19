# Changelog

All notable changes to `parsimony-fred` will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-04-17

### Added

- Initial release.
- `fred_search` connector — tool-tagged keyword search.
- `fred_fetch` connector — observation-level time series fetch with metadata.
- `enumerate_fred_release` enumerator — catalog indexing for a FRED release.
- Plugin entry-point registration under `parsimony.providers`.
- Release-blocking conformance test against `parsimony.testing.assert_plugin_valid`.
