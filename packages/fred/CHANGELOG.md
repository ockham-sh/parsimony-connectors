# Changelog

All notable changes to `parsimony-fred` will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] — 2026-04-21

### Breaking

- **Requires `parsimony-core>=0.3,<0.5`.** Aligns with the kernel flat-module
  refactor: the old `parsimony.transport.http` submodule was collapsed into a
  single `parsimony.transport` module, and the `Namespace("x")` annotation
  class was replaced by the string sentinel `Annotated[str, "ns:x"]`. Both
  changes are mechanical but user-visible if you had imported the old symbols
  transitively.
- **`CatalogSpec` / `@enumerator(catalog=...)` removed** in the kernel. The
  plugin now declares its publish surface through the module-level
  ``CATALOGS = [("fred", enumerate_fred)]`` list consumed by
  ``parsimony publish --provider fred``.

### Added

- ``enumerate_fred`` — param-less catalog enumerator that walks every FRED
  release and emits one row per unique series. Drives
  ``parsimony publish --provider fred --target <url>/{namespace}``.
- ``FredEnumerateAllParams`` — empty pydantic model kept visible on the
  public surface so the kernel's ``Connector.param_type()()`` construction
  round-trips for callers who invoke the enumerator directly.
- Module-level ``CATALOGS`` export — canonical publish target list.

### Changed

- ``series_id`` annotation now uses ``Annotated[str, "ns:fred"]`` (string
  sentinel) instead of ``Annotated[str, Namespace("fred")]``.
- ``HttpClient`` imported from ``parsimony.transport`` (was
  ``parsimony.transport.http``).

## [0.1.0] — 2026-04-17

### Added

- Initial release.
- `fred_search` connector — tool-tagged keyword search.
- `fred_fetch` connector — observation-level time series fetch with metadata.
- `enumerate_fred_release` enumerator — catalog indexing for a FRED release.
- Plugin entry-point registration under `parsimony.providers`.
- Release-blocking conformance test against `parsimony.testing.assert_plugin_valid`.
