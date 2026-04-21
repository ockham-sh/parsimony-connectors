# Changelog — parsimony-sdmx

## [0.3.0] — 2026-04-21

### Breaking

- **Requires `parsimony-core>=0.3,<0.5`.** The kernel flat-module refactor
  removed `parsimony.bundles` (and with it `CatalogSpec`, `CatalogPlan`,
  `to_async`), removed namespace templates on `Column`, and replaced the
  `Namespace` annotation class with the string sentinel
  `Annotated[str, "ns:x"]`. This plugin moves to the new contract.
- **`@enumerator(catalog=...)` kwarg removed.** Both enumerators
  (`enumerate_sdmx_datasets`, `enumerate_sdmx_series`) drop the
  `catalog=CatalogSpec(...)` declaration.
- **Namespace templates removed.** `enumerate_sdmx_series` no longer
  declares `Column(namespace="sdmx_series_{agency}_{dataset_id}")`. Its
  KEY column has no namespace — the catalog's ``name`` supplies the
  namespace at ingest time.
- **`parsimony_sdmx._catalog_planning` removed.** The per-dataset
  fan-out logic moves to the top-level ``CATALOGS`` async generator in
  ``parsimony_sdmx``.

### Added

- ``parsimony_sdmx.CATALOGS`` — async generator consumed by
  ``parsimony publish --provider sdmx``. Yields one static
  ``sdmx_datasets`` catalog plus one
  ``sdmx_series_<agency>_<dataset_id>`` catalog per ``(agency, dataset_id)``
  found on disk.
- ``parsimony_sdmx.RESOLVE_CATALOG`` — on-demand namespace → callable
  resolver used by ``parsimony publish --only <namespace>``. Parses
  ``sdmx_series_<agency_lower>_<dataset_id_lower>`` back into
  ``(agency, dataset_id)`` and binds them into a per-dataset callable,
  preferring the longest agency match (so ``imf_data_pgi`` resolves as
  ``IMF_DATA`` + ``pgi``).
- ``parsimony_sdmx.connectors.enumerate_series.series_namespace`` —
  pure helper to compose the per-dataset namespace from
  ``(agency, dataset_id)``; shared by the CATALOGS producer and the
  RESOLVE_CATALOG consumer.

### Changed

- ``PROVIDER_METADATA.namespace_templates`` replaced with
  ``PROVIDER_METADATA.namespaces`` (static list + dynamic pattern).

## [0.2.0] — prior scaffold
