# Changelog — parsimony-sdmx

## [0.3.0] — 2026-04-21

Single-step publishing: every SDMX operation now hits the live agency
endpoint through ``parsimony publish``. The separate ``parsimony-sdmx``
builder CLI and its on-disk parquet cache are gone — the plugin
contract now matches every other parsimony connector. The proven
subprocess-isolation machinery that the builder used per-dataset is
preserved as internal infrastructure under ``parsimony_sdmx._isolation``.

### Breaking

- **Requires `parsimony-core>=0.3,<0.5`.** The kernel flat-module
  refactor removed `parsimony.bundles` (with it `CatalogSpec`,
  `CatalogPlan`, `to_async`), removed namespace templates on `Column`,
  merged `SemanticTableResult` into `Result`, and replaced the
  `Namespace` annotation class with the string sentinel
  `Annotated[str, "ns:x"]`. All three surface changes propagate here.
- **`parsimony-sdmx` CLI removed.** The user-facing builder entry
  points (``parsimony_sdmx.cli.main``, ``.args``, ``.orchestrator``,
  ``.summary``, ``.orphan_sweep``) are deleted along with the
  ``[project.scripts] parsimony-sdmx`` entry point. Catalog building
  now happens inline during ``parsimony publish`` — there is no
  intermediate on-disk cache under ``outputs/{AGENCY}/...`` and no
  ``PARSIMONY_SDMX_OUTPUTS_ROOT`` env var.
- **`@enumerator(catalog=...)` kwarg removed.** Both enumerators drop
  the ``catalog=CatalogSpec(...)`` declaration.
- **Namespace templates removed.** ``enumerate_sdmx_series`` no longer
  declares ``Column(namespace="sdmx_series_{agency}_{dataset_id}")``.
  Its KEY column carries no namespace; the catalog's ``name`` supplies
  it at ingest time.
- **`parsimony_sdmx._catalog_planning` removed.** The per-dataset
  fan-out logic moves to the top-level ``CATALOGS`` async generator.

### Added

- ``parsimony_sdmx._isolation`` — internal package holding the
  subprocess-isolation machinery that was previously under
  ``parsimony_sdmx.cli``. Exports ``list_datasets`` (spawns a child
  that calls ``provider.list_datasets()``, returns tuples via queue
  with the drain-before-join ordering that dodges the mp.Queue /
  pipe-buffer deadlock) and ``fetch_series`` (spawns a child that
  writes the series parquet to a caller-supplied tmpdir, returns only
  a small :class:`DatasetOutcome` via queue; parent reads the parquet
  back and discards the tmpdir).
- The underlying rationale — ``sdmx1`` caches parsed structure messages
  at module scope with no invalidation hook; process death is the only
  way to flush and prevent parent OOM — is preserved verbatim from the
  legacy module docstrings. Future maintainers need to know this is
  non-negotiable architecture, not over-engineering.
- ``parsimony_sdmx.CATALOGS`` — async generator consumed by
  ``parsimony publish --provider sdmx``. Yields one static
  ``sdmx_datasets`` catalog plus one
  ``sdmx_series_<agency>_<dataset_id>`` catalog per live-discovered
  ``(agency, dataset_id)``. Agencies that fail listing are skipped
  with a warning; the run continues.
- ``parsimony_sdmx.RESOLVE_CATALOG`` — on-demand namespace resolver
  used by ``parsimony publish --only <namespace>``. Pure string
  parsing, no SDMX calls; longest-agency match wins so
  ``sdmx_series_imf_data_pgi`` resolves as ``IMF_DATA`` + ``pgi``.
- ``parsimony_sdmx.connectors.enumerate_series.series_namespace`` —
  pure helper to compose the per-dataset namespace from
  ``(agency, dataset_id)``; used by CATALOGS and mirrored by the
  RESOLVE_CATALOG parser.
- ``fetch_timeout_s`` kwarg on both enumerators — per-subprocess
  wall-clock budget (default 900s for series, 600s for dataset
  listing). A timeout raises ``ListDatasetsError`` /
  ``FetchSeriesError`` which the kernel publisher catches per-namespace.
- Canonical-case reconstruction in :func:`parsimony_sdmx.RESOLVE_CATALOG`.
  Namespaces are stored lowercase (``sdmx_series_ecb_ame``) but every
  SDMX agency we wire up today (ECB, ESTAT, IMF_DATA, WB_WDI) uses
  uppercase dataflow IDs upstream — so parsing the namespace tail back
  to a dataset_id is ``.upper()``. In the old builder flow this case
  was preserved incidentally by the on-disk parquet filename; deleting
  the builder CLI forced a pure-Python replacement. If a future agency
  surfaces a different convention, push a per-provider
  ``canonical_dataset_id`` method onto the ``CatalogProvider`` Protocol.

### Changed

- ``enumerate_sdmx_datasets`` and ``enumerate_sdmx_series`` now hit
  the live agency endpoint on every call (routed through
  ``_isolation.list_datasets`` and ``_isolation.fetch_series``).
  Previous behaviour read from on-disk parquet caches produced by the
  now-deleted builder CLI.
- ``PROVIDER_METADATA.namespace_templates`` replaced with
  ``PROVIDER_METADATA.namespaces`` (static list + dynamic pattern).
- ``parsimony_sdmx.cli`` → ``parsimony_sdmx._isolation`` (rename).
  The worker / listing / layout / memory_monitor / parquet modules
  that were inside the user-CLI package now live as private internal
  infrastructure. No behavioural change; every import is local to the
  plugin.

### Removed

- ``parsimony_sdmx.cli.main`` — user-CLI entry point (``parsimony-sdmx``
  shell command).
- ``parsimony_sdmx.cli.args`` — argparse config.
- ``parsimony_sdmx.cli.orchestrator`` — batch driver with per-agency
  Pool + retry / skip / resume.
- ``parsimony_sdmx.cli.orphan_sweep`` — post-run temp-file cleanup.
- ``parsimony_sdmx.cli.summary`` — human-readable run report.
- Tests for the deleted user-CLI surface: ``test_args.py``,
  ``test_catalog_planning.py``, ``test_main.py``, ``test_orchestrator.py``,
  ``test_orphan_sweep.py``, ``test_summary.py``.

## [0.2.0] — prior scaffold
