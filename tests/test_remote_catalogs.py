"""Remote catalog compatibility and curated search probes.

Disabled by default (see ``pyproject.toml`` addopts). Enable with::

    PARSIMONY_RUN_REMOTE_CATALOGS=1 \\
    uv run pytest tests/test_remote_catalogs.py -m remote_catalog

Optional::

    PARSIMONY_CATALOG_URL=hf://parsimony-dev-staging/riksbank \\
    PARSIMONY_ALLOW_MISSING_REMOTE_CATALOG=1
"""

from __future__ import annotations

import pytest

from parsimony_test_support.catalog_remote import (
    REPO_ROOT,
    allow_missing_remote,
    catalog_url_override,
    import_catalog_validate,
    remote_catalogs_enabled,
    skip_unless_remote_catalogs,
)

pytestmark = [pytest.mark.remote_catalog, pytest.mark.integration]


def _provider_ids() -> list[str]:
    load_queries_file, PROVIDER_SPECS, _, _ = import_catalog_validate()
    ids: list[str] = []
    for name, spec in PROVIDER_SPECS.items():
        if (REPO_ROOT / spec.queries_file).exists():
            ids.append(name)
    return ids


@pytest.fixture(params=_provider_ids() or ["riksbank"])
def provider_spec(request: pytest.FixtureRequest):
    skip_unless_remote_catalogs()
    _, PROVIDER_SPECS, _, _ = import_catalog_validate()
    return PROVIDER_SPECS[request.param]


def test_provider_catalog_schema_and_probes(provider_spec) -> None:
    load_queries_file, _, _, validate_catalog = import_catalog_validate()
    url = catalog_url_override(provider_spec.default_url)
    queries_path = REPO_ROOT / provider_spec.queries_file
    query_set = load_queries_file(queries_path) if queries_path.exists() else None
    catalog_root = query_set.catalog_root if query_set else None
    entry_url = url
    if catalog_root and query_set is not None:
        first_ns = next((q.namespace for q in query_set.queries if q.namespace), None)
        if first_ns and url.rstrip("/") == catalog_root.rstrip("/"):
            entry_url = f"{catalog_root.rstrip('/')}/{first_ns}"
    report = validate_catalog(
        entry_url,
        query_set,
        allow_missing=allow_missing_remote(),
        catalog_root=catalog_root,
    )
    if report.skipped:
        pytest.skip(report.skip_reason)
    assert report.schema_ok, f"{url} must be schema_version 1"
    assert report.entry_count > 0
    if query_set is not None and query_set.queries:
        min_recall = query_set.thresholds.get("min_required_recall", 1.0)
        assert report.required_recall >= min_recall, report.probe_results


def test_sdmx_agency_datasets_catalog_schema() -> None:
    skip_unless_remote_catalogs()
    load_queries_file, _, SDMX_QUERIES_FILE, validate_catalog = import_catalog_validate()
    queries_path = REPO_ROOT / SDMX_QUERIES_FILE
    query_set = load_queries_file(queries_path) if queries_path.exists() else None
    root = catalog_url_override("hf://parsimony-dev/sdmx")
    url = f"{root}/sdmx_datasets_ecb"
    report = validate_catalog(
        url,
        query_set,
        allow_missing=allow_missing_remote(),
        catalog_root=root,
    )
    if report.skipped:
        pytest.skip(report.skip_reason)
    assert report.schema_ok
    assert report.entry_count > 0


def test_sdmx_codelist_catalog_schema() -> None:
    skip_unless_remote_catalogs()
    _, _, _, validate_catalog = import_catalog_validate()
    root = catalog_url_override("hf://parsimony-dev/sdmx")
    url = f"{root}/sdmx_codelist_ecb_cl_freq"
    report = validate_catalog(
        url,
        None,
        allow_missing=allow_missing_remote(),
        catalog_root=root,
    )
    if report.skipped:
        pytest.skip(report.skip_reason)
    assert report.schema_ok
    assert report.entry_count > 0


def test_remote_catalog_marker_registered() -> None:
    """Sanity: remote catalog suite is opt-in."""
    assert not remote_catalogs_enabled()
