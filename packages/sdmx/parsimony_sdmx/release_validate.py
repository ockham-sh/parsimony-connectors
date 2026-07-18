"""Strict validation for SDMX catalog release roots."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from parsimony.catalog import Catalog
from parsimony.catalog.validation import validate_catalog_snapshot

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.catalog_series import SERIES_AGENCIES, is_series_catalog
from parsimony_sdmx.core.agencies import ALL_AGENCIES
from parsimony_sdmx.core.namespaces import datasets_namespace

_FORBIDDEN_SUBDIRS = frozenset({"resolver"})
_FORBIDDEN_META_KEYS = frozenset({"store_kind", "migrated_from"})


def release_artifact_violations(catalog_dir: Path) -> list[str]:
    """Return release-blocking filesystem or metadata violations."""
    errors: list[str] = []
    for subdir in _FORBIDDEN_SUBDIRS:
        if (catalog_dir / subdir).exists():
            errors.append(f"forbidden subdir: {subdir}")
    meta_path = catalog_dir / "meta.json"
    if meta_path.is_file():
        try:
            import json

            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            for key in _FORBIDDEN_META_KEYS:
                if key in meta:
                    errors.append(f"forbidden meta key: {key}")
            if catalog_dir.name.startswith("sdmx_series_") and (catalog_dir / "entries.parquet").is_file():
                errors.append("forbidden file: entries.parquet")
        except (OSError, ValueError) as exc:
            errors.append(f"unreadable meta.json: {exc}")
    return errors


@dataclass
class ValidationReport:
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    dataset_catalogs: list[str] = field(default_factory=list)
    series_catalogs: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.ok = False
        self.errors.append(message)


def validate_release_catalog_dir(catalog_dir: Path) -> list[str]:
    errors: list[str] = []
    errors.extend(release_artifact_violations(catalog_dir))
    try:
        validate_catalog_snapshot(catalog_dir)
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def validate_release_root(
    layout: BuildRoot,
    *,
    require_all_agencies: bool = True,
    require_series_agencies: bool = True,
    sample_search: bool = False,
) -> ValidationReport:
    report = ValidationReport()
    catalogs = layout.catalogs

    for agency in ALL_AGENCIES:
        ns = datasets_namespace(agency)
        path = catalogs / ns
        if not path.is_dir():
            if require_all_agencies:
                report.fail(f"missing dataset catalog: {ns}")
            continue
        errs = validate_release_catalog_dir(path)
        for err in errs:
            report.fail(f"{ns}: {err}")
        else:
            report.dataset_catalogs.append(ns)
            try:
                Catalog.load(f"file://{path.resolve()}")
            except ValueError as exc:
                report.fail(f"{ns}: Catalog.load failed: {exc}")
            if sample_search:
                cat = Catalog.load(f"file://{path.resolve()}")
                if len(cat.search("data", limit=1)) == 0 and len(cat) > 0:
                    report.warnings.append(f"{ns}: sample search returned no hits")

    for child in sorted(catalogs.iterdir()):
        if not child.is_dir():
            continue
        name = child.name
        if name.startswith("sdmx_series_"):
            if not is_series_catalog(child):
                report.fail(f"{name}: not a v1 series catalog")
                continue
            errs = validate_release_catalog_dir(child)
            for err in errs:
                report.fail(f"{name}: {err}")
            else:
                report.series_catalogs.append(name)
                if sample_search:
                    cat = Catalog.load(f"file://{child.resolve()}")
                    if len(cat.search("monthly", limit=1)) == 0 and len(cat) > 0:
                        report.warnings.append(f"{name}: sample search returned no hits")
        elif not name.startswith("sdmx_datasets_"):
            report.fail(f"{name}: unexpected directory in release root (only series/datasets catalogs ship)")

    if require_series_agencies:
        for agency in SERIES_AGENCIES:
            count = sum(1 for ns in report.series_catalogs if f"sdmx_series_{agency.value.lower()}_" in ns)
            if count == 0:
                report.fail(f"no series catalogs for agency {agency.value}")

    debt_log = layout.debt_log
    if debt_log.is_file() and debt_log.stat().st_size > 0:
        report.fail(f"release root has unresolved build debt: {debt_log}")

    return report
