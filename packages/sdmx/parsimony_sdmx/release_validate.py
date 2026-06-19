"""Strict validation for SDMX catalog release roots."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast

from parsimony.catalog import Catalog
from parsimony.catalog.storage import read_meta
from parsimony.catalog.validation import ReleaseManifest, ReleaseManifestEntry, validate_catalog_snapshot
from pydantic import ValidationError

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.catalog_series import CATALOG_KIND, SERIES_AGENCIES, is_series_catalog
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES
from parsimony_sdmx.connectors.enumerate_datasets import datasets_namespace

VALIDATOR_VERSION = "sdmx_release_v1"

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
    codelist_count: int = 0
    structure_marker_count: int = 0

    def fail(self, message: str) -> None:
        self.ok = False
        self.errors.append(message)


def _git_commit_sha() -> str | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
        )
        return proc.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def validate_release_catalog_dir(catalog_dir: Path) -> list[str]:
    errors: list[str] = []
    errors.extend(release_artifact_violations(catalog_dir))
    try:
        validate_catalog_snapshot(catalog_dir)
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def validate_series_catalog(catalog_dir: Path) -> list[str]:
    errors = validate_release_catalog_dir(catalog_dir)
    if errors:
        return errors

    meta = read_meta(catalog_dir)
    sdmx = meta.sdmx or {}
    if sdmx.get("catalog_kind") != CATALOG_KIND:
        errors.append(f"{catalog_dir.name}: sdmx.catalog_kind != {CATALOG_KIND}")
    if int(sdmx.get("series_count") or 0) != meta.entry_count:
        errors.append(f"{catalog_dir.name}: series_count mismatch meta.entry_count")
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
        if name.startswith("sdmx_codelist_"):
            report.codelist_count += 1
            errs = validate_release_catalog_dir(child)
            for err in errs:
                report.fail(f"{name}: {err}")
        elif name.startswith("sdmx_structure_"):
            report.structure_marker_count += 1
            if not (child / "structure.json").is_file():
                report.fail(f"{name}: missing structure.json")
        elif name.startswith("sdmx_series_"):
            if not is_series_catalog(child):
                report.fail(f"{name}: not a v1 series catalog")
                continue
            errs = validate_series_catalog(child)
            for err in errs:
                report.fail(err)
            else:
                report.series_catalogs.append(name)
                if sample_search:
                    cat = Catalog.load(f"file://{child.resolve()}")
                    if len(cat.search("monthly", limit=1)) == 0 and len(cat) > 0:
                        report.warnings.append(f"{name}: sample search returned no hits")

    if require_series_agencies:
        for agency in SERIES_AGENCIES:
            count = sum(1 for ns in report.series_catalogs if f"sdmx_series_{agency.value.lower()}_" in ns)
            if count == 0:
                report.fail(f"no series catalogs for agency {agency.value}")

    debt_log = layout.debt_log
    if debt_log.is_file() and debt_log.stat().st_size > 0:
        report.fail(f"release root has unresolved build debt: {debt_log}")

    return report


def build_release_manifest(
    layout: BuildRoot,
    *,
    release_id: str,
    skipped_flows: list[dict[str, str]] | None = None,
) -> ReleaseManifest:
    entries: list[ReleaseManifestEntry] = []
    structure_marker_count = 0
    for child in sorted(layout.catalogs.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith("sdmx_structure_"):
            if (child / "structure.json").is_file():
                structure_marker_count += 1
            continue
        if not (child / "meta.json").is_file():
            continue
        try:
            meta = read_meta(child)
        except (OSError, ValidationError, ValueError):
            continue
        if child.name.startswith("sdmx_series_"):
            catalog_kind = "series"
        elif child.name.startswith("sdmx_datasets_"):
            catalog_kind = "datasets"
        elif child.name.startswith("sdmx_codelist_"):
            catalog_kind = "codelist"
        else:
            continue
        sdmx = meta.sdmx or {}
        agency_val = sdmx.get("agency")
        flow_val = sdmx.get("flow_id")
        entries.append(
            ReleaseManifestEntry(
                namespace=child.name,
                catalog_kind=cast(Literal["datasets", "codelist", "series"], catalog_kind),
                agency=str(agency_val) if agency_val is not None else None,
                flow_id=str(flow_val) if flow_val is not None else None,
                entry_count=meta.entry_count,
                content_sha256=meta.build.content_sha256,
                manifest_contract_sha256=meta.build.manifest_contract_sha256,
                path=str(child.relative_to(layout.root)),
            )
        )

    built_at = ""
    if entries:
        first_meta = read_meta(layout.catalogs / entries[0].namespace)
        built_at = first_meta.build.built_at.isoformat()

    return ReleaseManifest(
        release_id=release_id,
        built_at=built_at,
        package_commit_sha=_git_commit_sha(),
        validator_version=VALIDATOR_VERSION,
        agencies=[a.value for a in ALL_AGENCIES],
        dataset_catalogs=[e.namespace for e in entries if e.catalog_kind == "datasets"],
        codelist_count=sum(1 for e in entries if e.catalog_kind == "codelist"),
        structure_marker_count=structure_marker_count,
        series_catalog_count=sum(1 for e in entries if e.catalog_kind == "series"),
        skipped_flows=skipped_flows or [],
        catalogs=entries,
    )


def write_release_manifest(layout: BuildRoot, manifest: ReleaseManifest) -> Path:
    path = layout.root / "RELEASE_MANIFEST.json"
    path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return path
