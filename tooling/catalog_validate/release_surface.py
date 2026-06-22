"""Release surface definitions and local/remote hygiene audits for catalog snapshots.

Operator-only: defines which HF artifacts are runtime-loadable vs build-time excess.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from catalog_validate.registry import MACRO_CATALOG_PROVIDER_IDS, PROVIDER_SPECS

CANONICAL_CATALOG_ROOT = Path("/tmp/parsimony-catalogs-v1")

# Hugging Face multi-bundle repos must not expose a loadable catalog at repo root.
MULTI_BUNDLE_REPOS: frozenset[str] = frozenset({"boj", "sdmx"})

# SDMX build-time markers — never publish to HF.
SDMX_STRUCTURE_PREFIX = "sdmx_structure_"

# SDMX runtime bundle prefixes (publishable).
SDMX_RUNTIME_PREFIXES: tuple[str, ...] = (
    "sdmx_datasets_",
    "sdmx_codelist_",
    "sdmx_series_",
)

# BoJ runtime bundle prefixes.
BOJ_RUNTIME_PREFIXES: tuple[str, ...] = (
    "boj_databases",
    "boj_series_",
)

# Files allowed at multi-bundle repo root (not catalog bundles).
MULTI_BUNDLE_ROOT_ALLOWLIST: frozenset[str] = frozenset(
    {
        ".gitattributes",
        "README.md",
        "RELEASE_MANIFEST.json",
    }
)

BundleKind = Literal["flat", "boj", "sdmx_datasets", "sdmx_codelist", "sdmx_series", "excess"]


@dataclass(frozen=True, slots=True)
class BundleSpec:
    provider: str
    name: str
    kind: BundleKind
    local_rel: str
    hf_url: str


@dataclass
class LocalAuditReport:
    catalog_root: Path
    bundles: list[BundleSpec] = field(default_factory=list)
    excess: list[str] = field(default_factory=list)
    missing_required: list[str] = field(default_factory=list)
    ok: bool = True

    def fail(self, message: str) -> None:
        self.ok = False
        self.missing_required.append(message)


def _has_catalog_meta(path: Path) -> bool:
    return (path / "meta.json").is_file()


def classify_bundle_name(name: str) -> BundleKind:
    if name.startswith(SDMX_STRUCTURE_PREFIX):
        return "excess"
    if name.startswith("sdmx_datasets_"):
        return "sdmx_datasets"
    if name.startswith("sdmx_codelist_"):
        return "sdmx_codelist"
    if name.startswith("sdmx_series_"):
        return "sdmx_series"
    if name == "boj_databases" or name.startswith("boj_series_"):
        return "boj"
    return "flat"


def is_publishable_local_bundle(path: Path) -> bool:
    """Return True when *path* is a runtime catalog snapshot suitable for HF upload."""
    if not _has_catalog_meta(path):
        return False
    kind = classify_bundle_name(path.name)
    return kind != "excess"


def iter_publishable_bundles(catalog_root: Path) -> list[Path]:
    """Walk *catalog_root* and return publishable bundle directories."""
    bundles: list[Path] = []
    if not catalog_root.is_dir():
        return bundles

    for provider in sorted(MACRO_CATALOG_PROVIDER_IDS):
        flat = catalog_root / provider
        if _has_catalog_meta(flat):
            bundles.append(flat)

    for sub in sorted(catalog_root.iterdir()):
        if not sub.is_dir():
            continue
        if sub.name in {"boj", "sdmx"}:
            for ns in sorted(sub.iterdir()):
                if ns.is_dir() and is_publishable_local_bundle(ns):
                    bundles.append(ns)
            continue
        if _has_catalog_meta(sub) and sub.name not in MACRO_CATALOG_PROVIDER_IDS and is_publishable_local_bundle(sub):
            bundles.append(sub)

    return bundles


def audit_local_root(catalog_root: Path, *, require_bdf: bool = True) -> LocalAuditReport:
    """Audit a local pre-warm root against the initial-release surface."""
    report = LocalAuditReport(catalog_root=catalog_root.resolve())

    for provider in sorted(MACRO_CATALOG_PROVIDER_IDS):
        if provider == "boj":
            continue
        spec = PROVIDER_SPECS[provider]
        flat = catalog_root / provider
        if _has_catalog_meta(flat):
            report.bundles.append(
                BundleSpec(
                    provider=provider,
                    name=provider,
                    kind="flat",
                    local_rel=str(flat.relative_to(catalog_root)),
                    hf_url=spec.default_url,
                )
            )
        elif provider == "bdf" and require_bdf:
            report.fail(f"missing required flat bundle: {provider}")
        elif provider != "bdf":
            report.fail(f"missing flat bundle: {provider}")

    boj_root = catalog_root / "boj"
    if boj_root.is_dir():
        for ns in sorted(boj_root.iterdir()):
            if not ns.is_dir():
                continue
            if _has_catalog_meta(ns):
                if is_publishable_local_bundle(ns):
                    report.bundles.append(
                        BundleSpec(
                            provider="boj",
                            name=ns.name,
                            kind="boj",
                            local_rel=str(ns.relative_to(catalog_root)),
                            hf_url=f"hf://parsimony-dev/boj/{ns.name}",
                        )
                    )
                continue
            report.excess.append(str(ns.relative_to(catalog_root)))
        if not (boj_root / "boj_databases" / "meta.json").is_file():
            report.fail("missing boj/boj_databases")
    else:
        report.fail("missing boj/")

    sdmx_root = catalog_root / "sdmx"
    if sdmx_root.is_dir():
        for name in ("meta.json", "series.parquet", "entries.parquet"):
            if (sdmx_root / name).is_file():
                report.excess.append(f"sdmx/{name}")
        for ns in sorted(sdmx_root.iterdir()):
            if not ns.is_dir():
                if ns.name not in MULTI_BUNDLE_ROOT_ALLOWLIST:
                    report.excess.append(f"sdmx/{ns.name}")
                continue
            kind = classify_bundle_name(ns.name)
            if kind == "excess":
                report.excess.append(str(ns.relative_to(catalog_root)))
                continue
            if _has_catalog_meta(ns) and is_publishable_local_bundle(ns):
                report.bundles.append(
                    BundleSpec(
                        provider="sdmx",
                        name=ns.name,
                        kind=kind,  # type: ignore[arg-type]
                        local_rel=str(ns.relative_to(catalog_root)),
                        hf_url=f"hf://parsimony-dev/sdmx/{ns.name}",
                    )
                )
        for agency in ("ecb", "estat", "imf_data", "wb_wdi"):
            ds = sdmx_root / f"sdmx_datasets_{agency}"
            if not _has_catalog_meta(ds):
                report.fail(f"missing sdmx/{ds.name}")
    else:
        report.fail("missing sdmx/")

    if report.missing_required:
        report.ok = False
    return report


def bundle_index_paths(bundle_dir: Path) -> set[str]:
    """Return relative paths referenced by a catalog snapshot's index tree."""
    meta_path = bundle_dir / "meta.json"
    if not meta_path.is_file():
        return set()
    raw = json.loads(meta_path.read_text(encoding="utf-8"))
    index_fields: dict[str, str] = raw.get("index_fields") or {}
    paths: set[str] = {"meta.json"}
    rows_name = (raw.get("backend") or {}).get("rows_filename") or "entries.parquet"
    if (bundle_dir / rows_name).is_file():
        paths.add(rows_name)
    for field_name, _kind in index_fields.items():
        base = bundle_dir / "indexes" / field_name
        if not base.is_dir():
            continue
        for p in base.rglob("*"):
            if p.is_file():
                paths.add(str(p.relative_to(bundle_dir)))
    return paths


def unreferenced_files_in_bundle(bundle_dir: Path) -> list[str]:
    """Find on-disk files not reachable from meta.json (stale index dirs, etc.)."""
    referenced = bundle_index_paths(bundle_dir)
    extras: list[str] = []
    for p in bundle_dir.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(bundle_dir))
        if rel not in referenced:
            extras.append(rel)
    return sorted(extras)
