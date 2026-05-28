"""Shared helpers for remote catalog validation tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOLING_DIR = REPO_ROOT / "tooling"
VALIDATE_SCRIPT = TOOLING_DIR / "validate_catalog.py"


def allow_missing_remote() -> bool:
    return os.environ.get("PARSIMONY_ALLOW_MISSING_REMOTE_CATALOG", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def remote_catalogs_enabled() -> bool:
    return os.environ.get("PARSIMONY_RUN_REMOTE_CATALOGS", "").strip() == "1"


def skip_unless_remote_catalogs() -> None:
    if not remote_catalogs_enabled():
        pytest.skip("set PARSIMONY_RUN_REMOTE_CATALOGS=1 to run remote catalog probes")


def catalog_url_override(default: str) -> str:
    return os.environ.get("PARSIMONY_CATALOG_URL", default).strip() or default


def import_catalog_validate():
    """Import maintainer validation helpers (scripts/ is not an installed package)."""
    import sys

    if str(TOOLING_DIR) not in sys.path:
        sys.path.insert(0, str(TOOLING_DIR))
    from catalog_validate.fixtures import load_queries_file
    from catalog_validate.registry import PROVIDER_SPECS, SDMX_QUERIES_FILE
    from catalog_validate.runner import validate_catalog

    return load_queries_file, PROVIDER_SPECS, SDMX_QUERIES_FILE, validate_catalog
