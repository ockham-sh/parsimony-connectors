"""Credential resolution for maintainer catalog build scripts."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def connectors_repo_root(*, script_path: Path) -> Path:
    """Return parsimony-connectors root from ``packages/<pkg>/scripts/build_catalog.py``."""

    return script_path.resolve().parents[3]


def ensure_tooling_on_path(*, script_path: Path) -> Path:
    """Insert ``tooling/`` on ``sys.path`` so ``catalog_validate`` imports work."""

    repo_root = connectors_repo_root(script_path=script_path)
    tooling = repo_root / "tooling"
    tooling_str = str(tooling)
    if tooling_str not in sys.path:
        sys.path.insert(0, tooling_str)
    return repo_root


def resolve_api_key(
    *,
    cli_value: str | None,
    env_var: str,
    required: bool = False,
) -> str:
    """Resolve an API key from CLI flag or environment (never logged)."""

    key = (cli_value or os.environ.get(env_var, "")).strip()
    if required and not key:
        raise ValueError(f"Set --api-key or export {env_var}")
    return key
