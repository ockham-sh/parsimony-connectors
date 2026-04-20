#!/usr/bin/env python3
"""Generate ``registry.json`` from the workspace packages — AST-parse only.

Why AST parse and not ``importlib.import_module``:

* Importing a plugin runs its top-level code inside this process (CI
  runner). Plugins with eager-import side effects — network calls,
  heavy model loads, state-holding singletons — block or crash the
  generator, and a malicious merged PR could execute arbitrary code
  with the CI environment's OIDC publish token in scope. AST parse
  refuses anything that isn't a literal ``dict`` / ``list`` / ``str`` /
  ``int`` / ``float`` / ``bool`` / ``None`` / ``tuple`` / ``set``.
* The constraint is load-bearing in both directions: it forces
  connector authors to keep declarative metadata literal, and it
  isolates this generator from every connector's transitive
  dependency tree.

Invocation::

    python tools/gen_registry.py            # Regenerate and overwrite registry.json
    python tools/gen_registry.py --check    # Fail if committed registry.json is stale
"""

from __future__ import annotations

import argparse
import ast
import difflib
import importlib.util
import json
import sys
import tomllib
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_schema_module() -> ModuleType:
    """Load ``registry_schema.py`` directly, without importing its package.

    We deliberately bypass the ``parsimony_mcp`` package hierarchy —
    importing ``parsimony_mcp`` eagerly pulls the MCP SDK and pandas
    (via ``parsimony_mcp/__init__.py``), which the generator doesn't
    need and shouldn't need. This preserves the "single owner" rule
    (one Pydantic model) without forcing the generator to install the
    consumer's runtime deps.
    """
    schema_path = _REPO_ROOT / "packages" / "mcp" / "parsimony_mcp" / "cli" / "registry_schema.py"
    spec = importlib.util.spec_from_file_location(
        "_parsimony_mcp_registry_schema", schema_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load registry_schema from {schema_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_schema = _load_schema_module()
SCHEMA_VERSION: int = _schema.SCHEMA_VERSION
ConnectorPackage = _schema.ConnectorPackage
EnvVar = _schema.EnvVar
Registry = _schema.Registry

_PACKAGES_DIR = _REPO_ROOT / "packages"
_REGISTRY_PATH = _REPO_ROOT / "registry.json"

# Decorator names that carry `tags=[...]` on connector module functions.
_DECORATOR_NAMES = frozenset({"connector", "enumerator", "loader"})


class RegistryGenError(Exception):
    """A connector's metadata cannot be statically extracted.

    Raised with an actionable prose message that gets printed to
    stderr when the generator aborts. Connector authors fix by moving
    computed values out of ``ENV_VARS`` / ``PROVIDER_METADATA`` so
    they're expressible as module-level literals.
    """


def _iter_package_dirs() -> Iterable[Path]:
    """Yield every ``packages/<name>/`` directory with a pyproject.toml."""
    if not _PACKAGES_DIR.is_dir():
        return
    for child in sorted(_PACKAGES_DIR.iterdir()):
        if child.is_dir() and (child / "pyproject.toml").is_file():
            yield child


def _read_pyproject(pkg_dir: Path) -> dict[str, Any]:
    with (pkg_dir / "pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def _opt_conformance_skip(pyproject: dict[str, Any]) -> bool:
    return bool(
        pyproject.get("tool", {}).get("parsimony", {}).get("conformance", {}).get("skip")
    )


def _literal(node: ast.AST, where: str) -> Any:
    """Safely evaluate ``node`` as a Python literal, or raise."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError) as exc:
        msg = (
            f"{where} is not a module-level literal. "
            f"Registry metadata must be statically analyzable; "
            f"move computed values out of ENV_VARS / PROVIDER_METADATA / "
            f"@connector(tags=...)."
        )
        raise RegistryGenError(msg) from exc


def _find_top_level_value(module: ast.Module, name: str) -> ast.AST | None:
    """Return the RHS node of a top-level assignment to ``name``, or None."""
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.value is not None
        ):
            return node.value
    return None


def _best_effort_dict(node: ast.AST, where: str) -> dict[str, Any]:
    """Extract whichever dict entries are literal; warn on the rest.

    Used for ``PROVIDER_METADATA`` where older connectors embed
    non-literal values (computed version strings, generator
    expressions). ENV_VARS and decorator tags stay strict — they're
    simple enough that there's no excuse for non-literal values.
    """
    if not isinstance(node, ast.Dict):
        _warn(f"{where} is not a dict literal; skipping PROVIDER_METADATA entirely")
        return {}
    out: dict[str, Any] = {}
    for key_node, val_node in zip(node.keys, node.values, strict=False):
        if key_node is None:  # **spread
            _warn(f"{where} contains a **-spread entry; skipping")
            continue
        try:
            key = ast.literal_eval(key_node)
        except (ValueError, SyntaxError):
            _warn(f"{where}: non-literal key; skipping")
            continue
        if not isinstance(key, str):
            _warn(f"{where}: non-string key {key!r}; skipping")
            continue
        try:
            out[key] = ast.literal_eval(val_node)
        except (ValueError, SyntaxError):
            _warn(f"{where}['{key}']: non-literal value; dropping from registry")
    return out


def _warn(message: str) -> None:
    sys.stderr.write(f"::warning::{message}\n")


def _extract_decorator_tags(module: ast.Module, pkg_name: str) -> tuple[str, ...]:
    """Collect the union of ``tags=[...]`` across every decorator call.

    Looks for @connector / @enumerator / @loader calls — whether used
    as ``@connector(...)`` or ``@parsimony.connector(...)``. Only
    top-level functions are considered; nested-def tags don't ship.
    """
    tags: set[str] = set()
    for node in module.body:
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue
            deco_name = _decorator_simple_name(deco)
            if deco_name not in _DECORATOR_NAMES:
                continue
            for kw in deco.keywords:
                if kw.arg == "tags":
                    value = _literal(kw.value, f"{pkg_name}:{node.name}:tags")
                    if isinstance(value, list | tuple | set):
                        tags.update(str(t) for t in value)
    return tuple(sorted(tags))


def _decorator_simple_name(deco: ast.Call) -> str:
    """Return ``connector`` from ``@connector(...)`` or ``@x.connector(...)``."""
    func = deco.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _build_env_vars(env_vars_map: dict[str, str] | None) -> tuple[EnvVar, ...]:
    """Convert ``{"api_key": "FRED_API_KEY"}`` into a list of ``EnvVar``."""
    if not env_vars_map:
        return ()
    if not isinstance(env_vars_map, dict):
        raise RegistryGenError(
            f"ENV_VARS must be a dict[str, str]; got {type(env_vars_map).__name__}"
        )
    out: list[EnvVar] = []
    for dep_name, env_name in sorted(env_vars_map.items()):
        if not isinstance(env_name, str) or not env_name:
            raise RegistryGenError(
                f"ENV_VARS['{dep_name}'] must be a non-empty string; got {env_name!r}"
            )
        out.append(EnvVar(name=env_name, required=True))
    return tuple(out)


def _build_display(pypi_name: str, provider_metadata: dict[str, Any] | None) -> str:
    """Short friendly name for the menu.

    Priority: ``PROVIDER_METADATA['display']`` if present → fallback to
    the PyPI name with ``parsimony-`` stripped and dashes upper-cased
    by convention. ``parsimony-fred`` → ``FRED``.
    """
    if provider_metadata and isinstance(provider_metadata.get("display"), str):
        display = provider_metadata["display"].strip()
        if display:
            return display
    stem = pypi_name.removeprefix("parsimony-")
    return stem.upper().replace("-", " ")


def _build_package_record(pkg_dir: Path) -> ConnectorPackage | None:
    """Build a registry entry for ``pkg_dir``; return None if skipped."""
    pyproject = _read_pyproject(pkg_dir)
    if _opt_conformance_skip(pyproject):
        return None

    project = pyproject.get("project", {})
    pypi_name: str = project.get("name", "")
    if not pypi_name:
        raise RegistryGenError(f"{pkg_dir.name}: [project].name missing in pyproject.toml")
    summary: str = project.get("description", "").strip() or pypi_name

    module_dirname = pypi_name.removeprefix("parsimony-").replace("-", "_")
    init_path = pkg_dir / f"parsimony_{module_dirname}" / "__init__.py"
    if not init_path.is_file():
        raise RegistryGenError(
            f"{pypi_name}: expected module __init__.py at {init_path.relative_to(_REPO_ROOT)} — "
            f"PyPI-name / module-name mismatch?"
        )

    source = init_path.read_text(encoding="utf-8")
    module = ast.parse(source, filename=str(init_path))

    # ENV_VARS: strict — simple dict[str, str], no excuse for non-literal
    env_vars_node = _find_top_level_value(module, "ENV_VARS")
    env_vars_map: dict[str, str] | None = None
    if env_vars_node is not None:
        env_vars_map = _literal(env_vars_node, f"{pypi_name}:ENV_VARS")

    # PROVIDER_METADATA: best-effort — older connectors (e.g. sdmx) embed
    # runtime-computed values. Extract literal keys, warn and drop the rest.
    # Track B follow-up: migrate all PROVIDER_METADATA to pure literals
    # and tighten this path back to strict.
    provider_metadata: dict[str, Any] = {}
    pm_node = _find_top_level_value(module, "PROVIDER_METADATA")
    if pm_node is not None:
        provider_metadata = _best_effort_dict(pm_node, f"{pypi_name}:PROVIDER_METADATA")

    tags = _extract_decorator_tags(module, pypi_name)

    return ConnectorPackage(
        package=pypi_name,
        display=_build_display(pypi_name, provider_metadata),
        summary=summary,
        homepage=_str_or_none(provider_metadata.get("homepage")),
        pricing=_str_or_none(provider_metadata.get("pricing")),
        rate_limits=_str_or_none(provider_metadata.get("rate_limits")),
        tags=tags,
        env_vars=_build_env_vars(env_vars_map),
    )


def _str_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _build_registry() -> Registry:
    connectors: list[ConnectorPackage] = []
    for pkg_dir in _iter_package_dirs():
        record = _build_package_record(pkg_dir)
        if record is not None:
            connectors.append(record)
    # Deterministic order for reproducible output
    connectors.sort(key=lambda c: c.package)
    return Registry(schema_version=SCHEMA_VERSION, connectors=tuple(connectors))


def _serialize(registry: Registry) -> str:
    # ``exclude_none=True`` drops optional fields that aren't set —
    # ``homepage``, ``pricing``, ``rate_limits`` on a connector package
    # and ``get_url`` on an env var. The consumer treats missing and
    # null identically (both become ``None`` after pydantic load), so
    # the nulls are pure noise. Skipping them keeps registry.json
    # readable and shrinks the wire payload.
    payload = registry.model_dump(mode="json", exclude_none=True)
    # Stable, diff-friendly output: 2-space indent, sorted within each record
    return json.dumps(payload, indent=2, sort_keys=False) + "\n"


def _cmd_write(argv: argparse.Namespace) -> int:
    registry = _build_registry()
    _REGISTRY_PATH.write_text(_serialize(registry), encoding="utf-8")
    print(
        f"wrote {_REGISTRY_PATH.relative_to(_REPO_ROOT)} "
        f"({len(registry.connectors)} connectors, schema v{registry.schema_version})"
    )
    return 0


def _cmd_check(argv: argparse.Namespace) -> int:
    fresh = _serialize(_build_registry())
    if not _REGISTRY_PATH.is_file():
        sys.stderr.write(
            f"::error::{_REGISTRY_PATH.relative_to(_REPO_ROOT)} is missing — "
            f"run `python tools/gen_registry.py` and commit the result.\n"
        )
        return 1
    committed = _REGISTRY_PATH.read_text(encoding="utf-8")
    if fresh == committed:
        print(f"{_REGISTRY_PATH.relative_to(_REPO_ROOT)} is up to date")
        return 0
    sys.stderr.write(
        f"::error::{_REGISTRY_PATH.relative_to(_REPO_ROOT)} is stale. "
        f"Run `python tools/gen_registry.py` and commit the result.\n\n"
    )
    diff = difflib.unified_diff(
        committed.splitlines(keepends=True),
        fresh.splitlines(keepends=True),
        fromfile="committed registry.json",
        tofile="freshly generated",
    )
    sys.stderr.writelines(diff)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate or verify registry.json from the workspace packages."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if committed registry.json is out of sync (CI mode).",
    )
    args = parser.parse_args(argv)
    try:
        return _cmd_check(args) if args.check else _cmd_write(args)
    except RegistryGenError as exc:
        sys.stderr.write(f"::error::{exc}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
