"""Post-install plugin introspection in an isolated subprocess.

After ``uv sync`` finishes, each connector's declared env-vars
should match what the registry advertised. Drift means one of:

* The registry is stale (the connector's source updated but the
  monorepo's ``registry.json`` was not regenerated).
* The connector has a side-effect-heavy ``__init__`` that doesn't
  play well with static AST extraction.

Either way we WARN and move on — it's CI signal, not a user
problem. Never crash the wizard over introspection drift; the
user doesn't care.

Why a subprocess per plugin: the kernel's whole premise is that
plugins discover at import. Some plugins load pandas, connect to
Redis, spawn threads. We do NOT want those side effects in the
wizard's process, and we categorically do NOT want a
malicious-or-broken plugin to hang the wizard indefinitely. A
subprocess with a 10s wall clock timeout gives us:

* Memory isolation (a plugin that leaks 1 GB only leaks for 1 run).
* Crash isolation (segfault in a native extension kills one subprocess).
* Termination guarantee (the timeout kills runaway imports).

Output contract: the subprocess prints exactly one JSON object on
stdout. Anything else (traceback, warning, print) goes to stderr
and is captured for the WARN log. A plugin whose subprocess
returns non-zero, times out, or emits invalid JSON produces a
WARN entry with the failure mode — and the wizard proceeds.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

_LOG = logging.getLogger("parsimony_mcp.cli.introspect")

_TIMEOUT_SECONDS: float = 10.0

# The subprocess imports a single package and emits its declared
# metadata as JSON. The snippet is inlined rather than shipped as a
# helper script because (a) we don't want it on PATH and (b) users
# debugging failures should see exactly what the wizard ran.
_INTROSPECT_SNIPPET = """\
import importlib, json, sys

try:
    module_name = sys.argv[1]
    m = importlib.import_module(module_name)
    env_vars = getattr(m, 'ENV_VARS', {}) or {}
    metadata = getattr(m, 'PROVIDER_METADATA', {}) or {}
    if not isinstance(env_vars, dict):
        raise TypeError(f'ENV_VARS must be dict, got {type(env_vars).__name__}')
    if not isinstance(metadata, dict):
        raise TypeError(f'PROVIDER_METADATA must be dict, got {type(metadata).__name__}')
    # Stringify non-JSON-safe metadata values rather than drop them;
    # the introspection caller only uses the shape, not deep types.
    _JSON_SAFE = (str, int, float, bool, list, dict, tuple, type(None))
    safe_metadata = {
        k: (v if isinstance(v, _JSON_SAFE) else repr(v))
        for k, v in metadata.items()
    }
    out = {'ok': True, 'env_vars': env_vars, 'metadata': safe_metadata}
except Exception as exc:
    out = {'ok': False, 'error_type': type(exc).__name__, 'error_message': str(exc)}

json.dump(out, sys.stdout)
"""


@dataclass(frozen=True, slots=True)
class IntrospectionResult:
    """One plugin's introspection outcome.

    ``ok`` is False for any failure mode (import error, timeout,
    non-JSON output, non-zero exit); the wizard treats all of them
    as WARN-and-continue.
    """

    package: str
    module: str
    ok: bool
    env_vars: dict[str, str]
    metadata: dict[str, object]
    warning: str | None


def introspect_packages(
    packages: Iterable[str],
    *,
    target_dir: Path,
    python_executable: str | None = None,
) -> list[IntrospectionResult]:
    """Introspect every installed ``parsimony-<name>`` distribution.

    Each ``packages`` entry is a PyPI distribution name
    (``parsimony-fred``). The subprocess imports the corresponding
    Python module (``parsimony_fred``) and emits JSON.

    ``python_executable`` defaults to the project's ``.venv`` python
    if it exists, falling back to the parent's ``sys.executable``.
    Running against the just-synced venv is the point: that's what
    the user's coding agent will eventually launch.
    """
    python = python_executable or _resolve_project_python(target_dir)
    results: list[IntrospectionResult] = []
    for pkg in packages:
        module_name = pkg.removeprefix("parsimony-").replace("-", "_")
        module_name = f"parsimony_{module_name}"
        result = _introspect_one(pkg, module_name, python=python)
        results.append(result)
    return results


def _resolve_project_python(target_dir: Path) -> str:
    """Find the Python to use for introspection.

    Prefer the ``.venv`` ``uv sync`` just created. Only fall back to
    the wizard's own interpreter if the venv doesn't exist (tests,
    ``--skip-install`` runs).
    """
    candidates = [
        target_dir / ".venv" / "bin" / "python",
        target_dir / ".venv" / "Scripts" / "python.exe",
    ]
    for cand in candidates:
        if cand.is_file():
            return str(cand)
    return sys.executable


def _introspect_one(
    package: str, module: str, *, python: str
) -> IntrospectionResult:
    cmd = [python, "-c", _INTROSPECT_SNIPPET, module]
    try:
        proc = subprocess.run(  # noqa: S603 — cmd is [python-exe, '-c', static-snippet, module-name]; no shell, module name is validated by caller
            cmd,
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return IntrospectionResult(
            package=package,
            module=module,
            ok=False,
            env_vars={},
            metadata={},
            warning=f"{package}: import timed out after {_TIMEOUT_SECONDS}s (eager side effect?)",
        )
    except (OSError, FileNotFoundError) as exc:
        return IntrospectionResult(
            package=package,
            module=module,
            ok=False,
            env_vars={},
            metadata={},
            warning=f"{package}: could not spawn python subprocess: {exc}",
        )

    if proc.returncode != 0:
        return IntrospectionResult(
            package=package,
            module=module,
            ok=False,
            env_vars={},
            metadata={},
            warning=(
                f"{package}: introspection exited {proc.returncode}. "
                f"stderr: {proc.stderr[:400] if proc.stderr else '<empty>'}"
            ),
        )

    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return IntrospectionResult(
            package=package,
            module=module,
            ok=False,
            env_vars={},
            metadata={},
            warning=f"{package}: subprocess emitted non-JSON output",
        )

    if not payload.get("ok"):
        return IntrospectionResult(
            package=package,
            module=module,
            ok=False,
            env_vars={},
            metadata={},
            warning=(
                f"{package}: import failed: "
                f"{payload.get('error_type')}: {payload.get('error_message')}"
            ),
        )

    return IntrospectionResult(
        package=package,
        module=module,
        ok=True,
        env_vars=dict(payload.get("env_vars") or {}),
        metadata=dict(payload.get("metadata") or {}),
        warning=None,
    )


def detect_env_drift(
    results: Iterable[IntrospectionResult],
    *,
    registry_env_vars: dict[str, set[str]],
) -> list[str]:
    """Compare post-install env vars against what the registry declared.

    ``registry_env_vars`` is a mapping of ``{package: {ENV_VAR, ...}}``.
    The introspection reports the installed value; divergence means
    the registry needs regenerating or the connector changed. We
    return a list of human-readable WARN messages.
    """
    warnings: list[str] = []
    for result in results:
        if not result.ok:
            if result.warning:
                warnings.append(result.warning)
            continue
        declared = registry_env_vars.get(result.package, set())
        installed = set(result.env_vars.values())
        if declared != installed:
            warnings.append(
                f"{result.package}: registry declared {sorted(declared)} but "
                f"installed package declares {sorted(installed)}. "
                f"Regenerate registry.json."
            )
    return warnings
