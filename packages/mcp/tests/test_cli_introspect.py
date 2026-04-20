"""Tests for the post-install plugin introspection layer."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from parsimony_mcp.cli._introspect import (
    IntrospectionResult,
    detect_env_drift,
    introspect_packages,
)


def _write_fake_module(tmp_path: Path, name: str, body: str) -> Path:
    """Write a throwaway package that introspection will import.

    Returns the ``sys.path`` entry needed to resolve ``name``.
    """
    pkg_dir = tmp_path / name
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / "__init__.py").write_text(textwrap.dedent(body), encoding="utf-8")
    return tmp_path


def test_introspect_success_captures_env_vars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sys_path = _write_fake_module(
        tmp_path,
        "parsimony_fake",
        """
        ENV_VARS = {"api_key": "FAKE_API_KEY"}
        PROVIDER_METADATA = {"homepage": "https://example.test"}
        """,
    )
    monkeypatch.setenv("PYTHONPATH", str(sys_path))

    results = introspect_packages(["parsimony-fake"], target_dir=tmp_path)
    assert len(results) == 1
    res = results[0]
    assert res.ok is True, res.warning
    assert res.env_vars == {"api_key": "FAKE_API_KEY"}
    assert res.metadata["homepage"] == "https://example.test"


def test_introspect_handles_import_error_as_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sys_path = _write_fake_module(
        tmp_path,
        "parsimony_broken",
        """
        raise RuntimeError('deliberate failure at import time')
        """,
    )
    monkeypatch.setenv("PYTHONPATH", str(sys_path))

    results = introspect_packages(["parsimony-broken"], target_dir=tmp_path)
    assert results[0].ok is False
    assert "deliberate failure" in (results[0].warning or "")


def test_introspect_unknown_module_yields_warning(tmp_path: Path) -> None:
    # Nothing written for parsimony-unknown; subprocess will raise
    # ModuleNotFoundError and we should downgrade to a warning.
    results = introspect_packages(
        ["parsimony-unknown-dne"], target_dir=tmp_path
    )
    assert results[0].ok is False
    assert results[0].warning is not None


# ---------------------------------------------------------------------- drift


def test_detect_drift_flags_mismatch() -> None:
    results = [
        IntrospectionResult(
            package="parsimony-fred",
            module="parsimony_fred",
            ok=True,
            env_vars={"api_key": "FRED_API_KEY_RENAMED"},
            metadata={},
            warning=None,
        ),
    ]
    warnings = detect_env_drift(
        results,
        registry_env_vars={"parsimony-fred": {"FRED_API_KEY"}},
    )
    assert len(warnings) == 1
    assert "FRED_API_KEY" in warnings[0]


def test_detect_drift_silent_on_match() -> None:
    results = [
        IntrospectionResult(
            package="parsimony-fred",
            module="parsimony_fred",
            ok=True,
            env_vars={"api_key": "FRED_API_KEY"},
            metadata={},
            warning=None,
        ),
    ]
    warnings = detect_env_drift(
        results,
        registry_env_vars={"parsimony-fred": {"FRED_API_KEY"}},
    )
    assert warnings == []


def test_detect_drift_propagates_import_warning() -> None:
    results = [
        IntrospectionResult(
            package="parsimony-broken",
            module="parsimony_broken",
            ok=False,
            env_vars={},
            metadata={},
            warning="parsimony-broken: import failed: RuntimeError: boom",
        ),
    ]
    warnings = detect_env_drift(results, registry_env_vars={})
    assert "boom" in warnings[0]


