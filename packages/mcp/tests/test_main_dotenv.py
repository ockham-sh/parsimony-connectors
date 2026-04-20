"""Tests for the .env load-at-startup behavior in __main__."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from parsimony_mcp.__main__ import _load_project_env


def test_loads_env_from_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SMOKE_TEST_KEY=loaded_from_dotenv\n")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SMOKE_TEST_KEY", raising=False)

    loaded = _load_project_env()
    assert loaded == env_file
    assert os.environ.get("SMOKE_TEST_KEY") == "loaded_from_dotenv"


def test_walks_upward_to_find_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """.env may live above CWD — mirrors how users run from a subdir."""
    (tmp_path / ".env").write_text("SMOKE_PARENT_KEY=from_parent\n")
    subdir = tmp_path / "src" / "nested"
    subdir.mkdir(parents=True)

    monkeypatch.chdir(subdir)
    monkeypatch.delenv("SMOKE_PARENT_KEY", raising=False)

    loaded = _load_project_env()
    assert loaded == tmp_path / ".env"
    assert os.environ.get("SMOKE_PARENT_KEY") == "from_parent"


def test_no_env_returns_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert _load_project_env() is None


def test_existing_env_wins_over_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-existing os.environ must not be clobbered — host-managed secrets win.

    The agent host's `mcpServers.*.env` block is the security-preferred
    path (per Task 11 — refuse-inlined-secrets + env-ref-only); .env
    is a convenience fallback, not an override.
    """
    env_file = tmp_path / ".env"
    env_file.write_text("SMOKE_OVERRIDE_KEY=from_dotenv\n")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SMOKE_OVERRIDE_KEY", "from_shell")

    _load_project_env()
    assert os.environ["SMOKE_OVERRIDE_KEY"] == "from_shell"


def test_project_dir_env_var_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PARSIMONY_MCP_PROJECT_DIR pins the search root regardless of CWD.

    Useful when the agent host spawns the server from an unpredictable
    working directory but the user knows exactly which project the
    server should pick up.
    """
    project = tmp_path / "project"
    project.mkdir()
    (project / ".env").write_text("SMOKE_PROJECT_KEY=from_pinned_dir\n")

    unrelated = tmp_path / "somewhere_else"
    unrelated.mkdir()

    monkeypatch.chdir(unrelated)
    monkeypatch.setenv("PARSIMONY_MCP_PROJECT_DIR", str(project))
    monkeypatch.delenv("SMOKE_PROJECT_KEY", raising=False)

    loaded = _load_project_env()
    assert loaded == project / ".env"
    assert os.environ.get("SMOKE_PROJECT_KEY") == "from_pinned_dir"
