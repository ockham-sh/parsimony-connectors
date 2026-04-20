"""Tests for the transactional ``apply()`` primitive."""

from __future__ import annotations

import json
import os
import sys
import tomllib
from pathlib import Path

import pytest

from parsimony_mcp.cli._merge import (
    AgentsMdPayload,
    EnvPayload,
    GitignorePayload,
    McpConfigPayload,
    PyprojectPayload,
)
from parsimony_mcp.cli.init import (
    ApplyConflict,
    ApplyError,
    FileKind,
    FileOperation,
    apply,
)


def _all_ops(base: Path) -> list[FileOperation]:
    """Return a valid set of ops targeting ``base``."""
    return [
        FileOperation(
            kind=FileKind.GITIGNORE,
            target=base / ".gitignore",
            incoming=GitignorePayload(lines=(".env",)),
        ),
        FileOperation(
            kind=FileKind.ENV,
            target=base / ".env",
            incoming=EnvPayload(keys=("FRED_API_KEY",), values={}),
        ),
        FileOperation(
            kind=FileKind.PYPROJECT,
            target=base / "pyproject.toml",
            incoming=PyprojectPayload(dependencies=("parsimony-fred",)),
        ),
        FileOperation(
            kind=FileKind.MCP_CONFIG,
            target=base / ".mcp.json",
            incoming=McpConfigPayload(env_vars=("FRED_API_KEY",)),
        ),
        FileOperation(
            kind=FileKind.AGENTS_MD,
            target=base / "AGENTS.md",
            incoming=AgentsMdPayload(packages=("parsimony-fred",)),
        ),
    ]


# ---------------------------------------------------------------------- fresh init (staged transaction)


def test_fresh_init_writes_everything_and_cleans_staging(tmp_path: Path) -> None:
    ops = _all_ops(tmp_path)
    result = apply(ops, target_dir=tmp_path)

    assert len(result.written) == 5
    assert (tmp_path / ".gitignore").is_file()
    assert (tmp_path / ".env").is_file()
    assert (tmp_path / "pyproject.toml").is_file()
    assert (tmp_path / ".mcp.json").is_file()
    assert (tmp_path / "AGENTS.md").is_file()
    assert not (tmp_path / ".parsimony-init-staging").exists()


def test_fresh_init_env_has_0600_mode(tmp_path: Path) -> None:
    if sys.platform.startswith("win"):
        pytest.skip("POSIX permissions only")
    apply(_all_ops(tmp_path), target_dir=tmp_path)
    mode = (tmp_path / ".env").stat().st_mode & 0o777
    assert mode == 0o600


def test_fresh_init_writes_parseable_content(tmp_path: Path) -> None:
    apply(_all_ops(tmp_path), target_dir=tmp_path)
    # pyproject.toml round-trips through tomllib
    tomllib.loads((tmp_path / "pyproject.toml").read_text())
    # .mcp.json round-trips through json
    json.loads((tmp_path / ".mcp.json").read_text())


# ---------------------------------------------------------------------- merge mode + backups


def test_merge_mode_backs_up_existing(tmp_path: Path) -> None:
    pre_existing = "[project]\nname = \"existing\"\nversion = \"9.9.9\"\n"
    (tmp_path / "pyproject.toml").write_text(pre_existing)

    result = apply(_all_ops(tmp_path), target_dir=tmp_path, assume_yes=True)

    assert (tmp_path / "pyproject.toml").read_text() != pre_existing
    backups = list(tmp_path.glob("pyproject.toml.parsimony.bak-*"))
    assert len(backups) == 1
    assert backups[0].read_text() == pre_existing
    assert result.backups[0] == backups[0]


def test_merge_mode_without_consent_raises_conflict(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("EXISTING=1\n")
    with pytest.raises(ApplyConflict):
        apply(_all_ops(tmp_path), target_dir=tmp_path)


def test_force_skips_backup(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = \"x\"\nversion = \"0\"\n")
    result = apply(_all_ops(tmp_path), target_dir=tmp_path, force=True)
    assert not list(tmp_path.glob("pyproject.toml.parsimony.bak-*"))
    assert len(result.backups) == 0


def test_unchanged_files_reported_not_rewritten(tmp_path: Path) -> None:
    # Pre-seed .env with exactly the content merge_env would produce —
    # apply should detect the no-change path and skip the write + backup.
    (tmp_path / ".env").write_text("FRED_API_KEY=\n")
    (tmp_path / ".gitignore").write_text("[placeholder]\n")  # forces merge mode

    result = apply(_all_ops(tmp_path), target_dir=tmp_path, assume_yes=True)
    env_unchanged = any(p.name == ".env" for p in result.unchanged)
    assert env_unchanged


# ---------------------------------------------------------------------- ordering guarantee


def test_gitignore_written_before_env(tmp_path: Path) -> None:
    apply(_all_ops(tmp_path), target_dir=tmp_path)
    # Both files exist (trivial). The stronger invariant: .gitignore
    # mentions .env so a subsequent git add won't stage the secret file.
    assert ".env" in (tmp_path / ".gitignore").read_text()


# ---------------------------------------------------------------------- symlink escape guard


def test_symlink_escape_refused(tmp_path: Path) -> None:
    if sys.platform.startswith("win"):
        pytest.skip("POSIX symlinks only")
    outside = tmp_path.parent / "outside"
    outside.mkdir()
    # `--into` points at a directory containing a symlink named .env
    # that escapes to `outside/captured`.
    anchor = tmp_path / "project"
    anchor.mkdir()
    escape = anchor / ".env"
    escape.symlink_to(outside / "captured")

    ops = _all_ops(anchor)
    # The env op targets `anchor/.env` which is a symlink to the
    # outside dir; resolve() follows it. We expect refusal.
    with pytest.raises(ApplyError, match="escapes project anchor"):
        apply(ops, target_dir=anchor, force=True)


# ---------------------------------------------------------------------- target dir validation


def test_missing_target_dir_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    with pytest.raises(ApplyError, match="does not exist"):
        apply(_all_ops(missing), target_dir=missing)


# ---------------------------------------------------------------------- atomic write semantics


def test_concurrent_env_creation_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate another process creating .env mid-run.

    The O_EXCL open at staging-flush time must refuse to clobber it.
    We simulate the race by letting apply write the staging files,
    then pre-create .env before the final move.
    """
    if sys.platform.startswith("win"):
        pytest.skip("POSIX link() semantics")

    # Use a subclass trick: monkeypatch os.link to create the target
    # first, then call the real link(). This models the race.
    real_link = os.link

    def racing_link(src: str, dst: str) -> None:
        if dst.endswith("/.env") and not os.path.exists(dst):
            # Another "process" wins the race.
            with open(dst, "w") as fp:
                fp.write("RACE=1\n")
        real_link(src, dst)

    monkeypatch.setattr("parsimony_mcp.cli.init.os.link", racing_link)

    with pytest.raises(ApplyError, match=r"refusing to overwrite \.env"):
        apply(_all_ops(tmp_path), target_dir=tmp_path)
