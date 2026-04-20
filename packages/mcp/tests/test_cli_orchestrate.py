"""Tests for the uv-sync orchestration layer."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from parsimony_mcp.cli._orchestrate import (
    LockHeld,
    UvSyncOutcome,
    classify_output,
    project_lock,
    run_uv_sync,
)

# ---------------------------------------------------------------------- classify_output


def test_classify_compiler_marker() -> None:
    stderr = "error: linker `cc` not found; is the C toolchain installed?"
    assert classify_output(stderr) == UvSyncOutcome.NEEDS_COMPILER


def test_classify_network_marker() -> None:
    stderr = "Failed to fetch https://pypi.org/simple/parsimony-fred/"
    assert classify_output(stderr) == UvSyncOutcome.NETWORK


def test_classify_unknown_falls_through() -> None:
    assert classify_output("some unrelated error") == UvSyncOutcome.UNKNOWN


# ---------------------------------------------------------------------- run_uv_sync error-shaping


def test_missing_uv_maps_to_unknown_with_recipe(tmp_path: Path) -> None:
    result = run_uv_sync(tmp_path, uv_executable="/nonexistent/uv-binary-for-tests")
    assert result.outcome == UvSyncOutcome.UNKNOWN
    assert result.returncode == 127
    assert "--skip-install" in result.message


def test_failed_sync_preserves_returncode(tmp_path: Path) -> None:
    """Point uv_executable at `false` — always exits 1.

    Serves as a fast proxy for a 'something went wrong' path
    without depending on real network / package state.
    """
    if sys.platform.startswith("win"):
        pytest.skip("POSIX `false` only")
    result = run_uv_sync(tmp_path, uv_executable="false")
    assert result.outcome in {UvSyncOutcome.UNKNOWN, UvSyncOutcome.NETWORK}
    assert result.returncode == 1


# ---------------------------------------------------------------------- project_lock


def test_project_lock_serializes_concurrent_holders(tmp_path: Path) -> None:
    if sys.platform.startswith("win"):
        pytest.skip("fcntl.flock POSIX-only")
    lock_a = project_lock(tmp_path)
    lock_b = project_lock(tmp_path)
    with lock_a, pytest.raises(LockHeld, match="another parsimony-mcp init"):
        lock_b.__enter__()


def test_project_lock_releases_on_exit(tmp_path: Path) -> None:
    if sys.platform.startswith("win"):
        pytest.skip("fcntl.flock POSIX-only")
    # Two sequential holders both succeed — the first releases on
    # `__exit__` so the second can acquire.
    with project_lock(tmp_path):
        pass
    with project_lock(tmp_path):
        pass
