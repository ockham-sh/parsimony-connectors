"""Tests for the list-datasets subprocess helper."""

from __future__ import annotations

import multiprocessing as mp
import subprocess
import sys
import textwrap
import time
from typing import Any

import pytest

from parsimony_sdmx._isolation.listing import (
    ListDatasetsError,
    list_datasets,
    run_in_child,
)


def _emit_large_payload_child(
    result_q: mp.Queue[Any], n_records: int
) -> None:
    """Module-level child that emits ``n_records`` tuples via the queue.

    Used to stress the parent-side pipe-buffer handling. Must be module
    level so ``multiprocessing.spawn`` can pickle the reference.
    """
    payload = [(f"DS_{i:06d}", "TEST", f"title {i}") for i in range(n_records)]
    result_q.put(("ok", payload))
    result_q.close()
    result_q.join_thread()


class TestListDatasetsErrorShape:
    def test_carries_kind_message_traceback(self) -> None:
        exc = ListDatasetsError("http_error", "upstream 503", "traceback here")
        assert exc.kind == "http_error"
        assert exc.message == "upstream 503"
        assert exc.traceback_str == "traceback here"
        assert str(exc) == "upstream 503"
        assert isinstance(exc, RuntimeError)


class TestParentStaysSdmxFree:
    """The architectural guarantee: parent must not import sdmx1.

    Run in a fresh subprocess because pytest's own process may have
    imported sdmx1 via other tests. This test fails iff merely importing
    :mod:`parsimony_sdmx._isolation` drags sdmx1 into the parent's
    module table.
    """

    def test_isolation_import_does_not_pull_sdmx(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            import parsimony_sdmx._isolation  # noqa: F401

            leaked = sorted(m for m in sys.modules if m == "sdmx" or m.startswith("sdmx."))
            if leaked:
                print(f"LEAKED: {leaked}")
                sys.exit(1)
            print("CLEAN")
            """
        )
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"parent leaked sdmx imports — stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )
        assert "CLEAN" in result.stdout

    def test_plugin_surface_import_does_not_pull_sdmx(self) -> None:
        # ``parsimony publish`` imports ``parsimony_sdmx`` to read
        # ``CATALOGS`` / ``RESOLVE_CATALOG`` / ``CONNECTORS``. That
        # import must stay sdmx-free — sdmx1 only gets loaded inside
        # spawned children.
        script = textwrap.dedent(
            """
            import sys
            import parsimony_sdmx  # noqa: F401

            leaked = sorted(m for m in sys.modules if m == "sdmx" or m.startswith("sdmx."))
            if leaked:
                print(f"LEAKED: {leaked}")
                sys.exit(1)
            print("CLEAN")
            """
        )
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"plugin surface leaked sdmx — stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )


@pytest.mark.slow
class TestRunInChildLargePayload:
    """Regression guard for the pipe-buffer deadlock.

    The ESTAT listing (8k+ dataflows, ~800 KB pickled) used to hang
    because the parent joined the child before reading the queue. The
    child's feeder thread couldn't flush through the 64 KB OS pipe
    buffer until the parent read — classic mp.Queue deadlock.

    This test emits 100k synthetic records (~8 MB pickled), well past
    the pipe buffer size. If anyone restores the old join-before-read
    order, this test hangs until timeout.
    """

    def test_large_payload_does_not_deadlock(self) -> None:
        started = time.monotonic()
        payload = run_in_child(
            _emit_large_payload_child,
            (100_000,),
            timeout_s=30.0,
            label="stress",
        )
        elapsed = time.monotonic() - started
        status, records = payload
        assert status == "ok"
        assert len(records) == 100_000
        assert elapsed < 15.0, (
            f"run_in_child took {elapsed:.1f}s for 100k records — "
            "likely a pipe-buffer deadlock regression"
        )


@pytest.mark.slow
class TestListDatasetsSubprocess:
    """Live fork coverage. Marked slow because each test spawns a real
    Python interpreter."""

    def test_unknown_agency_raises_classified_error(self) -> None:
        with pytest.raises(ListDatasetsError) as exc_info:
            list_datasets("NOPE_NOT_A_REAL_AGENCY", timeout_s=30.0)
        assert exc_info.value.kind == "unknown"
        assert "NOPE_NOT_A_REAL_AGENCY" in str(exc_info.value)
        assert exc_info.value.traceback_str
