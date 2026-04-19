"""Orchestrator tests.

Most tests use ``isolate_subprocess=False`` (inline mode) for speed.
The subprocess path itself is covered by the ``@pytest.mark.slow``
tests below which fork real ``mp.Process`` children — skip them with
``-m 'not slow'`` to keep the default suite fast.

Resume is filesystem-based: a dataset is skipped iff its canonical
series parquet exists, so tests that exercise resume must have the
worker actually create the file (``touch_worker`` below).
"""

import os
import time
from pathlib import Path

import pytest

from parsimony_sdmx.cli.layout import oom_dir, series_parquet
from parsimony_sdmx.cli.memory_monitor import _write_oom_marker
from parsimony_sdmx.cli.orchestrator import OrchestratorConfig, run_agency
from parsimony_sdmx.core.outcomes import DatasetOutcome, FailureKind, OutcomeStatus


# Module-level workers so multiprocessing (if used) could pickle them.
def ok_worker(agency_id: str, dataset_id: str, output_base: str) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.OK,
        rows=42,
    )


def touch_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    """Writes the canonical parquet path so filesystem resume skips it next run."""
    path = series_parquet(Path(output_base), agency_id, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.OK,
        rows=0,
        parquet_path=str(path),
    )


def failing_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    if dataset_id == "FAIL":
        raise RuntimeError("synthetic failure")
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.OK,
    )


def always_raise_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    raise ValueError(f"bad dataset: {dataset_id}")


# --- Module-level subprocess workers (must be picklable for spawn) ---


def subprocess_ok_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    """Clean happy path for the spawn child."""
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.OK,
        rows=7,
    )


def subprocess_sleep_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    """Sleeps longer than the orchestrator's per-dataset timeout."""
    time.sleep(10.0)
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.OK,
    )


def subprocess_oom_then_exit_worker(
    agency_id: str, dataset_id: str, output_base: str
) -> DatasetOutcome:
    """Simulates the monitor's OOM sequence: write marker, then exit non-zero.

    The real monitor writes the marker and SIGKILLs the child; the child
    never returns. ``os._exit(137)`` mirrors the kernel exit code for a
    SIGKILL (128 + 9 = 137) so the parent's non-zero-exit branch runs.
    """
    _write_oom_marker(
        Path(output_base),
        agency_id,
        os.getpid(),
        rss_bytes=9_000_000_000,
        system_percent=91.2,
        worker_data={"dataset_id": dataset_id, "phase": "fetching"},
    )
    os._exit(137)  # pragma: no cover - process exits before return


class TestRunAgency:
    def test_happy_path_inline(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        outcomes = run_agency(
            "ECB", tmp_path, ["YC", "CPI", "GDP"], ok_worker, cfg
        )
        assert [o.dataset_id for o in outcomes] == ["YC", "CPI", "GDP"]
        assert all(o.status == OutcomeStatus.OK for o in outcomes)
        assert all(o.rows == 42 for o in outcomes)

    def test_timing_fields_stamped(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        outcomes = run_agency("ECB", tmp_path, ["YC"], ok_worker, cfg)
        o = outcomes[0]
        assert o.started_at is not None
        assert o.finished_at is not None
        assert o.duration_s >= 0.0

    def test_one_failing_dataset_does_not_abort_the_run(
        self, tmp_path: Path
    ) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        outcomes = run_agency(
            "ECB", tmp_path, ["YC", "FAIL", "CPI"], failing_worker, cfg
        )
        assert [o.dataset_id for o in outcomes] == ["YC", "FAIL", "CPI"]
        assert outcomes[0].status == OutcomeStatus.OK
        assert outcomes[1].status == OutcomeStatus.FAILED
        assert outcomes[1].kind == FailureKind.UNKNOWN  # RuntimeError classification
        assert "synthetic failure" in (outcomes[1].error_message or "")
        assert outcomes[2].status == OutcomeStatus.OK

    def test_bare_value_error_classified_as_unknown(self, tmp_path: Path) -> None:
        # Narrowed classification: a bare ValueError from a worker is a
        # programmer bug, so it must surface as UNKNOWN (not buried as
        # PARSE_ERROR which is reserved for concrete parser exceptions).
        cfg = OrchestratorConfig(isolate_subprocess=False)
        outcomes = run_agency(
            "ECB", tmp_path, ["X"], always_raise_worker, cfg
        )
        assert outcomes[0].status == OutcomeStatus.FAILED
        assert outcomes[0].kind == FailureKind.UNKNOWN

    def test_resume_skips_datasets_whose_parquet_exists(
        self, tmp_path: Path
    ) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        run_agency("ECB", tmp_path, ["YC", "CPI"], touch_worker, cfg)

        # Second run — both parquets exist, so only GDP runs.
        second = run_agency("ECB", tmp_path, ["YC", "CPI", "GDP"], touch_worker, cfg)
        assert [o.dataset_id for o in second] == ["GDP"]

    def test_resume_does_not_skip_when_parquet_missing(
        self, tmp_path: Path
    ) -> None:
        """A failure-only worker creates no parquet → next run retries."""
        cfg = OrchestratorConfig(isolate_subprocess=False)
        run_agency("ECB", tmp_path, ["YC"], always_raise_worker, cfg)
        second = run_agency("ECB", tmp_path, ["YC"], touch_worker, cfg)
        assert [o.dataset_id for o in second] == ["YC"]
        assert second[0].status == OutcomeStatus.OK

    def test_force_overrides_resume(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        run_agency("ECB", tmp_path, ["YC"], touch_worker, cfg)

        cfg_force = OrchestratorConfig(isolate_subprocess=False, force=True)
        second = run_agency("ECB", tmp_path, ["YC", "CPI"], touch_worker, cfg_force)
        assert [o.dataset_id for o in second] == ["YC", "CPI"]

    def test_empty_dataset_list_is_noop(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=False)
        outcomes = run_agency("ECB", tmp_path, [], ok_worker, cfg)
        assert outcomes == []

    def test_sweep_runs_at_start(self, tmp_path: Path) -> None:
        from parsimony_sdmx.cli.layout import tmp_dir

        td = tmp_dir(tmp_path, "ECB")
        td.mkdir(parents=True)
        (td / "orphan.parquet").write_text("leftover")

        cfg = OrchestratorConfig(isolate_subprocess=False)
        run_agency("ECB", tmp_path, ["YC"], ok_worker, cfg)

        assert not (td / "orphan.parquet").exists()


@pytest.mark.slow
class TestSubprocessPath:
    """Real-spawn coverage for ``_invoke_subprocess``.

    These tests take ~1-3s each because they start fresh Python
    interpreters. Skipped with ``-m 'not slow'``. Per review finding #17,
    the subprocess path owns four distinct branches (clean exit,
    timeout, non-zero exit + OOM marker, non-zero exit without marker)
    and must be exercised at least once end-to-end — a `return None`
    replacing ``_invoke_subprocess`` would pass every inline test.
    """

    def test_clean_exit_returns_worker_outcome(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=True)
        outcomes = run_agency(
            "ECB", tmp_path, ["YC"], subprocess_ok_worker, cfg
        )
        assert len(outcomes) == 1
        assert outcomes[0].status == OutcomeStatus.OK
        assert outcomes[0].rows == 7
        # Timing fields are stamped by the parent, not the child.
        assert outcomes[0].started_at is not None
        assert outcomes[0].finished_at is not None

    def test_worker_exception_classified_in_child(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=True)
        outcomes = run_agency(
            "ECB", tmp_path, ["FAIL"], failing_worker, cfg
        )
        assert outcomes[0].status == OutcomeStatus.FAILED
        # RuntimeError falls through to UNKNOWN (narrowed classification).
        assert outcomes[0].kind == FailureKind.UNKNOWN
        assert "synthetic failure" in (outcomes[0].error_message or "")
        # Traceback is preserved for debugging.
        assert outcomes[0].traceback is not None

    def test_timeout_classified_as_timeout(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(
            isolate_subprocess=True, per_dataset_timeout_s=0.5
        )
        outcomes = run_agency(
            "ECB", tmp_path, ["SLOW"], subprocess_sleep_worker, cfg
        )
        assert outcomes[0].status == OutcomeStatus.FAILED
        assert outcomes[0].kind == FailureKind.TIMEOUT
        assert "timeout" in (outcomes[0].error_message or "").lower()

    def test_oom_marker_classifies_as_oom_killed(self, tmp_path: Path) -> None:
        cfg = OrchestratorConfig(isolate_subprocess=True)
        outcomes = run_agency(
            "ECB",
            tmp_path,
            ["BOOM"],
            subprocess_oom_then_exit_worker,
            cfg,
        )
        assert outcomes[0].status == OutcomeStatus.FAILED
        assert outcomes[0].kind == FailureKind.OOM_KILLED
        assert "OOM" in (outcomes[0].error_message or "")
        # The marker made it from the child into the outcome error message.
        assert "91.2" in (outcomes[0].error_message or "") or "91" in (
            outcomes[0].error_message or ""
        )
        # Marker ended up where the monitor would have written it.
        marker_dir = oom_dir(tmp_path, "ECB")
        assert marker_dir.exists()
