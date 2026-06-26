"""SQLite task ledger and filesystem scaffold for SDMX catalog release builds."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_ROOT = Path("/tmp/parsimony-catalogs-v1/sdmx-build")

PHASE_AGENCY_BATCH = "agency_batch"
PHASE_SERIES_FETCH = "series_fetch"
PHASE_SERIES_INDEX = "series_index"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_DEBT = "debt"


class TaskStatus(StrEnum):
    PENDING = STATUS_PENDING
    RUNNING = STATUS_RUNNING
    DONE = STATUS_DONE
    FAILED = STATUS_FAILED
    DEBT = STATUS_DEBT


@dataclass(frozen=True, slots=True)
class BuildRoot:
    """Resolved layout under the catalog build root."""

    root: Path
    catalogs: Path
    state: Path
    logs: Path
    staging: Path
    manifest_db: Path
    debt_log: Path
    progress_json: Path
    build_log: Path

    @classmethod
    def create(cls, root: Path | str | None = None) -> BuildRoot:
        base = Path(root or DEFAULT_ROOT).expanduser().resolve()
        layout = cls(
            root=base,
            catalogs=base / "catalogs",
            state=base / "state",
            logs=base / "logs",
            staging=base / "staging",
            manifest_db=base / "state" / "manifest.sqlite",
            debt_log=base / "state" / "debt.jsonl",
            progress_json=base / "logs" / "progress.json",
            build_log=base / "logs" / "build.log",
        )
        layout.catalogs.mkdir(parents=True, exist_ok=True)
        layout.state.mkdir(parents=True, exist_ok=True)
        layout.logs.mkdir(parents=True, exist_ok=True)
        layout.staging.mkdir(parents=True, exist_ok=True)
        return layout


def configure_build_logging(log_path: Path) -> None:
    """Append INFO logs to the build log file (idempotent handler attach)."""
    root_logger = logging.getLogger()
    if root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)
    target = str(log_path.resolve())
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", "") == target:
            return
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root_logger.addHandler(fh)
    has_stream = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root_logger.handlers
    )
    if not has_stream:
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        root_logger.addHandler(sh)


class Manifest:
    """WAL-mode sqlite task ledger."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn = sqlite3.connect(str(db_path), timeout=60.0, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                agency TEXT NOT NULL,
                phase TEXT NOT NULL,
                flow_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                series_count INTEGER,
                error TEXT,
                updated_at REAL NOT NULL,
                PRIMARY KEY (agency, phase, flow_id)
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_phase ON tasks(phase)")

    def reset_stale_running(self) -> int:
        now = time.time()
        cur = self._conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE status=?",
            (STATUS_PENDING, now, STATUS_RUNNING),
        )
        return cur.rowcount

    def upsert_task(
        self,
        *,
        agency: str,
        phase: str,
        flow_id: str = "",
        status: str = STATUS_PENDING,
        series_count: int | None = None,
        error: str | None = None,
        increment_attempts: bool = False,
    ) -> None:
        now = time.time()
        row = self.get_task(agency, phase, flow_id)
        attempts = (row["attempts"] if row else 0) + (1 if increment_attempts else 0)
        self._conn.execute(
            """
            INSERT INTO tasks (agency, phase, flow_id, status, attempts, series_count, error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(agency, phase, flow_id) DO UPDATE SET
                status=excluded.status,
                attempts=excluded.attempts,
                series_count=COALESCE(excluded.series_count, tasks.series_count),
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (agency, phase, flow_id, status, attempts, series_count, error, now),
        )

    def get_task(self, agency: str, phase: str, flow_id: str = "") -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT agency, phase, flow_id, status, attempts, series_count, error, updated_at "
            "FROM tasks WHERE agency=? AND phase=? AND flow_id=?",
            (agency, phase, flow_id),
        )
        row = cur.fetchone()
        if row is None:
            return None
        keys = ("agency", "phase", "flow_id", "status", "attempts", "series_count", "error", "updated_at")
        return dict(zip(keys, row, strict=True))

    def list_tasks(
        self,
        *,
        agency: str | None = None,
        phase: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if agency is not None:
            clauses.append("agency=?")
            params.append(agency)
        if phase is not None:
            clauses.append("phase=?")
            params.append(phase)
        if status is not None:
            clauses.append("status=?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cur = self._conn.execute(
            f"SELECT agency, phase, flow_id, status, attempts, series_count, error, updated_at "
            f"FROM tasks {where} ORDER BY agency, phase, flow_id",
            params,
        )
        keys = ("agency", "phase", "flow_id", "status", "attempts", "series_count", "error", "updated_at")
        return [dict(zip(keys, row, strict=True)) for row in cur.fetchall()]

    def count_by_status(self, *, agency: str | None = None, phase: str | None = None) -> dict[str, int]:
        clauses: list[str] = []
        params: list[Any] = []
        if agency is not None:
            clauses.append("agency=?")
            params.append(agency)
        if phase is not None:
            clauses.append("phase=?")
            params.append(phase)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cur = self._conn.execute(
            f"SELECT status, COUNT(*) FROM tasks {where} GROUP BY status",
            params,
        )
        return {str(status): int(count) for status, count in cur.fetchall()}

    def reset_debt(self, *, agency: str | None = None, phase: str | None = None) -> int:
        clauses = ["status=?"]
        params: list[Any] = [STATUS_DEBT]
        if agency is not None:
            clauses.append("agency=?")
            params.append(agency)
        if phase is not None:
            clauses.append("phase=?")
            params.append(phase)
        where = " AND ".join(clauses)
        now = time.time()
        cur = self._conn.execute(
            f"UPDATE tasks SET status=?, error=NULL, updated_at=? WHERE {where}",
            (STATUS_PENDING, now, *params),
        )
        return cur.rowcount

    def ensure_series_tasks(self, agency: str, flow_ids: list[str]) -> None:
        for flow_id in flow_ids:
            for phase in (PHASE_SERIES_FETCH, PHASE_SERIES_INDEX):
                if self.get_task(agency, phase, flow_id) is None:
                    self.upsert_task(agency=agency, phase=phase, flow_id=flow_id, status=STATUS_PENDING)


def append_debt(debt_log: Path, *, agency: str, phase: str, flow_id: str, reason: str) -> None:
    record = {
        "ts": time.time(),
        "agency": agency,
        "phase": phase,
        "flow_id": flow_id,
        "reason": reason[:2000],
    }
    with debt_log.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")


def write_progress(progress_path: Path, payload: dict[str, Any]) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = progress_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(progress_path)


def catalog_shard_done(catalogs_dir: Path, namespace: str) -> bool:
    """True when a columnar series store is fully built for *namespace*."""
    from parsimony_sdmx.catalog_series import is_series_catalog

    return is_series_catalog(catalogs_dir / namespace)


def list_structure_flows(catalogs_dir: Path, agency: str) -> list[str]:
    prefix = f"sdmx_structure_{agency.lower()}_"
    flows: list[str] = []
    if not catalogs_dir.is_dir():
        return flows
    for sub in sorted(catalogs_dir.iterdir()):
        if not sub.is_dir() or not sub.name.startswith(prefix):
            continue
        flow_id = sub.name[len(prefix) :]
        if (sub / "structure.json").is_file():
            flows.append(flow_id)
    return flows


__all__ = [
    "BuildRoot",
    "Manifest",
    "PHASE_AGENCY_BATCH",
    "PHASE_SERIES_FETCH",
    "PHASE_SERIES_INDEX",
    "STATUS_DEBT",
    "STATUS_DONE",
    "STATUS_FAILED",
    "STATUS_PENDING",
    "STATUS_RUNNING",
    "append_debt",
    "catalog_shard_done",
    "configure_build_logging",
    "list_structure_flows",
    "write_progress",
]
