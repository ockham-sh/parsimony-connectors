"""Structured per-dataset outcome returned by worker subprocesses.

The worker *always* returns a ``DatasetOutcome``; it never raises across
the ``multiprocessing.Pool`` boundary. The parent driver streams
outcomes via ``imap_unordered`` so one failing dataset cannot abort the
agency run.

This module is I/O-free by contract. Exception → ``FailureKind``
classification lives in :mod:`parsimony_sdmx.io.http` so ``core/`` has no
transport dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class OutcomeStatus(StrEnum):
    OK = "ok"
    EMPTY = "empty"
    FAILED = "failed"


class FailureKind(StrEnum):
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    PARSE_ERROR = "parse_error"
    OOM_KILLED = "oom_killed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class DatasetOutcome:
    dataset_id: str
    agency_id: str
    status: OutcomeStatus
    rows: int = 0
    bytes: int = 0
    duration_s: float = 0.0
    attempts: int = 1
    parquet_path: str | None = None
    kind: FailureKind | None = None
    error_message: str | None = None
    traceback: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
