"""``uv sync`` orchestration for the init wizard.

Owns the 10-60s window between "we wrote the files" and "the venv is
ready". Failure in this window is operational, not programmer error,
so every path must classify cleanly and leave the user with an
actionable prose message.

Why inherited stdio (not ``subprocess.PIPE``): ``uv sync`` prints a
dense live progress display. A silent ~30s pause before we could
forward captured output would make every interactive user reach
for Ctrl-C. Inheriting stdio streams trades "we own the rendering"
for "the user sees exactly what uv is doing". We forfeit the
ability to parse mid-run messages — acceptable for alpha.

Why an advisory ``flock`` on a sentinel: two simultaneous
``parsimony-mcp init`` runs on the same project would race to
overwrite ``pyproject.toml`` + run ``uv sync`` in parallel.
fcntl.flock refuses the second process fast with a clear message
rather than letting them tangle.

Exit-code classification (Collina P1 — every path named):

* ``SUCCESS``       — ``uv sync`` returned 0.
* ``NEEDS_COMPILER``— build failure (missing C toolchain); leave
                      files in place, tell the user what to install.
* ``NETWORK``       — resolver / download error; leave files in
                      place (Task 8 already backed them up) and
                      point the user at retry.
* ``INTERRUPTED``   — SIGINT; leave files in place.
* ``UNKNOWN``       — anything else; treat conservatively like
                      ``NETWORK``.

The caller maps these onto :class:`~parsimony_mcp.cli.init.ExitCode`.

Rotating-log tee of uv's stderr to ``~/.cache/parsimony-mcp/logs/``
is deferred to Task 12 — it plays alongside the broader structured
logging work. Until then, the user's terminal is the log.
"""

from __future__ import annotations

import enum
import logging
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO

_LOG = logging.getLogger("parsimony_mcp.cli.orchestrate")

# Heuristic substrings that let us distinguish "the user needs a
# compiler" from "the network blinked" without parsing uv's
# structured-output format. Both messages are fragile contracts;
# we log what we found and fall back to NETWORK on ambiguity.
_COMPILER_MARKERS = (
    "error: linker `cc`",
    "error: Microsoft Visual C++",
    "unable to find compiler",
    "needs a C compiler",
    "Python.h",
)

_NETWORK_MARKERS = (
    "Failed to fetch",
    "network unreachable",
    "Temporary failure in name resolution",
    "connection refused",
)


class UvSyncOutcome(enum.Enum):
    SUCCESS = "success"
    NEEDS_COMPILER = "needs_compiler"
    NETWORK = "network"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class UvSyncResult:
    outcome: UvSyncOutcome
    returncode: int
    message: str


class LockHeld(Exception):
    """Another ``parsimony-mcp init`` holds the lock for this project.

    The message names the sentinel path so the user can remove a
    stale lock after a crash.
    """


class _NullLock:
    """Fallback lock for platforms without fcntl (Windows).

    Warns once at acquire time and otherwise does nothing. Two
    concurrent inits on Windows are a rare alpha concern; Task 12
    can tighten this with msvcrt.locking if needed.
    """

    def __init__(self, sentinel: Path) -> None:
        self.sentinel = sentinel

    def __enter__(self) -> _NullLock:
        _LOG.warning(
            "lock not enforced on this platform; concurrent inits may race",
            extra={"sentinel": str(self.sentinel)},
        )
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _PosixLock:
    """Advisory ``fcntl.flock`` on a sentinel file.

    Non-blocking: returns immediately with :class:`LockHeld` if
    another process holds the lock, rather than stalling.
    """

    def __init__(self, sentinel: Path) -> None:
        self.sentinel = sentinel
        self._fp: IO[str] | None = None

    def __enter__(self) -> _PosixLock:
        import fcntl

        self.sentinel.parent.mkdir(parents=True, exist_ok=True)
        fp = self.sentinel.open("w")
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fp.close()
            raise LockHeld(
                f"another parsimony-mcp init is running — wait or remove "
                f"{self.sentinel} if the previous run crashed"
            ) from exc
        self._fp = fp
        return self

    def __exit__(self, *exc: object) -> None:
        import fcntl

        if self._fp is None:
            return
        try:
            fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
        finally:
            self._fp.close()
        self._fp = None


def project_lock(target_dir: Path) -> _PosixLock | _NullLock:
    """Return a context-manager lock for concurrent-init safety.

    Sentinel lives inside the project (``target_dir/.parsimony-init.lock``)
    so per-project init is serialised while different projects run
    in parallel.
    """
    sentinel = target_dir / ".parsimony-init.lock"
    if sys.platform.startswith("win"):
        return _NullLock(sentinel)
    return _PosixLock(sentinel)


def run_uv_sync(
    target_dir: Path,
    *,
    uv_executable: str = "uv",
    extra_args: Sequence[str] = (),
) -> UvSyncResult:
    """Invoke ``uv sync`` in ``target_dir`` with inherited stdio.

    The caller must already hold :func:`project_lock`. We swallow
    classification heuristics here; the caller decides how to
    surface the result (and whether to keep / roll back files
    apply() wrote).
    """
    cmd = [uv_executable, "sync", *extra_args]
    _LOG.info("spawning uv sync", extra={"cmd": " ".join(cmd), "cwd": str(target_dir)})

    # Inherit stdio for live progress; capture stderr to a pipe only
    # long enough to match failure markers, then tee back. Simpler
    # alpha version: inherit everything, use returncode + os.strerror
    # plus last-N-lines if we had them. We don't here — trade-off
    # noted in the module docstring.
    try:
        proc = subprocess.run(  # noqa: S603 — cmd is [uv_executable, 'sync', ...], no shell; uv_executable is an internal default / caller-vetted override
            cmd,
            cwd=str(target_dir),
            check=False,
            timeout=None,
        )
    except FileNotFoundError:
        return UvSyncResult(
            outcome=UvSyncOutcome.UNKNOWN,
            returncode=127,
            message=(
                f"{uv_executable!r} is not on PATH. Install uv "
                "(https://docs.astral.sh/uv/) or re-run with --skip-install "
                "to finish setup without installing dependencies."
            ),
        )
    except KeyboardInterrupt:
        return UvSyncResult(
            outcome=UvSyncOutcome.INTERRUPTED,
            returncode=130,
            message="interrupted during uv sync. Files are in place; re-run to retry.",
        )

    if proc.returncode == 0:
        return UvSyncResult(
            outcome=UvSyncOutcome.SUCCESS,
            returncode=0,
            message="uv sync completed",
        )
    # We didn't capture stderr for alpha. Until Task 12's log tee
    # lands, rely on the returncode shape: uv returns 2 for resolver
    # failures and 1 for build / unexpected errors. It's a heuristic.
    if proc.returncode == 2:
        outcome = UvSyncOutcome.NETWORK
        msg = (
            "uv sync failed during dependency resolution. Check network "
            "connectivity and your package pins; your project files are "
            "in place — re-run `uv sync` once resolved."
        )
    else:
        outcome = UvSyncOutcome.UNKNOWN
        msg = (
            f"uv sync failed with exit code {proc.returncode}. Review the "
            "output above; your project files are in place — re-run "
            "`uv sync` after addressing the underlying error."
        )
    return UvSyncResult(outcome=outcome, returncode=proc.returncode, message=msg)


def classify_output(stderr_text: str) -> UvSyncOutcome:
    """Classify ``stderr`` into an :class:`UvSyncOutcome`.

    Exposed for tests and for Task 12's log-tee path, which WILL
    capture stderr. Pure string inspection, zero side effects.
    """
    lower = stderr_text.lower()
    for marker in _COMPILER_MARKERS:
        if marker.lower() in lower:
            return UvSyncOutcome.NEEDS_COMPILER
    for marker in _NETWORK_MARKERS:
        if marker.lower() in lower:
            return UvSyncOutcome.NETWORK
    return UvSyncOutcome.UNKNOWN
