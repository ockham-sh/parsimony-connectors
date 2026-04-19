"""Reap files from ``.tmp/`` and ``.oom/`` left by prior crashed runs."""

from __future__ import annotations

import logging
from pathlib import Path

from parsimony_sdmx.cli.layout import oom_dir, tmp_dir

logger = logging.getLogger(__name__)


def sweep_orphans(output_base: Path, agency_id: str) -> int:
    """Delete every file under the agency's ``.tmp/`` and ``.oom/`` dirs.

    Runs once at orchestrator startup before the Pool is created. Returns
    the number of files removed (for logging).
    """
    removed = 0
    for d in (tmp_dir(output_base, agency_id), oom_dir(output_base, agency_id)):
        if not d.exists():
            continue
        for entry in d.iterdir():
            if entry.is_file():
                try:
                    entry.unlink()
                    removed += 1
                except OSError:
                    logger.warning("Failed to unlink orphan %s", entry, exc_info=True)
    if removed:
        logger.info("Swept %d orphan file(s) for agency %s", removed, agency_id)
    return removed
