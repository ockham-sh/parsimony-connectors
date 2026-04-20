"""Entry point: ``parsimony-mcp`` (or ``python -m parsimony_mcp``).

Dispatch model:

* Bare ``parsimony-mcp`` (no args, or only server-relevant args) runs
  the MCP stdio server. This is load-bearing: existing ``.mcp.json``
  entries configured as ``{"command": "parsimony-mcp"}`` must keep
  working after we add subcommands. DO NOT rename this path to a
  ``serve`` subcommand.
* ``parsimony-mcp init [...]`` dispatches to :mod:`parsimony_mcp.cli.init`.
* Future subcommands slot in as additional branches — one entry
  point, argparse dispatch, no parallel console scripts.

Console scripts cannot reference coroutines directly, so :func:`main` is
the synchronous zero-arg entry point that wraps :func:`_run` in
``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import mcp.server.stdio
from dotenv import load_dotenv
from parsimony.discovery import build_connectors_from_env

from parsimony_mcp._logging import configure_logging
from parsimony_mcp.cli import init as cli_init
from parsimony_mcp.server import create_server

logger = logging.getLogger("parsimony_mcp.main")

_SLOW_DISCOVERY_MS = 2000

_KNOWN_SUBCOMMANDS = frozenset({"init"})


def _load_project_env() -> Path | None:
    """Load a project-local ``.env`` into ``os.environ`` before discovery.

    Walks from ``$PARSIMONY_MCP_PROJECT_DIR`` (if set) or the current
    working directory upward to the filesystem root, loading the
    first ``.env`` found. Pre-existing environment variables always
    win — ``.env`` is a default, not an override, which matches the
    agent-host case where secrets come from the host's
    ``mcpServers.*.env`` block.

    Without this, connectors whose declared env vars aren't in the
    launching shell get silently skipped by the kernel's
    :func:`build_connectors_from_env` — a confusing "0 tools" UX
    even when the user has filled in ``.env``.
    """
    override = os.environ.get("PARSIMONY_MCP_PROJECT_DIR")
    start = Path(override).resolve() if override else Path.cwd().resolve()

    for directory in (start, *start.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            return candidate
    return None


async def _run_server() -> None:
    configure_logging()

    env_path = _load_project_env()
    if env_path is not None:
        logger.info("loaded project env", extra={"path": str(env_path)})

    start = time.monotonic()
    all_connectors = build_connectors_from_env()
    discovery_ms = int((time.monotonic() - start) * 1000)

    tool_connectors = all_connectors.filter(tags=["tool"])
    count = len(list(tool_connectors))

    if count == 0:
        logger.warning(
            "parsimony-mcp started with 0 connectors tagged 'tool'; install a plugin "
            "(e.g. `pip install parsimony-fred`) to populate the tool catalog",
            extra={"discovery_ms": discovery_ms},
        )
    else:
        logger.info(
            "loaded connectors",
            extra={"count": count, "discovery_ms": discovery_ms},
        )

    if discovery_ms > _SLOW_DISCOVERY_MS:
        logger.warning(
            "slow plugin discovery — check for plugins with heavy eager imports",
            extra={"discovery_ms": discovery_ms, "threshold_ms": _SLOW_DISCOVERY_MS},
        )

    server = create_server(tool_connectors)
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def _dispatch(argv: Sequence[str]) -> int:
    """Route ``argv`` to the server or a subcommand.

    A subcommand is the first positional argument if it matches
    :data:`_KNOWN_SUBCOMMANDS`. Anything else (empty ``argv``, or
    unknown first token) keeps the existing stdio-server behaviour,
    so legacy launchers that pass flags to the server don't regress.
    """
    if argv and argv[0] in _KNOWN_SUBCOMMANDS:
        sub = argv[0]
        rest = argv[1:]
        if sub == "init":
            return cli_init.run(rest)
        # Unreachable: _KNOWN_SUBCOMMANDS is a closed set.
        raise AssertionError(f"no dispatch for subcommand {sub!r}")

    try:
        asyncio.run(_run_server())
    except KeyboardInterrupt:
        return int(cli_init.ExitCode.SIGINT)
    return int(cli_init.ExitCode.OK)


def main() -> None:
    """Synchronous console-script entry point."""
    try:
        code = _dispatch(sys.argv[1:])
    except KeyboardInterrupt:
        code = int(cli_init.ExitCode.SIGINT)
    sys.exit(code)


if __name__ == "__main__":
    main()
