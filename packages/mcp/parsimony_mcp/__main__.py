"""Entry point: ``parsimony-mcp`` (or ``python -m parsimony_mcp``).

Starts the MCP server over stdio, exposing all connectors tagged ``tool``
that :func:`parsimony.discovery.build_connectors_from_env` produces.

Console scripts cannot reference coroutines directly, so :func:`main` is
the synchronous zero-arg entry point that wraps :func:`_run` in
``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import logging
import time

import mcp.server.stdio
from parsimony.discovery import build_connectors_from_env

from parsimony_mcp._logging import configure_logging
from parsimony_mcp.server import create_server

logger = logging.getLogger("parsimony_mcp.main")

_SLOW_DISCOVERY_MS = 2000


async def _run() -> None:
    configure_logging()

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


def main() -> None:
    """Synchronous console-script entry point."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
