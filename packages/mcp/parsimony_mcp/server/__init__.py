"""Parsimony MCP server wiring.

The stdio/protocol layer of ``parsimony-mcp``: builds an MCP ``Server``
from a :class:`parsimony.connector.Connectors` collection and bridges
connector results into MCP ``TextContent``.

Lives behind the ``server`` subpackage to keep the stdio concern
separate from the ``cli`` subpackage's filesystem-rendering concerns.
"""

from __future__ import annotations

from parsimony_mcp.server._core import create_server
from parsimony_mcp.server.bridge import connector_to_tool, result_to_content, translate_error

__all__ = ["connector_to_tool", "create_server", "result_to_content", "translate_error"]
