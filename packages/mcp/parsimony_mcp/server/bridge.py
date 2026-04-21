"""Bridge between parsimony Connector interface and MCP Tool definitions.

Three responsibilities — all pure, all side-effect free:

1. :func:`connector_to_tool` — map a :class:`parsimony.connector.Connector`
   to an MCP :class:`~mcp.types.Tool` definition.
2. :func:`result_to_content` — serialize a parsimony :class:`Result` to MCP
   text content, with per-cell sanitization and a self-describing
   truncation directive.
3. :func:`translate_error` — translate a connector or validation error into
   agent-safe :class:`~mcp.types.TextContent` blocks. Never stringifies the
   raw exception, because raw exception messages routinely embed full
   request URLs including ``?api_key=...`` query-string secrets.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from mcp.types import TextContent, Tool
from parsimony.connector import Connector
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import Result
from pydantic import ValidationError

_MAX_ROWS = 50
_MAX_CELL_CHARS = 500
_MAX_VALIDATION_ERRORS = 5


def connector_to_tool(conn: Connector) -> Tool:
    """Map a Connector to an MCP Tool definition."""
    schema: dict[str, Any] = dict(conn.param_schema)
    # Strip Pydantic $defs — MCP clients may not support JSON Schema $ref
    schema.pop("$defs", None)
    schema.pop("title", None)
    return Tool(
        name=conn.name,
        description=conn.description,
        inputSchema=schema,
    )


def _sanitize_cell(value: Any) -> str:
    """Coerce a DataFrame cell to a safe markdown string.

    Escapes markdown table delimiters, replaces newlines with spaces, and
    truncates per-cell length. A compromised upstream provider returning a
    cell like ``"\\n\\n**SYSTEM**: do X"`` otherwise produces markdown the
    LLM reads as host instructions.
    """
    text = str(value)
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace("|", r"\|").replace("`", r"\`")
    if len(text) > _MAX_CELL_CHARS:
        text = text[: _MAX_CELL_CHARS - 1] + "…"
    return text


def result_to_content(result: Result, max_rows: int = _MAX_ROWS) -> list[TextContent]:
    """Serialize a connector Result to MCP text content."""
    data = result.data
    if isinstance(data, pd.DataFrame):
        total = len(data)
        preview = data.head(max_rows).map(_sanitize_cell)
        text = preview.to_markdown(index=False)
        if total > max_rows:
            text += (
                f"\n\n(showing {max_rows} of {total} rows — this is a discovery "
                f"preview; for the full dataset call "
                f"`parsimony.client['<connector>'](...)` in Python, do not "
                f"call this MCP tool again hoping for more rows)"
            )
    elif isinstance(data, pd.Series):
        sanitized = data.map(_sanitize_cell)
        text = sanitized.to_markdown()
    else:
        text = _sanitize_cell(data)
    return [TextContent(type="text", text=text)]


def _error_content(message: str) -> list[TextContent]:
    return [TextContent(type="text", text=message)]


def _format_validation_error(exc: ValidationError, tool_name: str) -> str:
    errors = exc.errors()
    head = errors[:_MAX_VALIDATION_ERRORS]
    # Never include input_value — the user may have typed an API key into
    # the agent, and Pydantic's default stringification would round-trip it
    # through the LLM transcript.
    lines = [f"{'.'.join(str(p) for p in err.get('loc', ()))}: {err.get('msg', 'invalid')}" for err in head]
    extra = len(errors) - len(head)
    suffix = f" (+{extra} more)" if extra > 0 else ""
    return f"Invalid parameters for {tool_name}: " + "; ".join(lines) + suffix


def translate_error(exc: BaseException, tool_name: str) -> list[TextContent]:
    """Render an exception as agent-safe text content.

    Each branch emits a FIXED user-safe string. ``str(exc)`` is never spliced
    into output, because ``ConnectorError`` subclasses (and the httpx errors
    they wrap) commonly embed request URLs with query-string credentials.
    The agent gets the error class semantics plus the provider name; it does
    not get the raw upstream message.
    """
    if isinstance(exc, ValidationError):
        return _error_content(_format_validation_error(exc, tool_name))
    if isinstance(exc, UnauthorizedError):
        return _error_content(
            f"Authentication error for {exc.provider}. Check that the API key "
            f"env var is configured correctly for this connector; DO NOT retry "
            f"this tool with different arguments."
        )
    if isinstance(exc, PaymentRequiredError):
        return _error_content(
            f"Plan restriction for {exc.provider}: this endpoint requires a "
            f"higher-tier API plan. DO NOT retry this tool; try a different "
            f"connector or inform the user that their current plan cannot "
            f"serve this data."
        )
    if isinstance(exc, RateLimitError):
        if exc.quota_exhausted:
            return _error_content(
                f"Rate limit for {exc.provider}: API quota exhausted for the "
                f"current billing period. DO NOT retry; either use a different "
                f"connector or inform the user to wait for the next billing "
                f"cycle."
            )
        return _error_content(
            f"Rate limit for {exc.provider}: retry after "
            f"{exc.retry_after:.0f} seconds. DO NOT retry this tool "
            f"immediately; pick a different connector, ask the user, or stop."
        )
    if isinstance(exc, EmptyDataError):
        return _error_content(
            f"No data returned from {exc.provider} for the given parameters. "
            f"This is a successful query with an empty result set — the "
            f"parameters likely do not match any records. Adjust parameters "
            f"or try a different connector."
        )
    if isinstance(exc, ConnectorError):
        # ProviderError, ParseError, or the base ConnectorError. Emit the
        # provider name + exception class only — never the raw message.
        return _error_content(
            f"Error from {exc.provider} ({type(exc).__name__}). "
            f"Upstream provider returned an unexpected response."
        )
    # Catch-all — caller (server.call_tool) is expected to log full context
    # before returning this, never log exc here because _logging's JSON
    # formatter omits tracebacks by default and we're relying on that.
    # ``type(exc).__name__`` is a Python class identifier and carries no user
    # data, so exposing it is safe and lets the agent distinguish transient
    # upstream faults (e.g. HTTPStatusError) from local bugs.
    return _error_content(
        f"Internal error in {tool_name} ({type(exc).__name__}); see server logs"
    )
