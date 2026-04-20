# Changelog

All notable changes to `parsimony-mcp` will be documented in this file. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0a1] — Unreleased

First standalone release. `parsimony-mcp` was previously shipped inside the `parsimony-core` kernel at `parsimony.mcp`; the kernel rewrite extracted it into this package.

### Added

- `create_server(connectors)` builder that wires a `parsimony.Connectors` collection into an `mcp.server.lowlevel.Server`, ready to be attached to any MCP transport. Stdio transport is provided by the `parsimony-mcp` console script (alias for `python -m parsimony_mcp`).
- `connector_to_tool(conn)` and `result_to_content(result)` as re-exported pure helpers for callers embedding MCP handlers in their own server.
- Per-call `asyncio.timeout(30s)` on connector invocations. Timeouts surface as a deterministic error observation with `isError=True`.
- 5-branch typed-error handling (`UnauthorizedError`, `PaymentRequiredError`, `RateLimitError` with `quota_exhausted`/`retry_after`, `EmptyDataError`, generic `ConnectorError`) plus Pydantic `ValidationError`, kernel `TypeError("Missing params")`, unknown-tool, and catch-all. Every error response carries the MCP-protocol `isError=True` flag and a behavioral directive in the text (`DO NOT retry` where appropriate) so agents don't tight-loop.
- DataFrame cell sanitization in `result_to_content` — escapes `|` and backticks, replaces newlines with spaces, caps per-cell length at 500 chars. A compromised upstream provider cannot forge markdown rows or inject system-prompt-shaped strings into agent observations.
- Self-describing truncation directive: `(showing N of M rows — this is a discovery preview; for the full dataset call parsimony.client['<connector>'](...) in Python)`.
- Instruction template with a clearly delimited `<catalog>...</catalog>` block so plugin-author-controlled connector docstrings cannot override host instructions.
- Stderr JSON structured logging (`parsimony_mcp._logging`). Honors `PARSIMONY_MCP_LOG_LEVEL` env var (default `WARN`). Never emits exception messages or tracebacks to logs — only `exc_type` and `tool` — because wrapped `httpx` errors commonly embed bearer tokens through `__cause__`/`__context__`.
- Startup observability in `__main__._run()`: discovery timing, connector count, warning if zero connectors, warning if discovery exceeds 2000ms.

### Changed

- `__init__.py` now eagerly exports `create_server`, `connector_to_tool`, `result_to_content`, and derives `__version__` from `importlib.metadata`. The obsolete lazy-import alias pointing at `parsimony.mcp.server` (which no longer exists in the kernel) has been removed.
- `__main__.py` exposes a synchronous `main()` wrapper around `asyncio.run(_run())` so `[project.scripts]` can reference it — console scripts cannot point at coroutines.
- The `call_tool` handler disables MCP SDK's default JSON Schema validation (`validate_input=False`) and handles all validation through `parsimony.connector.Connector.__call__`'s Pydantic layer, routed through `translate_error`. This keeps error formatting consistent and the redaction rules in one place.

### Security

- Connector exception messages are **never** spliced into tool responses. Each `ConnectorError` branch emits a fixed user-safe string naming only the exception class and `provider` attribute. Raw messages (which routinely embed `?api_key=...` query strings) are redacted before ever leaving this package.
- Pydantic `ValidationError` responses surface up to 5 `loc: msg` entries but never include `input_value`. If a user accidentally types an API key as a tool argument, it does not round-trip through the LLM transcript.

### Dependencies

- `parsimony-core >=0.1.0a1, <0.3`
- `mcp >=1.0, <2`
- `tabulate >=0.9.0, <1`
- `pandas >=2.0, <3`

### Python support

- CPython 3.11, 3.12, 3.13.

[0.1.0a1]: https://github.com/ockham-sh/parsimony-mcp/releases/tag/v0.1.0a1
