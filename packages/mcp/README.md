# parsimony-mcp

> MCP (Model Context Protocol) stdio server adapter for [parsimony](https://parsimony.dev) — exposes any installed `parsimony-*` connector as an MCP tool to Claude Desktop, Cursor, Continue, or any other MCP-compatible agent runtime.

**Alpha — `0.1.0a1`.** API surface is stable; error messages, instruction template, and logging format may iterate.

---

## Quickstart

**1. Install** — in whatever Python environment your MCP client launches from:

```bash
pip install parsimony-mcp
# Install at least one connector plugin — without plugins, the server starts with 0 tools:
pip install parsimony-fred  # example; replace with whichever you need
```

**2. Wire it into your MCP client.** For Claude Desktop, paste this into `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "parsimony": {
      "command": "/absolute/path/to/your/venv/bin/parsimony-mcp",
      "env": {
        "FRED_API_KEY": "your-fred-key-here",
        "PARSIMONY_MCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

Replace `/absolute/path/to/your/venv/bin/parsimony-mcp` with the output of `which parsimony-mcp` in the env where you installed the package. Restart Claude Desktop after editing the config.

**Project-scoped config (Claude Code, Cursor, etc.)** — drop a `.mcp.json` in the project root. Two equivalent patterns, both fine:

```json
// Pattern A — hardcoded key in the env block
{
  "mcpServers": {
    "parsimony": {
      "command": "uv",
      "args": ["run", "parsimony-mcp"],
      "env": { "FRED_API_KEY": "your-fred-key-here" }
    }
  }
}
```

```json
// Pattern B — load keys from a .env file (gitignore the .env!)
{
  "mcpServers": {
    "parsimony": {
      "command": "uv",
      "args": ["run", "--env-file", ".env", "parsimony-mcp"]
    }
  }
}
```

Do **not** rely on shell-style `${VAR}` substitution inside the `env` block — several MCP clients (Claude Code included) pass the literal string through unchanged, which produces opaque 4xx errors from upstream providers. Either hardcode the value or use Pattern B.

**3. Verify.** In Claude Desktop's chat, type "list parsimony tools" — you should see the connectors from whichever plugins you installed. If you see "no tools" check the Claude Desktop log pane and the troubleshooting matrix below.

---

## What this server exposes

`parsimony-mcp` is a **discovery** layer. It surfaces connectors whose authors tagged them `tool` — typically search, list, and metadata endpoints. For **bulk data retrieval** (full time series, multi-year history), the MCP instructions tell the agent to write Python using `parsimony.client[...]` and execute it in a separate code-execution tool (e.g. `computer_use`, a Jupyter kernel, or `repl`).

The exact tool surface depends on which `parsimony-*` plugins are installed in your venv. See the [parsimony-connectors monorepo](https://github.com/ockham-sh/parsimony-connectors) for the authoritative list of official connectors, their auth requirements, and rate-limit classes.

**Security note.** Every installed `parsimony-*` package grants it code execution inside every MCP session on your machine. Only install plugins you trust. If you need to lock down which plugins load, set the `PARSIMONY_PROVIDERS_ALLOWLIST` env var (handled by the kernel's discovery layer; see `parsimony` docs).

---

## Configuration

| Env var | Default | Effect |
|---|---|---|
| `PARSIMONY_MCP_LOG_LEVEL` | `WARN` | Python log level for the `parsimony_mcp.*` logger family. Set `INFO` to see startup connector count + discovery timing; set `DEBUG` for per-call traces. All logs go to stderr (stdout is owned by the MCP protocol). |
| `<PLUGIN>_API_KEY` et al. | — | Each connector plugin has its own credential env vars. See the plugin's README. |

The server itself takes no CLI flags. The console script is `parsimony-mcp`; equivalent to `python -m parsimony_mcp`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Claude Desktop shows "Server disconnected" or the server never appears | Wrong path to `parsimony-mcp` in the config `command` field | Run `which parsimony-mcp` in the venv where you installed the package; paste the full absolute path into the config. Restart Claude Desktop. |
| "0 tools available" / no tools listed | No plugin packages installed | `pip install parsimony-fred` (or any other `parsimony-*` package); restart Claude Desktop. |
| Tool call returns "Authentication error for X" | Connector-specific env var missing | Check the plugin's README for the required env var name; add it to the `env` block in `claude_desktop_config.json`. |
| Tool call returns `HTTPStatusError` / opaque 4xx after editing `.mcp.json` | Client cached the old config at session start; reconnect re-uses stale child process | Fully quit and relaunch the client (not just `/mcp` reconnect). Also check for an `env: { KEY: "${KEY}" }` block — the `${…}` is passed literally by Claude Code and others, breaking auth. |
| Tool call returns "Rate limit for X" with `DO NOT retry` directive | Upstream provider rate-limited you | The agent will not retry automatically. Either wait, pick a different connector, or upgrade the upstream plan. |
| Tool call returns "timed out after 30s" | Upstream is slow or network partition | The 30s timeout is deliberate — an agent running `call_tool` for minutes helps nobody. Retry manually if upstream recovers. |
| JSON parse errors in Claude Desktop's MCP log | Something is writing to stdout that isn't MCP JSON-RPC | Check for plugins that `print()` at import time. Report the plugin to its author; the MCP adapter's stdout is reserved for protocol framing. |

---

## Programmatic use

If you need to embed the server in your own Python process (custom transport, multi-tenant runtime, etc.):

```python
from parsimony.discovery import build_connectors_from_env
from parsimony_mcp import create_server

connectors = build_connectors_from_env().filter(tags=["tool"])
server = create_server(connectors)
# `server` is an mcp.server.lowlevel.Server — attach any transport you like.
```

The three re-exports from `parsimony_mcp` (`create_server`, `connector_to_tool`, `result_to_content`) are the stable public API.

---

## Development

```bash
git clone https://github.com/ockham-sh/parsimony-mcp
cd parsimony-mcp
uv venv
uv pip install -e ".[dev]"
uv run pytest       # 47 tests, ~1s
uv run ruff check parsimony_mcp tests
uv run mypy parsimony_mcp
```

`parsimony-core` is a dependency; during development you'll typically install it editable alongside:

```bash
uv pip install -e ../parsimony -e ".[dev]"
```

---

## License

Apache-2.0 — see [LICENSE](LICENSE).
