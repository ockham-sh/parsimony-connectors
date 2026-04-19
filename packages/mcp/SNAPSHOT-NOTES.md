# Snapshot from parsimony kernel — handoff notes

**Branch:** `snapshot-from-kernel`
**Source:** `ockham-sh/parsimony` at `merge/juraj-wins` ≈ commit `612603f`
**Date:** 2026-04-19

This branch holds a raw copy of the MCP subtree that used to live in the kernel at `parsimony/mcp/`. The kernel drops MCP entirely on `merge/juraj-wins` (see `PLAN-juraj-merge.md` Phase 1 in the parsimony repo); this repo takes over.

## What was copied

| From (kernel) | To (this repo) |
|---|---|
| `parsimony/mcp/__init__.py` | `parsimony_mcp/__init__.py` |
| `parsimony/mcp/__main__.py` | `parsimony_mcp/__main__.py` |
| `parsimony/mcp/bridge.py` | `parsimony_mcp/bridge.py` |
| `parsimony/mcp/server.py` | `parsimony_mcp/server.py` |
| `tests/test_mcp/__init__.py` | `tests/__init__.py` |
| `tests/test_mcp/conftest.py` | `tests/conftest.py` |
| `tests/test_mcp/test_bridge.py` | `tests/test_bridge.py` |
| `tests/test_mcp/test_server.py` | `tests/test_server.py` |
| `tests/test_mcp_server_coverage.py` | `tests/test_server_coverage.py` |

## What the next session must do

These files reference the old kernel path. Imports like:

```python
from parsimony.mcp.bridge import connector_to_tool, result_to_content
```

must be rewritten to:

```python
from parsimony_mcp.bridge import connector_to_tool, result_to_content
```

Grep for `parsimony.mcp` across the tree — every hit needs updating.

## Design changes to fold in (from juraj-refactor)

When finalising `parsimony_mcp/server.py`:

1. Replace the `_MCP_HEADER` prose constant + `connectors.to_llm(header=..., heading=...)` call with juraj's `_MCP_SERVER_INSTRUCTIONS` template string and the `create_server` composition:
   ```python
   instructions = _MCP_SERVER_INSTRUCTIONS.format(catalog=connectors.to_llm())
   ```
   This matches the clean-slate kernel where `Connectors.to_llm()` is pure serialization with no `header`/`heading` kwargs.
2. **Keep** the richer exception-handling chain main already has (`UnauthorizedError`, `PaymentRequiredError`, `RateLimitError`, `EmptyDataError` explicit branches). Juraj collapsed these — do not.

## Package scaffolding still needed

- `pyproject.toml` — distribution name `parsimony-mcp`, deps `parsimony-core>=0.1.0a1`, `mcp>=1.0.0`, `tabulate>=0.9.0`; script `parsimony-mcp = "parsimony_mcp.__main__:main"`.
- `.github/workflows/` — CI (test matrix) + publish (single-package trusted publish to PyPI).
- `LICENSE`, proper `README.md`, `CHANGELOG.md`.

This snapshot branch is intentionally minimal. Do the design work in a fresh branch off `main` after the snapshot merges.
