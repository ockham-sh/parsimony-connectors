# Council Plan: parsimony-mcp finalization (Track A)

**Scope:** Finalize `parsimony-mcp` as a shippable standalone PyPI distribution on a fresh branch off `main` — rewrite stale `parsimony.mcp.*` imports, fold in juraj's instruction template, keep main's richer exception handling, add pyproject/CI/release/LICENSE/README/CHANGELOG, configure PyPI OIDC trusted publisher.
**Context:** The package is at `/home/espinet/ockham/parsimony-mcp/`, currently on the throwaway `snapshot-from-kernel` branch — 4 Python files copied verbatim from the old kernel plus 4 test files, no pyproject, no CI, no real metadata. Target version `0.1.0a1`. Python 3.11/3.12/3.13 matrix. Reference shape to converge on: `/home/espinet/ockham/parsimony-juraj/packages/parsimony-mcp/`.
**Boundaries:** No HTTP/SSE transport (stdio only). No kernel changes. No connector-side work (that's Track B). No first PyPI release in this plan — OIDC setup is listed, actual release is post-merge. No eval harness / no observability dashboard (deferred). No tier/auth gating (kernel owns it).
**Council dispatched:** Hunt (10 recs), Fowler (8 recs), Dodds (12 recs), Collina (7 recs), Leach (no recs — no database), Python Performance (no actionable recs — optimization candidates rejected as premature), Willison (9 recs), Saarinen (no recs — no web UI), Friedman (README + stderr UX recs for the developer-user surface).

---

## Task Sequence

### 1. Create fresh branch off `main`; rewrite `parsimony.mcp.*` → `parsimony_mcp.*` imports across code and tests

| | |
|---|---|
| **Domain** | Chair — handoff hygiene |
| **Ref** | handoff §Track A.1–2 |
| **Depends on** | — |

Start a fresh branch off `origin/main` (`snapshot-from-kernel` is throwaway — squash-merge at the end, delete the branch). Rewrite every `from parsimony.mcp.<x>` import to `from parsimony_mcp.<x>` in `parsimony_mcp/__init__.py`, `parsimony_mcp/__main__.py`, `parsimony_mcp/bridge.py`, `parsimony_mcp/server.py`, `tests/test_bridge.py`, `tests/test_server.py`, `tests/test_server_coverage.py`. Verify with a fresh grep that no `parsimony.mcp` strings remain.

---

### 2. Rewrite `__init__.py`: re-export three public symbols + derive `__version__` from metadata

| | |
|---|---|
| **Domain** | Dodds × Carmack — Principle 1 (AHA) + Principle 2 (eliminate derivable state) |
| **Ref** | `references/quality-frontend.md` → Principles 1 + 2 |
| **Depends on** | Task 1 |

Drop the obsolete lazy-import alias (it points at `parsimony.mcp.server`, a path the kernel no longer exposes — it would fail at first attribute access). Re-export exactly three symbols — `create_server`, `connector_to_tool`, `result_to_content` — via plain eager imports, with `__all__` declaring them explicitly. Derive `__version__ = importlib.metadata.version("parsimony-mcp")` so the installed wheel's version is the single source of truth. Do not introduce `__getattr__` lazy loading — every realistic consumer pays the `mcp` SDK import cost immediately anyway. Cross-ref: Fowler #5 also informed this.

---

### 3. Rewrite `server.py`: adopt juraj's instruction template, keep main's 5-branch exception handler routed through a single `_translate_error` helper, set `isError: True` on error responses, bound each connector call with `asyncio.timeout`

| | |
|---|---|
| **Domain** | Collina × Carmack — Principle 5 (resource cleanup) + Principle 6 (structural errors) |
| **Ref** | `references/quality-backend.md` → Principles 1 + 5 + 7; `references/quality-llm.md` → Principle 1 |
| **Depends on** | Task 1 |

Replace the inline `_MCP_HEADER` prose constant with juraj's `_MCP_SERVER_INSTRUCTIONS` template string at module scope, consumed via `_MCP_SERVER_INSTRUCTIONS.format(catalog=connectors.to_llm())` in `create_server`. Strip the SDMX-specific workflow prescription ("list_datasets → dsd → codelist → ...") and the `client['fred']` hardcoded example — both leak connector implementation details into the host prompt and rot silently. Keep the generic "discover via MCP → fetch via `parsimony.client[<name>]` → analyze" framing.

Extract the exception-dispatch cascade into a single private helper `_translate_error(exc, tool_name)` in `bridge.py` (see Task 4) so `call_tool` reads as a linear pipeline. Keep the 5 explicit parsimony-error branches (`UnauthorizedError`, `PaymentRequiredError`, `RateLimitError` with `quota_exhausted`+`retry_after`, `EmptyDataError`, then `ConnectorError` fallback) plus `ValidationError`, unknown-tool, and the catch-all — no dispatch table, no collapse.

Wrap `await conn(**arguments)` in `asyncio.timeout(30)` (Python 3.11+ context manager form). Catch `TimeoutError` as its own branch returning a structured message. Set MCP's `isError=True` on every error response so clients can distinguish failure from successful text output. Rewrite `RateLimitError` and `PaymentRequiredError` messages to include behavioral directives ("DO NOT retry this tool; try a different connector or inform the user") — the error text IS the observation the agent acts on, and a bare `retry_after=30` invites tight retry loops the agent cannot actually wait out.

Cross-ref: Fowler #1 (keep `server.py` monolithic, don't extract `prompts.py`), Fowler #3 (straight-line dispatch, not a table), Willison #1/#2/#3 also informed this.

---

### 4. Harden `bridge.py`: sanitize DataFrame cells, self-describe truncation, centralize error redaction in `_translate_error`

| | |
|---|---|
| **Domain** | Hunt × Carmack — Principle 9 (assume breach — information leakage) |
| **Ref** | `references/security.md` → Principles 1 + 9 |
| **Depends on** | Task 1 |

Rewrite `result_to_content` to coerce every DataFrame cell to `str`, escape `|` and backticks, replace newlines with spaces, and truncate per-cell length to ~500 chars before calling `to_markdown`. Replace the bare `"(N more rows omitted)"` suffix with a self-describing directive: "(showing 50 of N rows — this is a discovery preview; for the full dataset call `parsimony.client[<connector>](...)` in Python)." This both prevents a compromised upstream from forging markdown rows / instructions and prevents agents from looping on truncated results hallucinating pagination params.

Add `_translate_error(exc, tool_name) -> list[TextContent]` as the single funnel for Task 3's exception cascade. For each error class, emit a FIXED user-safe string — never `str(exc)` — because raw connector exceptions routinely embed full request URLs including `?api_key=...` query-string secrets. For `ValidationError`, iterate `exc.errors()` and emit at most the first 5 entries as `"<loc>: <msg>"` — never include `input_value`, which leaks a secret-shaped arg the user accidentally typed into the agent.

Cross-ref: Hunt #1 (redact errors), Hunt #7 (sanitize markdown), Hunt #8 (ValidationError scoping), Collina #5 (compact validation messages), Willison #7 (truncation guidance) all informed this.

---

### 5. Rewrite `__main__.py`: synchronous `main()` wrapping `asyncio.run(_run())`; measure and log plugin discovery; warn on zero connectors; fail loudly on per-plugin import errors

| | |
|---|---|
| **Domain** | Fowler × Carmack — Principle 2 (extract pure logic, keep mutations visible) |
| **Ref** | `references/refactoring.md` → Principle 2 |
| **Depends on** | Task 1, Task 6 |

Define `def main() -> None: asyncio.run(_run())` as the zero-arg console-script entry point — `[project.scripts]` cannot reference coroutines. `_run()` is async and owns `build_connectors_from_env()`, `.filter(tags=["tool"])`, `create_server`, and the stdio transport. The sync wrapper is the only place `asyncio.run` appears.

Inside `_run()`, time the discovery call with `time.monotonic()` — log `"discovery took Xms, loaded N connectors"` at info level, warn if > 2000ms. If N == 0, emit a clear warning pointing the user at the plugin install story (e.g. `pip install parsimony-fred`) — do NOT fail-fast on zero; empty is a valid-if-unhelpful config. If an individual plugin's import raised, log the plugin name and exception class only (never the traceback from an untrusted plugin) and continue with the remaining plugins — this surfaces partial loads without hiding them.

Do not wrap discovery in `asyncio.wait_for`; interrupting `importlib` mid-walk leaves Python's import state in an undefined condition. Visibility via timing log is the right lever, not timeout.

Cross-ref: Collina #2/#3/#7 informed the observability design; Hunt #11 ("fail-closed") is softened to "log loudly and continue" because, at this alpha stage with no connector ecosystem yet, hard-failing on first missing plugin would be hostile to users.

---

### 6. Wire stderr JSON structured logging before any log calls fire

| | |
|---|---|
| **Domain** | Collina × Carmack — Principle 7 (structured logging as correctness) |
| **Ref** | `references/quality-backend.md` → Principle 7 |
| **Depends on** | Task 1 |

Configure a single `logging.StreamHandler(sys.stderr)` with a JSON formatter (~20 lines, no external dep) at the top of `_run()` and BEFORE any import-time logger calls. stdout is owned by the MCP SDK's JSON-RPC framing; logging to stdout corrupts the wire protocol. JSON fields: `ts`, `level`, `logger`, `event`, plus context dict that callers can extend with `tool`, `exc_type`, `duration_ms`, `connector_count`. Honor `PARSIMONY_MCP_LOG_LEVEL` env var, default `WARN` so steady state is quiet and agents don't get drowned by heartbeats in Claude Desktop's log pane. Scrub tracebacks out of the catch-all branch — log `exc_type` and `tool`, not the full stack chain (which commonly embeds bearer tokens through `__cause__`/`__context__`).

Cross-ref: Hunt #2 (log scrubbing), Willison #8 (per-invocation structured logs) also informed this.

---

### 7. Consolidate `test_server.py` + `test_server_coverage.py` into one `test_server.py`; remove misleading `pytest.importorskip("mcp")`; add coverage for new branches (timeout, isError, redaction, markdown sanitization)

| | |
|---|---|
| **Domain** | Fowler × Carmack — Principle 4 (names reveal design) + Principle 1 (economic test) |
| **Ref** | `references/refactoring.md` → Principle 4 |
| **Depends on** | Task 3, Task 4 |

Merge the two server test files into one `test_server.py` with well-named classes: `TestListTools`, `TestCallToolSuccess`, `TestCallToolErrorHandling`, `TestInstructions`, `TestLazyImports`. `test_bridge.py` stays separate — it maps to the pure-function module. The split "test_server_coverage.py" is an artifact of how error branches were added after the happy path, not a meaningful seam; keeping it split entrenches shotgun-surgery on every future exception change.

Remove `pytest.importorskip("mcp", reason="mcp is an optional dependency")` from all three test files — `mcp` is a hard runtime dependency of this package, and the importorskip lies about the contract, letting a green CI ship a package that literally cannot be imported.

Add new test cases for the coverage gained this release: `asyncio.timeout` → `TimeoutError` branch, `isError=True` assertion on every error path, `_translate_error` never emits `input_value` or raw `str(exc)`, markdown cell escaping (pipe/backtick/newline injection), truncation directive wording, startup warning on zero connectors. Use `_FakeEmbedder`-style pattern from `/home/espinet/ockham/parsimony/tests/test_lazy_namespace_catalog.py` if any test needs a real Catalog.

Dodds #7 argued for keeping the split ("they answer different confidence questions"); Fowler's counter — that both files drive the same Server through the same handler, and the split is accidental not architectural — wins on the economic test. Merging is one-shot cheap; splitting is free later if a genuine axis emerges.

---

### 8. Write `pyproject.toml` with narrow dependency bounds, inline ruff/mypy-strict config, correct classifiers

| | |
|---|---|
| **Domain** | Dodds × Carmack — Principle 3 (type system as armour) + Principle 6 (errors are structural) |
| **Ref** | `references/quality-frontend.md` → Principles 3 + 6 |
| **Depends on** | — |

Distribution name `parsimony-mcp`, version `0.1.0a1`, `requires-python = ">=3.11"`. Dependencies: `parsimony-core>=0.1.0a1,<0.3`, `mcp>=1.0,<2`, `tabulate>=0.9.0,<1`. An unbounded `mcp>=1.0.0` means a future silent-breaking 2.x pulls in via `pip install` — upper bounds tell pip "these are the versions I've been tested against" and force a review via Dependabot when upstream majors move. `[project.optional-dependencies] dev = [pytest, pytest-asyncio, pytest-cov, ruff, mypy]`. `[project.scripts] parsimony-mcp = "parsimony_mcp.__main__:main"`.

Drop the `Framework :: Parsimony :: Contract 1` classifier — this package does NOT declare a `parsimony.providers` entry point, so claiming Contract 1 conformance is false signalling. Keyword `parsimony` + `mcp` + `agents` gives ecosystem discoverability honestly.

`[tool.mypy] strict = true` for the package code, with a `[[tool.mypy.overrides]] module = "tests.*"` relaxation for fixture ergonomics. `[tool.ruff]` rules inline — copy from parsimony-connectors once, don't share via git submodule or symlink (cross-repo coupling is worse than mild duplication). Explicitly pin `mypy python_version = "3.11"` — write to the floor, no `sys.version_info` branches for features 3.11 already has.

Build backend `hatchling`. `[tool.uv.sources]` is NOT needed — this is its own repo, not a workspace member.

Cross-ref: Hunt #10 (pin `mcp<2`) informed this.

---

### 9. Port CI workflow from parsimony-connectors: strip the matrix fan-out, drop the conformance step, add `pip-audit`

| | |
|---|---|
| **Domain** | Fowler × Carmack — Principle 6 (architecture earns its boundaries) |
| **Ref** | `references/refactoring.md` → Principle 6 |
| **Depends on** | Task 8 |

Copy `/home/espinet/ockham/parsimony-connectors/.github/workflows/ci.yml` and delete the `discover` job (pointless for a single-package repo) and the `Conformance verify` step (parsimony-mcp is a consumer of the kernel contract, not a `parsimony.providers` plugin — running `parsimony conformance verify parsimony-mcp` would fail for the wrong reason). Keep the Python 3.11/3.12/3.13 matrix on one `test` job with lint + type + test + build. Add a `pip-audit` step against the resolved lockfile on every CI run — a CVE in the MCP SDK or any transitive dep must not ship silently.

Pin every GitHub Action by commit SHA (not `@v4`); a compromised tag on `actions/checkout` would otherwise execute on every CI run. Concurrency group + cancel-in-progress on PR runs stays as-is.

---

### 10. Port release workflow: OIDC-only publish, SHA-pinned actions, protected `main` with required review before merge

| | |
|---|---|
| **Domain** | Hunt × Carmack — Principle 5 (shrink the attack surface — OWASP A08 CI/CD integrity) |
| **Ref** | `references/security.md` → Principle 5 |
| **Depends on** | Task 8 |

Copy `/home/espinet/ockham/parsimony-connectors/.github/workflows/release.yml`, collapse the per-package matrix (there is one package), and remove the package-name input validation (no longer needed — the repo IS `parsimony-mcp`). Keep `workflow_dispatch` as the only trigger. The publish job has `id-token: write` scoped to it only; no `PYPI_API_TOKEN` secret in the repo (OIDC is the only publish path — refuse the fallback). Pin `pypa/gh-action-pypi-publish` and `actions/checkout` and `astral-sh/setup-uv` by SHA.

Configure branch protection on `main` (outside the workflow — see External Setup): require 1 approving review, require CI green, disallow force-push. Squash-merge only. Without this, a compromised maintainer PR can self-approve and publish a malicious wheel to every installing developer's machine.

---

### 11. Write README with paste-ready MCP client config block, tool capabilities table, troubleshooting matrix; write CHANGELOG; write LICENSE (Apache-2.0)

| | |
|---|---|
| **Domain** | Friedman × Carmack — Principle 2 (design all five states) + Principle 8 (preview consequences) |
| **Ref** | `references/quality-ux.md` → Principles 2 + 5 + 8 |
| **Depends on** | Task 3, Task 5, Task 6 |

The README's consumer is a developer trying to wire `parsimony-mcp` into Claude Desktop (or equivalent) in under five minutes. Required structure:

1. **One paragraph at the top** — what this is, who it's for, why you'd install it. Written for a reader who knows Claude Desktop but not Parsimony.
2. **A paste-ready `claude_desktop_config.json` block as the FIRST code block**, with env-var slots commented with "get this from X" pointers. Do NOT hide it below 200 lines of prose.
3. **A "what this server can do" section** — list the tools that `build_connectors_from_env()` will surface when each official connector plugin (`parsimony-fred`, `parsimony-sdmx`, etc.) is installed. Agents inherit these privileges; developers must see the consequences before wiring it in.
4. **A troubleshooting matrix (table)** — at minimum: "Server disconnected in Claude Desktop" → check Python path, venv activation, env vars; "0 tools available" → install a connector plugin; "Auth error" → set the plugin-specific env var.
5. **Documented env vars**, including `PARSIMONY_MCP_LOG_LEVEL` (default WARN; set DEBUG for diagnosis).
6. **Security note** — installing any `parsimony-*` package grants it code execution in every MCP session on this machine; document the `PARSIMONY_MCP_ALLOWED_PROVIDERS` env var if shipped (see Watchpoints).

`CHANGELOG.md` starts with a `0.1.0a1 — (date)` entry leading with what the package IS (not the migration history from the in-kernel version — that's noise for new readers). `LICENSE` is Apache-2.0 verbatim from parsimony-core.

Cross-ref: Hunt #6 (plugin supply-chain disclosure) and Dodds #3 (honest metadata) informed this. Friedman's full stderr five-state design is captured via Tasks 5–6 already — the README task is the documentation layer.

---

### 12. Squash-merge `snapshot-from-kernel` → `main`, delete branch

| | |
|---|---|
| **Domain** | Chair — handoff hygiene |
| **Ref** | handoff §Track A.8 |
| **Depends on** | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 |

Before merging: run `uv pip install -e .[dev]` in a fresh venv, verify `pytest` green, verify `parsimony-mcp --help` works, verify `python -c "import parsimony_mcp; print(parsimony_mcp.__version__)"` prints `0.1.0a1`. Open a PR from the working branch (not `snapshot-from-kernel`) into `main`, get review, squash-merge. Delete both the working branch and `snapshot-from-kernel` after merge.

---

## Risks & Watchpoints

Expert-attributed risks that aren't their own tasks but need awareness during build.

- **Hunt — Plugin supply-chain boundary (Principle 5):** `build_connectors_from_env()` executes arbitrary code from any installed `parsimony.providers` entry point. Out of scope to fix this release, but if opt-in lockdown becomes needed, the shape is an env var `PARSIMONY_MCP_ALLOWED_PROVIDERS=fred,sec-edgar` that filters the entry-point iterator BEFORE loading. Defer until a user asks or a compromised plugin hits PyPI. Document the exposure in the README.
- **Hunt — Credential keys in tool arguments (Principle 3/4):** The plan currently relies on each connector's Pydantic schema to reject `api_key`-shaped kwargs. If a connector author sloppily adds an `api_key: str | None = None` override, the adapter has no adapter-level defense. If connectors grow, add a pre-dispatch regex check `(?i)(api[_-]?key|token|secret|password|authorization|bearer)` on argument keys in `call_tool`. Not blocking for alpha — the current kernel connectors don't have this field.
- **Collina — Error-log cascade during network partitions (Principle 1):** If every connector starts failing simultaneously (e.g. a user loses WiFi), the agent gets a flood of "Internal error" responses. Dedupe by `(tool, exc_type, msg_hash)` in the logger — log once plus a "repeat" marker — is a future ergonomics win. Pair with @pair-collina when a real user complaint lands.
- **Dodds — `mcp>=1.0,<2` cap creates Dependabot churn (Principle 6):** The MCP SDK is young and bumps frequently. Expect the cap to feel tight. Bump proactively; don't widen the cap.
- **Willison — Instruction-template drift (Principle 3):** Every edit to `_MCP_SERVER_INSTRUCTIONS` is a prompt change. No eval harness ships in this plan, so regressions won't be caught automatically. When the template is edited in the future, do a manual 5-scenario smoke test against a real Claude model (unemployment query, SEC filing lookup, rate-limited retry path, unknown-tool recovery, empty-data handling) before merging. Eventual eval harness is a post-alpha item. Pair with @pair-willison if the template is rewritten non-trivially.
- **Willison — Plugin docstrings as prompt-injection surface (Principle 2):** Connector-author-controlled descriptions land directly in the instruction block via `Connectors.to_llm()`. Current mitigation: Task 3 delimits the catalog inside the static template. Hardening the kernel side (size cap per description, strip control chars) is an upstream change and out of scope. Document the trust boundary in the README.
- **Friedman — Claude Desktop stderr is the only feedback loop:** If the server hangs at startup with no stderr output, the developer has no signal. Task 5's timing log + Task 6's JSON logger cover the steady-state view; the first-run-no-config case is covered by Task 11's troubleshooting matrix. Monitor the first external user bug report — if "it just doesn't start" shows up, there's a gap.
- **Chair — Python 3.13 alignment across repos:** parsimony-connectors CI is currently 3.11+3.12. Adding 3.13 here (per user request "all repos should be aligned on this") implies a matching bump in parsimony-connectors. Out of scope for Track A; Track B should follow.
- **Chair — `CONTRACT_VERSION` entry-point metadata:** The kernel contract (docs/contract.md) asks plugins to declare `Framework :: Parsimony :: Contract 1` in their entry-point metadata. parsimony-mcp is NOT a plugin (no `parsimony.providers` entry point) so this doesn't apply. Do not add a spurious entry point to make the metadata fit.

---

## External Setup Required

Actions outside the codebase that must be completed for specific tasks to be merge-able or publishable.

| # | What | Why | Blocking task |
|---|------|-----|---------------|
| 1 | Create PyPI OIDC trusted publisher for `parsimony-mcp` (pypi.org → Publishing → Add trusted publisher: GitHub org `ockham-sh`, repo `parsimony-mcp`, workflow `release.yml`, environment `pypi-parsimony-mcp`) | The `release.yml` workflow uses `id-token: write` OIDC to publish — without the trusted publisher registered, `pypa/gh-action-pypi-publish` fails at the final step | Task 10 (release workflow) for a real publish; not blocking merge |
| 2 | Configure GitHub branch protection on `main`: require 1 approving review, require CI green, disallow force-push, squash-merge only | Without protection, a compromised PR can self-approve and publish a malicious wheel (Hunt's Principle 5 mitigation) | Task 10 (release workflow) for effective security; not blocking merge |
| 3 | Configure GitHub environment `pypi-parsimony-mcp` with required reviewers (at minimum: espinetandreu) | `workflow_dispatch` on `release.yml` should pause for manual approval before publishing — this is the last human gate on what goes to PyPI | Task 10 for first release |
| 4 | (Optional) Register the `parsimony-mcp` PyPI name now at `0.0.0` placeholder with the trusted-publisher identity locked in, to prevent name-squat between now and first real release | Name-squat on `parsimony-mcp` by an attacker between plan approval and first release would force a rename | Task 10 nice-to-have, not blocking |

---

## Summary

| # | Task | Domain | Depends on |
|---|------|--------|------------|
| 1 | Branch off `main`; rewrite `parsimony.mcp.*` imports | Chair | — |
| 2 | `__init__.py`: 3 re-exports + `__version__` from metadata | Dodds | 1 |
| 3 | `server.py`: instruction template + 5-branch handler via helper + `isError` + `asyncio.timeout` | Collina | 1 |
| 4 | `bridge.py`: `_translate_error` + markdown sanitization + truncation directive | Hunt | 1 |
| 5 | `__main__.py`: sync `main()` wrapper + discovery timing + zero-connector warning | Fowler | 1, 6 |
| 6 | Stderr JSON logging scaffold | Collina | 1 |
| 7 | Consolidate `test_server*.py`; remove `importorskip`; add coverage for new branches | Fowler | 3, 4 |
| 8 | `pyproject.toml`: pinned deps + strict mypy + ruff inline + correct classifiers | Dodds | — |
| 9 | CI workflow: strip matrix + drop conformance + add pip-audit + SHA-pin actions | Fowler | 8 |
| 10 | Release workflow: OIDC-only + SHA-pinned + branch protection | Hunt | 8 |
| 11 | README with paste-ready config + tool table + troubleshooting matrix; CHANGELOG; LICENSE | Friedman | 3, 5, 6 |
| 12 | Squash-merge working branch → `main`; delete `snapshot-from-kernel` | Chair | 1–11 |

## Verdict

The most important architectural decision in this plan is **Task 3** — finalizing `server.py` — because it fixes the agent-facing contract for this release: instruction template, error taxonomy exposed via MCP `isError`, behavioral directives in rate-limit/payment messages, and a server-side timeout that makes the failure mode deterministic. Everything else is scaffold. Start there after the import rewrite.

Collina's domain is the critical one: this package is an async stdio adapter that bridges an LLM agent to connectors that make real outbound HTTP. The failure modes that matter — stuck upstream calls, rate-limit retry storms, unhelpful error prose, silent catch-all — all live in the `call_tool` handler. The `asyncio.timeout` wrap + `isError` flag + directive-bearing error messages are the three non-negotiables; everything else (JSON logging, discovery timing, consolidated tests) supports observability of those three correctness guarantees.

If pairing agents are available during build, use @pair-collina on Task 3 and @pair-hunt on Task 4 — those are the two tasks where specific judgment calls (timeout budget, error-message redaction) compound if wrong.

Skip Willison's structured-error-payload / eval-harness recommendations for this alpha. They're real ideas but YAGNI for 0.1.0a1 — ship the prose contract cleanly, measure real agent behavior, then invest in structure when a specific failure demands it. Carmack would ship. Take notes; don't build.
