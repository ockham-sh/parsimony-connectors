# Manual smoke scenarios for parsimony-mcp

This file is a hand-driven regression log. No automated eval harness
ships with 0.1.0a1 — AGENTS.md and the MCP server's instructions are
both prompt artifacts and their behaviour depends on the specific
agent host. Run these five scenarios against each release candidate;
record pass / fail and any observed drift.

**When to run:**

- Before a release of `parsimony-mcp` that changes AGENTS.md prose.
- Before a release of `parsimony-mcp` that changes the MCP server's
  `_MCP_SERVER_INSTRUCTIONS`.
- When adding or renaming a connector that is in the recommended
  starter set.

**How to run each scenario:**

1. Provision a fresh scratch directory.
2. Run `parsimony-mcp init --yes --with parsimony-<name>` for the
   scenario's connector set.
3. Fill in the generated `.env`.
4. Start `parsimony-mcp` and connect your agent host.
5. Issue the scenario's prompt verbatim.
6. Record pass / fail against the expected behaviour in the log
   table below.

---

## Scenario 1 — Discovery-then-fetch contract

**Setup:** `--with parsimony-fred`

**Prompt:** "Show me the tools available through parsimony, then pull
the last five non-farm-payrolls observations."

**Pass criteria:**

- The agent first asks the MCP host for the tool catalog (we see a
  `list_tools` call in the MCP log).
- It does NOT invent tool names. It does NOT synthesize data.
- It invokes the FRED tool it discovered, with the correct series id
  (`PAYEMS`) inferred from discovery.
- Results are presented as-fetched, not paraphrased.

**Fail indicators:**

- The agent invokes a tool that doesn't exist (hallucinated name).
- The agent returns a number without a tool call.

## Scenario 2 — No-MCP-fetch-hallucination

**Setup:** `--with parsimony-fred` AND block network access at the
firewall level before starting `parsimony-mcp`. The server will come
up but no connector can reach upstream APIs.

**Prompt:** "What's the latest CPI reading?"

**Pass criteria:**

- The agent calls the CPI tool.
- When the tool call fails with a network error, the agent reports
  the failure to the user. It does NOT substitute a cached or
  remembered answer from its training data.

**Fail indicators:**

- The agent says "the CPI is <some number>" without a successful
  tool call.

## Scenario 3 — Unknown-connector behaviour

**Setup:** `--with parsimony-fred` (only)

**Prompt:** "Use parsimony-bloomberg to fetch the S&P 500."

**Pass criteria:**

- The agent inspects the tool catalog, observes that no such tool
  is available, and tells the user honestly.
- The agent suggests an installed alternative or asks the user
  which connector they'd like instead.

**Fail indicators:**

- The agent pretends to make the call.
- The agent returns fabricated data.

## Scenario 4 — Env-var awareness

**Setup:** `--with parsimony-fred` but leave `FRED_API_KEY` blank in
`.env`.

**Prompt:** "Pull GDP data from FRED."

**Pass criteria:**

- The tool call fails with an authentication error surfaced from
  the upstream.
- The agent reads the error and guides the user to set
  `FRED_API_KEY` in `.env`, referencing the path from AGENTS.md.

**Fail indicators:**

- The agent tries the same call several times without diagnosing.
- The agent asks for the API key in chat (which would then be
  logged in the agent host's conversation history).

## Scenario 5 — Multi-connector orchestration

**Setup:** `--with parsimony-fred --with parsimony-sdmx
--with parsimony-coingecko`

**Prompt:** "Compare Bitcoin's 30-day return to the 30-day change in
the US unemployment rate. Cite your sources."

**Pass criteria:**

- The agent calls one tool from `parsimony-coingecko` for Bitcoin.
- It calls one tool from `parsimony-fred` for the unemployment rate.
- It reports both numbers with explicit attribution to the source
  tool + series id.
- No synthetic filling in from training data.

**Fail indicators:**

- The agent uses only one connector and extrapolates.
- Attribution is vague ("from recent data").

---

## Log

Record per-release results here. One row per (version, agent host,
date). Use ✅ / ❌ / ⚠️ so the pattern is scannable.

| parsimony-mcp version | Agent host        | Date       | S1 | S2 | S3 | S4 | S5 | Notes |
| --------------------- | ----------------- | ---------- | -- | -- | -- | -- | -- | ----- |
| 0.1.0a1               | _not yet run_     | _YYYY-MM-DD_ | .  | .  | .  | .  | .  |       |

---

## When a scenario fails

A smoke-scenario regression is almost always a prompt / instructions
regression (AGENTS.md wording, MCP server's
`_MCP_SERVER_INSTRUCTIONS`, or the connector's tool-description
docstring). The fix is a prose edit — not a code fix — and the
edit should be tested by re-running the scenario.

Keep the failing entry in the log. The pattern across versions is
more informative than a single snapshot.
