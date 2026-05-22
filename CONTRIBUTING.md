# Contributing to parsimony-connectors

Thanks for considering a contribution. This monorepo hosts every
officially-maintained parsimony connector. Anyone may contribute.

This document covers the **how** of contributing. For the **what** —
acceptance criteria, stewardship, deprecation, graduation — see
[GOVERNANCE.md](GOVERNANCE.md).

---

## 1. Before you write code

**Check whether your connector belongs here.** This monorepo ships
Apache-2.0 wrapper code around providers' documented HTTP APIs. Every
contribution must:

- Be your own code — no copy-paste from a provider's official SDK.
- Ship no recorded response data (respx mocks must be hand-authored).
- Use provider names nominatively, without affiliation claims.

See [GOVERNANCE.md §6](GOVERNANCE.md#6-licence) for the full statement.

**Check whether someone is already working on it.** Open an issue with the
provider name and a one-line description before you invest time.

---

## 2. Local development

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for dependency and workspace management
- A local checkout of [ockham-sh/parsimony](https://github.com/ockham-sh/parsimony) if you're iterating against unreleased kernel changes (optional)

### Setup

```bash
git clone https://github.com/ockham-sh/parsimony-connectors.git
cd parsimony-connectors
uv sync
```

`uv sync` resolves every workspace member and its dependencies into a single
`.venv`. Individual packages share the venv but have independent
`pyproject.toml` files.

### Running tests

```bash
# Every package:
uv run pytest

# One package:
uv run pytest packages/fred

# Conformance against the installed kernel (strict listing fails on any non-conforming plugin):
uv run parsimony list --strict
```

### Iterating against unreleased kernel changes

If you need a kernel change not yet released to PyPI, add a `[tool.uv.sources]`
entry at the workspace root pointing at your local parsimony checkout:

```toml
[tool.uv.sources]
parsimony = { path = "../parsimony", editable = true }
```

Don't commit this — it's developer-local.

---

## 3. Adding a new connector

Scaffold `packages/foo/` by copying the smallest existing plugin (e.g.
`packages/polymarket/`) and adapting it. Each plugin must contain:

- `pyproject.toml` — pin `parsimony-core>=0.7,<0.8`, declare a
  `[project.entry-points."parsimony.providers"]` line, and set
  `[project.urls] Homepage`. See the kernel's
  [`docs/guide-new-plugin.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/guide-new-plugin.md)
  for the canonical template.
- `parsimony_foo/__init__.py` — the connector module. Must export
  `CONNECTORS`. Catalog build workflows belong in provider-owned scripts,
  not in the user-facing module.
  Define plain async connector functions and keep any auth/env fallback
  inside the connector implementation. Use `.bind(...)` in operator code
  when a credential or other fixed value should be hidden from the public
  call surface. Providers may optionally expose a side-effect-light
  `load(...)` / `configure(...)` helper that returns bound connectors or
  sets provider-local runtime defaults — this is a convention, not a kernel
  requirement. Do not download catalogs, enumerate upstream entities, or
  build indexes at import time. See the kernel's
  [`docs/contract.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md)
  for the full spec.
- `tests/` — a conformance test (`test_conformance.py`) plus a
  happy-path / error-mapping test file (`test_<name>_connectors.py`)
  following [`docs/testing-template.md`](docs/testing-template.md).
- `README.md` — see any existing plugin for the standard shape.
- `scripts/build_catalog.py` *(only if maintainers build a hosted
  catalog)* — operator driver that calls the enumerator, converts with
  `entries_from_result`, configures one top-level index per field (use
  `HybridIndex` to fuse BM25 + vector within a field), sets
  `default_field`, calls `await catalog.build()`, then
  `await catalog.save(...)` for local paths or `hf://...` uploads.

Before opening a PR:

```bash
uv run pytest packages/foo
uv run ruff check packages/foo
uv run mypy packages/foo/parsimony_foo
uv run parsimony list --strict   # conformance for every installed provider
```

All four must pass. `parsimony list --strict` imports every plugin and
runs the kernel-side conformance check (`CONNECTORS` is well-formed,
connector descriptions are present, etc.).

---

## 4. PR checklist

The conformance suite is the merge gate — not founder judgement. A PR is
mergeable when every item below is satisfied:

- [ ] The connector is under `packages/<snake_case_name>/`.
- [ ] The PyPI distribution name is `parsimony-<name>` (hyphenated).
- [ ] The Python package name is `parsimony_<name>` (underscored, matches the PyPI name).
- [ ] `uv run parsimony list --strict` passes (kernel-side conformance).
- [ ] `uv run pytest packages/<name>` passes locally and in CI.
- [ ] A conformance test exists under `packages/<name>/tests/` and passes.
- [ ] The connector declares an active maintainer in `CODEOWNERS`.
- [ ] The PR description names the data provider, its pricing model, any ToS caveats, and links to the provider's API documentation.
- [ ] No secrets, no API keys, no `.env` files committed.
- [ ] All respx mocks are hand-authored from upstream API documentation — no live-session recordings. `packages/*/tests/fixtures/**` is gitignored; override per-file if you need a hand-authored fixture checked in.
- [ ] No provider-SDK code copy-pasted; no affiliation or endorsement claims in README.

---

## 5. Reporting bugs

Open a GitHub issue with:

- The connector name (e.g. `parsimony-fred`)
- The connector version (`pip show parsimony-fred` or `uv pip show`)
- The parsimony kernel version
- A minimal reproduction (ideally a failing test)
- The full traceback

For security issues, see the kernel's `SECURITY.md` — do not open a public
issue.

---

## 6. Code style

- **Formatter:** `ruff format` (120-char lines, the workspace root `pyproject.toml` configures this)
- **Linter:** `ruff check` with the rules selected in the workspace root
- **Types:** `mypy` clean. Public connector signatures are flat top-level parameters; Pydantic models are optional internal validators. Return types are `Result` or a subclass.
- **Imports:** absolute imports only; no `from parsimony.*` star imports.
- **Docstrings:** every `@connector`-decorated function needs a one-line summary (tool-tagged connectors need ≥40 chars — the first sentence becomes the MCP tool description).

---

## 7. Taking over an abandoned connector

If a connector steward has been unresponsive to issues and PRs for 90 days,
anyone may open a takeover PR. See [GOVERNANCE.md §2](GOVERNANCE.md#2-stewardship)
for the full policy.

---

## 8. Getting help

- Open a discussion on GitHub Discussions
- Ask in the parsimony issue tracker
- Read the kernel's `docs/contract.md` for the spec details

---

*This document may be amended by PR. Amendments that change acceptance
criteria or stewardship policy require a corresponding update to
[GOVERNANCE.md](GOVERNANCE.md).*
