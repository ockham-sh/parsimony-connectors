# Contributing to parsimony-connectors

Thanks for considering a contribution. This monorepo hosts every
officially-maintained parsimony connector. Anyone may contribute.

This document covers the **how** of contributing. For the **what** —
acceptance criteria, stewardship, deprecation, graduation — see
[GOVERNANCE.md](GOVERNANCE.md).

---

## 1. Before you write code

**Check whether your connector belongs here.** Use the binary rule from the
kernel's design freeze:

> Can the code be shared publicly under Apache 2.0?
> **Yes** → contribute it here (publishes as a `parsimony-<name>` package).
> **No** → publish it as a separate package; register via the entry-point contract.

If the provider's terms of service forbid Apache 2.0 redistribution, the
connector ships externally, not from this repo. See [GOVERNANCE.md §6](GOVERNANCE.md#6-licence).

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

# Conformance against the installed kernel:
uv run parsimony conformance verify parsimony-fred
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

```bash
uv run python tools/gen_pyproject.py --name foo --pypi-name parsimony-foo
```

This scaffolds `packages/foo/` with the per-connector `pyproject.toml`
template, an empty `parsimony_foo/__init__.py`, and `tests/test_conformance.py`.
See `tools/pyproject_template.toml` for the source of truth on boilerplate.

Fill in:

- `parsimony_foo/__init__.py` — the connector module. Must export `CONNECTORS`; optionally `ENV_VARS`, `PROVIDER_METADATA`. See the kernel's [`docs/contract.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md) for the full spec.
- `tests/` — a conformance test (`test_conformance.py`) plus a happy-path / error-mapping test file (`test_<name>_connectors.py`) following [`docs/testing-template.md`](docs/testing-template.md).

Before opening a PR:

```bash
uv run python tools/check_pyproject.py packages/foo
uv run pytest packages/foo
uv run ruff check packages/foo
uv run mypy packages/foo/parsimony_foo
```

All four must pass. `check_pyproject.py` enforces that your pyproject
matches the template on boilerplate (kernel contract pin, Python
classifiers, build system); per-connector dep variation is explicit.

---

## 4. PR checklist

The conformance suite is the merge gate — not founder judgement. A PR is
mergeable when every item below is satisfied:

- [ ] The connector is under `packages/<snake_case_name>/`.
- [ ] The PyPI distribution name is `parsimony-<name>` (hyphenated).
- [ ] The Python package name is `parsimony_<name>` (underscored, matches the PyPI name).
- [ ] `tools/check_pyproject.py` passes.
- [ ] `uv run pytest packages/<name>` passes locally and in CI.
- [ ] A conformance test exists under `packages/<name>/tests/` and passes.
- [ ] The connector declares an active maintainer in `CODEOWNERS`.
- [ ] The PR description names the data provider, its pricing model, any ToS caveats, and links to the provider's API documentation.
- [ ] No secrets, no API keys, no `.env` files committed.
- [ ] All respx mocks are hand-authored from upstream API documentation — no live-session recordings. `packages/*/tests/fixtures/**` is gitignored; override per-file if you need a hand-authored fixture checked in.
- [ ] For a commercial provider: the PR description confirms the provider's terms allow Apache 2.0 redistribution. (If uncertain, [GOVERNANCE.md §6](GOVERNANCE.md#6-licence) describes the audit path.)

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
- **Types:** `mypy` clean. Connectors MUST type the `params` argument with a Pydantic model; return types are `Result` or a subclass.
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
