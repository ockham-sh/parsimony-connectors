<!-- Thanks for contributing to parsimony-connectors. -->
<!-- For acceptance criteria see GOVERNANCE.md §1; for the PR checklist see CONTRIBUTING.md §4. -->

## What this PR changes

<!-- Short, specific. "Add Tiingo connector" or "Fix FRED date-range parsing" — not "improvements". -->

## Provider details (required for new connectors)

- **Provider name:**
- **Provider API docs:**
- **Pricing model:** (free / freemium / paid)
- **Apache 2.0 redistribution allowed:** (yes / no / see licence-audit.md)
- **ToS link + date reviewed:**

If the provider is commercial and not already in `docs/licence-audit.md`,
the audit must complete before merge.

## PR checklist

- [ ] Connector lives under `packages/<snake_case_name>/`
- [ ] PyPI distribution name is `parsimony-<name>` (hyphenated)
- [ ] Python package name is `parsimony_<name>` (underscored)
- [ ] `uv run python tools/check_pyproject.py packages/<name>` passes
- [ ] `uv run pytest packages/<name>` passes
- [ ] `uv run ruff check packages/<name>` clean
- [ ] `uv run mypy packages/<name>/parsimony_<name>` clean
- [ ] Conformance test present under `packages/<name>/tests/`
- [ ] `CODEOWNERS` updated with the connector's steward
- [ ] No secrets, API keys, or `.env` files committed
