<!-- Thanks for contributing to parsimony-connectors. -->
<!-- For acceptance criteria see GOVERNANCE.md §1; for the PR checklist see CONTRIBUTING.md §4. -->

## What this PR changes

<!-- Short, specific. "Add Tiingo connector" or "Fix FRED date-range parsing" — not "improvements". -->

## Provider details (required for new connectors)

- **Provider name:**
- **Provider API docs:**
- **Pricing model:** (free / freemium / paid)

## PR checklist

- [ ] Connector lives under `packages/<snake_case_name>/`
- [ ] PyPI distribution name is `parsimony-<name>` (hyphenated)
- [ ] Python package name is `parsimony_<name>` (underscored)
- [ ] `uv run parsimony list --strict` passes (kernel-side conformance)
- [ ] `uv run pytest packages/<name>` passes
- [ ] `uv run ruff check packages/<name>` clean
- [ ] `uv run mypy packages/<name>/parsimony_<name>` clean
- [ ] Conformance test present under `packages/<name>/tests/`
- [ ] `CODEOWNERS` updated with the connector's steward
- [ ] `packages/<name>/CHANGELOG.md` updated under `[Unreleased]`
- [ ] No secrets, API keys, or `.env` files committed
- [ ] HTTP client written from scratch — no provider-SDK copy-paste
- [ ] Respx mocks hand-authored from provider API docs — no recorded cassettes
- [ ] README uses the provider's name nominatively — no affiliation claims
