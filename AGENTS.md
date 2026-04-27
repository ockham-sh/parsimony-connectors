# parsimony-connectors

## Commands

```bash
make sync                   # uv sync --all-extras --all-packages
make verify PKG=<name>      # ruff + mypy + pytest + strict plugin listing
make verify-all             # verify across every package under packages/*
uv run pytest               # every package
uv run parsimony list --strict   # conformance across all installed plugins
```

## Key files

| What | Where |
|------|-------|
| Workspace root | `pyproject.toml` |
| Connector packages | `packages/<name>/` |
| Per-connector manifest | `packages/<name>/pyproject.toml` |
| Connector module | `packages/<name>/parsimony_<name>/__init__.py` |
| Per-connector tests | `packages/<name>/tests/` |
| Per-connector conformance | `packages/<name>/tests/test_conformance.py` |
| Contribution rules | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Acceptance / stewardship policy | [GOVERNANCE.md](GOVERNANCE.md) |
| Per-connector stewardship | [CODEOWNERS](CODEOWNERS) |
| Supply-chain posture | [SECURITY.md](SECURITY.md) |

## Rules

- Python 3.11+; `X | None` not `Optional[X]`; line length 120
- PyPI name `parsimony-<name>` (hyphenated); Python package `parsimony_<name>` (underscored)
- Every package declares `[project.entry-points."parsimony.providers"]`
- Env vars live on the decorator: `@connector(env={"api_key": "FOO_API_KEY"})`
- `CONNECTORS` is the required export; `CATALOGS` and `RESOLVE_CATALOG` are optional
- Pin `parsimony-core>=0.4,<0.5` — contract-version pin, not a floor
- Respx mocks are hand-authored from upstream API docs; no recorded cassettes (`.gitignore` enforces)
- No provider-SDK copy-paste; no affiliation claims in READMEs
- Run `make verify PKG=<name>` before committing changes to one package; `make verify-all` before releasing
