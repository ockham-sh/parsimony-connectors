# Connector registry

[`registry.json`](../registry.json) at the repo root enumerates every officially-maintained `parsimony-<name>` distribution along with the metadata the MCP init wizard needs to present them: display name, summary, homepage, pricing, rate limits, decorator tags, and declared environment variables. This monorepo is the editorial source of truth: a package is "official" iff it appears here.

The registry is served to end users via the GitHub raw URL:

```
https://raw.githubusercontent.com/ockham-sh/parsimony-connectors/main/registry.json
```

The MCP init wizard (`parsimony-mcp init`) fetches, caches, and falls back to a bundled copy — see `packages/mcp/parsimony_mcp/cli/registry.py`.

## How it's generated

`registry.json` is NOT hand-edited. It is regenerated from the workspace packages by `tools/gen_registry.py`, which AST-parses each `packages/<name>/parsimony_<name>/__init__.py` to extract `ENV_VARS`, `PROVIDER_METADATA`, and the `tags=[...]` from `@connector` / `@enumerator` / `@loader` decorators. Importing a connector is deliberately avoided — see the generator's module docstring for the rationale.

To regenerate after adding or modifying a connector:

```bash
python tools/gen_registry.py
git add registry.json
```

CI enforces drift via `python tools/gen_registry.py --check`, which fails with a unified diff if the committed file is stale.

## Schema

The Pydantic schema lives at [`packages/mcp/parsimony_mcp/cli/registry_schema.py`](../packages/mcp/parsimony_mcp/cli/registry_schema.py) (strict + `extra="forbid"` + `frozen`). It is the single owner shared between the generator and the init-wizard consumer — bumping the schema version is a coordinated change in one PR.
