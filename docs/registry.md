# Connector registry

Every officially-maintained `parsimony-<name>` distribution declares itself
to the kernel through entry-point metadata in its `pyproject.toml`:

```toml
[project.entry-points."parsimony.providers"]
<provider_id> = "parsimony_<name>"
```

The value is a bare importable module path, not a `module:attr` target.
The kernel imports that module and reads its top-level
`CONNECTORS = Connectors([...])` export.

Consumers (agent frameworks, CLIs) discover installed providers at
runtime via the kernel's `parsimony.discover` surface (`discover.load_all()`
and `discover.load(*names)`). Entry-point metadata on the installed
distribution is the authoritative source; this monorepo does not ship a
separate index file.

## Adding an officially-maintained connector

1. Add `packages/<name>/` with a `pyproject.toml` that declares a
   `[project.entry-points."parsimony.providers"]` stanza (CI enforces
   this invariant; see `.github/workflows/ci.yml` → `discover` job).
2. Follow the conformance contract in the kernel's `parsimony.testing`
   module — CI runs `assert_plugin_valid` on every package.
3. Publish through the monorepo's per-package release workflow.
