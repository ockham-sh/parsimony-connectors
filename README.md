# parsimony-connectors

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Officially-maintained connectors for the [parsimony](https://github.com/ockham-sh/parsimony) framework. Every publicly-shareable connector lives here; each publishes as its own `parsimony-<name>` package on PyPI from this repository's CI.

## Architecture

This is a **uv workspace** monorepo. Every connector is an independent workspace member under `packages/`. One repository, N PyPI distributions:

```
parsimony-connectors/
├── pyproject.toml           # uv workspace root
└── packages/
    ├── fred/                # → parsimony-fred on PyPI
    │   ├── pyproject.toml
    │   ├── parsimony_fred/
    │   └── tests/
    ├── sdmx/                # → parsimony-sdmx on PyPI
    └── ...
```

Installation is per-connector:

```bash
pip install parsimony parsimony-fred parsimony-sdmx
```

The `parsimony` kernel discovers every installed connector through the [entry-point contract](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md). There is no central registry and no bundle install; users pick what they need.

## Contributing

- **First read:** [CONTRIBUTING.md](CONTRIBUTING.md) — local dev workflow, conformance gate, how to add a new connector.
- **Governance:** [GOVERNANCE.md](GOVERNANCE.md) — acceptance criteria, stewardship, deprecation, graduation.
- **Kernel contract:** [ockham-sh/parsimony `docs/contract.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md) — the public spec every connector implements.

Anyone may contribute. The conformance suite is the merge gate.

## Relation to the parsimony kernel

The kernel is a thin shell: connector primitives, entry-point discovery, conformance, scaffolding. It knows nothing about specific providers. Connectors are independent of the kernel's release cadence except for a declared contract-version pin.

## License

Apache 2.0. Every connector that ships from this repository agrees to Apache 2.0 redistribution. Commercial providers whose terms of service forbid Apache 2.0 redistribution ship as separate external packages, not from this monorepo — see GOVERNANCE.md §6.
