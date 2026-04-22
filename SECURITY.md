# Security

## Reporting a vulnerability

Please do NOT open a public GitHub issue for security-sensitive reports.

Email: security@ockham.sh (or open a GitHub security advisory on this repo).

For kernel-level contract or entry-point-loading vulnerabilities, see also
the parsimony kernel's `SECURITY.md` at
<https://github.com/ockham-sh/parsimony/blob/main/SECURITY.md>.

---

## Supply-chain posture

Every package in this repository publishes to PyPI via
[OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/) —
no long-lived `PYPI_API_TOKEN` secret exists in this repository. Every
third-party GitHub Action is pinned by commit SHA (see
`.github/workflows/*.yml`).

The monorepo workflow
(`.github/workflows/ci.yml`, `.github/workflows/release.yml`)
enforces:

- Regex-validated package names on every job (`^[a-z][a-z0-9_-]*$`).
- `hatchling.build` as the only permitted build backend.
- No direct-URL (`@…`) or VCS (`git+…`) dependencies in any published package.
- Version-not-already-on-PyPI check before every publish.
- Per-package CODEOWNERS review required before merge to `main`.

---

## Migration provenance

### MCP host — now lives in `ockham-sh/parsimony-mcp`

The MCP host adapter (`parsimony-mcp` on PyPI) briefly lived in this
monorepo as `packages/mcp/` (imported from `ockham-sh/parsimony-mcp`
on 2026-04-20 via `git-filter-repo`). With the introduction of the
kernel's `parsimony.discover` surface, the MCP host became a pure
consumer of the kernel contract and no longer shared build/test
infrastructure with the `parsimony.providers` plugins in this repo,
so it was moved back out to its own repository at
[`ockham-sh/parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp).
This monorepo is now reserved exclusively for `parsimony.providers`
plugin packages (enforced by the `Enforce plugin-only monorepo` step
in `.github/workflows/ci.yml`).

Full commit history for the MCP host is preserved in the
`ockham-sh/parsimony-mcp` repository. Contributions and
security-sensitive reports for the MCP host belong there.
