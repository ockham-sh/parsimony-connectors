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

This monorepo is reserved exclusively for `parsimony.providers` plugin
packages (enforced by the `Enforce plugin-only monorepo` step in
`.github/workflows/ci.yml`).
