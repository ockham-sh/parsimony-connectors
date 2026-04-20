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

### `packages/mcp/` — imported from `ockham-sh/parsimony-mcp` on 2026-04-20

The MCP host adapter (`parsimony-mcp` on PyPI) was previously developed
in its own repository at `ockham-sh/parsimony-mcp`. On 2026-04-20 that
repository was consolidated into this monorepo as `packages/mcp/`,
preserving full commit history via `git-filter-repo`.

- **Source repository:** `ockham-sh/parsimony-mcp`
- **Source branch:** `feat/finalize-0.1.0a1`
- **Source HEAD SHA at migration:** `475f6ee0350a6c0681923f482f8dfdc8fd636e00`
- **git-filter-repo version:** `2.47.0`
- **Rewrite command:**
  ```
  git filter-repo --force \
      --path-rename ':packages/mcp/' \
      --commit-callback 'commit.gpgsig = None'
  ```
- **Migration commit in this repo:** see the `chore(mcp): import parsimony-mcp history into packages/mcp/` merge commit.

The `--commit-callback` strips `gpgsig` headers defensively; source
commits were unsigned at the time of migration, so no verifiable
signatures were invalidated. The original repository
`ockham-sh/parsimony-mcp` was archived (not deleted) at migration time
to preserve external links — it serves as a reference copy of the
pre-migration history for auditors who wish to verify the rewrite was
faithful.

Auditing the rewrite: clone both repositories at the migration commit,
and compare (in the source repo) every file's blob SHA against
(in this monorepo) the same file under `packages/mcp/<path>`. Blob SHAs
must match exactly, as `git-filter-repo --path-rename` changes only
tree entries, never blob contents.
