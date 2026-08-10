# PyPI trusted publisher setup

Each `parsimony-<name>` distribution needs a PyPI trusted publisher configured once before `release.yml` can publish.

**Reference:** [PyPI trusted-publisher docs](https://docs.pypi.org/trusted-publishers/) and [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish).

## Per-package setup

On PyPI, under the project's *Publishing* → *Manage trusted publishers*, add a new GitHub publisher:
- Project Name: `parsimony-<name>`
- Owner: `ockham-sh`
- Repository: `parsimony-connectors`
- Workflow: `release.yml`
- Environment: `pypi-<name>` (matches the `environment:` block in the publish job — hyphen-separated, matches the package name)

The environment name is the PEP 503 canonical form of the folder name, with a leading underscore stripped: `packages/alpha_vantage` → `pypi-alpha-vantage`, `packages/_shared` → `pypi-shared`. The `normalize` job in `release.yml` computes it; don't derive it by hand.

## Environment protection

A trusted publisher makes the upload credential-free, not deliberate. Without a protection rule on `pypi-<name>`, any `workflow_dispatch` from any branch publishes straight to PyPI with no pause — which is how all 23 packages shipped unattended on 2026-08-10. Every `pypi-<name>` environment carries the same two rules as `parsimony`'s `pypi` environment:

- **Required reviewer** — the run parks on "waiting for approval" before the `publish` job starts. Self-review is allowed (`prevent_self_review: false`); with a single maintainer, forbidding it would make releases impossible.
- **Deployment branch policy** — `main` only. `workflow_dispatch` otherwise lets any branch publish arbitrary code under the trusted publisher.

When adding a package, create the environment with both rules before the first release:

```bash
env=pypi-<hyphen-name>
gh api -X PUT "repos/ockham-sh/parsimony-connectors/environments/$env" \
  -F wait_timer=0 -F prevent_self_review=false \
  -f 'reviewers[][type]=User' -F 'reviewers[][id]=<github-user-id>' \
  -F 'deployment_branch_policy[protected_branches]=false' \
  -F 'deployment_branch_policy[custom_branch_policies]=true'
gh api -X POST "repos/ockham-sh/parsimony-connectors/environments/$env/deployment-branch-policies" \
  -f name=main -f type=branch
```

## Release gotchas

**Don't check "is this version already on PyPI?" with `https://pypi.org/pypi/<name>/<version>/json`.** That endpoint sits behind a CDN that lags a publish by minutes — on 2026-08-10 it reported nine packages as missing a version they had in fact just uploaded. PyPI purges the *simple* index on upload, so `https://pypi.org/simple/<name>/` with `Accept: application/vnd.pypi.simple.v1+json` is the answer that is actually current; its PEP 700 `versions` array gives an exact membership test. `release.yml`'s guard uses that, and treats an unreachable PyPI as "couldn't tell" rather than "not published" — a timeout is not proof of absence.