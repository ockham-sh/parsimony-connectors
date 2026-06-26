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

Restrict the `pypi-<name>` environment to the `main` branch under GitHub's environment protection rules.