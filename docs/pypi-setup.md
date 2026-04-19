# PyPI trusted publisher setup

Each `parsimony-<name>` distribution needs a PyPI trusted publisher configured once before `release.yml` can publish.

**Reference:** [PyPI trusted-publisher docs](https://docs.pypi.org/trusted-publishers/) and [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish).

## Per-package setup

On PyPI, under the project's *Publishing* → *Manage trusted publishers*, add a new GitHub publisher:

- Owner: `ockham-sh`
- Repository: `parsimony-connectors`
- Workflow: `release.yml`
- Environment: `pypi-<name>` (matches the `environment:` block in the publish job — hyphen-separated, matches the package name)

Restrict the `pypi-<name>` environment to the `main` branch under GitHub's environment protection rules.

## Cutover for packages with an existing trusted publisher

`parsimony-fred` and `parsimony-sdmx` already publish from their standalone repos. For each:

1. Remove the old trusted publisher entry in PyPI.
2. Add the new one pointing at this monorepo's `release.yml` + `pypi-<name>` environment.
3. Bump the version in `packages/<name>/pyproject.toml` and tag `<name>@v<new-version>` to execute the cutover.

Do the cutover during a quiet period — between steps 1 and 2 neither repo can publish a hotfix.
