# CLI and tooling reference

Two command-line surfaces sit around this repo: the `parsimony` CLI (shipped by `parsimony-core`,
available wherever the kernel is installed) and the repo's `make` targets (the developer
workbench for working on the connector packages themselves). This page covers both, plus the
raw `uv`/`pytest` equivalents and the environment variables that affect runtime.

## The `parsimony` CLI

`parsimony-core` installs a `parsimony` console script with two verbs: `list` and `cache`.
Inside the repo, run it through `uv` (`uv run parsimony …`); installed system-wide it is just
`parsimony …`.

### `parsimony list`

Enumerates the installed connector plugins discovered through the `parsimony.providers`
entry-point group — one row per `parsimony-<name>` distribution present in the environment.

```bash
parsimony list                 # table of plugins, versions, connector counts
parsimony list --json          # the same as JSON
parsimony list --strict        # also run conformance; exit non-zero on any failure
```

| Flag | Effect |
| --- | --- |
| `--json` | Emit JSON instead of the table. |
| `--strict` | Import each plugin, run the conformance suite, and list each connector's declared credential parameter **names** (e.g. `api_key`, never values). Exits non-zero if any plugin fails. |

Without `--strict` the command is metadata-only — it does not import plugin code, so the
`CONFORMANCE` column reads `skipped` and `SECRETS` reads `?`. The table shape:

```text
NAME       VERSION  CONNECTORS  CONFORMANCE  SECRETS
---------  -------  ----------  -----------  -------
fred       0.0.1    2           pass         api_key
treasury   0.0.1    4           pass         -
sdmx       0.0.1    6           pass         -

3 plugin(s) discovered.
```

In `--strict` mode the `SECRETS` cell shows the declared credential parameter names, `-` when
a plugin declares none, and a trailing `! <name>: <detail>` line is printed for any failure.
With no plugins installed the command prints a hint to `pip install parsimony-fred`.

Conformance is `parsimony.testing.assert_plugin_valid` — a six-check suite (a `CONNECTORS`
export, non-empty descriptions, and similar contract requirements). It is the merge gate in
this repo's CI, and `make verify` runs the strict listing as its final gate.

### `parsimony cache`

Inspects and clears the global parsimony cache (HF catalog snapshots, ONNX models, connector
scratch, staging). The cache root resolves through `PARSIMONY_CACHE_DIR`, defaulting to the
platform user-cache dir, and contains four subdirectories: `catalogs`, `models`, `connectors`,
`staging`.

```bash
parsimony cache path                       # print the resolved cache root
parsimony cache info                       # per-subdir occupancy table
parsimony cache info --json                # the same as JSON
parsimony cache clear                      # remove all subdirs (prompts first)
parsimony cache clear --subdir catalogs    # remove only one subdir
parsimony cache clear --subdir catalogs --yes   # skip the confirmation prompt
```

| Action | Flags | Effect |
| --- | --- | --- |
| `path` | — | Print the resolved cache root and exit. |
| `info` | `--json` | Show files/size/path per subdir; footer prints `root:`. |
| `clear` | `--subdir NAME`, `--yes` | Remove a named subdir (or all four). Prompts for confirmation unless `--yes`. An unknown `--subdir` exits with an error naming the valid set. |

`parsimony cache info` shape:

```text
SUBDIR      FILES  SIZE      PATH
----------  -----  --------  -------------------------------------------
catalogs    18     412.6 MB  /home/you/.cache/parsimony/catalogs
models      3      88.1 MB   /home/you/.cache/parsimony/models
connectors  -      -         /home/you/.cache/parsimony/connectors
staging     -      -         /home/you/.cache/parsimony/staging

root: /home/you/.cache/parsimony
```

A subdir that does not exist on disk shows `-` for files and size.

## Make targets

The repo's `Makefile` is the developer workbench. Every target runs through `uv`, and
`make verify` mirrors CI exactly, so "green locally" means "green in CI".

| Target | Runs | Purpose |
| --- | --- | --- |
| `make sync` | `uv sync --all-extras --all-packages` | Resolve and install the whole uv workspace (every package, all extras). |
| `make verify PKG=<name>` | `ruff check` → `mypy` → `pytest` → `parsimony list --strict`, all scoped to `packages/<name>` | Four-gate check on one package. `PKG` must match a directory under `packages/`. |
| `make verify-all` | `make verify` for every `packages/*/` (skips `_shared`) | The four-gate check across all packages. |
| `make readme-roster` | `uv run python scripts/gen_roster.py --update-readme` | Regenerate the roster table in `README.md` and `docs/index.md`. |
| `make clean` | `rm -rf` of `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `__pycache__` | Wipe local tool caches. |

```bash
make sync                  # bootstrap the workspace
make verify PKG=treasury   # ruff + mypy + pytest + strict listing for one package
make verify-all            # the same, across every package
make readme-roster         # refresh the roster table after adding/changing a connector
```

### Raw `uv` / `pytest` equivalents

The Make targets are thin wrappers; you can run the underlying commands directly when you want
a narrower scope:

```bash
# what `make sync` runs
uv sync --all-extras --all-packages

# the four gates of `make verify PKG=treasury`, individually
uv run ruff check packages/treasury
uv run mypy packages/treasury
uv run pytest packages/treasury
uv run parsimony list --strict

# run a single package's tests, or the whole suite
uv run pytest packages/fred
uv run pytest

# include the live-API integration tests (each package marks them; deselected by default)
uv run pytest packages/fred -m integration
```

Integration tests hit live provider APIs and require the relevant `<PROVIDER>_API_KEY` (and,
for `sec_edgar`, `SEC_EDGAR_USER_AGENT`). They are excluded by default via each package's
`addopts = "-m 'not integration'"`.

## Runtime environment variables

These affect connector behaviour at call time, regardless of how the connector was launched.

### Credentials

Required- and optional-key connectors resolve their key from `<PROVIDER>_API_KEY` when none is
bound (see [../concepts/credentials.md](../concepts/credentials.md)):

```text
ALPHA_VANTAGE_API_KEY  BDF_API_KEY  COINGECKO_API_KEY  EIA_API_KEY  EODHD_API_KEY
FINNHUB_API_KEY  FMP_API_KEY  FRED_API_KEY  TIINGO_API_KEY        # required
BLS_API_KEY  RIKSBANK_API_KEY                                      # optional (quota only)
```

`SEC_EDGAR_USER_AGENT` is **not** a secret — it is the required `User-Agent` header
(`name email`) that SEC's fair-access policy demands. A missing value fast-fails before any
network call.

### Catalog and cache

| Variable | Effect |
| --- | --- |
| `PARSIMONY_CACHE_DIR` | Override the entire cache root (catalogs, models, connectors, staging). Useful on CI runners, HF Spaces, or alternate disks. |
| `PARSIMONY_<PROVIDER>_CATALOG_URL` | Point a single catalog-backed connector's `<provider>_search` at an alternate snapshot (e.g. `PARSIMONY_TREASURY_CATALOG_URL=file:///tmp/treasury`). Takes precedence over the default `hf://parsimony-dev/<provider>`. Not a credential. |

The per-provider override and the default Hub home are part of the catalog resolution order
(explicit `catalog_url` arg → env var → `hf://` default → on-disk cache → cold rebuild); see
[../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md).

### Publishing

| Variable | Effect |
| --- | --- |
| `HF_TOKEN` | Read by `huggingface_hub` when an operator pushes a built catalog to a `hf://` URL (`catalog.save(...)` / `scripts/build_catalog.py --push`). Only needed for the publish step, never at fetch time. |

For the operator build-and-publish workflow, see
[../guides/building-catalogs.md](../guides/building-catalogs.md). For adding a new connector
package, see [../contributing/authoring-a-connector.md](../contributing/authoring-a-connector.md).

## See also

- [providers.md](providers.md) — the per-provider auth, env var, and connector reference.
- [../guides/building-catalogs.md](../guides/building-catalogs.md) — building, validating, and
  publishing catalog snapshots.
- [../contributing/authoring-a-connector.md](../contributing/authoring-a-connector.md) —
  adding a new connector package and passing the conformance gate.
