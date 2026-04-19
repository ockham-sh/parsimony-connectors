# parsimony-sdmx

Flat SDMX catalog builder. Harvests dataflows from statistical agencies (ECB, Eurostat, IMF, World Bank) and writes two parquet files per agency — a dataset-level catalog and one series-level catalog per dataset — ready to feed a FAISS or SQL index.

## Layout

```
outputs/{AGENCY}/datasets.parquet        # columns: dataset_id, agency_id, title
outputs/{AGENCY}/series/{DATASET}.parquet # columns: id, dataset_id, title
```

Titles are composed as `"CODE1: label1 - CODE2: label2 - …"` over the DSD's non-`TIME_PERIOD` dimensions in DSD order. ECB series additionally append `TITLE` / `TITLE_COMPL` fetched from the per-series XML endpoint. HTML-embedded descriptions (common on Eurostat) are stripped before they reach parquet.

## Install

Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
make install              # uv sync --all-extras
```

## Usage

```bash
# Print every dataset the agency exposes
parsimony-sdmx -a ESTAT --list-datasets

# Write only outputs/{AGENCY}/datasets.parquet (no series fetched)
parsimony-sdmx -a ECB --catalog

# Fetch one dataset
parsimony-sdmx -a ECB -d YC

# Fetch every dataset the agency exposes
parsimony-sdmx -a ESTAT --all

# Preview what an --all run would write, without fetching
parsimony-sdmx -a ESTAT --all --dry-run

# Rebuild datasets whose parquet already exists (resume contract: file present → skip)
parsimony-sdmx -a ECB -d YC --force
```

Or via Make shortcuts:

```bash
make catalog AGENCY=ECB            # datasets.parquet only
make fetch AGENCY=ECB DATASET=YC   # single dataset
make fetch-all AGENCY=ESTAT        # every dataset for the agency
make list AGENCY=IMF_DATA          # enumerate datasets
```

Supported agencies: `ECB`, `ESTAT`, `IMF_DATA`, `WB_WDI`. Exit codes: `0` every dataset ok/empty, `1` at least one failed.

## Architecture

Four packages under `parsimony_sdmx/`:

- **`core/`** — pure, I/O-free: record dataclasses, title composition, codelist resolution, outcome types, domain exceptions.
- **`io/`** — boundary effects: atomic parquet writers, hardened lxml iterparse, HTTPS-only bounded HTTP session, path safety helpers, exception classification.
- **`providers/`** — per-agency adapters behind a narrow `CatalogProvider` protocol; ECB/ESTAT/IMF share a common sdmx1 flow helper, WB diverges with a path × decade sweep because its SDMX endpoint doesn't expose `series_keys`.
- **`cli/`** — argparse front-end, orchestrator that forks one subprocess per dataset (`mp.spawn`) for memory isolation, a psutil-backed memory monitor that kills the largest child above a threshold and writes OOM markers, atomic `.tmp/` cleanup, and an operator-readable end-of-run summary.

### Why subprocess-per-dataset

`sdmx1` caches structure messages at module level with no public invalidation hook. Running every dataset in its own subprocess is the only reliable way to start each fetch with a clean cache. Large catalogs (8 k+ Eurostat dataflows) have hit real OOMs in production; the memory monitor catches runaway workers before the kernel OOM killer does, preserving a classifiable failure instead of an opaque `exit 137`.

### Resume

Filesystem-backed: a dataset is skipped if `outputs/{AGENCY}/series/{DATASET}.parquet` already exists. Writes land in `.tmp/` first and are `os.replace`-d atomically, so the canonical path exists iff the previous run completed. `--force` overrides (and logs a count of overwrites).

## Development

```bash
make check         # ruff + mypy strict + fast tests
make test          # fast tests only (skip subprocess-path tests)
make test-slow     # subprocess tests (fork real mp.Process children)
make test-all      # everything
make format        # ruff format + --fix
make clean         # wipe caches + build artifacts
```

Hardening enforced by default:

- `mypy --strict` across source and tests
- `ruff` with `E, F, W, I, B, UP, S` (security rules on)
- Hardened `lxml.iterparse` (no entity resolution, no DTD load, no network)
- HTTPS-only `bounded_get` with a configurable byte cap
- Path traversal guards on every on-disk write

## Project layout

```
parsimony_sdmx/
├── core/      # pure domain logic
├── io/        # boundary-layer effects
├── providers/ # per-agency adapters
└── cli/       # argparse → orchestrator → worker → parquet

tests/        # flat: test_<module>.py per source module
```

Test suite: 312 tests. 4 of them (`@pytest.mark.slow`) fork real subprocesses to exercise the orchestrator's timeout / OOM classification / clean-exit paths; the other 308 run in < 2 s.
