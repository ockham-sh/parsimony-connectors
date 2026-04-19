# OFFICIAL_PLUGINS manifest

[`OFFICIAL_PLUGINS.json`](../OFFICIAL_PLUGINS.json) at the repo root enumerates every officially-maintained `parsimony-<name>` distribution. This monorepo is the editorial source of truth: a package is "official" iff it appears here.

The kernel ships a copy of this file bundled inside its wheel at `parsimony/discovery/_data/OFFICIAL_PLUGINS.json` and loads it via `importlib.resources`. Each kernel release refreshes the bundled copy from `main` of this repo.

## When to update

Edit `OFFICIAL_PLUGINS.json` when a connector is added or removed. The PR moves in one repo; the next kernel release picks up the change.

## Schema

```json
{
  "version": 1,
  "distributions": ["parsimony-<name>", ...]
}
```
