#!/usr/bin/env python3
"""Prune a Hugging Face catalog path, then upload a local snapshot.

Maintainer tooling only. Ensures removed index files do not linger after rebuilds.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "tooling"))

from catalog_validate.release_surface import is_publishable_local_bundle  # noqa: E402

logger = logging.getLogger(__name__)


def _parse_hf_url(url: str) -> tuple[str, str]:
    rest = url.removeprefix("hf://")
    parts = rest.split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid hf catalog URL: {url!r}")
    repo_id = f"{parts[0]}/{parts[1]}"
    subpath = "/".join(parts[2:]) if len(parts) > 2 else ""
    return repo_id, subpath


def _hf_cli() -> list[str]:
    return ["uv", "tool", "run", "hf"]


def _run_hf(args: list[str]) -> None:
    cmd = _hf_cli() + args
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _delete_remote_path(repo_id: str, path_in_repo: str) -> None:
    if not path_in_repo:
        return
    # Use the Hub API, not the CLI: the ``hf`` CLI has no ``delete`` subcommand,
    # so a CLI call no-ops and silently leaves stale files behind. A path may be
    # a folder (e.g. ``indexes``) or a file (``meta.json``); try folder first,
    # then file, and treat "already absent" (both shapes fail) as nothing to do.
    from huggingface_hub import HfApi

    api = HfApi()
    errors: list[str] = []
    for delete, kind in ((api.delete_folder, "folder"), (api.delete_file, "file")):
        try:
            delete(path_in_repo=path_in_repo, repo_id=repo_id, repo_type="dataset")
            return
        except Exception as exc:  # noqa: BLE001 - try the other shape before giving up
            errors.append(f"{kind}: {exc}")
    logger.debug("Nothing to delete at %s/%s (%s)", repo_id, path_in_repo, "; ".join(errors))


def _list_remote_top_level(repo_id: str) -> list[str]:
    from huggingface_hub import HfApi

    api = HfApi()
    names: list[str] = []
    for item in api.list_repo_tree(repo_id, repo_type="dataset", recursive=False):
        names.append(getattr(item, "path", str(item)))
    return names


def prune_multi_bundle_repo(repo_id: str, *, keep_paths: set[str], dry_run: bool = False) -> list[str]:
    """Delete remote top-level entries not in *keep_paths* or allowlist."""
    from catalog_validate.release_surface import MULTI_BUNDLE_ROOT_ALLOWLIST, SDMX_STRUCTURE_PREFIX

    removed: list[str] = []
    for name in _list_remote_top_level(repo_id):
        if name in MULTI_BUNDLE_ROOT_ALLOWLIST:
            continue
        if name in keep_paths:
            continue
        if name.startswith(SDMX_STRUCTURE_PREFIX):
            removed.append(name)
            if not dry_run:
                _delete_remote_path(repo_id, name)
            continue
        # Root-level catalog files on multi-bundle repos are excess.
        if name in {"meta.json", "series.parquet", "entries.parquet"}:
            removed.append(name)
            if not dry_run:
                _delete_remote_path(repo_id, name)
            continue
        # Stale bundle directories from prior releases.
        if name not in keep_paths:
            removed.append(name)
            if not dry_run:
                _delete_remote_path(repo_id, name)
    return removed


def push_catalog(
    catalog_url: str,
    local_dir: Path,
    *,
    commit_message: str,
    prune: bool = True,
    dry_run: bool = False,
) -> None:
    if not local_dir.is_dir():
        raise SystemExit(f"Local snapshot not found: {local_dir}")
    if not (local_dir / "meta.json").is_file():
        raise SystemExit(f"Expected meta.json at {local_dir / 'meta.json'}")
    if not is_publishable_local_bundle(local_dir):
        raise SystemExit(f"Refusing to publish non-runtime bundle: {local_dir.name}")

    repo_id, subpath = _parse_hf_url(catalog_url)

    if dry_run:
        logger.info("DRY RUN: would push %s -> %s (subpath=%r)", local_dir, repo_id, subpath)
        return

    _run_hf(["repos", "create", repo_id, "--repo-type", "dataset", "--exist-ok"])

    if prune and subpath:
        _delete_remote_path(repo_id, subpath)
    elif prune and not subpath:
        # Flat repo: delete all remote files except allowlist, then upload fresh tree.
        for name in _list_remote_top_level(repo_id):
            if name in {".gitattributes", "README.md"}:
                continue
            _delete_remote_path(repo_id, name)

    upload_args = ["upload", repo_id, str(local_dir)]
    if subpath:
        upload_args.append(subpath)
    upload_args.extend(["--repo-type", "dataset", "--commit-message", commit_message])
    _run_hf(upload_args)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog_url", help="hf://org/repo or hf://org/repo/subpath")
    parser.add_argument("local_dir", type=Path)
    parser.add_argument("--commit-message", default="catalog snapshot schema v1 rebuild")
    parser.add_argument("--no-prune", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    push_catalog(
        args.catalog_url,
        args.local_dir.resolve(),
        commit_message=args.commit_message,
        prune=not args.no_prune,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
