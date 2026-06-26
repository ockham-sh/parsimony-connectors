#!/usr/bin/env python3
"""Gated bulk publish of schema-v1 catalogs to parsimony-dev/* on Hugging Face."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO / "tooling"))

from catalog_validate.registry import MACRO_CATALOG_PROVIDER_IDS, PROVIDER_SPECS  # noqa: E402
from catalog_validate.release_surface import (  # noqa: E402
    CANONICAL_CATALOG_ROOT,
    audit_local_root,
    is_publishable_local_bundle,
)
from prune_and_push_catalog import prune_multi_bundle_repo, push_catalog  # noqa: E402

logger = logging.getLogger(__name__)

FLAT_PROVIDERS = [p for p in sorted(MACRO_CATALOG_PROVIDER_IDS) if p != "boj"]


def _hf_auth_ok() -> bool:
    try:
        subprocess.run(["uv", "tool", "run", "hf", "auth", "whoami"], check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        return False


def publish_all(src: Path, *, dry_run: bool = False, require_bdf: bool = True) -> int:
    audit = audit_local_root(src, require_bdf=require_bdf)
    if not audit.ok:
        for msg in audit.missing_required:
            logger.error("Missing required bundle: %s", msg)
        return 1

    bundles = audit.bundles
    if not bundles:
        logger.error("No publishable bundles under %s", src)
        return 1

    # Prune multi-bundle repos before uploading fresh bundles.
    for repo in ("boj", "sdmx"):
        keep = {b.name for b in audit.bundles if b.provider == repo}
        if not keep:
            continue
        repo_id = f"parsimony-dev/{repo}"
        logger.info("Pruning stale paths on %s (keeping %d bundles)", repo_id, len(keep))
        removed = prune_multi_bundle_repo(repo_id, keep_paths=keep, dry_run=dry_run)
        if removed:
            verb = "Would remove" if dry_run else "Removed"
            logger.info("%s %d stale paths from %s", verb, len(removed), repo_id)

    for bundle in audit.bundles:
        local = src / bundle.local_rel
        if not is_publishable_local_bundle(local):
            logger.error("Skipping non-publishable bundle: %s", local)
            return 1
        logger.info("Publishing %s <- %s", bundle.hf_url, local)
        push_catalog(bundle.hf_url, local, commit_message="schema v1 clean rebuild", dry_run=dry_run)

    # Refresh multi-bundle dataset cards.
    if not dry_run:
        for repo, subdir in (("boj", "boj"), ("sdmx", "sdmx")):
            card_root = src / subdir
            if not card_root.is_dir():
                continue
            subprocess.run(
                [
                    "uv",
                    "run",
                    "python",
                    str(_REPO / "tooling" / "publish_catalog_dataset_card.py"),
                    "--repo-id",
                    f"parsimony-dev/{repo}",
                    "--from-local",
                    str(card_root),
                    "--preserve-body",
                    "--commit-message",
                    "Refresh dataset card",
                ],
                check=True,
            )
        # Flat repos get cards from push_catalog when uploading repo root.
        for provider in FLAT_PROVIDERS:
            flat = src / provider
            if not (flat / "meta.json").is_file():
                continue
            spec = PROVIDER_SPECS[provider]
            repo_id = spec.default_url.removeprefix("hf://")
            subprocess.run(
                [
                    "uv",
                    "run",
                    "python",
                    str(_REPO / "tooling" / "publish_catalog_dataset_card.py"),
                    "--repo-id",
                    repo_id,
                    "--from-local",
                    str(flat),
                    "--preserve-body",
                    "--commit-message",
                    "Refresh dataset card",
                ],
                check=True,
            )

    print("\nPost-publish validation:")
    print("  PARSIMONY_RUN_REMOTE_CATALOGS=1 uv run pytest tests/test_remote_catalogs.py -m remote_catalog")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--src",
        type=Path,
        default=CANONICAL_CATALOG_ROOT,
        help=f"Local catalog root (default: {CANONICAL_CATALOG_ROOT})",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-bdf-requirement", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.dry_run and not _hf_auth_ok() and not __import__("os").environ.get("HF_TOKEN"):
        logger.error("Set HF_TOKEN or run 'hf auth login' before publishing.")
        return 1

    return publish_all(args.src.resolve(), dry_run=args.dry_run, require_bdf=not args.skip_bdf_requirement)


if __name__ == "__main__":
    raise SystemExit(main())
