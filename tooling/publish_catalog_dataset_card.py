#!/usr/bin/env python3
"""Publish or update a Hugging Face dataset card for Parsimony catalog viewer support.

Maintainer tooling only — uploads README.md with YAML ``configs`` so the HF Dataset
Viewer can browse ``entries.parquet`` without re-uploading index artifacts.

Examples:
  uv run python scripts/publish_catalog_dataset_card.py \\
    --repo-id parsimony-dev/riksbank

  uv run python scripts/publish_catalog_dataset_card.py \\
    --repo-id parsimony-dev/sdmx --from-local /tmp/parsimony-catalogs/sdmx --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "scripts"))

from catalog_dataset_card import (  # noqa: E402
    configs_from_repo_tree,
    flat_catalog_config,
    render_dataset_card,
    strip_frontmatter,
)
from catalog_validate.registry import PROVIDER_SPECS, SDMX_ROOT_DEFAULT  # noqa: E402

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_ROOT = Path("/tmp/parsimony-catalogs")
_MULTI_BUNDLE_PROVIDERS = frozenset({"boj", "sdmx"})


def _repo_id_from_hf_url(url: str) -> str:
    return url.removeprefix("hf://").rstrip("/")


def _local_root(provider: str, catalog_root: Path) -> Path:
    return catalog_root / provider


def _publish_one(
    repo_id: str,
    *,
    from_local: Path | None,
    preserve_body: bool,
    dry_run: bool,
    commit_message: str,
    config: str,
    entry_path: str,
) -> None:
    if from_local is not None:
        configs = configs_from_repo_tree(from_local)
        if not configs:
            raise SystemExit(f"No catalog bundles with entries.parquet under {from_local}")
    else:
        configs = [flat_catalog_config(config_name=config, entry_path=entry_path)]

    existing_body = _fetch_existing_body(repo_id) if preserve_body else None
    readme = render_dataset_card(repo_id=repo_id, configs=configs, existing_body=existing_body)

    if dry_run:
        print(f"# {repo_id} ({len(configs)} config(s))")
        print(readme)
        return

    logger.info("Uploading README.md to %s (%d config(s))", repo_id, len(configs))
    _upload_readme(repo_id, readme, commit_message=commit_message)
    logger.info("Done: https://huggingface.co/datasets/%s", repo_id)


def _fetch_existing_body(repo_id: str) -> str | None:
    from huggingface_hub import hf_hub_download

    try:
        path = hf_hub_download(repo_id=repo_id, filename="README.md", repo_type="dataset")
    except Exception:
        return None
    content = Path(path).read_text(encoding="utf-8")
    body = strip_frontmatter(content).strip()
    return body or None


def _upload_readme(repo_id: str, content: str, *, commit_message: str) -> None:
    from huggingface_hub import HfApi

    api = HfApi()
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=commit_message,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _iter_batch_targets(args: argparse.Namespace) -> list[tuple[str, Path]]:
    catalog_root = args.catalog_root
    targets: list[tuple[str, Path]] = []

    if args.all or args.provider:
        providers = sorted(PROVIDER_SPECS) if args.all else [args.provider]
        for provider in providers:
            if provider in _MULTI_BUNDLE_PROVIDERS:
                continue
            local = _local_root(provider, catalog_root)
            if not local.is_dir():
                logger.warning("Skipping %s — no local tree at %s", provider, local)
                continue
            repo_id = _repo_id_from_hf_url(PROVIDER_SPECS[provider].default_url)
            targets.append((repo_id, local))

    if args.all or args.provider == "sdmx":
        local = catalog_root / "sdmx"
        if local.is_dir():
            targets.append((_repo_id_from_hf_url(SDMX_ROOT_DEFAULT), local))
        elif args.provider == "sdmx":
            raise SystemExit(f"No SDMX tree at {local}")

    if args.all or args.provider == "boj":
        local = catalog_root / "boj"
        if local.is_dir():
            targets.append((_repo_id_from_hf_url(PROVIDER_SPECS["boj"].default_url), local))
        elif args.provider == "boj":
            raise SystemExit(f"No BoJ tree at {local}")

    return targets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-id", help="HF dataset repo, e.g. parsimony-dev/riksbank")
    parser.add_argument("--provider", choices=[*sorted(PROVIDER_SPECS), "sdmx"], help="Publish from registry + local tree")
    parser.add_argument("--all", action="store_true", help="Publish cards for every provider with a local tree")
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=_DEFAULT_CATALOG_ROOT,
        help="Local catalog root (default: /tmp/parsimony-catalogs)",
    )
    parser.add_argument("--config", default="default", help="HF config/subset name (flat catalogs)")
    parser.add_argument("--entry-path", default="entries.parquet", help="Parquet path relative to repo root")
    parser.add_argument(
        "--from-local",
        type=Path,
        help="Discover configs from a local catalog tree (flat or multi-bundle)",
    )
    parser.add_argument("--preserve-body", action="store_true", help="Keep existing README markdown body")
    parser.add_argument("--dry-run", action="store_true", help="Print README.md without uploading")
    parser.add_argument(
        "--commit-message",
        default="Add dataset card for HF Dataset Viewer (entries.parquet)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    batch = _iter_batch_targets(args)
    if batch:
        for repo_id, local in batch:
            _publish_one(
                repo_id,
                from_local=local,
                preserve_body=args.preserve_body,
                dry_run=args.dry_run,
                commit_message=args.commit_message,
                config=args.config,
                entry_path=args.entry_path,
            )
        return

    if args.repo_id is None:
        raise SystemExit("Provide --repo-id, --provider, or --all")

    _publish_one(
        args.repo_id,
        from_local=args.from_local,
        preserve_body=args.preserve_body,
        dry_run=args.dry_run,
        commit_message=args.commit_message,
        config=args.config,
        entry_path=args.entry_path,
    )


if __name__ == "__main__":
    main()
