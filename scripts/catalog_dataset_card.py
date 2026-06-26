"""Generate Hugging Face dataset cards for Parsimony catalog snapshots.

Maintainer tooling only — enables the HF Dataset Viewer on catalog repos by
declaring ``entries.parquet`` paths in README.md YAML frontmatter.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_FRONTMATTER_RE = re.compile(r"^---\s*\n.*?\n---\s*\n?", re.DOTALL)


@dataclass(frozen=True, slots=True)
class CatalogViewerConfig:
    """One HF config/subset pointing at a catalog ``entries.parquet``."""

    config_name: str
    entry_path: str
    split: str = "train"


def _yaml_quote(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def render_frontmatter(
    configs: list[CatalogViewerConfig],
    *,
    viewer: bool = True,
    license_id: str = "apache-2.0",
) -> str:
    """Render YAML frontmatter for a Parsimony catalog dataset card."""

    lines = ["---", f"viewer: {'true' if viewer else 'false'}", f"license: {_yaml_quote(license_id)}"]
    if configs:
        lines.append("configs:")
        for cfg in configs:
            lines.append(f"  - config_name: {_yaml_quote(cfg.config_name)}")
            lines.append("    data_files:")
            lines.append(f"      - split: {_yaml_quote(cfg.split)}")
            lines.append(f"        path: {_yaml_quote(cfg.entry_path)}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def default_markdown_body(*, repo_id: str, configs: list[CatalogViewerConfig]) -> str:
    """Default README body when no existing markdown is present."""

    config_list = ", ".join(f"``{c.config_name}``" for c in configs) or "``default``"
    return f"""# {repo_id}

Parsimony catalog snapshot published for in-process search (FAISS/BM25 indexes).

The Hugging Face Dataset Viewer shows indexed **entries** from ``entries.parquet`` only.
Search index artifacts (``indexes/``, ``embeddings.faiss``, etc.) are ignored by the viewer.

**Configs:** {config_list}

Load programmatically:

```python
from parsimony.catalog import Catalog

catalog = Catalog.load("hf://{repo_id}")
```
"""


def render_dataset_card(
    *,
    repo_id: str,
    configs: list[CatalogViewerConfig],
    existing_body: str | None = None,
    viewer: bool = True,
    license_id: str = "apache-2.0",
) -> str:
    """Render a full README.md (frontmatter + markdown body)."""

    body = existing_body.strip() if existing_body and existing_body.strip() else default_markdown_body(
        repo_id=repo_id, configs=configs
    )
    if not body.endswith("\n"):
        body += "\n"
    return render_frontmatter(configs, viewer=viewer, license_id=license_id) + "\n" + body


def strip_frontmatter(content: str) -> str:
    """Return markdown body without leading YAML frontmatter."""

    match = _FRONTMATTER_RE.match(content)
    if match is None:
        return content
    return content[match.end() :]


def flat_catalog_config(*, config_name: str = "default", entry_path: str = "entries.parquet") -> CatalogViewerConfig:
    """Viewer config for a single-bundle catalog at repo root."""

    return CatalogViewerConfig(config_name=config_name, entry_path=entry_path)


def bundle_catalog_config(bundle_name: str) -> CatalogViewerConfig:
    """Viewer config for one sub-bundle inside a multi-bundle repo."""

    return CatalogViewerConfig(config_name=bundle_name, entry_path=f"{bundle_name}/entries.parquet")


def configs_from_repo_tree(repo_root: Path) -> list[CatalogViewerConfig]:
    """Discover viewer configs from a local catalog tree (flat or multi-bundle)."""

    root_entries = repo_root / "entries.parquet"
    if root_entries.is_file():
        return [flat_catalog_config()]

    configs: list[CatalogViewerConfig] = []
    for child in sorted(repo_root.iterdir()):
        if not child.is_dir():
            continue
        if (child / "entries.parquet").is_file() and (child / "meta.json").is_file():
            configs.append(bundle_catalog_config(child.name))
    return configs


__all__ = [
    "CatalogViewerConfig",
    "bundle_catalog_config",
    "configs_from_repo_tree",
    "flat_catalog_config",
    "render_dataset_card",
    "render_frontmatter",
    "strip_frontmatter",
]
