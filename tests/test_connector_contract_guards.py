"""Static guards: connector bodies must not construct framework envelopes."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_PACKAGES = Path(__file__).resolve().parents[1] / "packages"

_BANNED_SOURCE_PATTERNS = (
    re.compile(r"return\s+Result\s*\("),
    re.compile(r"return\s+TabularResult"),
    re.compile(r"return\s+\w+\.build_table_result\s*\("),
    re.compile(r"return\s+\w+\.build_entities\s*\("),
    re.compile(r"return\s+\([^)]+,\s*\{"),
    re.compile(r"Provenance\s*\("),
    re.compile(r"\.with_properties\s*\("),
)

_LEGACY_DOC_NAMES = ("CatalogEntry", "build_entries", "set_entries")


def _connector_modules() -> list[Path]:
    paths: list[Path] = []
    for init in _PACKAGES.glob("*/parsimony_*/__init__.py"):
        paths.append(init)
    for helper in _PACKAGES.glob("*/parsimony_*/*.py"):
        if helper.name in {"_http.py", "_screener.py", "search.py", "outputs.py"}:
            paths.append(helper)
    for extra in _PACKAGES.glob("*/parsimony_*/connectors/*.py"):
        if extra.name.startswith("_"):
            continue
        paths.append(extra)
    return sorted(set(paths))


@pytest.mark.parametrize("path", _connector_modules(), ids=lambda p: p.parent.name)
def test_connector_source_has_no_banned_return_patterns(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    for pattern in _BANNED_SOURCE_PATTERNS:
        assert not pattern.search(text), f"{path}: forbidden pattern {pattern.pattern}"


@pytest.mark.parametrize("path", _connector_modules(), ids=lambda p: p.parent.name)
def test_enumerator_functions_do_not_return_list_entity(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        if not node.name.startswith("enumerate_"):
            continue
        ret = node.returns
        if ret is None:
            continue
        ann = ast.unparse(ret)
        assert "Entity" not in ann, f"{path}:{node.name} must return pd.DataFrame, not {ann}"


def test_docs_do_not_document_removed_catalog_entry_vocabulary() -> None:
    repo = Path(__file__).resolve().parents[1]
    doc_paths = [
        p
        for p in (
            *(repo / "docs").glob("**/*.md"),
            repo.parent / "parsimony" / "docs" / "contract.md",
            repo.parent / "parsimony" / "README.md",
        )
        if p.is_file() and "CHANGELOG" not in p.name
    ]
    for path in doc_paths:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for name in _LEGACY_DOC_NAMES:
            assert name not in text, f"{path} still mentions removed primitive {name!r}"
