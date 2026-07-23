"""Unit tests for ``scripts/gen_roster.py``.

Covers the two behaviors added on top of the existing README-table
generator: deriving ``keyless`` from the same AST sweep that counts
connectors, and the ``connectors.json`` manifest (render, idempotent write,
``--check`` freshness gate, and the versioned envelope contract shared with
``ockham-sh/parsimony#97`` / ``ockham-sh/landing-page#12``).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import gen_roster  # noqa: E402 — path insert must precede this import

# ---------------------------------------------------------------------------
# keyless derivation
# ---------------------------------------------------------------------------


def test_count_connectors_keyless_true_with_no_secrets(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\ndef toy_fetch(series_id: str) -> pd.DataFrame: ...\n',
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert keyless is True


def test_count_connectors_keyless_false_when_any_connector_declares_secrets(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=SEARCH_OUTPUT, tags=["macro", "tool"], secrets=("api_key",))\n'
        'def toy_search(search_text: str, api_key: str = "") -> pd.DataFrame: ...\n\n'
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\n'
        "def toy_fetch(series_id: str) -> pd.DataFrame: ...\n",
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 2
    assert keyless is False


def test_count_connectors_ignores_docstring_mentions_of_secrets(tmp_path: Path) -> None:
    """A module documenting that it has *no* secrets (treasury/sec_edgar style) stays keyless.

    The prose mentions the literal substring ``secrets=`` outside any decorator's
    call — AST-based scanning never sees docstring/comment text as a keyword
    argument, so this needs no special-case guard.
    """
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '"""Toy connector.\n\n'
        "Keyless — no ``secrets=``/``bind()``/``UnauthorizedError``; deliberately "
        'not declared via ``secrets=``.\n"""\n\n'
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\n'
        "def toy_fetch(series_id: str) -> pd.DataFrame: ...\n",
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert keyless is True


def test_count_connectors_counts_search_factory_as_keyless(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        "toy_search = make_local_search_connector(\n"
        '    provider="toy",\n'
        '    default_url="https://example.test/catalog",\n'
        '    catalog_url_env_var="TOY_CATALOG_URL",\n'
        '    tags=["macro"],\n'
        '    description="Search the toy catalog.",\n'
        ")\n",
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert keyless is True


def test_count_connectors_missing_module_dir_is_keyless_with_zero_count(tmp_path: Path) -> None:
    count, keyless = gen_roster._count_connectors(tmp_path / "does_not_exist")

    assert count == 0
    assert keyless is True


def test_count_connectors_not_truncated_by_nested_calls_in_decorator_kwargs(tmp_path: Path) -> None:
    """A nested call before ``secrets=`` must not hide the keyword argument.

    A text-based ``[^)]*``-style capture would stop at the first ``)``; the
    AST walk sees the full keyword list regardless of nested calls like
    ``Column(...)`` inside the decorator's own arguments.
    """
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=Column(name="x"), secrets=("api_key",))\ndef f(): ...\n',
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert keyless is False


def test_count_connectors_ignores_bare_decorator_without_call(tmp_path: Path) -> None:
    """A decorator referenced without a call (e.g. ``@connector``, no parens) is not ours to count.

    The kernel's ``@connector``/``@enumerator``/``@loader`` are always used as
    calls (they take at least ``output=``), so a bare name here is not a real
    connector declaration.
    """
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        "@connector\ndef f(): ...\n",
        encoding="utf-8",
    )

    count, keyless = gen_roster._count_connectors(module_dir)

    assert count == 0
    assert keyless is True


def test_count_connectors_raises_on_syntax_error(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text("def f(:\n", encoding="utf-8")

    with pytest.raises(SystemExit):
        gen_roster._count_connectors(module_dir)


# ---------------------------------------------------------------------------
# connectors.json rendering
# ---------------------------------------------------------------------------


def _row(**overrides: object) -> gen_roster.PackageInfo:
    defaults: dict[str, object] = {
        "name": "parsimony-toy",
        "description": "Toy connector for the parsimony framework",
        "homepage": "https://example.test",
        "entry_point": "toy",
        "module": "parsimony_toy",
        "connector_count": 2,
        "keyless": True,
    }
    defaults.update(overrides)
    return gen_roster.PackageInfo(**defaults)  # type: ignore[arg-type]


def test_render_json_shape_matches_manifest_contract() -> None:
    rows = [_row(name="parsimony-zeta", keyless=False), _row(name="parsimony-alpha", keyless=True)]

    payload = gen_roster._render_json(rows)

    assert set(payload.keys()) == {"schema_version", "generated_at", "connectors"}
    assert payload["schema_version"] == gen_roster.MANIFEST_SCHEMA_VERSION
    assert [p["package"] for p in payload["connectors"]] == ["parsimony-alpha", "parsimony-zeta"]
    entry = payload["connectors"][0]
    assert set(entry.keys()) == {"package", "provider", "entry_point", "connector_count", "keyless"}
    assert entry["provider"] == "Toy"
    assert entry["entry_point"] == "toy"
    assert entry["connector_count"] == 2
    assert entry["keyless"] is True


# ---------------------------------------------------------------------------
# connectors.json idempotent write
# ---------------------------------------------------------------------------


def test_update_connectors_json_writes_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "connectors.json"
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", target)
    payload = gen_roster._render_json([_row()])

    gen_roster._update_connectors_json(payload)

    assert json.loads(target.read_text(encoding="utf-8")) == payload


def test_update_connectors_json_noop_when_connectors_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "connectors.json"
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", target)
    payload = gen_roster._render_json([_row()])
    stale_but_same_connectors = {
        "schema_version": payload["schema_version"],
        "generated_at": "2020-01-01",
        "connectors": payload["connectors"],
    }
    target.write_text(json.dumps(stale_but_same_connectors), encoding="utf-8")

    gen_roster._update_connectors_json(payload)

    # generated_at was NOT bumped to today — the write was a no-op because
    # `connectors` didn't change.
    assert json.loads(target.read_text(encoding="utf-8"))["generated_at"] == "2020-01-01"


def test_update_connectors_json_rewrites_when_connectors_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "connectors.json"
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", target)
    old_payload = gen_roster._render_json([_row(connector_count=1)])
    stale = {
        "schema_version": old_payload["schema_version"],
        "generated_at": "2020-01-01",
        "connectors": old_payload["connectors"],
    }
    target.write_text(json.dumps(stale), encoding="utf-8")

    new_payload = gen_roster._render_json([_row(connector_count=99)])
    gen_roster._update_connectors_json(new_payload)

    written = json.loads(target.read_text(encoding="utf-8"))
    assert written["connectors"][0]["connector_count"] == 99


def test_update_connectors_json_tolerates_corrupt_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "connectors.json"
    target.write_text("{not valid json", encoding="utf-8")
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", target)
    payload = gen_roster._render_json([_row()])

    gen_roster._update_connectors_json(payload)

    assert json.loads(target.read_text(encoding="utf-8")) == payload


# ---------------------------------------------------------------------------
# package/entry-point metadata validation
# ---------------------------------------------------------------------------


def _write_provider_package(
    packages_dir: Path,
    *,
    dirname: str,
    name: str,
    entry_points: dict[str, str],
) -> None:
    pkg_dir = packages_dir / dirname
    pkg_dir.mkdir(parents=True)
    ep_lines = "\n".join(f'{k} = "{v}"' for k, v in entry_points.items())
    (pkg_dir / "pyproject.toml").write_text(
        f'[project]\nname = "{name}"\ndescription = "Toy connector for the parsimony framework"\n'
        f'[project.urls]\nHomepage = "https://example.test"\n'
        f'[project.entry-points."parsimony.providers"]\n{ep_lines}\n',
        encoding="utf-8",
    )


def test_gather_skips_package_with_no_entry_points_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    packages_dir = tmp_path / "packages"
    pkg_dir = packages_dir / "shared"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "pyproject.toml").write_text(
        '[project]\nname = "parsimony-shared"\ndescription = "Shared helpers."\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(gen_roster, "PACKAGES", packages_dir)

    rows = gen_roster._gather()

    assert rows == []


def test_gather_raises_on_malformed_package_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    packages_dir = tmp_path / "packages"
    _write_provider_package(
        packages_dir,
        dirname="bad",
        name="not-parsimony-named",
        entry_points={"bad": "parsimony_bad"},
    )
    monkeypatch.setattr(gen_roster, "PACKAGES", packages_dir)

    with pytest.raises(SystemExit):
        gen_roster._gather()


def test_gather_treats_declared_but_empty_entry_points_table_as_not_a_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``[project.entry-points."parsimony.providers"]`` header with zero keys under it
    parses to ``{}`` — indistinguishable from the table being absent altogether — so
    ``_gather`` takes the same "not a provider package" skip path as ``_shared`` rather
    than raising. TOML cannot express a header with a nonzero-but-empty entry count, so
    this is the only reachable shape for "declared but empty".
    """
    packages_dir = tmp_path / "packages"
    pkg_dir = packages_dir / "empty"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "pyproject.toml").write_text(
        '[project]\nname = "parsimony-empty"\ndescription = "Toy connector for the parsimony framework"\n'
        '[project.entry-points."parsimony.providers"]\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(gen_roster, "PACKAGES", packages_dir)

    rows = gen_roster._gather()

    assert rows == []


def test_gather_raises_when_entry_points_table_has_multiple_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packages_dir = tmp_path / "packages"
    _write_provider_package(
        packages_dir,
        dirname="multi",
        name="parsimony-multi",
        entry_points={"multi_a": "parsimony_multi_a", "multi_b": "parsimony_multi_b"},
    )
    monkeypatch.setattr(gen_roster, "PACKAGES", packages_dir)

    with pytest.raises(SystemExit):
        gen_roster._gather()


# ---------------------------------------------------------------------------
# --check mode
# ---------------------------------------------------------------------------


@pytest.fixture
def _fresh_roster_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Wire gen_roster's module-level path constants at a tmp tree, then
    materialize a fully fresh README/docs/index.md/connectors.json via
    ``--update-readme`` so ``--check`` starts from a known-clean baseline.
    """
    packages_dir = tmp_path / "packages" / "toy"
    packages_dir.mkdir(parents=True)
    (packages_dir / "pyproject.toml").write_text(
        "[project]\n"
        'name = "parsimony-toy"\n'
        'description = "Toy connector for the parsimony framework"\n'
        "[project.urls]\n"
        'Homepage = "https://example.test"\n'
        '[project.entry-points."parsimony.providers"]\n'
        'toy = "parsimony_toy"\n',
        encoding="utf-8",
    )
    module_dir = packages_dir / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\ndef toy_fetch(): ...\n',
        encoding="utf-8",
    )

    readme = tmp_path / "README.md"
    readme.write_text("# toy\n\n<!-- roster:start -->\n<!-- roster:end -->\n", encoding="utf-8")
    docs_index = tmp_path / "docs" / "index.md"
    connectors_json = tmp_path / "connectors.json"

    monkeypatch.setattr(gen_roster, "ROOT", tmp_path)
    monkeypatch.setattr(gen_roster, "PACKAGES", tmp_path / "packages")
    monkeypatch.setattr(gen_roster, "README", readme)
    monkeypatch.setattr(gen_roster, "DOCS_INDEX", docs_index)
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", connectors_json)

    assert gen_roster.main(["--update-readme"]) == 0
    return {"readme": readme, "docs_index": docs_index, "connectors_json": connectors_json}


def test_check_passes_on_freshly_generated_tree(_fresh_roster_tree: dict[str, Path]) -> None:
    assert gen_roster.main(["--check"]) == 0


def test_check_fails_when_connectors_json_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["connectors_json"].write_text(
        json.dumps({"schema_version": 1, "generated_at": "2020-01-01", "connectors": []})
    )

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_connectors_json_schema_version_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    current = json.loads(_fresh_roster_tree["connectors_json"].read_text(encoding="utf-8"))
    current["schema_version"] = current["schema_version"] + 1
    _fresh_roster_tree["connectors_json"].write_text(json.dumps(current), encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_readme_table_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    readme = _fresh_roster_tree["readme"]
    readme.write_text("# toy\n\n<!-- roster:start -->\nstale\n<!-- roster:end -->\n", encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_docs_index_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["docs_index"].write_text("stale", encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_update_readme_and_check_agree_they_can_never_drift(_fresh_roster_tree: dict[str, Path]) -> None:
    """Re-running ``--update-readme`` on an already-fresh tree is a no-op, and
    ``--check`` immediately after still passes — the two modes never disagree.
    """
    assert gen_roster.main(["--update-readme"]) == 0
    assert gen_roster.main(["--check"]) == 0
