"""Unit tests for ``scripts/gen_roster.py``.

Covers the behaviors added on top of the existing README-table generator:
extracting ``requires=`` env vars (and ``secrets=`` presence) from the same
AST sweep that counts connectors, the schema-v2 ``connectors.json`` manifest
(render, idempotent write, ``--check`` freshness gate, and the versioned
envelope contract shared with ``ockham-sh/parsimony#97`` /
``ockham-sh/landing-page#12``), and the generated auth docs — the Auth /
Env-var cells of ``docs/reference/providers.md`` and the four marker-
delimited lists in ``docs/concepts/credentials.md``.
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
# requires / secrets extraction
# ---------------------------------------------------------------------------


def test_count_connectors_no_requires_no_secrets(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\ndef toy_fetch(series_id: str) -> pd.DataFrame: ...\n',
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ()
    assert has_secrets is False


def test_count_connectors_extracts_requires_tuple_literal(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, secrets=("api_key",), requires=("TOY_API_KEY",))\n'
        'def toy_fetch(series_id: str, api_key: str = "") -> pd.DataFrame: ...\n',
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ("TOY_API_KEY",)
    assert has_secrets is True


def test_count_connectors_accepts_requires_list_literal(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, requires=["TOY_USER_AGENT"])\ndef toy_fetch(): ...\n',
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ("TOY_USER_AGENT",)
    assert has_secrets is False


def test_count_connectors_requires_is_sorted_union_across_sites_and_files(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=FETCH_OUTPUT, requires=("B_VAR", "A_VAR"))\ndef toy_fetch(): ...\n\n'
        '@connector(output=SEARCH_OUTPUT, requires=("A_VAR",))\ndef toy_search(): ...\n',
        encoding="utf-8",
    )
    (module_dir / "_extra.py").write_text(
        '@enumerator(output=ENUM_OUTPUT, requires=("C_VAR",))\ndef toy_enumerate(): ...\n',
        encoding="utf-8",
    )

    count, requires, _has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 3
    assert requires == ("A_VAR", "B_VAR", "C_VAR")


def test_count_connectors_non_literal_requires_fails_loudly(tmp_path: Path) -> None:
    """A ``requires=`` that references a constant (ast can't resolve names) is a hard error."""
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '_REQUIRED = ("TOY_API_KEY",)\n\n'
        "@connector(output=FETCH_OUTPUT, requires=_REQUIRED)\ndef toy_fetch(): ...\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as excinfo:
        gen_roster._count_connectors(module_dir)

    message = str(excinfo.value)
    assert "toy_fetch" in message
    assert "__init__.py" in message


def test_count_connectors_non_string_element_in_requires_fails_loudly(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        "@connector(output=FETCH_OUTPUT, requires=(3,))\ndef toy_fetch(): ...\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit):
        gen_roster._count_connectors(module_dir)


def test_count_connectors_secrets_present_without_requires(tmp_path: Path) -> None:
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=SEARCH_OUTPUT, tags=["macro", "tool"], secrets=("api_key",))\n'
        'def toy_search(search_text: str, api_key: str = "") -> pd.DataFrame: ...\n\n'
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\n'
        "def toy_fetch(series_id: str) -> pd.DataFrame: ...\n",
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 2
    assert requires == ()
    assert has_secrets is True


def test_count_connectors_ignores_docstring_mentions_of_kwargs(tmp_path: Path) -> None:
    """A module documenting that it has *no* secrets/requires stays keyless.

    The prose mentions the literal substrings outside any decorator's call —
    AST-based scanning never sees docstring/comment text as a keyword
    argument, so this needs no special-case guard.
    """
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '"""Toy connector.\n\n'
        "Keyless — no ``secrets=``/``requires=``/``bind()``/``UnauthorizedError``; deliberately "
        'not declared via ``secrets=``.\n"""\n\n'
        '@connector(output=FETCH_OUTPUT, tags=["macro"])\n'
        "def toy_fetch(series_id: str) -> pd.DataFrame: ...\n",
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ()
    assert has_secrets is False


def test_count_connectors_search_factory_contributes_nothing(tmp_path: Path) -> None:
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

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ()
    assert has_secrets is False


def test_count_connectors_missing_module_dir(tmp_path: Path) -> None:
    count, requires, has_secrets = gen_roster._count_connectors(tmp_path / "does_not_exist")

    assert count == 0
    assert requires == ()
    assert has_secrets is False


def test_count_connectors_not_truncated_by_nested_calls_in_decorator_kwargs(tmp_path: Path) -> None:
    """A nested call before ``secrets=``/``requires=`` must not hide the keyword argument.

    A text-based ``[^)]*``-style capture would stop at the first ``)``; the
    AST walk sees the full keyword list regardless of nested calls like
    ``Column(...)`` inside the decorator's own arguments.
    """
    module_dir = tmp_path / "parsimony_toy"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text(
        '@connector(output=Column(name="x"), secrets=("api_key",), requires=("X_API_KEY",))\ndef f(): ...\n',
        encoding="utf-8",
    )

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 1
    assert requires == ("X_API_KEY",)
    assert has_secrets is True


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

    count, requires, has_secrets = gen_roster._count_connectors(module_dir)

    assert count == 0
    assert requires == ()
    assert has_secrets is False


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
        "requires": (),
        "has_secrets": False,
    }
    defaults.update(overrides)
    return gen_roster.PackageInfo(**defaults)  # type: ignore[arg-type]


def test_render_json_shape_matches_manifest_contract() -> None:
    rows = [
        _row(name="parsimony-zeta", requires=("ZETA_API_KEY",), has_secrets=True),
        _row(name="parsimony-alpha"),
    ]

    payload = gen_roster._render_json(rows)

    assert set(payload.keys()) == {"schema_version", "generated_at", "connectors"}
    assert payload["schema_version"] == gen_roster.MANIFEST_SCHEMA_VERSION == 1
    assert [p["package"] for p in payload["connectors"]] == ["parsimony-alpha", "parsimony-zeta"]
    alpha, zeta = payload["connectors"]
    # `requires` is on the wire; `keyless` is not — consumers derive
    # keyless = not requires.
    assert set(alpha.keys()) == {"package", "provider", "entry_point", "connector_count", "requires"}
    assert alpha["provider"] == "Toy"
    assert alpha["entry_point"] == "toy"
    assert alpha["connector_count"] == 2
    assert alpha["requires"] == []
    assert zeta["requires"] == ["ZETA_API_KEY"]


def test_render_json_requires_is_a_sorted_array() -> None:
    payload = gen_roster._render_json([_row(requires=("A_VAR", "B_VAR"))])

    assert payload["connectors"][0]["requires"] == ["A_VAR", "B_VAR"]


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


def test_update_connectors_json_rewrites_when_schema_version_is_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file stamped with a stale schema_version and identical rows is still
    rewritten to the current version — the update path repairs exactly what
    ``--check`` flags as stale.
    """
    target = tmp_path / "connectors.json"
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", target)
    payload = gen_roster._render_json([_row()])
    stale = {"schema_version": 0, "generated_at": "2020-01-01", "connectors": payload["connectors"]}
    target.write_text(json.dumps(stale), encoding="utf-8")

    gen_roster._update_connectors_json(payload)

    assert json.loads(target.read_text(encoding="utf-8"))["schema_version"] == 1


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
# auth classification + docs/reference/providers.md cell rewrite
# ---------------------------------------------------------------------------


def test_auth_shape_covers_all_four_quadrants() -> None:
    assert gen_roster._auth_shape(_row(requires=("K",), has_secrets=True))[0] == "required key"
    assert gen_roster._auth_shape(_row(requires=("UA",), has_secrets=False))[0] == "UA-required"
    assert gen_roster._auth_shape(_row(requires=(), has_secrets=True))[0] == "optional key (quota)"
    assert gen_roster._auth_shape(_row(requires=(), has_secrets=False))[0] == "keyless"


_TABLE_HEADER = "| Package (PyPI) | Source | Auth | Env var | Discovery | Key connectors |"
_TABLE_SEPARATOR = "| --- | --- | --- | --- | --- | --- |"


def _providers_doc(*rows: str) -> str:
    table = "\n".join([_TABLE_HEADER, _TABLE_SEPARATOR, *rows])
    return f"# Providers\n\nintro prose\n\n{table}\n\ntrailing prose\n"


def _toy_table_row(auth: str = "stale auth", env: str = "stale env") -> str:
    return (
        "| [`parsimony-toy`](https://pypi.org/project/parsimony-toy/) | [Toy](https://example.test) "
        f"| {auth} | {env} | catalog | `toy_fetch` |"
    )


def test_rewrite_providers_doc_replaces_only_auth_and_env_cells() -> None:
    original_row = _toy_table_row()
    text = _providers_doc(original_row)
    rows = [_row(requires=("TOY_API_KEY", "TOY_TOKEN"), has_secrets=True)]

    out = gen_roster._rewrite_providers_doc(text, rows)

    (new_row,) = [line for line in out.split("\n") if line.startswith("| [`parsimony-toy`")]
    new_cells = new_row.split("|")
    old_cells = original_row.split("|")
    assert new_cells[3] == " required key "
    assert new_cells[4] == " `TOY_API_KEY`, `TOY_TOKEN` "
    # Every other cell is byte-identical.
    assert [new_cells[i] for i in (0, 1, 2, 5, 6, 7)] == [old_cells[i] for i in (0, 1, 2, 5, 6, 7)]
    # Surrounding prose is untouched.
    assert "intro prose" in out
    assert "trailing prose" in out


def test_rewrite_providers_doc_keyless_env_cell_is_a_dash() -> None:
    out = gen_roster._rewrite_providers_doc(_providers_doc(_toy_table_row()), [_row()])

    (new_row,) = [line for line in out.split("\n") if line.startswith("| [`parsimony-toy`")]
    cells = new_row.split("|")
    assert cells[3] == " keyless "
    assert cells[4] == " — "


def test_rewrite_providers_doc_is_idempotent_on_fresh_text() -> None:
    rows = [_row(requires=("TOY_API_KEY",), has_secrets=True)]
    once = gen_roster._rewrite_providers_doc(_providers_doc(_toy_table_row()), rows)

    assert gen_roster._rewrite_providers_doc(once, rows) == once


def test_rewrite_providers_doc_fails_on_table_row_for_unknown_package() -> None:
    unknown = "| [`parsimony-ghost`](https://pypi.org/project/parsimony-ghost/) | X | keyless | — | catalog | `g` |"
    text = _providers_doc(_toy_table_row(), unknown)

    with pytest.raises(SystemExit, match="parsimony-ghost"):
        gen_roster._rewrite_providers_doc(text, [_row()])


def test_rewrite_providers_doc_fails_on_package_missing_from_table() -> None:
    text = _providers_doc(_toy_table_row())
    rows = [_row(), _row(name="parsimony-other", entry_point="other")]

    with pytest.raises(SystemExit, match="parsimony-other"):
        gen_roster._rewrite_providers_doc(text, rows)


def test_rewrite_providers_doc_fails_when_table_header_is_missing() -> None:
    with pytest.raises(SystemExit, match="header not found"):
        gen_roster._rewrite_providers_doc("# Providers\n\nno table here\n", [_row()])


# ---------------------------------------------------------------------------
# docs/concepts/credentials.md marker-block regeneration
# ---------------------------------------------------------------------------

_CREDENTIAL_SLUGS = ("required-key", "optional-key", "keyless", "header-required")


def _credentials_doc() -> str:
    parts = ["# Credentials\n"]
    for slug in _CREDENTIAL_SLUGS:
        parts.append(
            f"### {slug}\n\n"
            f"<!-- credentials:{slug}:start -->\n"
            "```text\nold  stale\n```\n"
            f"<!-- credentials:{slug}:end -->\n"
        )
    parts.append("closing prose\n")
    return "\n".join(parts)


def test_rewrite_credentials_doc_regenerates_all_four_lists() -> None:
    rows = [
        _row(name="parsimony-a", entry_point="alpha", requires=("A_API_KEY",), has_secrets=True),
        _row(name="parsimony-b", entry_point="beta", requires=(), has_secrets=True),
        _row(name="parsimony-c", entry_point="gamma"),
        _row(name="parsimony-d", entry_point="delta", requires=("D_USER_AGENT",), has_secrets=False),
        _row(name="parsimony-e", entry_point="epsilon"),
    ]

    out = gen_roster._rewrite_credentials_doc(_credentials_doc(), rows)

    assert "<!-- credentials:required-key:start -->\n```text\nalpha\n```\n<!-- credentials:required-key:end -->" in out
    assert "<!-- credentials:optional-key:start -->\n```text\nbeta\n```\n<!-- credentials:optional-key:end -->" in out
    assert "<!-- credentials:keyless:start -->\n```text\nepsilon  gamma\n```\n<!-- credentials:keyless:end -->" in out
    assert (
        "<!-- credentials:header-required:start -->\n```text\ndelta\n```\n<!-- credentials:header-required:end -->"
        in out
    )
    assert "closing prose" in out
    assert "stale" not in out


def test_rewrite_credentials_doc_renders_empty_list_as_none_placeholder() -> None:
    out = gen_roster._rewrite_credentials_doc(_credentials_doc(), [_row()])

    assert "<!-- credentials:required-key:start -->\n_(none)_\n<!-- credentials:required-key:end -->" in out


def test_rewrite_credentials_doc_fails_on_missing_markers() -> None:
    with pytest.raises(SystemExit, match="credentials:"):
        gen_roster._rewrite_credentials_doc("# Credentials\n\nno markers\n", [_row()])


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
    materialize fully fresh artifacts (README, docs/index.md,
    connectors.json, providers.md, credentials.md) via ``--update-readme``
    so ``--check`` starts from a known-clean baseline.
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
    providers_doc = tmp_path / "docs" / "reference" / "providers.md"
    providers_doc.parent.mkdir(parents=True)
    providers_doc.write_text(_providers_doc(_toy_table_row()), encoding="utf-8")
    credentials_doc = tmp_path / "docs" / "concepts" / "credentials.md"
    credentials_doc.parent.mkdir(parents=True)
    credentials_doc.write_text(_credentials_doc(), encoding="utf-8")

    monkeypatch.setattr(gen_roster, "ROOT", tmp_path)
    monkeypatch.setattr(gen_roster, "PACKAGES", tmp_path / "packages")
    monkeypatch.setattr(gen_roster, "README", readme)
    monkeypatch.setattr(gen_roster, "DOCS_INDEX", docs_index)
    monkeypatch.setattr(gen_roster, "CONNECTORS_JSON", connectors_json)
    monkeypatch.setattr(gen_roster, "DOCS_PROVIDERS", providers_doc)
    monkeypatch.setattr(gen_roster, "DOCS_CREDENTIALS", credentials_doc)

    assert gen_roster.main(["--update-readme"]) == 0
    return {
        "readme": readme,
        "docs_index": docs_index,
        "connectors_json": connectors_json,
        "providers_doc": providers_doc,
        "credentials_doc": credentials_doc,
    }


def test_check_passes_on_freshly_generated_tree(_fresh_roster_tree: dict[str, Path]) -> None:
    assert gen_roster.main(["--check"]) == 0


def test_update_readme_freshens_auth_docs(_fresh_roster_tree: dict[str, Path]) -> None:
    providers_text = _fresh_roster_tree["providers_doc"].read_text(encoding="utf-8")
    (toy_row,) = [line for line in providers_text.split("\n") if line.startswith("| [`parsimony-toy`")]
    assert toy_row.split("|")[3] == " keyless "
    assert toy_row.split("|")[4] == " — "

    text = _fresh_roster_tree["credentials_doc"].read_text(encoding="utf-8")
    assert "<!-- credentials:keyless:start -->\n```text\ntoy\n```\n<!-- credentials:keyless:end -->" in text
    assert "<!-- credentials:required-key:start -->\n_(none)_\n<!-- credentials:required-key:end -->" in text


def test_check_fails_when_connectors_json_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["connectors_json"].write_text(
        json.dumps({"schema_version": 1, "generated_at": "2020-01-01", "connectors": []})
    )

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_connectors_json_schema_version_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    current = json.loads(_fresh_roster_tree["connectors_json"].read_text(encoding="utf-8"))
    current["schema_version"] = 0
    _fresh_roster_tree["connectors_json"].write_text(json.dumps(current), encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_readme_table_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    readme = _fresh_roster_tree["readme"]
    readme.write_text("# toy\n\n<!-- roster:start -->\nstale\n<!-- roster:end -->\n", encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_docs_index_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["docs_index"].write_text("stale", encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_providers_doc_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["providers_doc"].write_text(
        _providers_doc(_toy_table_row(auth="required key", env="`TOY_API_KEY`")), encoding="utf-8"
    )

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_when_credentials_doc_is_stale(_fresh_roster_tree: dict[str, Path]) -> None:
    _fresh_roster_tree["credentials_doc"].write_text(_credentials_doc(), encoding="utf-8")

    assert gen_roster.main(["--check"]) == 1


def test_check_fails_loudly_when_package_row_is_missing_from_providers_table(
    _fresh_roster_tree: dict[str, Path],
) -> None:
    _fresh_roster_tree["providers_doc"].write_text(_providers_doc(), encoding="utf-8")

    with pytest.raises(SystemExit, match="parsimony-toy"):
        gen_roster.main(["--check"])


def test_update_readme_and_check_agree_they_can_never_drift(_fresh_roster_tree: dict[str, Path]) -> None:
    """Re-running ``--update-readme`` on an already-fresh tree is a no-op, and
    ``--check`` immediately after still passes — the two modes never disagree.
    """
    assert gen_roster.main(["--update-readme"]) == 0
    assert gen_roster.main(["--check"]) == 0
