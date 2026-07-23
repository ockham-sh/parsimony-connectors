"""Generate the connector roster table for the README, plus a machine-readable manifest.

Reads every ``packages/*/pyproject.toml`` and the connector source files,
emits a Markdown table with one row per published distribution. Connector
counts (and each package's ``requires=`` env vars) come from an AST sweep
of the connector source tree, looking for ``@connector``/``@enumerator``/
``@loader`` decorators and ``make_local_search_connector(...)`` calls — the
same shapes the kernel itself dispatches on. The same gathered rows also
produce ``connectors.json`` at the repo root — the versioned, machine-readable
manifest consumed by ``parsimony.dev`` and the ``parsimony`` kernel — plus
the generated auth cells in ``docs/reference/providers.md`` and the four
auth-shape lists in ``docs/concepts/credentials.md``, so none of the
artifacts can drift relative to each other.

Usage:
    python scripts/gen_roster.py                     # print table to stdout
    python scripts/gen_roster.py --update-readme     # rewrite the table
                                                       block inside README.md,
                                                       regenerate docs/index.md,
                                                       write connectors.json,
                                                       and refresh the auth
                                                       docs
    python scripts/gen_roster.py --check             # exit non-zero if any
                                                       of the committed
                                                       artifacts are stale

The README block is delimited by:

    <!-- roster:start -->
    ...generated table...
    <!-- roster:end -->

``docs/index.md`` is regenerated wholesale from README content with
relative repo links rewritten to absolute GitHub URLs so it can be
imported into the parsimony kernel mkdocs site via the
``mkdocs-multirepo-plugin`` ``!import`` directive.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ROOT / "packages"
README = ROOT / "README.md"
DOCS_INDEX = ROOT / "docs" / "index.md"
DOCS_PROVIDERS = ROOT / "docs" / "reference" / "providers.md"
DOCS_CREDENTIALS = ROOT / "docs" / "concepts" / "credentials.md"
CONNECTORS_JSON = ROOT / "connectors.json"

ROSTER_START = "<!-- roster:start -->"
ROSTER_END = "<!-- roster:end -->"

# Schema version of the manifest envelope written to connectors.json. Bump
# only on a breaking change to the envelope or row shape (adding an optional
# field is not breaking and does not require a bump) — see the shared
# contract coordinated with ockham-sh/parsimony#97 and
# ockham-sh/landing-page#12. v2: rows carry ``requires`` (the env vars a
# call needs to succeed) and ``keyless`` came off the wire — consumers
# derive it as ``not requires``.
MANIFEST_SCHEMA_VERSION = 2

# Base URL for rewriting relative links when emitting docs/index.md.
# Points at the canonical branch on GitHub so every link in the rendered
# docs page is clickable from docs.parsimony.dev.
GITHUB_BLOB_BASE = "https://github.com/ockham-sh/parsimony-connectors/blob/main"

# Every published provider distribution must be named parsimony-<name>
# (lowercase, hyphen-separated) — mirrors the Python package convention
# (parsimony_<name>) enforced by CONTRIBUTING.md / CODEOWNERS.
_PACKAGE_NAME_RE = re.compile(r"^parsimony-[a-z0-9]+(?:-[a-z0-9]+)*$")

# Decorator / factory names the kernel dispatches connectors through. Matched
# by trailing attribute name so both `@connector(...)` (the normal import
# shape, `from parsimony.connector import connector`) and a hypothetical
# `@parsimony.connector.connector(...)` are recognized alike.
_CONNECTOR_DECORATOR_NAMES = frozenset({"connector", "enumerator", "loader"})
_SEARCH_FACTORY_NAME = "make_local_search_connector"

# Google's favicon service. More reliable than DDG for niche domains
# (central banks, statistical agencies). Returns a generic icon for
# unknown domains, so the visual stays consistent. Requests from
# github.com are proxied through GitHub Camo, so README readers do
# not hit Google directly.
FAVICON_TEMPLATE = "https://www.google.com/s2/favicons?domain={domain}&sz=64"


@dataclass(frozen=True)
class PackageInfo:
    name: str
    description: str
    homepage: str
    entry_point: str
    module: str
    connector_count: int
    requires: tuple[str, ...]
    has_secrets: bool


def _read_pyproject(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _entry_point(pyproject_path: Path, data: dict) -> tuple[str, str]:
    """Return ``(entry_point_name, module)`` for a provider package.

    Raises :class:`SystemExit` if the package declares the
    ``parsimony.providers`` table but not with exactly one entry — that is
    malformed metadata for a provider package, not something to skip
    silently. Callers must check the table exists and is non-empty first;
    this only validates *cardinality*, not presence, so a genuinely
    non-provider package (no table at all, e.g. ``_shared``) never reaches
    here.
    """
    eps = data.get("project", {}).get("entry-points", {}).get("parsimony.providers", {})
    if len(eps) != 1:
        raise SystemExit(
            f"{pyproject_path}: expected exactly one "
            f'[project.entry-points."parsimony.providers"] entry, found {len(eps)}'
        )
    return next(iter(eps.items()))


def _provider_name(description: str) -> str:
    """Extract the provider's display name from a pyproject description.

    The convention is ``"<Provider Name> connector for the parsimony
    framework"``, sometimes with a parenthetical clarifier. Strips both
    the inline ``"connector"`` and the trailing ``"for the parsimony
    framework"`` suffix to leave the human-readable provider name.
    """
    suffix = " for the parsimony framework"
    s = description.replace(" connector ", " ").removesuffix(" connector")
    if s.endswith(suffix):
        s = s[: -len(suffix)]
    return s.strip()


def _favicon_url(homepage: str) -> str | None:
    """Resolve a provider's homepage to a favicon URL, or ``None`` if missing.

    The favicon service falls back to a generic icon for unknown domains,
    so the only case that needs explicit handling here is an absent
    homepage. The ``www.`` prefix is stripped so the apex domain is used
    canonically; some hosts only serve favicons on the apex.
    """
    if not homepage:
        return None
    netloc = urlparse(homepage).netloc
    if not netloc:
        return None
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return FAVICON_TEMPLATE.format(domain=netloc)


def _callee_name(node: ast.expr) -> str | None:
    """Return the trailing name of a call target.

    ``connector(...)`` and a hypothetical ``mod.connector(...)`` both
    resolve to ``"connector"`` — the module-qualification doesn't matter,
    only the call's identity.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _has_secrets_kwarg(call: ast.Call) -> bool:
    return any(kw.arg == "secrets" for kw in call.keywords)


def _requires_env_vars(call: ast.Call, path: Path, connector: str) -> tuple[str, ...]:
    """Return the env-var names in a decorator's ``requires=`` kwarg (absent → ``()``).

    ``requires=`` is policed as a *literal* tuple/list of string constants —
    the ast walk cannot resolve a name reference to a cross-module constant,
    so anything else fails loudly rather than silently under-reporting the
    package's env vars.
    """
    for kw in call.keywords:
        if kw.arg != "requires":
            continue
        value = kw.value
        if isinstance(value, ast.Tuple | ast.List):
            names = [
                elt.value for elt in value.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            ]
            if len(names) == len(value.elts):
                return tuple(names)
        raise SystemExit(
            f"{path}: connector {connector!r} passes a non-literal requires= — it must be a "
            "literal tuple/list of env-var name strings (the AST sweep cannot resolve names)"
        )
    return ()


def _count_connectors(module_dir: Path) -> tuple[int, tuple[str, ...], bool]:
    """Return ``(connector_count, requires, has_secrets)`` for *module_dir*.

    Parses every ``*.py`` file with :mod:`ast` and counts:

    * one hit per ``@connector``/``@enumerator``/``@loader`` decorator on a
      function definition;
    * one hit per ``make_local_search_connector(...)`` call anywhere in the
      module (it is used as a plain assignment, not a decorator).

    ``requires`` is the sorted union of every matched decorator's
    ``requires=`` env vars; ``has_secrets`` is ``True`` iff any matched
    decorator passes a ``secrets=`` keyword argument. Because this walks the
    *parsed* syntax tree rather than scanning raw text, a docstring or
    comment that happens to mention either kwarg can never be mistaken for a
    real declaration — unlike a text-based sweep, no separate guard is
    needed for that case. ``make_local_search_connector`` calls contribute
    nothing (the factory has neither parameter).
    """
    if not module_dir.exists():
        return 0, (), False
    total = 0
    requires: set[str] = set()
    has_secrets = False
    for py in sorted(module_dir.rglob("*.py")):
        text = py.read_text(encoding="utf-8")
        try:
            tree = ast.parse(text, filename=str(py))
        except SyntaxError as exc:
            raise SystemExit(f"{py}: failed to parse for connector counting: {exc}") from exc

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                for deco in node.decorator_list:
                    if isinstance(deco, ast.Call) and _callee_name(deco.func) in _CONNECTOR_DECORATOR_NAMES:
                        total += 1
                        if _has_secrets_kwarg(deco):
                            has_secrets = True
                        requires.update(_requires_env_vars(deco, py, node.name))
            elif isinstance(node, ast.Call) and _callee_name(node.func) == _SEARCH_FACTORY_NAME:
                total += 1
    return total, tuple(sorted(requires)), has_secrets


def _gather() -> list[PackageInfo]:
    rows: list[PackageInfo] = []
    for pyproject in sorted(PACKAGES.glob("*/pyproject.toml")):
        data = _read_pyproject(pyproject)
        project = data.get("project", {})
        name = project.get("name", "")
        if not name:
            continue

        entry_points_table = project.get("entry-points", {}).get("parsimony.providers")
        if not entry_points_table:
            # Not a provider package (e.g. `_shared`, a helper library
            # consumed by other packages) — legitimately skipped, not
            # malformed. CI's "Enforce plugin-only monorepo" step is the
            # place that polices which directories are exempt from even
            # having a `parsimony.providers` table.
            continue

        if not _PACKAGE_NAME_RE.match(name):
            raise SystemExit(
                f"{pyproject}: provider package name {name!r} must match 'parsimony-<name>' "
                "(lowercase, hyphen-separated)"
            )

        ep_name, module = _entry_point(pyproject, data)
        module_dir = pyproject.parent / module.split(".")[0]
        total, requires, has_secrets = _count_connectors(module_dir)
        rows.append(
            PackageInfo(
                name=name,
                description=project.get("description", ""),
                homepage=project.get("urls", {}).get("Homepage", ""),
                entry_point=ep_name,
                module=module,
                connector_count=total,
                requires=requires,
                has_secrets=has_secrets,
            )
        )
    return rows


def _render(rows: list[PackageInfo]) -> str:
    headers = ["", "Package", "Source", "Connectors"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in sorted(rows, key=lambda r: r.name):
        favicon = _favicon_url(row.homepage)
        pypi_url = f"https://pypi.org/project/{row.name}/"
        icon_cell = f'<a href="{pypi_url}"><img src="{favicon}" width="16" height="16" alt="" /></a>' if favicon else ""
        link = f"[`{row.name}`]({pypi_url})"
        provider = _provider_name(row.description).replace("|", "\\|")
        source_md = f"[{provider}]({row.homepage})" if row.homepage else provider
        lines.append(f"| {icon_cell} | {link} | {source_md} | {row.connector_count} |")
    return "\n".join(lines)


def _render_json(rows: list[PackageInfo]) -> dict[str, Any]:
    """Build the ``connectors.json`` payload from the same gathered rows as ``_render``.

    Envelope shape is the shared contract coordinated with
    ``ockham-sh/parsimony#97`` and ``ockham-sh/landing-page#12``:
    ``schema_version`` (int), ``generated_at`` (bare date), and
    ``connectors`` — a flat lookup table for an agent's routing step, not a
    package index. Each row carries five fields (package, provider,
    entry_point, connector_count, requires); ``requires`` is the sorted
    env-var list and consumers derive keyless as ``not requires``.
    ``generated_at`` is a bare date so a same-day rerun with an unchanged
    connector set is byte-identical to what's committed.
    """
    connectors = [
        {
            "package": row.name,
            "provider": _provider_name(row.description),
            "entry_point": row.entry_point,
            "connector_count": row.connector_count,
            "requires": list(row.requires),
        }
        for row in sorted(rows, key=lambda r: r.name)
    ]
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": date.today().isoformat(),
        "connectors": connectors,
    }


def _update_connectors_json(payload: dict[str, Any]) -> None:
    """Write ``connectors.json``, but only touch the file if ``connectors`` changed.

    Compares against the committed ``schema_version`` + ``connectors`` so
    ``generated_at`` doesn't churn — and the file doesn't show up as
    modified in every PR — when the regenerated rows are identical to
    what's already on disk. ``schema_version`` participates so an update
    run repairs exactly what ``_check`` flags as stale.
    """
    if CONNECTORS_JSON.exists():
        try:
            existing = json.loads(CONNECTORS_JSON.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None
        if (
            isinstance(existing, dict)
            and existing.get("schema_version") == payload["schema_version"]
            and existing.get("connectors") == payload["connectors"]
        ):
            return
    CONNECTORS_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _update_readme(table: str) -> None:
    text = README.read_text(encoding="utf-8")
    if ROSTER_START not in text or ROSTER_END not in text:
        raise SystemExit(f"README is missing the delimiters {ROSTER_START!r} / {ROSTER_END!r}.")
    pattern = re.compile(
        re.escape(ROSTER_START) + r".*?" + re.escape(ROSTER_END),
        re.DOTALL,
    )
    replacement = f"{ROSTER_START}\n{table}\n{ROSTER_END}"
    new_text = pattern.sub(replacement, text)
    if new_text == text:
        return
    README.write_text(new_text, encoding="utf-8")


# Matches the ``](path)`` portion of a Markdown link where ``path`` is
# repo-relative (no scheme, no leading slash, no anchor-only). Skipping
# the text portion of the link sidesteps nested-bracket cases like badge
# images: ``[![License](shields.io/...)](LICENSE)`` — the regex still
# matches the trailing ``](LICENSE)`` correctly.
_RELATIVE_LINK_RE = re.compile(r"\]\(((?!https?://|mailto:|#|/)[^)#\s]+)(#[^)\s]+)?\)")


def _rewrite_relative_links(markdown: str) -> str:
    """Rewrite README-relative links to absolute GitHub URLs.

    The kernel docs site imports this file via ``mkdocs-multirepo-plugin``
    and serves it from ``docs.parsimony.dev/connectors/``. Repo-relative
    paths (``CONTRIBUTING.md``, ``packages/fred/README.md``, ``LICENSE``)
    would 404 there, so they are rebased onto ``GITHUB_BLOB_BASE``.

    Absolute URLs, mailto links, and pure ``#anchor`` links are left
    untouched.
    """

    def replace(match: re.Match[str]) -> str:
        target, fragment = match.group(1), match.group(2) or ""
        return f"]({GITHUB_BLOB_BASE}/{target}{fragment})"

    parts = re.split(r"(```[\s\S]*?```)", markdown)
    rewritten: list[str] = []
    for idx, part in enumerate(parts):
        if idx % 2 == 1:
            rewritten.append(part)
        else:
            rewritten.append(_RELATIVE_LINK_RE.sub(replace, part))
    return "".join(rewritten)


def _update_docs_index(readme_text: str) -> None:
    """Regenerate ``docs/index.md`` from README with absolute links."""
    DOCS_INDEX.parent.mkdir(parents=True, exist_ok=True)
    rewritten = _rewrite_relative_links(readme_text)
    DOCS_INDEX.write_text(rewritten, encoding="utf-8")


# The hand-maintained provider table in docs/reference/providers.md. Only
# the Auth and Env-var cells are generated; the other four cells (and all
# surrounding prose) are editorial and never touched.
_PROVIDERS_TABLE_HEADER = "| Package (PyPI) | Source | Auth | Env var | Discovery | Key connectors |"

# Auth shape per package, derived from the (requires, secrets) pair. The
# labels are the exact Auth-cell wording in docs/reference/providers.md;
# the slugs name the marker-delimited lists in docs/concepts/credentials.md.
_AUTH_SHAPES: dict[tuple[bool, bool], tuple[str, str]] = {
    (True, True): ("required key", "required-key"),
    (True, False): ("UA-required", "header-required"),
    (False, True): ("optional key (quota)", "optional-key"),
    (False, False): ("keyless", "keyless"),
}


def _auth_shape(row: PackageInfo) -> tuple[str, str]:
    """Return ``(auth_label, list_slug)`` for a package's auth shape."""
    return _AUTH_SHAPES[(bool(row.requires), row.has_secrets)]


def _rewrite_providers_doc(text: str, rows: list[PackageInfo]) -> str:
    """Rewrite the Auth and Env-var cells of the provider table in *text*.

    Rows are matched by package name; every package gathered from
    ``packages/*`` must have a table row and vice versa — a mismatch is a
    hard error, not a silent skip. All other cells are left byte-identical.
    """
    by_name = {row.name: row for row in rows}
    lines = text.split("\n")
    try:
        header_idx = next(i for i, line in enumerate(lines) if line.strip() == _PROVIDERS_TABLE_HEADER)
    except StopIteration:
        raise SystemExit(
            f"{DOCS_PROVIDERS}: provider table header not found: {_PROVIDERS_TABLE_HEADER!r}"
        ) from None
    seen: set[str] = set()
    for i in range(header_idx + 2, len(lines)):
        line = lines[i]
        if not line.startswith("|"):
            break
        cells = line.split("|")
        if len(cells) != 8:  # leading/trailing '' + six cells
            raise SystemExit(f"{DOCS_PROVIDERS}: malformed provider table row: {line!r}")
        match = re.search(r"`([^`]+)`", cells[1])
        package = match.group(1) if match else cells[1].strip()
        row = by_name.get(package)
        if row is None:
            raise SystemExit(f"{DOCS_PROVIDERS}: table row for unknown package {package!r} — not in packages/*")
        seen.add(row.name)
        env_cell = ", ".join(f"`{var}`" for var in row.requires) or "—"
        cells[3] = f" {_auth_shape(row)[0]} "
        cells[4] = f" {env_cell} "
        lines[i] = "|".join(cells)
    missing = sorted(set(by_name) - seen)
    if missing:
        raise SystemExit(f"{DOCS_PROVIDERS}: packages missing from provider table: {', '.join(missing)}")
    return "\n".join(lines)


def _render_credentials_block(members: list[str]) -> str:
    if not members:
        return "_(none)_"
    return "```text\n" + "  ".join(members) + "\n```"


def _rewrite_credentials_doc(text: str, rows: list[PackageInfo]) -> str:
    """Regenerate the four auth-shape provider lists in docs/concepts/credentials.md.

    Each list lives between ``<!-- credentials:<slug>:start/end -->``
    markers (same idiom as the README roster block) so regeneration is
    surgical and the surrounding prose stays hand-maintained.
    """
    groups: dict[str, list[str]] = {slug: [] for _, slug in _AUTH_SHAPES.values()}
    for row in sorted(rows, key=lambda r: r.entry_point):
        groups[_auth_shape(row)[1]].append(row.entry_point)
    for slug, members in groups.items():
        start = f"<!-- credentials:{slug}:start -->"
        end = f"<!-- credentials:{slug}:end -->"
        if start not in text or end not in text:
            raise SystemExit(f"{DOCS_CREDENTIALS} is missing the delimiters {start!r} / {end!r}.")
        pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
        text = pattern.sub(f"{start}\n{_render_credentials_block(members)}\n{end}", text)
    return text


def _auth_docs() -> list[tuple[Path, Callable[[str, list[PackageInfo]], str]]]:
    """The two generated auth docs as (path, rewrite) pairs.

    Shared by the update and check paths so they can never disagree on what
    "fresh" means. A function (not a module constant) so tests that
    monkeypatch the path constants see their tmp copies.
    """
    return [(DOCS_PROVIDERS, _rewrite_providers_doc), (DOCS_CREDENTIALS, _rewrite_credentials_doc)]


def _update_auth_docs(rows: list[PackageInfo]) -> None:
    """Rewrite the generated portions of both auth docs, touching only stale files."""
    for path, rewrite in _auth_docs():
        text = path.read_text(encoding="utf-8")
        new_text = rewrite(text, rows)
        if new_text != text:
            path.write_text(new_text, encoding="utf-8")


def _check(table: str, payload: dict[str, Any], rows: list[PackageInfo]) -> list[str]:
    """Return a description of each stale artifact; empty means everything is fresh.

    Regenerates each artifact in memory and diffs it against what's
    committed — never writes. Lets CI catch a PR that edited
    ``packages/*/pyproject.toml`` (or a connector's decorators) without
    re-running ``make readme-roster``, the same way a stale generated file
    is normally caught.
    """
    stale: list[str] = []
    readme_text = README.read_text(encoding="utf-8")
    pattern = re.compile(re.escape(ROSTER_START) + r".*?" + re.escape(ROSTER_END), re.DOTALL)
    fresh_block = f"{ROSTER_START}\n{table}\n{ROSTER_END}"

    current_block = pattern.search(readme_text)
    if current_block is None:
        stale.append(f"README.md is missing the {ROSTER_START!r}/{ROSTER_END!r} delimiters")
    elif current_block.group(0) != fresh_block:
        stale.append("README.md roster table")

    fresh_readme = pattern.sub(fresh_block, readme_text)
    fresh_docs_index = _rewrite_relative_links(fresh_readme)
    if not DOCS_INDEX.exists() or DOCS_INDEX.read_text(encoding="utf-8") != fresh_docs_index:
        stale.append("docs/index.md")

    if not CONNECTORS_JSON.exists():
        stale.append("connectors.json (missing)")
    else:
        try:
            existing = json.loads(CONNECTORS_JSON.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None
        if (
            not isinstance(existing, dict)
            or existing.get("schema_version") != payload["schema_version"]
            or existing.get("connectors") != payload["connectors"]
        ):
            stale.append("connectors.json")

    for path, rewrite in _auth_docs():
        rel = path.relative_to(ROOT)
        if not path.exists():
            stale.append(f"{rel} (missing)")
            continue
        text = path.read_text(encoding="utf-8")
        if rewrite(text, rows) != text:
            stale.append(str(rel))

    return stale


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--update-readme",
        action="store_true",
        help="Rewrite the roster block in README.md, docs/index.md, connectors.json, "
        "and the generated auth docs instead of printing.",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if README.md, docs/index.md, connectors.json, or the "
        "generated auth docs are stale relative to packages/*.",
    )
    args = parser.parse_args(argv)
    rows = _gather()
    table = _render(rows)
    payload = _render_json(rows)

    if args.check:
        stale = _check(table, payload, rows)
        if stale:
            for item in stale:
                print(f"stale: {item}", file=sys.stderr)
            print("Run `make readme-roster` to regenerate.", file=sys.stderr)
            return 1
        print(f"Roster is fresh: {len(rows)} packages.", file=sys.stderr)
        return 0

    if args.update_readme:
        _update_readme(table)
        # docs/index.md is rebuilt from the updated README so the kernel
        # mkdocs site (which imports docs/) reflects the same roster.
        _update_docs_index(README.read_text(encoding="utf-8"))
        _update_connectors_json(payload)
        _update_auth_docs(rows)
        print(f"Updated roster: {len(rows)} packages", file=sys.stderr)
        print(f"Wrote {DOCS_INDEX.relative_to(ROOT)}", file=sys.stderr)
        print(f"Wrote {CONNECTORS_JSON.relative_to(ROOT)}", file=sys.stderr)
        print(f"Wrote {DOCS_PROVIDERS.relative_to(ROOT)}", file=sys.stderr)
        print(f"Wrote {DOCS_CREDENTIALS.relative_to(ROOT)}", file=sys.stderr)
    else:
        print(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
