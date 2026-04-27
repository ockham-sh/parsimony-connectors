"""Generate the connector roster table for the README.

Reads every ``packages/*/pyproject.toml`` and the connector source files,
emits a Markdown table with one row per published distribution. Counts of
``@connector`` decorations and how many carry the ``tool`` tag come from
a regex sweep of the connector source tree.

Usage:
    python scripts/gen_roster.py                     # print table to stdout
    python scripts/gen_roster.py --update-readme     # rewrite the table
                                                       block inside README.md

The README block is delimited by:

    <!-- roster:start -->
    ...generated table...
    <!-- roster:end -->
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ROOT / "packages"
README = ROOT / "README.md"

ROSTER_START = "<!-- roster:start -->"
ROSTER_END = "<!-- roster:end -->"

CONNECTOR_DECORATOR_RE = re.compile(r"@connector\s*\(([^)]*)\)", re.DOTALL)
TOOL_TAG_RE = re.compile(r'tags\s*=\s*\(?[^)]*["\']tool["\']')

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
    tool_count: int


def _read_pyproject(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _entry_point(data: dict) -> tuple[str, str]:
    eps = data.get("project", {}).get("entry-points", {}).get("parsimony.providers", {})
    if not eps:
        return "", ""
    name, module = next(iter(eps.items()))
    return name, module


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


def _count_connectors(module_dir: Path) -> tuple[int, int]:
    if not module_dir.exists():
        return 0, 0
    total = 0
    tools = 0
    for py in module_dir.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        for match in CONNECTOR_DECORATOR_RE.finditer(text):
            total += 1
            if TOOL_TAG_RE.search(match.group(1)):
                tools += 1
    return total, tools


def _gather() -> list[PackageInfo]:
    rows: list[PackageInfo] = []
    for pyproject in sorted(PACKAGES.glob("*/pyproject.toml")):
        data = _read_pyproject(pyproject)
        project = data.get("project", {})
        name = project.get("name", "")
        if not name:
            continue
        ep_name, module = _entry_point(data)
        if not ep_name:
            continue
        module_dir = pyproject.parent / module.split(".")[0]
        total, tools = _count_connectors(module_dir)
        rows.append(
            PackageInfo(
                name=name,
                description=project.get("description", ""),
                homepage=project.get("urls", {}).get("Homepage", ""),
                entry_point=ep_name,
                module=module,
                connector_count=total,
                tool_count=tools,
            )
        )
    return rows


def _render(rows: list[PackageInfo]) -> str:
    headers = ["Package", "Source", "Connectors", "Tool surface"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in sorted(rows, key=lambda r: r.name):
        favicon = _favicon_url(row.homepage)
        icon_md = (
            f'<img src="{favicon}" width="16" height="16" alt="" /> '
            if favicon
            else ""
        )
        link = f"{icon_md}[`{row.name}`](https://pypi.org/project/{row.name}/)"
        provider = _provider_name(row.description).replace("|", "\\|")
        source_md = f"[{provider}]({row.homepage})" if row.homepage else provider
        tool_cell = f"{row.tool_count} of {row.connector_count}" if row.connector_count else "n/a"
        lines.append(
            f"| {link} | {source_md} | {row.connector_count} | {tool_cell} |"
        )
    return "\n".join(lines)


def _update_readme(table: str) -> None:
    text = README.read_text(encoding="utf-8")
    if ROSTER_START not in text or ROSTER_END not in text:
        raise SystemExit(
            f"README is missing the delimiters {ROSTER_START!r} / {ROSTER_END!r}."
        )
    pattern = re.compile(
        re.escape(ROSTER_START) + r".*?" + re.escape(ROSTER_END),
        re.DOTALL,
    )
    replacement = f"{ROSTER_START}\n{table}\n{ROSTER_END}"
    new_text = pattern.sub(replacement, text)
    if new_text == text:
        return
    README.write_text(new_text, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update-readme",
        action="store_true",
        help="Rewrite the roster block in README.md instead of printing.",
    )
    args = parser.parse_args(argv)
    rows = _gather()
    table = _render(rows)
    if args.update_readme:
        _update_readme(table)
        print(f"Updated roster: {len(rows)} packages", file=sys.stderr)
    else:
        print(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
