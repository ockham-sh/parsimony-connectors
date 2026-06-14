"""Regenerate the frozen BoJ database registry from the live API tool.

The BOJ Time-Series Data Search API exposes **no endpoint that lists its
databases**, so ``parsimony_boj/databases.py`` freezes the 50-entry list. This
script re-derives that freeze from BoJ's own machine-readable source — the
``DB_Name`` sheet of ``api_tool.xlsx`` — so the registry is reproducible rather
than hand-maintained (the archetype-C "commit the harvester" rule).

Usage::

    uv run python packages/boj/scripts/harvest_databases.py            # print the literal
    uv run python packages/boj/scripts/harvest_databases.py --diff     # diff vs the frozen registry

Re-run whenever BoJ revises the manual (it added FF/CO/BIS/DER/OT and dropped a
phantom ``BP02`` once already). Uses only the standard library (no openpyxl).
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import urllib.request
import zipfile
from xml.etree import ElementTree as ET

XLSX_URL = "https://www.stat-search.boj.or.jp/info/api_tool.xlsx"
_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_CODE_RE = re.compile(
    r"^(IR0[1-4]|FM0[1-9]|PS0[12]|MD0[1-9]|MD1[0-4]|LA0[1-5]|BS0[12]|FF|OB0[12]|CO|PR0[1-4]|PF0[12]|BP01|BIS|DER|OT)$"
)


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    out: list[str] = []
    root = ET.fromstring(z.read("xl/sharedStrings.xml"))
    for si in root.findall(f"{_NS}si"):
        out.append("".join(t.text or "" for t in si.iter(f"{_NS}t")))
    return out


def _sheet_path(z: zipfile.ZipFile, name: str) -> str:
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    name_to_rid = {s.get("name"): s.get(f"{_REL}id") for s in wb.iter(f"{_NS}sheet")}
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    rid_to_target = {r.get("Id"): r.get("Target") for r in rels}
    target = rid_to_target[name_to_rid[name]]
    assert target is not None, f"no worksheet target for sheet {name!r}"
    return "xl/" + target


def harvest() -> list[tuple[str, str, str]]:
    """Download ``api_tool.xlsx`` and return ``(code, category, title)`` triples."""
    req = urllib.request.Request(XLSX_URL, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310 (trusted vendor URL)
        data = r.read()
    z = zipfile.ZipFile(io.BytesIO(data))
    ss = _shared_strings(z)
    sheet = ET.fromstring(z.read(_sheet_path(z, "DB_Name")))

    out: list[tuple[str, str, str]] = []
    for row in sheet.findall(f".//{_NS}row"):
        cells: list[str] = []
        for c in row.findall(f"{_NS}c"):
            v = c.find(f"{_NS}v")
            text = v.text if v is not None else None
            if text is None:
                val = ""
            elif c.get("t") == "s":
                val = ss[int(text)]
            else:
                val = text
            cells.append(val.strip())
        # The DB_Name sheet's English triple is columns (category, code, name) at index 3,4,5.
        if len(cells) >= 6 and _CODE_RE.match(cells[4]):
            out.append((cells[4], cells[3], cells[5]))
    return out


def _print_literal(rows: list[tuple[str, str, str]]) -> None:
    print(f"# {len(rows)} databases harvested from api_tool.xlsx DB_Name sheet")
    print("_BOJ_DATABASES: tuple[tuple[str, str, str], ...] = (")
    for code, cat, name in rows:
        print(f"    ({code!r}, {cat!r}, {name!r}),")
    print(")")


def _diff(rows: list[tuple[str, str, str]]) -> int:
    from parsimony_boj.databases import _BOJ_DATABASES

    harvested = {c: (cat, name) for c, cat, name in rows}
    frozen = {c: (cat, name) for c, cat, name in _BOJ_DATABASES}
    added = sorted(set(harvested) - set(frozen))
    removed = sorted(set(frozen) - set(harvested))
    changed = sorted(c for c in harvested.keys() & frozen.keys() if harvested[c] != frozen[c])
    print(f"harvested={len(harvested)} frozen={len(frozen)}")
    print(f"  codes added (in XLSX, not frozen):   {added}")
    print(f"  codes removed (frozen, not in XLSX): {removed}")
    print(f"  codes with changed category/title:   {changed}")
    if not (added or removed or changed):
        print("  -> registry is in sync with the live XLSX.")
        return 0
    print("  -> registry DRIFTED; update parsimony_boj/databases.py from --print output.")
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--diff", action="store_true", help="diff the live XLSX against the frozen registry")
    args = ap.parse_args()
    rows = harvest()
    if args.diff:
        return _diff(rows)
    _print_literal(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
