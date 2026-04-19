"""Filename sanitization and path-traversal guards."""

import re
from pathlib import Path

# ``$`` is needed for ESTAT's pseudo-dataflow IDs (e.g. ``LFST_HHEREDCH$DV_1343``).
# It's safe as a filename byte on every OS we target; shell-injection risk only
# appears at shell boundaries, and this codebase passes paths via ``pathlib`` /
# syscalls — never through ``shell=True``.
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9._$-]+$")
_MAX_NAME_LEN = 200
_RESERVED_NAMES = frozenset({"", ".", ".."})


def safe_filename(name: str) -> str:
    """Return ``name`` unchanged if it's a safe filename component.

    Rejects empty strings, directory separators, null bytes, relative
    traversal components (``.``, ``..``), names longer than 200 chars,
    and anything outside ``[A-Za-z0-9._$-]``.
    """
    if name in _RESERVED_NAMES:
        raise ValueError(f"Invalid filename: {name!r}")
    if len(name) > _MAX_NAME_LEN:
        raise ValueError(f"Filename too long: {len(name)} > {_MAX_NAME_LEN}")
    if "\x00" in name or "/" in name or "\\" in name:
        raise ValueError(f"Invalid filename: {name!r}")
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(f"Filename contains disallowed characters: {name!r}")
    return name


def ensure_within(base: Path, target: Path) -> Path:
    """Resolve ``target`` and assert it is a descendant of ``base``.

    Returns the resolved target path. Raises ``ValueError`` if resolution
    would escape ``base`` (via symlink, ``..``, or absolute path).
    """
    base_resolved = base.resolve()
    target_resolved = target.resolve()
    try:
        target_resolved.relative_to(base_resolved)
    except ValueError as exc:
        raise ValueError(
            f"Path escapes base: target={target_resolved} base={base_resolved}"
        ) from exc
    return target_resolved
