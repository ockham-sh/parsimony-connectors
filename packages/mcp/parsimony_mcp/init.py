"""Plug-and-play scaffolder: ``parsimony-mcp init``.

Writes three starter files in the project directory based on whichever
``parsimony-*`` plugins are installed in the venv:

* ``.mcp.json`` — wires the server into a project-scoped MCP client
  (Claude Code, Cursor, Continue) using ``uv run --env-file .env``.
* ``.env`` — empty ``KEY=`` lines grouped by connector with the URL
  where each key can be obtained.
* ``AGENTS.md`` — host-authored prompt artifact teaching the
  discover→fetch handshake and the truncation discipline.

The library core is three pure functions — :func:`discover_connectors`
(introspect installed plugins), :func:`render_files` (string
templates), :func:`write_files` (the only impure function) — plus a
thin argparse adapter. There is no ``InitInputs`` dataclass, no
``plan()`` / ``apply()`` triad, no dispatch table, no comment-
preserving file merger. Three functions, one entry point.

Security guards:

* ``.env`` is written with ``O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW`` at
  mode ``0o600``. ``O_EXCL`` is the kernel-level no-clobber guarantee
  that makes ``--force`` opt-in (no TOCTOU race between
  ``Path.exists()`` and ``open(..., 'w')``). ``O_NOFOLLOW`` blocks a
  malicious symlink at ``./.env`` from being silently overwritten.
  Mode ``0o600`` keeps the file owner-read-only.
* Before writing ``.env``, the project's ``.gitignore`` must already
  ignore it (checked via ``git check-ignore`` when ``.git`` exists,
  or by parsing ``.gitignore``). If absent, ``init`` refuses and
  tells the developer to add the rule first — leaked ``.env`` files
  are the single highest-impact failure mode for a local-secrets
  tool.
* ``.mcp.json``'s ``command`` and ``args`` fields are written from
  fixed string literals. No CLI flag interpolates user input into
  either field; ``.mcp.json`` is executed by the agent host on every
  project load.
* ``AGENTS.md`` is host-authored only — no plugin-supplied string
  (description, metadata, tags) is interpolated, because that file
  is loaded into every agent turn and any plugin string there is a
  prompt-injection vector.
"""

from __future__ import annotations

import argparse
import enum
import errno
import importlib
import importlib.metadata
import logging
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

logger = logging.getLogger("parsimony_mcp.init")

PROG = "parsimony-mcp init"

# Filenames the wizard owns. Order matters for write_files: .gitignore
# must already exist (gate), then .mcp.json (no secrets), then AGENTS.md
# (no secrets), then .env (the secret-bearing file, written last so a
# mid-run failure cannot leave it in the tree without the others).
_TARGET_GITIGNORE = ".gitignore"
_TARGET_MCP_JSON = ".mcp.json"
_TARGET_AGENTS_MD = "AGENTS.md"
_TARGET_ENV = ".env"

_WRITE_ORDER: tuple[str, ...] = (_TARGET_MCP_JSON, _TARGET_AGENTS_MD, _TARGET_ENV)


class ExitCode(enum.IntEnum):
    """Three values are enough for a one-shot scaffolder.

    ``OK`` — success (or dry-run / print completed).
    ``USAGE_ERROR`` — argparse rejected the args, or a target file
    exists and ``--force`` was not given.
    ``INTERNAL_ERROR`` — anything else (filesystem error, plugin
    introspection blew up unrecoverably).

    SIGINT lets ``KeyboardInterrupt`` propagate; Python's default
    handler exits with 130 without needing a named code.
    """

    OK = 0
    USAGE_ERROR = 2
    INTERNAL_ERROR = 1


# --------------------------------------------------------------------- discover


@dataclass(frozen=True, slots=True)
class ConnectorInfo:
    """One installed ``parsimony-*`` plugin's metadata.

    ``failed`` is True when import or attribute lookup raised; the
    summary lists failures under "Skipped" but the wizard still
    completes.
    """

    distribution: str
    entry_point_name: str
    env_vars: tuple[str, ...] = ()
    homepage: str | None = None
    failed: bool = False
    failure_reason: str | None = None


def discover_connectors() -> list[ConnectorInfo]:
    """Return one ``ConnectorInfo`` per installed ``parsimony.providers`` entry.

    Imports each plugin module to read ``ENV_VARS`` and
    ``PROVIDER_METADATA``. Plugins whose imports raise are returned
    with ``failed=True`` and a short reason — the wizard surfaces
    them under "Skipped" but does not abort.

    A plugin that hangs at import (e.g. opens a network connection)
    will hang the wizard. The single-user local CLI accepts this in
    return for in-process speed; press Ctrl-C if it happens. The
    deleted ``cli/_introspect.py`` ran each import in a 10s-bounded
    subprocess for this reason.
    """
    eps = importlib.metadata.entry_points(group="parsimony.providers")
    results: list[ConnectorInfo] = []
    for ep in eps:
        info = _introspect_one(ep)
        results.append(info)
    # Sort by distribution name so output is deterministic.
    results.sort(key=lambda c: c.distribution)
    return results


def _introspect_one(ep: importlib.metadata.EntryPoint) -> ConnectorInfo:
    distribution = _entry_point_distribution_name(ep)
    try:
        module = importlib.import_module(ep.value)
    except Exception as exc:
        return ConnectorInfo(
            distribution=distribution,
            entry_point_name=ep.name,
            failed=True,
            failure_reason=f"import failed: {type(exc).__name__}: {exc}",
        )

    env_vars_attr = getattr(module, "ENV_VARS", {}) or {}
    if not isinstance(env_vars_attr, dict):
        return ConnectorInfo(
            distribution=distribution,
            entry_point_name=ep.name,
            failed=True,
            failure_reason=f"ENV_VARS must be dict, got {type(env_vars_attr).__name__}",
        )
    env_vars = tuple(sorted(str(v) for v in env_vars_attr.values()))

    metadata_attr = getattr(module, "PROVIDER_METADATA", {}) or {}
    homepage = None
    if isinstance(metadata_attr, dict):
        raw_homepage = metadata_attr.get("homepage")
        if isinstance(raw_homepage, str) and raw_homepage:
            homepage = raw_homepage

    return ConnectorInfo(
        distribution=distribution,
        entry_point_name=ep.name,
        env_vars=env_vars,
        homepage=homepage,
    )


def _entry_point_distribution_name(ep: importlib.metadata.EntryPoint) -> str:
    """Return the PyPI distribution name that owns ``ep``, or its module name on failure."""
    dist = getattr(ep, "dist", None)
    if dist is not None:
        name = getattr(dist, "name", None)
        if name:
            return str(name)
    return ep.value


# --------------------------------------------------------------------- render


# AGENTS.md template — host-authored only. Do NOT interpolate any
# plugin-supplied string here (description, metadata, tags). The file
# is loaded into every agent turn; any plugin string would be promoted
# from "data inside <catalog>" to "host-level instruction" and become
# a prompt-injection vector.
_AGENTS_MD_TEMPLATE = """\
# parsimony tools — agent guidance

> Generated by `parsimony-mcp init`. Edit by hand if you need to;
> `parsimony-mcp init --force` will overwrite.

## What parsimony is

parsimony is a data layer that exposes search/list/metadata
endpoints from many providers (FRED, CoinGecko, SDMX, etc.) as MCP
tools you can call.

## How to use these tools

1. Call a `parsimony.*` MCP tool to discover or search — these
   return compact, context-friendly results.
2. For bulk retrieval (full time series, multi-year history), run
   Python from this project root using exactly this invocation:

   ```bash
   uv run --env-file .env python -c "
   import asyncio
   from parsimony import client

   async def main():
       result = await client['<connector-name>'](**params)
       print(result.data)

   asyncio.run(main())
   "
   ```

   The `--env-file .env` flag is critical — without it your API keys
   are not visible to the Python subprocess and the connectors
   silently disappear from the `client` registry. Do not use bare
   `python` or `python3`; they have neither parsimony nor the env
   vars.

   Do not invent connector names — only call connectors you saw in
   the catalog returned by an MCP tool.

## When you see a 50-row truncation footer

That output is a discovery preview, not the whole dataset. Do not
re-call the MCP tool with offsets hoping for more rows — switch to
the Python client above.

## When a tool returns an error starting with "DO NOT retry"

Obey the directive. Pick a different connector, ask the user, or
stop — do not paraphrase the call and try again.
"""


# .mcp.json template — fixed command and args, no interpolation. The
# `command` field is executed by the agent host on every project load,
# so it is treated as code, not config.
_MCP_JSON_TEMPLATE = """\
{
  "mcpServers": {
    "parsimony": {
      "command": "uv",
      "args": ["run", "--env-file", ".env", "parsimony-mcp"]
    }
  }
}
"""


_ENV_HEADER = "# parsimony-mcp env — fill the values, then restart your agent client.\n"
_ENV_BLANK_PLACEHOLDER = (
    "# parsimony-mcp env — no connectors detected.\n"
    "# Install one (e.g. `pip install parsimony-fred`) then re-run\n"
    "# `parsimony-mcp init --force` to populate this file.\n"
)


def render_files(connectors: Sequence[ConnectorInfo]) -> dict[str, str]:
    """Return ``{filename: content}`` for the three target files.

    Pure. Same input → same output. No filesystem access.
    """
    return {
        _TARGET_MCP_JSON: _MCP_JSON_TEMPLATE,
        _TARGET_AGENTS_MD: _AGENTS_MD_TEMPLATE,
        _TARGET_ENV: _render_env(connectors),
    }


def _render_env(connectors: Sequence[ConnectorInfo]) -> str:
    """Render the .env template grouped by connector with signup URLs.

    Connectors with zero env vars are omitted entirely (no empty
    headers). Connectors that failed to introspect are skipped (their
    env vars are unknown). If no connector contributes any env var, a
    placeholder file is rendered so the developer sees an actionable
    next step instead of an empty file.
    """
    groups: list[str] = []
    for c in connectors:
        if c.failed or not c.env_vars:
            continue
        header = f"# {c.distribution}"
        if c.homepage:
            header += f" — {c.homepage}"
        lines = [f"{key}=" for key in c.env_vars]
        groups.append(header + "\n" + "\n".join(lines))

    if not groups:
        return _ENV_BLANK_PLACEHOLDER
    return _ENV_HEADER + "\n" + "\n\n".join(groups) + "\n"


# --------------------------------------------------------------------- write


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of :func:`write_files`."""

    target_dir: Path
    written: tuple[Path, ...] = ()
    refused: tuple[Path, ...] = ()
    skipped: tuple[Path, ...] = field(default_factory=tuple)


class InitError(Exception):
    """A precondition failed (target file exists, gitignore missing, etc.)."""


class GitignoreMissingError(InitError):
    """``.env`` cannot be safely written because ``.gitignore`` does not ignore it."""


def write_files(
    files: dict[str, str],
    target_dir: Path,
    *,
    force: bool = False,
) -> WriteResult:
    """Write the three files into ``target_dir``.

    Refuses to overwrite an existing target unless ``force=True``.
    Refuses to write ``.env`` unless ``.gitignore`` (in ``target_dir``
    or anywhere up to the git root) already ignores it. Uses
    ``O_EXCL|O_NOFOLLOW`` so the no-clobber guarantee holds even
    against a concurrent writer or a symlink attack.
    """
    if not target_dir.is_dir():
        raise InitError(f"target directory does not exist: {target_dir}")

    # Check existing files first (before any write happens) so a refusal
    # is atomic — no partial write leaving the tree half-scaffolded.
    existing = [target_dir / name for name in files if (target_dir / name).exists()]
    if existing and not force:
        raise InitError(
            f"target file(s) already exist: {', '.join(p.name for p in existing)}; "
            f"pass --force to overwrite, delete the file(s) and re-run, or use "
            f"`parsimony-mcp init --print` to write the bundle to stdout for "
            f"manual merge."
        )

    if _TARGET_ENV in files and not _is_env_gitignored(target_dir):
        raise GitignoreMissingError(
            f".env is not gitignored in {target_dir}. Add `.env` to .gitignore "
            f"(or create one) before running parsimony-mcp init — leaked .env "
            f"files are the most common cause of API key compromise."
        )

    written: list[Path] = []
    for name in _WRITE_ORDER:
        content = files.get(name)
        if content is None:
            continue
        target = target_dir / name
        mode = 0o600 if name == _TARGET_ENV else 0o644
        _write_one(target, content, mode=mode, force=force)
        written.append(target)

    return WriteResult(target_dir=target_dir.resolve(), written=tuple(written))


def _write_one(target: Path, content: str, *, mode: int, force: bool) -> None:
    """Atomic, no-clobber, no-symlink-follow write."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW
    if force:
        flags |= os.O_TRUNC
    else:
        flags |= os.O_EXCL
    try:
        fd = os.open(str(target), flags, mode)
    except FileExistsError as exc:
        raise InitError(
            f"{target} appeared during init; refusing to clobber. Re-run with --force."
        ) from exc
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise InitError(
                f"refusing to write through symlink at {target}; remove the "
                f"symlink and re-run."
            ) from exc
        if exc.errno == errno.EACCES:
            raise InitError(f"no write permission for {target.parent}.") from exc
        raise InitError(f"failed to write {target}: {exc}") from exc
    try:
        os.write(fd, content.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _is_env_gitignored(target_dir: Path) -> bool:
    """Return True iff ``.env`` is gitignored in ``target_dir``.

    Prefers ``git check-ignore`` when ``.git`` exists (handles nested
    ``.gitignore`` files and global excludes). Falls back to a simple
    line-match against ``target_dir/.gitignore`` for non-git trees.
    Returns False if no ``.gitignore`` exists at all — the user is
    asked to create one.
    """
    if (target_dir / ".git").exists():
        try:
            # Fixed argv; PATH lookup of `git` is intentional (the user
            # has git installed if .git/ exists).
            result = subprocess.run(
                ["git", "check-ignore", "--quiet", ".env"],  # noqa: S607
                cwd=str(target_dir),
                capture_output=True,
                check=False,
                timeout=5,
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.SubprocessError):
            # git not on PATH or subprocess failure — fall through to
            # the line-match heuristic.
            pass

    gitignore = target_dir / ".gitignore"
    if not gitignore.is_file():
        return False
    for line in gitignore.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line in (".env", "/.env", "*.env"):
            return True
    return False


# --------------------------------------------------------------------- summary


def render_summary(
    result: WriteResult,
    connectors: Sequence[ConnectorInfo],
    *,
    dry_run: bool = False,
) -> str:
    """Render the success summary — Friedman's next-action-led layout.

    The numbered "Next steps" block sits at the bottom because that is
    where the eye lands after scanning, and it is the entire reason
    the developer ran the command.
    """
    verb = "would write" if dry_run else "wrote"
    lines: list[str] = []
    lines.append(f"\nparsimony-mcp init — {'dry run' if dry_run else 'done'}.")
    lines.append(f"  project:  {result.target_dir}")
    lines.append(f"  files ({len(result.written)}):")
    for path in result.written:
        lines.append(f"    {verb} {path.relative_to(result.target_dir)}")

    successful = [c for c in connectors if not c.failed]
    failed = [c for c in connectors if c.failed]
    if successful:
        lines.append("")
        lines.append(f"  Connectors discovered ({len(successful)}):")
        for c in successful:
            env_count = len(c.env_vars)
            note = f"({env_count} env var{'s' if env_count != 1 else ''})" if env_count else "(no env vars)"
            lines.append(f"    {c.distribution} {note}")
    if failed:
        lines.append("")
        lines.append(f"  Skipped ({len(failed)}):")
        for c in failed:
            lines.append(f"    {c.distribution}: {c.failure_reason}")
    if not connectors:
        lines.append("")
        lines.append("  Connectors discovered: 0")
        lines.append("    Install one with `pip install parsimony-fred` (or any")
        lines.append("    other parsimony-* plugin), then re-run with --force.")

    lines.append("")
    lines.append("  Next steps:")
    lines.append(f"    1. Open {_TARGET_ENV} and fill the empty values.")
    lines.append("    2. Restart Claude Desktop / Claude Code so it picks up .mcp.json.")
    lines.append("    3. In your client, ask 'list parsimony tools' to verify.")
    lines.append("")
    return "\n".join(lines)


def render_print_bundle(files: dict[str, str]) -> str:
    """Render the bundle to stdout with FILE separators for manual merge."""
    out: list[str] = []
    for name in _WRITE_ORDER:
        content = files.get(name)
        if content is None:
            continue
        out.append(f"# === FILE: {name} ===")
        out.append(content)
        out.append("")
    return "\n".join(out)


# --------------------------------------------------------------------- argparse + entry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Stamp .mcp.json + .env + AGENTS.md from installed parsimony-* plugins.",
    )
    parser.add_argument(
        "--into",
        metavar="DIR",
        default=".",
        help="Directory to scaffold (default: current directory).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing target files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be written without touching disk.",
    )
    parser.add_argument(
        "--print",
        dest="print_bundle",
        action="store_true",
        help="Write the file bundle to stdout (for manual merge into existing files).",
    )
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
) -> int:
    """CLI entry point. Returns an :class:`ExitCode`."""
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr

    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else int(ExitCode.USAGE_ERROR)
        return code

    target_dir = Path(args.into).resolve()

    try:
        connectors = discover_connectors()
    except Exception as exc:
        print(f"error: plugin discovery failed: {type(exc).__name__}: {exc}", file=err)
        return int(ExitCode.INTERNAL_ERROR)

    files = render_files(connectors)

    if args.print_bundle:
        print(render_print_bundle(files), file=out)
        return int(ExitCode.OK)

    if args.dry_run:
        # Build a synthetic WriteResult so the summary renders.
        synthetic_paths = tuple(target_dir / name for name in _WRITE_ORDER)
        synthetic = WriteResult(target_dir=target_dir, written=synthetic_paths)
        print(render_summary(synthetic, connectors, dry_run=True), file=out)
        return int(ExitCode.OK)

    try:
        result = write_files(files, target_dir, force=args.force)
    except GitignoreMissingError as exc:
        print(f"error: {exc}", file=err)
        return int(ExitCode.USAGE_ERROR)
    except InitError as exc:
        print(f"error: {exc}", file=err)
        return int(ExitCode.USAGE_ERROR)
    except OSError as exc:
        print(f"error: {exc}", file=err)
        return int(ExitCode.INTERNAL_ERROR)

    print(render_summary(result, connectors), file=out)
    return int(ExitCode.OK)
