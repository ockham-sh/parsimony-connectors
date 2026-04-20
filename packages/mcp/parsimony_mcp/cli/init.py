"""Init-wizard core — library-first, no orchestrator class.

The init workflow is a linear data flow:

    InitInputs  ─► plan()  ─►  list[FileOperation]  ─►  apply()

``InitInputs`` is a frozen dataclass carrying every decision made
before any file is touched (Dodds P2 — eliminate derivable state;
``InitInputs`` is the state, everything else is derived from it).
``plan()`` is a pure function — no I/O, no randomness, easy to test
and easy to show the user with ``--dry-run``. ``apply()`` is the
only disk-touching function and lives at the library edge.

No ``InitWizard`` class: there's one caller, one linear path, and
``self`` adds nothing but a place to hide state (Dodds P1 — AHA).
The prompt-interaction layer lands in Task 9 and calls the same
plan/apply pair the ``--yes`` path uses — so ``--dry-run`` and
``--yes`` both fall out as properties of the shared code path, not
parallel implementations.

Stability contract (within the 0.1.x line):

* Flag names in ``_add_arguments()``.
* Exit codes in :class:`ExitCode`.
* ``FileOperation.target`` paths (relative to ``--into``).

Changing any of these is a MINOR bump. The CLI snapshot test
fails on ``--help`` drift to make this contract enforceable rather
than aspirational (Dodds P4 — test behaviour, not implementation;
``--help`` is the behavioural surface).
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import enum
import errno
import logging
import os
import shutil
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from parsimony_mcp.cli._merge import (
    AgentsMdPayload,
    EnvPayload,
    GitignorePayload,
    McpConfigPayload,
    PyprojectPayload,
    merge_agents_md,
    merge_env,
    merge_gitignore,
    merge_mcp_config,
    merge_pyproject,
)
from parsimony_mcp.cli.registry import (
    DEFAULT_CACHE_PATH,
    DEFAULT_REGISTRY_URL,
    Registry,
    RegistryError,
    RegistrySource,
    fetch_registry,
)

_LOG = logging.getLogger("parsimony_mcp.cli.init")

_STAGING_DIRNAME = ".parsimony-init-staging"
_BACKUP_SUFFIX_FMT = ".parsimony.bak-{timestamp}"

PROG = "parsimony-mcp init"

# Packages pre-selected when the user runs with `--yes` and no
# explicit `--with`. The set is deliberately narrow: free-tier
# connectors that cover macro (FRED), multilateral data (SDMX),
# and crypto (CoinGecko) — the smallest catalog that still
# demonstrates the tool surface.
RECOMMENDED_STARTER_SET: tuple[str, ...] = (
    "parsimony-fred",
    "parsimony-sdmx",
    "parsimony-coingecko",
)


class ExitCode(enum.IntEnum):
    """Exit-code contract. Stable within the 0.1.x line.

    * ``0`` (OK)             — success, files written (or dry-run completed).
    * ``1`` (USER_CANCEL)    — user declined a confirm prompt.
    * ``2`` (USAGE)          — argparse-level usage error.
    * ``3`` (REGISTRY)       — registry unreachable + no fallback.
    * ``4`` (CONFLICT)       — existing file(s) present and no ``--force``.
    * ``5`` (FILESYSTEM)     — unrecoverable disk error during apply.
    * ``130`` (SIGINT)       — Ctrl-C. Nothing written.
    """

    OK = 0
    USER_CANCEL = 1
    USAGE = 2
    REGISTRY = 3
    CONFLICT = 4
    FILESYSTEM = 5
    SIGINT = 130


# --------------------------------------------------------------------- inputs


@dataclass(frozen=True, slots=True)
class InitInputs:
    """Everything the wizard needs to produce a plan. Immutable by design.

    Produced by the argparse adapter (non-interactive path) or by
    the prompt layer (Task 9). The plan is a deterministic function
    of these inputs — given the same ``InitInputs`` and the same
    ``Registry``, ``plan()`` returns the same list.
    """

    target_dir: Path
    selected_packages: tuple[str, ...]
    env_values: dict[str, str] = field(default_factory=dict)
    dry_run: bool = False
    assume_yes: bool = False
    force: bool = False
    registry_url: str = DEFAULT_REGISTRY_URL
    cache_path: Path | None = DEFAULT_CACHE_PATH
    show_keys: bool = False


# --------------------------------------------------------------------- plan


class FileKind(enum.Enum):
    """The five file types the wizard owns.

    Ordering is load-bearing — see :func:`plan`. ``.gitignore``
    before ``.env`` is a security invariant (Task 8 enforces the
    atomic write; Task 7 establishes the order).
    """

    GITIGNORE = ".gitignore"
    ENV = ".env"
    PYPROJECT = "pyproject.toml"
    MCP_CONFIG = ".mcp.json"
    AGENTS_MD = "AGENTS.md"


FilePayload = (
    GitignorePayload
    | EnvPayload
    | PyprojectPayload
    | McpConfigPayload
    | AgentsMdPayload
)


@dataclass(frozen=True, slots=True)
class FileOperation:
    """A single planned filesystem change.

    ``kind`` identifies the owned file type. ``target`` is the path
    relative to (or under) ``InitInputs.target_dir``. ``incoming``
    is the typed payload the matching merger in ``_merge`` consumes.
    """

    kind: FileKind
    target: Path
    incoming: FilePayload


_GITIGNORE_LINES: tuple[str, ...] = (
    ".env",
    "__pycache__/",
    ".venv/",
    f"{_STAGING_DIRNAME}/",
)


def plan(inputs: InitInputs, registry: Registry) -> list[FileOperation]:
    """Return the ordered list of file operations for ``inputs``.

    Pure. No I/O, no prompts, no side effects. The caller feeds
    the result to :func:`apply` or prints it under ``--dry-run``.

    Order matters: ``.gitignore`` must be written before ``.env``
    so the next ``git add`` in the user's project doesn't stage
    secrets. :func:`apply` honours the list order.
    """
    declared = {c.package: c for c in registry.connectors}
    unknown = [p for p in inputs.selected_packages if p not in declared]
    if unknown:
        raise ValueError(
            f"selected packages not present in registry: {', '.join(unknown)}. "
            f"Regenerate registry.json or remove them from --with."
        )

    selected = tuple(declared[p] for p in inputs.selected_packages)
    env_vars: list[str] = []
    for pkg in selected:
        env_vars.extend(v.name for v in pkg.env_vars)

    base = inputs.target_dir
    return [
        FileOperation(
            kind=FileKind.GITIGNORE,
            target=base / ".gitignore",
            incoming=GitignorePayload(lines=_GITIGNORE_LINES),
        ),
        FileOperation(
            kind=FileKind.ENV,
            target=base / ".env",
            incoming=EnvPayload(keys=tuple(env_vars), values=dict(inputs.env_values)),
        ),
        FileOperation(
            kind=FileKind.PYPROJECT,
            target=base / "pyproject.toml",
            incoming=PyprojectPayload(dependencies=tuple(p.package for p in selected)),
        ),
        FileOperation(
            kind=FileKind.MCP_CONFIG,
            target=base / ".mcp.json",
            incoming=McpConfigPayload(env_vars=tuple(env_vars)),
        ),
        FileOperation(
            kind=FileKind.AGENTS_MD,
            target=base / "AGENTS.md",
            incoming=AgentsMdPayload(packages=tuple(p.package for p in selected)),
        ),
    ]


# --------------------------------------------------------------------- apply


class ApplyError(Exception):
    """A file operation could not be completed.

    Raised by :func:`apply` when any step of the transaction fails.
    The exception's ``__cause__`` is the underlying ``OSError`` /
    ``ValueError`` so the CLI layer can surface actionable prose
    without leaking a traceback in steady-state logs.
    """


class ApplyConflict(ApplyError):
    """Target files exist and ``force`` was not set."""


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """Summary of a successful :func:`apply` run.

    The CLI surfaces the written / backed-up / skipped breakdown so
    the user can confirm what happened without re-reading the plan.
    """

    target_dir: Path
    written: tuple[Path, ...]
    backups: tuple[Path, ...]
    unchanged: tuple[Path, ...]


def apply(
    operations: Sequence[FileOperation],
    *,
    target_dir: Path,
    force: bool = False,
    assume_yes: bool = False,
) -> ApplyResult:
    """Execute ``operations`` with transactional semantics.

    Two modes, chosen automatically:

    * **Fresh-init** (no wizard-owned files exist under ``target_dir``):
      render all operations into ``.parsimony-init-staging/``,
      validate each staged file by round-tripping through its
      own parser, then atomic-rename each staged file into
      place. If any validation fails, the staging dir is removed
      and the user's tree is untouched (Collina P5 — resource
      management; the transaction is the unit of recovery).

    * **Merge mode** (at least one wizard-owned file exists):
      back up each existing file to
      ``<file>.parsimony.bak-<iso8601>`` before replacing it.
      Writes are still atomic per-file; backups are synchronous
      so a mid-run failure leaves a reviewable timestamped
      sibling to roll back from.

    Symlink escape guard: any ``operation.target`` that resolves
    outside ``target_dir.resolve()`` is refused with
    :class:`ApplyError` before the first write. Protects against
    a malicious ``--into`` pointing at a dotfile-symlink-rich
    directory.

    ``.env`` is always opened with ``O_CREAT|O_EXCL|O_WRONLY`` at
    mode 0600 when it doesn't already exist, so two concurrent
    wizards on the same project can't race and the file never
    lands world-readable. On Windows ``O_EXCL`` behaves differently;
    the guard downgrades to a non-atomic existence check with a
    WARN.

    ``assume_yes`` is accepted for interface symmetry with the
    prompt layer (Task 9). This function never prompts itself.
    """
    anchor = target_dir.resolve()
    if not anchor.is_dir():
        raise ApplyError(f"--into target does not exist or is not a directory: {anchor}")

    for op in operations:
        _verify_under_anchor(op.target, anchor)

    existing_files = [op.target for op in operations if op.target.exists()]
    is_merge_mode = bool(existing_files)
    # Merge mode backs up existing files rather than overwriting, but
    # the caller must explicitly opt in via --force OR --yes (the
    # programmatic consent equivalent of typing "y" at the prompt).
    if is_merge_mode and not force and not assume_yes:
        raise ApplyConflict(
            f"target directory already contains wizard-owned files: "
            f"{', '.join(str(p.relative_to(anchor)) for p in existing_files)}. "
            f"Pass --yes to back up and merge, or --force to skip the backup."
        )

    fresh_init = all(not p.exists() for p in (op.target for op in operations))
    if fresh_init:
        return _apply_fresh_init(operations, anchor=anchor)
    return _apply_merge(operations, anchor=anchor, force=force)


def _apply_fresh_init(operations: Sequence[FileOperation], *, anchor: Path) -> ApplyResult:
    """Staged-transaction fresh init.

    Render → validate → atomic rename. Any validation failure
    aborts before a single target file is touched.
    """
    staging = anchor / _STAGING_DIRNAME
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(mode=0o700)

    written: list[Path] = []
    try:
        # Render every operation to the staging dir, then validate
        # each one by round-tripping through its format. A planner
        # bug that produces malformed TOML / JSON / etc. must fail
        # here, not after we've clobbered the user's tree.
        staged_pairs: list[tuple[FileOperation, Path]] = []
        for op in operations:
            body = _render_operation(op, existing=None)
            _validate_roundtrip(op.kind, body)
            staged = staging / op.target.name
            _atomic_write(staged, body, exclusive_env=(op.kind is FileKind.ENV))
            staged_pairs.append((op, staged))

        for op, staged in staged_pairs:
            _atomic_move(staged, op.target, exclusive_env=(op.kind is FileKind.ENV))
            written.append(op.target)
    finally:
        with contextlib.suppress(OSError):
            if staging.exists():
                shutil.rmtree(staging)

    return ApplyResult(
        target_dir=anchor,
        written=tuple(written),
        backups=(),
        unchanged=(),
    )


def _apply_merge(
    operations: Sequence[FileOperation], *, anchor: Path, force: bool
) -> ApplyResult:
    """In-place merge with backups per existing file.

    Per-file sequence: read → merge → validate round-trip → back up
    existing → atomic replace. Ordering is honoured so ``.gitignore``
    always lands before ``.env`` (secret-leak guard).
    """
    written: list[Path] = []
    backups: list[Path] = []
    unchanged: list[Path] = []
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    for op in operations:
        existing = op.target.read_text(encoding="utf-8") if op.target.exists() else None
        body = _render_operation(op, existing=existing)
        if existing is not None and body == existing:
            unchanged.append(op.target)
            continue
        _validate_roundtrip(op.kind, body)

        if op.target.exists() and not force:
            backup = op.target.with_name(
                op.target.name + _BACKUP_SUFFIX_FMT.format(timestamp=timestamp)
            )
            shutil.copy2(op.target, backup)
            backups.append(backup)

        _atomic_write(op.target, body, exclusive_env=(op.kind is FileKind.ENV))
        written.append(op.target)

    return ApplyResult(
        target_dir=anchor,
        written=tuple(written),
        backups=tuple(backups),
        unchanged=tuple(unchanged),
    )


# --------------------------------------------------------------------- render / validate


def _render_operation(op: FileOperation, *, existing: str | None) -> str:
    """Dispatch the operation to the matching pure merger in ``_merge``."""
    if op.kind is FileKind.GITIGNORE:
        assert isinstance(op.incoming, GitignorePayload)
        return merge_gitignore(existing, op.incoming)
    if op.kind is FileKind.ENV:
        assert isinstance(op.incoming, EnvPayload)
        return merge_env(existing, op.incoming)
    if op.kind is FileKind.PYPROJECT:
        assert isinstance(op.incoming, PyprojectPayload)
        return merge_pyproject(existing, op.incoming)
    if op.kind is FileKind.MCP_CONFIG:
        assert isinstance(op.incoming, McpConfigPayload)
        return merge_mcp_config(existing, op.incoming)
    if op.kind is FileKind.AGENTS_MD:
        assert isinstance(op.incoming, AgentsMdPayload)
        return merge_agents_md(existing, op.incoming)
    raise AssertionError(f"unhandled FileKind: {op.kind!r}")


def _validate_roundtrip(kind: FileKind, body: str) -> None:
    """Parse ``body`` through its own format and raise on malformed output.

    Catches planner / merger bugs before a single target file is
    replaced. Especially valuable for the TOML merge: ``tomlkit``
    round-tripping through a comment-preserving parser is the
    safety net for unexpected user input.
    """
    try:
        if kind is FileKind.PYPROJECT:
            import tomllib

            tomllib.loads(body)
        elif kind is FileKind.MCP_CONFIG:
            import json

            json.loads(body)
        # .gitignore / .env / AGENTS.md have no parse step; the
        # mergers are string-level.
    except (ValueError, OSError) as exc:
        raise ApplyError(
            f"generated content for {kind.value} failed round-trip validation: {exc}"
        ) from exc


# --------------------------------------------------------------------- filesystem primitives


def _verify_under_anchor(target: Path, anchor: Path) -> None:
    """Refuse operation targets that escape ``anchor`` via symlink or ``..``.

    Two guards:

    * The parent chain resolved to a real directory must stay under
      ``anchor`` — blocks ``--into`` pointing at a directory whose
      parent is a relative-path escape.
    * If ``target`` itself is a symlink, its resolved destination
      must also stay under ``anchor`` — blocks the "attacker plants
      ``.env`` as a symlink to ``~/.ssh/id_rsa``" attack. A broken
      symlink (target doesn't exist) is likewise refused because we
      can't verify where it would eventually land.
    """
    try:
        resolved_parent = target.parent.resolve(strict=False)
    except OSError as exc:
        raise ApplyError(f"cannot resolve parent of {target}: {exc}") from exc
    try:
        resolved_parent.relative_to(anchor)
    except ValueError as exc:
        raise ApplyError(
            f"refusing to write {target.name!r} outside project anchor {anchor}"
        ) from exc

    if target.is_symlink():
        try:
            resolved_target = target.resolve(strict=False)
            resolved_target.relative_to(anchor)
        except (OSError, ValueError) as exc:
            raise ApplyError(
                f"refusing to write through symlink {target.name!r} "
                f"which escapes project anchor {anchor}"
            ) from exc


def _atomic_write(path: Path, content: str, *, exclusive_env: bool) -> None:
    """Write ``content`` to ``path`` atomically.

    Temp-file-in-same-directory + fsync + ``os.replace``. ``.env``
    gets ``O_CREAT|O_EXCL|O_WRONLY`` at 0600 when it doesn't already
    exist — concurrent wizards on the same project can't race.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if exclusive_env and not path.exists():
        _write_env_exclusive(path, content)
        return

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        os.write(fd, content.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        if exclusive_env:
            # We're replacing an existing .env in-place via merge
            # mode — tighten permissions before the rename so the
            # tighter mode is what lands atomically.
            os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, path)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _write_env_exclusive(path: Path, content: str) -> None:
    """Create ``path`` atomically with O_EXCL at mode 0600.

    Fails with :class:`ApplyError` if another process created the
    file between the apply-preflight existence check and this call.
    On Windows ``O_EXCL`` is honoured but mode 0600 has no meaning;
    we still emit the flag so concurrent creators race safely.
    """
    try:
        fd = os.open(
            str(path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError as exc:
        raise ApplyError(
            f"{path} was created by another process while we were running — "
            f"bail out and re-run the wizard"
        ) from exc
    except OSError as exc:
        if exc.errno == errno.EACCES:
            raise ApplyError(f"no write permission for {path.parent}") from exc
        raise
    try:
        os.write(fd, content.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_move(src: Path, dst: Path, *, exclusive_env: bool) -> None:
    """Atomically move ``src`` to ``dst``.

    Staging → target. For ``.env`` we preserve the O_EXCL guarantee:
    if the user created a .env themselves between preflight and
    move, we refuse rather than clobber.
    """
    if exclusive_env:
        # For .env, use O_EXCL semantics via link()-unlink() to
        # keep the atomic-no-clobber guarantee we set up in the
        # staging write.
        try:
            os.link(str(src), str(dst))
        except FileExistsError as exc:
            raise ApplyError(
                f"{dst} already exists; refusing to overwrite .env without explicit --force"
            ) from exc
        except OSError:
            # link() unsupported on some filesystems (e.g. WSL on
            # certain mount types); fall back to replace after
            # confirming dst still doesn't exist.
            if dst.exists():
                raise ApplyError(f"{dst} appeared unexpectedly during staging flush") from None
            os.replace(str(src), str(dst))
            return
        os.unlink(str(src))
        return
    os.replace(str(src), str(dst))


# --------------------------------------------------------------------- argparse adapter


def _add_arguments(parser: argparse.ArgumentParser, *, advanced: bool) -> None:
    """Add flags to ``parser``. Hides advanced flags from ``--help``.

    ``--help`` is the first-time-user surface. ``--help-advanced``
    adds: ``--registry``, ``--no-cache``, ``--show-keys``, ``--force``
    (Friedman — progressive disclosure). The advanced flags still
    parse either way; only the help text is gated.
    """
    parser.add_argument(
        "--into",
        metavar="DIR",
        default=".",
        help="Directory to initialize (default: current directory).",
    )
    parser.add_argument(
        "--with",
        dest="with_packages",
        metavar="PACKAGE",
        action="append",
        default=[],
        help=(
            "Pre-select a connector package (e.g. parsimony-fred). May be given "
            "multiple times. Required with --yes in a non-TTY shell."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the planned file operations and exit without writing anything.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive prompts; use --with packages (or recommended set).",
    )

    advanced_help = argparse.SUPPRESS if not advanced else None
    parser.add_argument(
        "--registry",
        metavar="URL",
        default=DEFAULT_REGISTRY_URL,
        help=(
            "Override the registry URL (advanced). Disables the default cache."
            if advanced
            else advanced_help
        ),
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help=(
            "Bypass the on-disk registry cache (advanced)."
            if advanced
            else advanced_help
        ),
    )
    parser.add_argument(
        "--show-keys",
        action="store_true",
        help=(
            "Echo API keys as they're entered (advanced; disabled by default)."
            if advanced
            else advanced_help
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite existing files without backup (advanced; rarely safe)."
            if advanced
            else advanced_help
        ),
    )
    parser.add_argument(
        "--help-advanced",
        action="store_true",
        help="Show flags for scripted or advanced use.",
    )


def build_parser(*, advanced: bool = False) -> argparse.ArgumentParser:
    """Return the init subcommand's parser.

    Factored so the snapshot test can introspect ``--help`` output
    for both the public and advanced surfaces without depending on
    the rest of ``__main__``.
    """
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Bootstrap a parsimony project that MCP-compatible coding agents can use.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            ""
            if advanced
            else "For scripted or advanced use, see `parsimony-mcp init --help-advanced`."
        ),
    )
    _add_arguments(parser, advanced=advanced)
    return parser


def inputs_from_args(args: argparse.Namespace, registry: Registry) -> InitInputs:
    """Translate parsed argparse into an immutable :class:`InitInputs`.

    Defaults are resolved here so the caller doesn't re-apply them.
    Selection fallback: explicit ``--with`` wins; else recommended
    starter set; else empty tuple (user will be prompted — Task 9).
    """
    target_dir = Path(args.into).resolve()

    selected: tuple[str, ...]
    if args.with_packages:
        selected = tuple(args.with_packages)
    elif args.yes:
        declared = {c.package for c in registry.connectors}
        selected = tuple(p for p in RECOMMENDED_STARTER_SET if p in declared)
    else:
        selected = ()

    # A non-default registry URL must never touch the shared cache.
    # --no-cache makes the same call site explicit.
    uses_default_registry = args.registry == DEFAULT_REGISTRY_URL and not args.no_cache
    cache_path: Path | None = DEFAULT_CACHE_PATH if uses_default_registry else None

    return InitInputs(
        target_dir=target_dir,
        selected_packages=selected,
        dry_run=args.dry_run,
        assume_yes=args.yes,
        force=args.force,
        registry_url=args.registry,
        cache_path=cache_path,
        show_keys=args.show_keys,
    )


def _render_dry_run(ops: Sequence[FileOperation], source: RegistrySource, out: IO[str]) -> None:
    """Human-readable plan summary for ``--dry-run``.

    Deliberately plain: one line per file operation, a trailing
    source line so the user knows whether the registry came from
    cache or network. No color, no box-drawing — pipes cleanly
    into reviews.
    """
    print(f"parsimony-mcp init (dry-run) — registry source: {source.origin} ({source.url})", file=out)
    if not ops:
        print("  (no operations planned)", file=out)
        return
    for op in ops:
        print(f"  {op.kind.value:<15}  {op.target}", file=out)


def _non_interactive_ready(inputs: InitInputs) -> bool:
    """Can we skip prompts entirely?

    True when the caller supplied enough inputs to produce a plan
    without asking questions: ``--yes`` AND at least one package
    (either via ``--with`` or resolved from the recommended set).
    """
    return inputs.assume_yes and bool(inputs.selected_packages)


def run(
    argv: Sequence[str] | None = None,
    *,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
) -> int:
    """CLI entry point for ``parsimony-mcp init``.

    Returns an :class:`ExitCode`. Never raises on expected failure
    modes — every branch maps to an exit code the shell / CI can
    react to. Unexpected exceptions propagate; ``__main__`` catches
    :class:`KeyboardInterrupt` once at the top.
    """
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--help-advanced", action="store_true")
    pre_args, _ = pre_parser.parse_known_args(argv)

    parser = build_parser(advanced=pre_args.help_advanced)
    if pre_args.help_advanced:
        parser.print_help(out)
        return ExitCode.OK

    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        # argparse exits 2 on usage errors; pass it through with the
        # contract-stable enum so callers can pattern-match.
        code = exc.code if isinstance(exc.code, int) else ExitCode.USAGE
        return int(code)

    uses_default_registry = args.registry == DEFAULT_REGISTRY_URL and not args.no_cache
    cache_path: Path | None = DEFAULT_CACHE_PATH if uses_default_registry else None
    try:
        registry, source = fetch_registry(url=args.registry, cache_path=cache_path)
    except RegistryError as exc:
        print(f"error: {exc}", file=err)
        return ExitCode.REGISTRY

    inputs = inputs_from_args(args, registry)

    try:
        operations = plan(inputs, registry)
    except ValueError as exc:
        print(f"error: {exc}", file=err)
        return ExitCode.USAGE

    if inputs.dry_run:
        _render_dry_run(operations, source, out)
        return ExitCode.OK

    if not _non_interactive_ready(inputs):
        # Task 9 lands the interactive prompt layer. Until then,
        # non-TTY / no-flags runs must fail loudly with the exact
        # scripted recipe — never silently proceed.
        print(
            "error: interactive prompts not available yet. "
            "Use --yes together with --with parsimony-<name> (or --dry-run).",
            file=err,
        )
        return ExitCode.USAGE

    try:
        result = apply(
            operations,
            target_dir=inputs.target_dir,
            force=inputs.force,
            assume_yes=inputs.assume_yes,
        )
    except ApplyConflict as exc:
        print(f"error: {exc}", file=err)
        return ExitCode.CONFLICT
    except ApplyError as exc:
        print(f"error: {exc}", file=err)
        return ExitCode.FILESYSTEM

    _render_apply_summary(result, source, out)
    return ExitCode.OK


def _render_apply_summary(result: ApplyResult, source: RegistrySource, out: IO[str]) -> None:
    """Print a concise report of what changed, what was backed up."""
    print(f"parsimony-mcp init — registry source: {source.origin} ({source.url})", file=out)
    print(f"  target: {result.target_dir}", file=out)
    for path in result.written:
        print(f"  wrote   {path.relative_to(result.target_dir)}", file=out)
    for path in result.backups:
        print(f"  backup  {path.relative_to(result.target_dir)}", file=out)
    for path in result.unchanged:
        print(f"  (no change) {path.relative_to(result.target_dir)}", file=out)
