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
import enum
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from parsimony_mcp.cli.registry import (
    DEFAULT_CACHE_PATH,
    DEFAULT_REGISTRY_URL,
    Registry,
    RegistryError,
    RegistrySource,
    fetch_registry,
)

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


@dataclass(frozen=True, slots=True)
class FileOperation:
    """A single planned filesystem change.

    ``kind`` identifies the owned file type. ``target`` is the path
    relative to ``InitInputs.target_dir``. ``incoming`` is the
    structured content produced by the planner — Task 8 serializes
    it for each file type. A ``None`` incoming is a "delete" stub
    (not used yet; reserved for future rollback primitives).
    """

    kind: FileKind
    target: Path
    incoming: object | None


def plan(inputs: InitInputs, registry: Registry) -> list[FileOperation]:
    """Return the ordered list of file operations for ``inputs``.

    Pure. No I/O, no prompts, no side effects. The caller feeds
    the result to :func:`apply` (Task 8) or prints it under
    ``--dry-run`` (this task).

    Order matters: ``.gitignore`` must be written before ``.env``
    so the next ``git add`` in the user's project doesn't stage
    secrets. Task 8's ``apply`` honours the list order.
    """
    declared = {c.package: c for c in registry.connectors}
    unknown = [p for p in inputs.selected_packages if p not in declared]
    if unknown:
        raise ValueError(
            f"selected packages not present in registry: {', '.join(unknown)}. "
            f"Regenerate registry.json or remove them from --with."
        )

    # Until Task 8 lands, `incoming` is a lightweight dict the
    # serializer layer fills in. Task 11 hardens the template
    # payloads. Keeping the shape minimal here avoids locking in
    # a contract before the consumer exists.
    selected = tuple(declared[p] for p in inputs.selected_packages)
    env_vars: list[str] = []
    for pkg in selected:
        env_vars.extend(v.name for v in pkg.env_vars)

    base = inputs.target_dir
    return [
        FileOperation(
            kind=FileKind.GITIGNORE,
            target=base / ".gitignore",
            incoming={"lines": [".env", "__pycache__/", ".venv/", ".parsimony-init-staging/"]},
        ),
        FileOperation(
            kind=FileKind.ENV,
            target=base / ".env",
            incoming={"keys": env_vars, "values": dict(inputs.env_values)},
        ),
        FileOperation(
            kind=FileKind.PYPROJECT,
            target=base / "pyproject.toml",
            incoming={"dependencies": [p.package for p in selected]},
        ),
        FileOperation(
            kind=FileKind.MCP_CONFIG,
            target=base / ".mcp.json",
            incoming={"env_vars": env_vars},
        ),
        FileOperation(
            kind=FileKind.AGENTS_MD,
            target=base / "AGENTS.md",
            incoming={"packages": [p.package for p in selected]},
        ),
    ]


# --------------------------------------------------------------------- apply (stub; Task 8 fills in)


def apply(operations: Sequence[FileOperation]) -> None:
    """Apply ``operations`` to disk. Placeholder until Task 8 lands.

    Task 8 replaces this body with: staged-transaction fresh-init,
    in-place merge mode with backups, atomic temp+fsync+replace
    writes, and ordering-aware failure rollback. This stub exists
    so the library surface and CLI adapter can be exercised
    end-to-end today without depending on unfinished work.
    """
    raise NotImplementedError(
        "apply() is implemented in Task 8. Use --dry-run today to exercise the planner."
    )


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
        apply(operations)
    except NotImplementedError as exc:
        # Task 8 lands apply(). Today, --yes paths that aren't
        # --dry-run fall here — surface the exact reason rather
        # than pretending to have written anything.
        print(f"error: {exc}", file=err)
        return ExitCode.FILESYSTEM

    return ExitCode.OK
