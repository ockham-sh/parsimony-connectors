"""Interactive prompt layer for ``parsimony-mcp init``.

Separated from :mod:`parsimony_mcp.cli.init` so the library core
stays pure: ``InitInputs`` → ``plan()`` → ``apply()``. This module
does the TTY-side work of turning an under-specified :class:`InitInputs`
into a complete one — connector selection, API-key entry, final
review — and nothing else.

Design rules (Friedman):

* Every phase gets a deliberate presentation for blank, loading,
  partial, error, and ideal states. A bare ``> `` prompt is the
  single failure mode to avoid; the user should always know where
  they are, what decision they're being asked for, and how to back
  out.
* No dead-end states. After every major decision the user sees
  a review screen with explicit next actions: continue, re-pick
  connectors, re-enter keys, cancel. Cancel always exits 0 with
  "nothing was written".
* API keys are never echoed and never accepted via argv — there
  is no ``--api-key`` flag for exactly this reason (Hunt P3 — no
  secrets on the command line, not even in argv).
* TTY is a precondition for interactive use. If ``stdin`` is not
  a TTY and the flags don't specify a complete scripted run, we
  refuse fast with the exact command-line recipe the caller
  should use.
"""

from __future__ import annotations

import getpass
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import IO, Protocol

from parsimony_mcp.cli.registry_schema import ConnectorPackage, EnvVar, Registry


class PromptAborted(Exception):
    """The user cancelled the interactive flow.

    Raised when the user picks ``cancel`` from a review screen. The
    caller maps this to exit code 1 (USER_CANCEL) so the shell can
    distinguish "the user said no" from "something broke".
    """


# Tags we promote to top-level menu groups. Everything else falls
# under "Other". Keep this small and stable — it's a UX decision,
# not a taxonomy, and the default rendering should fit on one
# screen.
_TAG_GROUP_ORDER: tuple[str, ...] = ("macro", "markets", "crypto", "regulatory", "other")

# The connectors pre-selected on a blank-state run. Narrow by design:
# each one demonstrates a different data shape and at least one
# works without an API key (CoinGecko free tier), so the user can
# exercise the tool surface before committing to sign-ups.
RECOMMENDED_STARTER_SET: tuple[str, ...] = (
    "parsimony-fred",
    "parsimony-sdmx",
    "parsimony-coingecko",
)


class PromptIO(Protocol):
    """The subset of stdin/stdout the prompt layer uses.

    Kept as a protocol so tests can feed scripted input through a
    deterministic fake instead of monkey-patching ``input`` /
    ``getpass`` module-globals.
    """

    def write(self, s: str) -> None: ...
    def readline(self) -> str: ...
    def getpass(self, prompt: str) -> str: ...
    def isatty(self) -> bool: ...


@dataclass
class TerminalIO:
    """Default :class:`PromptIO` bound to real stdin / stdout.

    Uses :func:`getpass.getpass` for echo-suppressed secret entry.
    The ``stream`` parameter lets the test suite redirect output
    without displacing ``sys.stdin`` (which getpass reads on POSIX).
    """

    stream: IO[str] = field(default_factory=lambda: sys.stdout)
    stdin: IO[str] = field(default_factory=lambda: sys.stdin)

    def write(self, s: str) -> None:
        self.stream.write(s)
        self.stream.flush()

    def readline(self) -> str:
        return self.stdin.readline()

    def getpass(self, prompt: str) -> str:
        # getpass's `stream` parameter is typed as TextIO; the IO[str]
        # protocol we accept is a superset, so cast through Any.
        from typing import Any, cast

        return getpass.getpass(prompt, stream=cast(Any, self.stream))

    def isatty(self) -> bool:
        return self.stdin.isatty()


@dataclass(frozen=True, slots=True)
class PromptChoices:
    """The under-specified fields the prompt layer fills in.

    Returned by :func:`collect`. The CLI layer merges these onto
    the argparse-derived :class:`~parsimony_mcp.cli.init.InitInputs`
    via ``dataclasses.replace``.
    """

    selected_packages: tuple[str, ...]
    env_values: dict[str, str]


class TTYUnavailable(Exception):
    """Interactive session requested but stdin is not a TTY.

    Raised when scripts drive the wizard without the flags required
    for a non-interactive run. The caller surfaces the exact recipe
    (``parsimony-mcp init --yes --with parsimony-fred``) rather
    than silently hanging on a readline that will never arrive.
    """


# --------------------------------------------------------------------- entry point


def collect(
    registry: Registry,
    *,
    initial_selection: Sequence[str] = (),
    show_keys: bool = False,
    io: PromptIO | None = None,
) -> PromptChoices:
    """Run the interactive flow and return the user's choices.

    ``initial_selection`` seeds the menu when the user supplied
    ``--with`` but no ``--yes``; an empty selection falls back to
    :data:`RECOMMENDED_STARTER_SET`.

    Raises :class:`TTYUnavailable` if stdin is not a TTY — never
    hangs on a readline that nobody is going to fulfil.
    Raises :class:`PromptAborted` if the user picks ``cancel``.
    """
    io_ = io or TerminalIO()
    if not io_.isatty():
        raise TTYUnavailable(
            "stdin is not a TTY. For scripted use, pass "
            "`--yes --with parsimony-<name>` (and repeat `--with` for each "
            "additional connector). Run `parsimony-mcp init --help` for the full list."
        )

    _write_intro(io_)

    preselect: tuple[str, ...]
    if initial_selection:
        preselect = tuple(initial_selection)
    else:
        declared = {c.package for c in registry.connectors}
        preselect = tuple(p for p in RECOMMENDED_STARTER_SET if p in declared)

    while True:
        selected = _pick_connectors(io_, registry, preselect=preselect)
        if not selected:
            io_.write("\nNo connectors selected. Exiting. Nothing was written.\n")
            raise PromptAborted("user selected zero connectors")

        env_values = _collect_env_values(io_, registry, selected, show_keys=show_keys)

        action = _review(io_, registry, selected, env_values)
        if action == "continue":
            return PromptChoices(
                selected_packages=tuple(selected), env_values=env_values
            )
        if action == "repick":
            preselect = tuple(selected)  # keep the user's choices as the new starting point
            continue
        if action == "rekeys":
            env_values = _collect_env_values(io_, registry, selected, show_keys=show_keys)
            # Loop back to review — the user may cycle between
            # re-picking and re-entering keys as many times as they need.
            preselect = tuple(selected)
            continue
        # action == "cancel"
        io_.write("\nCancelled. Nothing was written.\n")
        raise PromptAborted("user cancelled")


# --------------------------------------------------------------------- intro


def _write_intro(io: PromptIO) -> None:
    io.write(
        "\n"
        "parsimony-mcp init — Step 1 of 3: pick connectors\n"
        "-------------------------------------------------\n"
        "Choose which data sources you want available to your coding agent.\n"
        "You can change this later by re-running the wizard.\n\n"
    )


# --------------------------------------------------------------------- connector menu


def _pick_connectors(
    io: PromptIO,
    registry: Registry,
    *,
    preselect: tuple[str, ...],
) -> list[str]:
    """Show the grouped menu and return the user's final selection.

    The menu is driven by the registry entries, so adding a connector
    upstream shows up here on the next registry refresh with no
    code change required (Dodds P2 — derivable state).
    """
    groups = _group_connectors(registry.connectors)
    index: list[ConnectorPackage] = []
    for _, items in groups:
        index.extend(items)

    selected: set[str] = set(preselect)

    while True:
        io.write("Available connectors:\n")
        pos = 1
        for group_name, items in groups:
            io.write(f"\n  [{group_name}]\n")
            for pkg in items:
                mark = "x" if pkg.package in selected else " "
                key_flag = _required_key_label(pkg)
                io.write(
                    f"   {pos:>2}. [{mark}] {pkg.display:<14} — {pkg.summary}"
                    f"{key_flag}\n"
                )
                pos += 1

        io.write(
            "\nActions: number(s) to toggle, 'a' all, 'n' none, 'r' recommended, "
            "Enter to continue, 'q' to cancel.\n> "
        )
        raw = io.readline().strip()

        if raw == "":
            return [pkg.package for pkg in index if pkg.package in selected]
        if raw == "q":
            raise PromptAborted("user pressed q at connector menu")
        if raw == "a":
            selected = {pkg.package for pkg in index}
            continue
        if raw == "n":
            selected = set()
            continue
        if raw == "r":
            declared = {pkg.package for pkg in index}
            selected = {p for p in RECOMMENDED_STARTER_SET if p in declared}
            continue

        toggled = _parse_toggle_indices(raw)
        if toggled is None:
            io.write(f"  (didn't understand {raw!r} — enter numbers, 'a', 'n', 'r', or Enter)\n")
            continue
        for idx in toggled:
            if not 1 <= idx <= len(index):
                io.write(f"  (index {idx} is out of range 1..{len(index)})\n")
                continue
            pkg = index[idx - 1]
            if pkg.package in selected:
                selected.remove(pkg.package)
            else:
                selected.add(pkg.package)


def _required_key_label(pkg: ConnectorPackage) -> str:
    if not pkg.env_vars:
        return ""
    required = any(v.required for v in pkg.env_vars)
    return "  [required key]" if required else "  [optional key]"


def _group_connectors(
    connectors: Iterable[ConnectorPackage],
) -> list[tuple[str, list[ConnectorPackage]]]:
    """Return ``[(group_name, items)]`` in :data:`_TAG_GROUP_ORDER`.

    A connector belongs to the first group in the order that
    matches one of its tags; anything unmatched goes in "other".
    Items within a group are ordered by ``display``.
    """
    buckets: dict[str, list[ConnectorPackage]] = {g: [] for g in _TAG_GROUP_ORDER}
    for pkg in connectors:
        placed = False
        for tag in _TAG_GROUP_ORDER[:-1]:  # skip "other" until fallback
            if tag in pkg.tags:
                buckets[tag].append(pkg)
                placed = True
                break
        if not placed:
            buckets["other"].append(pkg)
    for group in buckets.values():
        group.sort(key=lambda p: p.display.lower())
    return [(g, buckets[g]) for g in _TAG_GROUP_ORDER if buckets[g]]


def _parse_toggle_indices(raw: str) -> list[int] | None:
    """Parse ``"1 3 5"`` / ``"1,3,5"`` / ``"2"`` into indices, or ``None``.

    Returns ``None`` when ``raw`` doesn't look like a selection
    expression; the caller reprompts rather than silently doing
    nothing.
    """
    parts = raw.replace(",", " ").split()
    if not parts:
        return None
    out: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        out.append(int(part))
    return out


# --------------------------------------------------------------------- API-key entry


def _collect_env_values(
    io: PromptIO,
    registry: Registry,
    selected_packages: Sequence[str],
    *,
    show_keys: bool,
) -> dict[str, str]:
    """Prompt for each declared env var across ``selected_packages``.

    Required keys get a signup URL if the registry carries one.
    Optional keys are clearly labelled — users who are happy with a
    free tier can leave them blank. Keys are read with
    :func:`getpass.getpass` unless ``show_keys`` is True.
    """
    by_pkg = {c.package: c for c in registry.connectors}
    keys_seen: set[str] = set()

    values: dict[str, str] = {}
    io.write(
        "\nStep 2 of 3: API keys\n"
        "---------------------\n"
        "Keys are not echoed. Leave blank to skip optional keys.\n\n"
    )

    any_required = False
    for pkg_name in selected_packages:
        pkg = by_pkg.get(pkg_name)
        if pkg is None:
            continue
        for env in pkg.env_vars:
            if env.name in keys_seen:
                continue
            keys_seen.add(env.name)
            label = _env_label(env, required=env.required)
            if env.required:
                any_required = True

            if show_keys:
                io.write(label)
                captured = io.readline().rstrip("\n")
            else:
                captured = io.getpass(label)

            captured = captured.strip()
            if captured:
                values[env.name] = captured
                _confirm_key(io, env.name, captured)

    if not any_required:
        io.write("  (no required keys for the selected connectors)\n")
    return values


def _env_label(env: EnvVar, *, required: bool) -> str:
    """Render ``ENV_VAR [required — https://...]:`` or ``[optional]``."""
    if required:
        if env.get_url:
            return f"{env.name} [required — get one at {env.get_url}]: "
        return f"{env.name} [required]: "
    return f"{env.name} [optional — free tier works without]: "


def _confirm_key(io: PromptIO, name: str, value: str) -> None:
    """Post-paste safety toggle: show the last 4 chars on demand.

    Users frequently mispaste keys (extra whitespace, missing prefix).
    The prompt is opt-in so the default path doesn't leak anything.
    """
    io.write(f"  {name} captured. Show last 4 chars to verify? [y/N]: ")
    answer = io.readline().strip().lower()
    if answer == "y":
        tail = value[-4:] if len(value) >= 4 else "(too short)"
        io.write(f"  ends in: ...{tail}\n")


# --------------------------------------------------------------------- review / back-nav


def _review(
    io: PromptIO,
    registry: Registry,
    selected: Sequence[str],
    env_values: dict[str, str],
) -> str:
    """Show the review screen and return the chosen next action.

    Returns one of ``"continue"``, ``"repick"``, ``"rekeys"``,
    ``"cancel"``. Any other input reprompts without changing state
    (no dead-end state — the user can always reach a known action).
    """
    by_pkg = {c.package: c for c in registry.connectors}
    io.write(
        "\nStep 3 of 3: review\n"
        "-------------------\n"
        "Selected connectors:\n"
    )
    for pkg_name in selected:
        pkg = by_pkg.get(pkg_name)
        if pkg is None:
            io.write(f"  - {pkg_name}  (unknown — will fail planner)\n")
            continue
        io.write(f"  - {pkg.display} ({pkg.package})\n")

    if env_values:
        io.write(f"\nCaptured {len(env_values)} key(s): {', '.join(sorted(env_values))}\n")
    else:
        io.write("\nNo keys captured (you can fill .env yourself later).\n")

    while True:
        io.write(
            "\nWhat next? (c)ontinue, re-pick (p), re-enter (k)eys, (x) cancel: "
        )
        raw = io.readline().strip().lower()
        if raw in ("c", "continue", ""):
            return "continue"
        if raw in ("p", "repick", "pick"):
            return "repick"
        if raw in ("k", "keys", "rekeys"):
            return "rekeys"
        if raw in ("x", "cancel", "q"):
            return "cancel"
        io.write(f"  (didn't understand {raw!r})\n")
