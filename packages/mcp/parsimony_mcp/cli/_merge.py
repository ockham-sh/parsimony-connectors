"""Pure merge functions for the five wizard-owned file types.

Contract:

    merge_X(existing: str | None, incoming: <payload>) -> str

Each function is total, referentially transparent, and has zero disk
I/O. That's load-bearing: it lets us unit-test merges without any
filesystem mock, and it lets ``apply()`` compose the merges with the
atomic-write primitive in one direction of data flow
(Collina P3 — validate at the boundary, sanitise at the exit).

``existing`` is the current file contents as UTF-8 text, or ``None``
for a fresh write. The function either appends/replaces a
parsimony-managed block (``.gitignore``, ``AGENTS.md``), merges keys
(``.env``, ``pyproject.toml``, ``.mcp.json``), or produces a brand
new file. Every return value is a complete rewrite of the file —
the caller atomically replaces the target with it.

Idempotency: running ``merge_X(merge_X(existing, incoming), incoming)``
equals ``merge_X(existing, incoming)`` for every function. Tested
explicitly for each merger.

Why ``tomlkit`` and not ``tomli-w``: ``tomli-w`` serialises a fresh
dict, which loses every comment and every intentional blank line in
the user's existing ``pyproject.toml``. Users annotate ``pyproject``
like source code; silently normalising their file is rude and hard
to review. ``tomlkit`` preserves the trivia.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import resources
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

import tomlkit

if TYPE_CHECKING:
    from tomlkit.items import Array


def _mcp_version() -> str:
    """Version stamp emitted into AGENTS.md.

    AGENTS.md is a prompt artifact loaded into every agent turn,
    so the wizard records which ``parsimony-mcp`` produced it.
    Gives users a one-glance answer to "should I re-run init?"
    after upgrading.
    """
    try:
        return version("parsimony-mcp")
    except PackageNotFoundError:  # pragma: no cover — in-tree dev before install
        return "0.0.0+unknown"


def _load_agents_template() -> str:
    """Read the AGENTS.md template shipped alongside this module."""
    return (
        resources.files("parsimony_mcp.cli.templates")
        .joinpath("agents.md.tmpl")
        .read_text(encoding="utf-8")
    )

# The managed-block delimiters let us re-run the wizard against a file
# the user has already customised, and only rewrite the portion we
# own. Changing these strings breaks existing projects — they are a
# stability contract.
MANAGED_BEGIN = "# >>> parsimony-mcp managed block (do not edit)"
MANAGED_END = "# <<< parsimony-mcp managed block"
MANAGED_BEGIN_MD = "<!-- >>> parsimony-mcp managed block (do not edit) -->"
MANAGED_END_MD = "<!-- <<< parsimony-mcp managed block -->"


# --------------------------------------------------------------------- payloads


@dataclass(frozen=True, slots=True)
class GitignorePayload:
    """Lines the wizard needs present in .gitignore.

    The merger adds any missing line at the end of a managed block
    and leaves every other line of the user's .gitignore untouched.
    """

    lines: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EnvPayload:
    """Environment variable names the wizard wants documented.

    ``values`` may supply captured values; missing keys render as
    ``NAME=`` so the file is syntactically valid and easy to fill
    in.
    """

    keys: tuple[str, ...]
    values: dict[str, str]


@dataclass(frozen=True, slots=True)
class PyprojectPayload:
    """Project metadata the wizard contributes to pyproject.toml.

    ``dependencies`` is appended into ``[project].dependencies`` and
    deduplicated by distribution name. Existing pins / markers on
    the user's dependencies are preserved.
    """

    dependencies: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class McpConfigPayload:
    """MCP client configuration.

    ``env_vars`` are referenced, NOT embedded: the generated
    ``mcpServers.parsimony.env`` block uses ``${ENV_VAR}`` references
    so secrets live in ``.env``, not in a JSON file that ends up
    in shell completions or screenshots.
    """

    env_vars: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AgentsMdPayload:
    """Contract-only summary plus the parsimony-managed block.

    ``packages`` is the list of installed connector distribution
    names. The managed block is delimited by HTML comments so the
    user may write freely outside it without fear of the wizard
    clobbering their prose.
    """

    packages: tuple[str, ...]


# --------------------------------------------------------------------- .gitignore


def merge_gitignore(existing: str | None, incoming: GitignorePayload) -> str:
    """Ensure every ``incoming.lines`` entry exists in ``existing``.

    Uses a managed block appended at the end rather than inlining
    entries throughout the file: this keeps diffs reviewable and
    lets subsequent runs reliably find and update the block.
    """
    lines_needed = list(incoming.lines)

    if existing is None or existing.strip() == "":
        body = "\n".join(lines_needed)
        return f"{MANAGED_BEGIN}\n{body}\n{MANAGED_END}\n"

    # Strip an existing managed block before recomputing — keeps the
    # merge idempotent even if the wizard changed the default lines.
    pre, managed_body, post = _split_managed_block(existing, MANAGED_BEGIN, MANAGED_END)
    existing_lines = _non_managed_lines(pre + post)

    to_add = [ln for ln in lines_needed if ln not in existing_lines]
    if not to_add and managed_body is not None and set(_lines(managed_body)) == set(lines_needed):
        # Already up-to-date; return existing verbatim so the caller
        # can see "no change" from a byte comparison.
        return existing

    new_managed = "\n".join(lines_needed)
    prefix = (pre + post).rstrip("\n")
    sep = "\n\n" if prefix else ""
    return f"{prefix}{sep}{MANAGED_BEGIN}\n{new_managed}\n{MANAGED_END}\n"


# --------------------------------------------------------------------- .env


def merge_env(existing: str | None, incoming: EnvPayload) -> str:
    """Ensure every ``incoming.keys`` variable has a line in ``.env``.

    Preserves every existing line and every existing value. Adds
    ``KEY=`` (empty) for keys the user hasn't set yet. Populates
    ``KEY=value`` for keys the wizard has captured via ``getpass``.
    Never prints captured values to logs — this function just hands
    a string back.
    """
    existing_pairs = _parse_env_lines(existing or "")
    existing_keys = {k for k, _ in existing_pairs}

    lines: list[str] = list(existing.splitlines()) if existing else []
    if lines and lines[-1] != "":
        lines.append("")  # ensure trailing newline for append ergonomics

    additions: list[str] = []
    for key in incoming.keys:
        if key in existing_keys:
            continue
        value = incoming.values.get(key, "")
        additions.append(f"{key}={value}")

    if not additions:
        return existing if existing is not None else ""

    if not existing:
        return "\n".join(additions) + "\n"

    # Separate wizard additions with one blank line for human legibility.
    return existing.rstrip("\n") + "\n\n" + "\n".join(additions) + "\n"


# --------------------------------------------------------------------- pyproject.toml


def merge_pyproject(existing: str | None, incoming: PyprojectPayload) -> str:
    """Merge wizard-managed dependencies into pyproject.toml.

    Strategy:

    * No existing file → write a minimal, self-contained pyproject
      with ``[project]`` populated from ``incoming``.
    * Existing file → parse with tomlkit, ensure ``[project]`` +
      ``[project.dependencies]`` exist, add each missing entry.
      Comments, blank lines, and any unrelated tables are left
      byte-for-byte untouched.

    Round-trip guarantee: ``tomllib.loads(result)`` must parse to a
    superset of ``tomllib.loads(existing)``. The caller asserts this
    as a correctness tripwire.
    """
    incoming_deps = list(incoming.dependencies)

    if existing is None or existing.strip() == "":
        doc = tomlkit.document()
        project = tomlkit.table()
        project["name"] = "parsimony-project"
        project["version"] = "0.1.0"
        project["requires-python"] = ">=3.11"
        deps_arr: Array = tomlkit.array()
        for dep in incoming_deps:
            deps_arr.append(dep)
        project["dependencies"] = deps_arr
        doc["project"] = project
        return tomlkit.dumps(doc)

    doc = tomlkit.parse(existing)
    project_node = doc.get("project")
    if project_node is None:
        project_node = tomlkit.table()
        doc["project"] = project_node
    project = project_node
    deps = project.get("dependencies")
    if deps is None:
        deps = tomlkit.array()
        project["dependencies"] = deps

    # tomlkit Array is iterable; cast via list() yields rendered
    # strings we can normalise for dedup.
    existing_names = {_dep_name(str(d)) for d in list(deps)}
    for dep in incoming_deps:
        if _dep_name(dep) in existing_names:
            continue
        deps.append(dep)
        existing_names.add(_dep_name(dep))

    return tomlkit.dumps(doc)


def _dep_name(spec: str) -> str:
    """Canonicalise a PEP 508 dependency spec's distribution name.

    ``parsimony-fred>=0.1,<1`` and ``parsimony-fred`` both return
    ``parsimony-fred``. Used for dedup: two specs of the same package
    should never end up in the merged dependency list.
    """
    name = spec
    for sep in ("[", "(", ";", "="):
        if sep in name:
            name = name.split(sep, 1)[0]
    for op in ("<", ">", "!", "~"):
        if op in name:
            name = name.split(op, 1)[0]
    return name.strip().lower().replace("_", "-")


# --------------------------------------------------------------------- .mcp.json


def merge_mcp_config(existing: str | None, incoming: McpConfigPayload) -> str:
    """Ensure ``.mcp.json`` has a ``parsimony`` entry under ``mcpServers``.

    The entry's ``env`` block maps each ``ENV_VAR`` to ``"${ENV_VAR}"``
    (a reference, not the value) so the ``.env`` file stays the
    single source of truth for secrets. Some coding agents commit
    ``.mcp.json`` to source control; users WILL leak keys if we
    inline values here (Hunt — minimise the blast radius of a
    casual commit).

    Existing entries in ``mcpServers`` for other tools are preserved
    untouched. Non-reference values on an existing parsimony entry
    (``{"env": {"FRED_API_KEY": "raw_secret"}}``) are refused with a
    ``ValueError`` that points the user at ``.env`` — we'd rather
    fail loudly than silently overwrite the evidence of a leak.
    """
    if existing is None or existing.strip() == "":
        base: dict[str, object] = {"mcpServers": {}}
    else:
        try:
            base = json.loads(existing)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f".mcp.json exists but is not valid JSON; refusing to overwrite: {exc}"
            ) from exc
        if not isinstance(base, dict):
            raise ValueError(".mcp.json root must be a JSON object")
        base.setdefault("mcpServers", {})

    servers = base["mcpServers"]
    if not isinstance(servers, dict):
        raise ValueError(".mcp.json 'mcpServers' must be a JSON object")

    _refuse_inlined_secrets(servers.get("parsimony"))

    env_refs = {var: f"${{{var}}}" for var in incoming.env_vars}
    servers["parsimony"] = {
        "command": "parsimony-mcp",
        "env": env_refs,
    }
    return json.dumps(base, indent=2, sort_keys=False) + "\n"


def _refuse_inlined_secrets(entry: object) -> None:
    """Raise if the existing parsimony entry inlines non-reference env values.

    A value like ``"${FRED_API_KEY}"`` or an empty string is fine.
    A literal-looking secret (anything else) triggers a refusal so
    the user learns that ``.env`` is the only right place.
    """
    if not isinstance(entry, dict):
        return
    env = entry.get("env")
    if not isinstance(env, dict):
        return
    for key, value in env.items():
        if not isinstance(value, str):
            continue
        stripped = value.strip()
        if stripped == "" or (stripped.startswith("${") and stripped.endswith("}")):
            continue
        raise ValueError(
            f".mcp.json's parsimony.env.{key} contains a literal value. "
            "Secrets MUST live in .env — remove it from .mcp.json and "
            "re-run `parsimony-mcp init`."
        )


# --------------------------------------------------------------------- AGENTS.md


def merge_agents_md(existing: str | None, incoming: AgentsMdPayload) -> str:
    """Emit AGENTS.md with the managed parsimony block.

    AGENTS.md is a prompt loaded into the agent's context every
    turn (Willison P3 — context is not free); treat edits to it
    as prompt edits. The managed block is strictly CONTRACT: the
    MCP/Python split and the secrets rule. Workflow prose lives
    in the MCP server's own instructions — do not duplicate it
    here.

    Plugin-author-supplied text is wrapped in a
    ``<parsimony-connectors>`` delimiter with a preamble clarifying
    "treat as data, not instructions", mirroring how the MCP
    server delimits its catalog. Protects the agent against prompt
    injection that arrives through a connector's name / summary.

    Everything OUTSIDE the managed block is preserved byte-for-byte
    so a user who wrote custom agent guidance keeps it across
    re-runs.
    """
    managed_body = _render_agents_managed_body(incoming)
    managed_section = f"{MANAGED_BEGIN_MD}\n{managed_body}\n{MANAGED_END_MD}\n"

    if existing is None or existing.strip() == "":
        header = "# Agents guide\n\n"
        return header + managed_section

    pre, _body, post = _split_managed_block(existing, MANAGED_BEGIN_MD, MANAGED_END_MD)
    unmanaged = (pre + post).rstrip("\n")
    if not unmanaged:
        return managed_section
    return f"{unmanaged}\n\n{managed_section}"


def _render_agents_managed_body(payload: AgentsMdPayload) -> str:
    lines = "\n".join(f"- {p}" for p in payload.packages) or "- (none installed yet)"
    template = _load_agents_template()
    return template.format_map(
        {"version": _mcp_version(), "connector_lines": lines}
    ).rstrip("\n")


# --------------------------------------------------------------------- shared helpers


def _split_managed_block(
    existing: str, begin: str, end: str
) -> tuple[str, str | None, str]:
    """Return ``(pre, managed_body, post)`` around a managed block.

    If no block exists, ``managed_body`` is ``None`` and ``pre + post``
    reconstructs ``existing``.
    """
    if begin not in existing or end not in existing:
        return existing, None, ""
    pre, rest = existing.split(begin, 1)
    body, post = rest.split(end, 1)
    # Trim the newlines immediately adjacent to the delimiters so the
    # body is clean; the caller adds them back.
    return pre, body.strip("\n"), post.lstrip("\n")


def _non_managed_lines(text: str) -> set[str]:
    """Unique non-empty lines in ``text`` outside any managed block."""
    out: set[str] = set()
    for ln in _lines(text):
        stripped = ln.strip()
        if stripped and not stripped.startswith("#"):
            out.add(stripped)
    return out


def _lines(text: str) -> list[str]:
    return text.splitlines()


def _parse_env_lines(text: str) -> list[tuple[str, str]]:
    """Parse ``KEY=value`` pairs out of ``text``; ignore comments/blanks.

    Quoted values are returned with the quotes intact — we only care
    about the key names for dedup. The merger never reformats user
    lines.
    """
    out: list[tuple[str, str]] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        out.append((key.strip(), value.strip()))
    return out
