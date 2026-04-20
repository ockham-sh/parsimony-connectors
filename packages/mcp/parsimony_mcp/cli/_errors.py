"""Three-part error prose convention for the init wizard.

Every user-facing error the wizard emits follows the same shape:

    error: <what happened>
      DO NOT: <what not to try — usually "immediately retry">
      DO:     <one concrete command the user can run next>

Why: a bare "error: HTTP 500" leaves the user staring at a cursor.
Telling them what to do AND what not to do collapses the feedback
loop (Friedman — no dead-end states; Collina P1 — operational
errors are actionable or they aren't).

Separate from structured logging: logs for machines, prose for
humans. Internal tracebacks (``httpx.ConnectError``, pydantic
``ValidationError``) never land in the prose — they go to the
debug log at ``~/.cache/parsimony-mcp/logs/`` (alpha: stderr when
``PARSIMONY_MCP_LOG_LEVEL=DEBUG``).

The convention is deliberately terse. A wizard error that sprawls
over a screen is user-hostile; each field fits on one line.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UserError:
    """Structured three-part error prose.

    Rendered verbatim to stderr. The caller constructs the message
    where the failure is classified; this type is just the shape
    that contract promises to the user.
    """

    what: str
    do_not: str
    do: str

    def render(self) -> str:
        return (
            f"error: {self.what}\n"
            f"  DO NOT: {self.do_not}\n"
            f"  DO:     {self.do}\n"
        )


# --------------------------------------------------------------------- registry


def registry_dns_failure(url: str) -> UserError:
    return UserError(
        what=f"no network detected (DNS resolution failed for {url!r}).",
        do_not="immediately retry — your machine appears offline.",
        do=(
            "reconnect to a network, or run `parsimony-mcp init --no-cache --yes "
            "--with parsimony-<name>` once back online."
        ),
    )


def registry_upstream_unreachable(url: str, status: int | None) -> UserError:
    status_fragment = f" (HTTP {status})" if status else ""
    return UserError(
        what=f"registry URL unreachable{status_fragment}: {url}.",
        do_not="retry for several minutes — the upstream appears down.",
        do=(
            "pass `--registry <mirror-url>` to use an alternate host, "
            "or wait and re-run `parsimony-mcp init`."
        ),
    )


def registry_malformed(url: str, reason: str) -> UserError:
    return UserError(
        what=f"registry at {url} is malformed ({reason}).",
        do_not="accept this output — the file is not a valid registry.",
        do="file an issue against parsimony-connectors with the URL above.",
    )


def registry_schema_mismatch(url: str, client_version: int) -> UserError:
    return UserError(
        what=(
            f"registry at {url} advertises a newer schema than this "
            f"client understands (v{client_version})."
        ),
        do_not="downgrade the registry.",
        do="upgrade `parsimony-mcp` (`uv tool upgrade parsimony-mcp`) and re-run init.",
    )


# --------------------------------------------------------------------- uv sync


def uv_missing() -> UserError:
    return UserError(
        what="`uv` is not on PATH.",
        do_not="alias `pip install`; we rely on uv's lockfile semantics.",
        do=(
            "install uv (https://docs.astral.sh/uv/), or re-run with "
            "`--skip-install` to finish without dependency resolution."
        ),
    )


def uv_compiler_missing() -> UserError:
    return UserError(
        what="a selected connector needs a C compiler that isn't installed.",
        do_not="retry without addressing the toolchain.",
        do=(
            "install a C toolchain (gcc / Xcode Command Line Tools / MSVC "
            "Build Tools), then run `uv sync` in this directory."
        ),
    )
