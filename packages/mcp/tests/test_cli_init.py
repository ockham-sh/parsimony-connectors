"""Tests for the init-wizard library core and the CLI adapter."""

from __future__ import annotations

import dataclasses
import io
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from parsimony_mcp.cli._merge import EnvPayload
from parsimony_mcp.cli.init import (
    RECOMMENDED_STARTER_SET,
    ExitCode,
    FileKind,
    FileOperation,
    InitInputs,
    build_parser,
    inputs_from_args,
    plan,
    run,
)
from parsimony_mcp.cli.registry_schema import ConnectorPackage, EnvVar, Registry


@pytest.fixture(autouse=True)
def _bypass_real_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "parsimony_mcp.cli.registry.socket.getaddrinfo",
        lambda host, port, *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 0))],
    )


def _sample_registry() -> Registry:
    return Registry(
        schema_version=1,
        connectors=(
            ConnectorPackage(
                package="parsimony-fred",
                display="FRED",
                summary="Federal Reserve Economic Data",
                tags=("macro", "tool"),
                env_vars=(EnvVar(name="FRED_API_KEY", required=True),),
            ),
            ConnectorPackage(
                package="parsimony-sdmx",
                display="SDMX",
                summary="SDMX multilateral data",
                tags=("macro", "tool"),
            ),
            ConnectorPackage(
                package="parsimony-coingecko",
                display="CoinGecko",
                summary="CoinGecko crypto data",
                tags=("crypto", "tool"),
            ),
        ),
    )


def _client_returning(registry: Registry | dict[str, Any]) -> httpx.Client:
    if isinstance(registry, Registry):
        body = registry.model_dump_json().encode("utf-8")
    else:
        body = json.dumps(registry).encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    return httpx.Client(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------- plan()


def test_plan_is_deterministic_and_ordered() -> None:
    inputs = InitInputs(
        target_dir=Path("proj"),
        selected_packages=("parsimony-fred", "parsimony-sdmx"),
        assume_yes=True,
    )
    registry = _sample_registry()

    first = plan(inputs, registry)
    second = plan(inputs, registry)

    # Identical inputs → identical output.
    assert first == second

    kinds = [op.kind for op in first]
    # Critical ordering invariant: .gitignore before .env.
    assert kinds.index(FileKind.GITIGNORE) < kinds.index(FileKind.ENV)
    # All five file kinds are present exactly once.
    assert set(kinds) == set(FileKind)
    assert len(kinds) == 5


def test_plan_rejects_unknown_package() -> None:
    inputs = InitInputs(
        target_dir=Path("proj"),
        selected_packages=("parsimony-does-not-exist",),
    )
    with pytest.raises(ValueError, match="not present in registry"):
        plan(inputs, _sample_registry())


def test_plan_collects_env_vars_from_selection() -> None:
    inputs = InitInputs(
        target_dir=Path("proj"),
        selected_packages=("parsimony-fred", "parsimony-sdmx"),
    )
    ops = plan(inputs, _sample_registry())
    env_op = next(op for op in ops if op.kind is FileKind.ENV)
    assert isinstance(env_op.incoming, EnvPayload)
    assert env_op.incoming.keys == ("FRED_API_KEY",)
    assert env_op.incoming.values == {}


def test_file_operation_is_frozen() -> None:
    op = FileOperation(
        kind=FileKind.ENV,
        target=Path("."),
        incoming=EnvPayload(keys=(), values={}),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        op.kind = FileKind.PYPROJECT  # type: ignore[misc]


# ---------------------------------------------------------------------- argparse adapter


def test_yes_without_with_picks_recommended_set() -> None:
    parser = build_parser()
    args = parser.parse_args(["--yes"])
    inputs = inputs_from_args(args, _sample_registry())
    assert inputs.selected_packages == RECOMMENDED_STARTER_SET
    assert inputs.assume_yes is True


def test_with_overrides_recommended() -> None:
    parser = build_parser()
    args = parser.parse_args(["--yes", "--with", "parsimony-fred"])
    inputs = inputs_from_args(args, _sample_registry())
    assert inputs.selected_packages == ("parsimony-fred",)


def test_custom_registry_disables_default_cache() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["--yes", "--with", "parsimony-fred", "--registry", "https://mirror.example/r.json"]
    )
    inputs = inputs_from_args(args, _sample_registry())
    assert inputs.cache_path is None


def test_no_cache_disables_default_cache() -> None:
    parser = build_parser()
    args = parser.parse_args(["--yes", "--with", "parsimony-fred", "--no-cache"])
    inputs = inputs_from_args(args, _sample_registry())
    assert inputs.cache_path is None


# ---------------------------------------------------------------------- --help snapshot (stability contract)

# The CLI flag surface is a documented stability contract for 0.1.x.
# These snapshots ensure that a drive-by PR cannot silently rename
# or delete a flag. Changes here are deliberate — update the snapshot
# AND bump the minor version in the same PR.

_EXPECTED_PUBLIC_HELP_FLAGS = {
    "--into",
    "--with",
    "--dry-run",
    "--yes",
    "--help-advanced",
    "-h",
    "--help",
}

_EXPECTED_ADVANCED_EXTRA_FLAGS = {
    "--registry",
    "--no-cache",
    "--show-keys",
    "--force",
    "--skip-install",
}


def _extract_flags(help_text: str) -> set[str]:
    """Return the set of flag strings mentioned in the help output."""
    flags = set()
    for raw in help_text.split():
        word = raw.rstrip(",").split("=")[0]
        if word.startswith("--") or word == "-h":
            flags.add(word)
    return flags


def test_public_help_flag_surface_stable() -> None:
    parser = build_parser(advanced=False)
    buf = io.StringIO()
    parser.print_help(buf)
    flags = _extract_flags(buf.getvalue())
    assert flags >= _EXPECTED_PUBLIC_HELP_FLAGS, (
        f"public --help is missing flags; drift: {_EXPECTED_PUBLIC_HELP_FLAGS - flags}"
    )
    # Advanced flags MUST NOT surface in public help (progressive disclosure).
    leaked = _EXPECTED_ADVANCED_EXTRA_FLAGS & flags
    assert not leaked, f"advanced flags leaked into public help: {leaked}"


def test_advanced_help_includes_expected_flags() -> None:
    parser = build_parser(advanced=True)
    buf = io.StringIO()
    parser.print_help(buf)
    flags = _extract_flags(buf.getvalue())
    assert flags >= _EXPECTED_PUBLIC_HELP_FLAGS | _EXPECTED_ADVANCED_EXTRA_FLAGS


# ---------------------------------------------------------------------- run() — CLI adapter end-to-end


def _patch_fetch(monkeypatch: pytest.MonkeyPatch, registry: Registry, origin: str = "network") -> None:
    """Bypass the real fetch path; return a fixed Registry + source."""
    from parsimony_mcp.cli.registry import RegistrySource

    def fake_fetch(**kwargs: Any) -> tuple[Registry, RegistrySource]:
        return registry, RegistrySource(
            origin=origin,
            url=kwargs.get("url", "https://example.test"),
            cache_path=kwargs.get("cache_path"),
            cache_age_seconds=None,
        )

    monkeypatch.setattr("parsimony_mcp.cli.init.fetch_registry", fake_fetch)


def test_run_dry_run_prints_plan_and_exits_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_fetch(monkeypatch, _sample_registry())
    out = io.StringIO()
    err = io.StringIO()

    rc = run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--dry-run"],
        stdout=out,
        stderr=err,
    )
    assert rc == ExitCode.OK
    text = out.getvalue()
    assert "dry-run" in text
    assert ".gitignore" in text
    assert ".env" in text
    # Ordering invariant is also visible in the dry-run output.
    assert text.index(".gitignore") < text.index(".env")


def test_run_without_yes_on_non_tty_emits_scripted_recipe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Default prompt_io under pytest sees sys.stdin which is not a TTY.

    The wizard must refuse with the exact scripted recipe rather
    than hang on an unfulfilled readline.
    """
    _patch_fetch(monkeypatch, _sample_registry())
    err = io.StringIO()
    rc = run(["--into", str(tmp_path)], stdout=io.StringIO(), stderr=err)
    assert rc == ExitCode.USAGE
    assert "--yes --with parsimony-" in err.getvalue()


def test_run_registry_error_maps_to_registry_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from parsimony_mcp.cli.registry import RegistryError

    def failing(**kwargs: Any) -> Any:
        raise RegistryError("network down")

    monkeypatch.setattr("parsimony_mcp.cli.init.fetch_registry", failing)
    err = io.StringIO()
    rc = run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"],
        stdout=io.StringIO(),
        stderr=err,
    )
    assert rc == ExitCode.REGISTRY
    assert "network down" in err.getvalue()


def test_run_unknown_package_maps_to_usage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_fetch(monkeypatch, _sample_registry())
    err = io.StringIO()
    rc = run(
        ["--yes", "--with", "parsimony-unknown", "--into", str(tmp_path)],
        stdout=io.StringIO(),
        stderr=err,
    )
    assert rc == ExitCode.USAGE
    assert "not present in registry" in err.getvalue()


def test_run_fresh_init_writes_all_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_fetch(monkeypatch, _sample_registry())
    out = io.StringIO()
    err = io.StringIO()
    rc = run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"],
        stdout=out,
        stderr=err,
    )
    assert rc == ExitCode.OK, err.getvalue()
    # All five wizard-owned files exist; the staging dir is gone.
    assert (tmp_path / ".gitignore").is_file()
    assert (tmp_path / ".env").is_file()
    assert (tmp_path / "pyproject.toml").is_file()
    assert (tmp_path / ".mcp.json").is_file()
    assert (tmp_path / "AGENTS.md").is_file()
    assert not (tmp_path / ".parsimony-init-staging").exists()
    # .gitignore mentions .env so a subsequent `git add` can't leak it.
    assert ".env" in (tmp_path / ".gitignore").read_text()


def test_run_interactive_fills_missing_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When neither --yes nor --with is given, run() drops into prompts."""
    from tests._prompt_helpers import ScriptedIO

    _patch_fetch(monkeypatch, _sample_registry())

    scripted = ScriptedIO(
        inputs=[
            "",        # accept default selection
            "secret",  # FRED_API_KEY (required)
            "n",       # don't show last 4
            "c",       # continue at review
        ],
    )
    rc = run(
        ["--into", str(tmp_path), "--skip-install"],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        prompt_io=scripted,
    )
    assert rc == ExitCode.OK
    assert (tmp_path / ".env").is_file()
    assert "FRED_API_KEY=secret" in (tmp_path / ".env").read_text()


def test_run_tty_unavailable_surfaces_scripted_recipe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tests._prompt_helpers import ScriptedIO

    _patch_fetch(monkeypatch, _sample_registry())
    err = io.StringIO()
    rc = run(
        ["--into", str(tmp_path)],
        stdout=io.StringIO(),
        stderr=err,
        prompt_io=ScriptedIO(inputs=[], tty=False),
    )
    assert rc == ExitCode.USAGE
    assert "--yes --with parsimony-" in err.getvalue()


def test_run_merge_mode_requires_yes_or_force(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_fetch(monkeypatch, _sample_registry())
    (tmp_path / "pyproject.toml").write_text("[project]\nname='existing'\n")

    err = io.StringIO()
    rc = run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"],
        stdout=io.StringIO(),
        stderr=err,
    )
    # --yes programmatically consents to merge + backup.
    assert rc == ExitCode.OK, err.getvalue()
    # Original pyproject.toml backed up with an ISO-8601 suffix.
    backups = list(tmp_path.glob("pyproject.toml.parsimony.bak-*"))
    assert len(backups) == 1


# ---------------------------------------------------------------------- __main__ dispatch


def test_main_dispatch_routes_init_to_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony_mcp import __main__ as entry

    called = {"argv": None}

    def fake_run(argv: Any) -> int:
        called["argv"] = argv
        return 0

    monkeypatch.setattr(entry.cli_init, "run", fake_run)
    rc = entry._dispatch(["init", "--yes", "--with", "parsimony-fred"])
    assert rc == 0
    assert called["argv"] == ["--yes", "--with", "parsimony-fred"]


def test_unknown_subcommand_falls_through_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony_mcp import __main__ as entry

    ran = {"server": False}

    async def fake_run_server() -> None:
        ran["server"] = True

    monkeypatch.setattr(entry, "_run_server", fake_run_server)
    # A bare call or an unknown first token should keep the legacy
    # stdio-server behaviour — critical for existing .mcp.json entries.
    rc = entry._dispatch(["--some-flag-the-server-ignores"])
    assert rc == 0
    assert ran["server"] is True
