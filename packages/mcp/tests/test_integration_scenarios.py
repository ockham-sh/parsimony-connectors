"""Golden-directory integration tests — five scenarios end-to-end.

Each scenario seeds a project tree, runs ``parsimony-mcp init``
against it, and asserts the shape of the result. Structural
assertions (file exists, contains marker, backup present) beat
byte-exact snapshots because version stamps and ISO-8601 backup
suffixes would dominate the diff.

Scenarios (from the plan):

a) empty dir + ``init --with parsimony-fred``
b) existing pyproject + ``init --into .``
c) existing ``.mcp.json`` with other servers + merge
d) ``init --force`` with backup skipped
e) non-TTY ``init --yes --with ...``
"""

from __future__ import annotations

import io
import json
import tomllib
from pathlib import Path
from typing import Any

import httpx
import pytest

from parsimony_mcp.cli import init as cli_init
from parsimony_mcp.cli.init import ExitCode, run
from parsimony_mcp.cli.registry import RegistrySource
from parsimony_mcp.cli.registry_schema import ConnectorPackage, EnvVar, Registry

# A deterministic DNS bypass and registry fixture shared across scenarios.


@pytest.fixture(autouse=True)
def _bypass_real_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "parsimony_mcp.cli.registry.socket.getaddrinfo",
        lambda host, port, *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 0))],
    )


def _registry() -> Registry:
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


@pytest.fixture
def patch_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Short-circuit the registry fetch with a known Registry."""

    def fake_fetch(**kwargs: Any) -> tuple[Registry, RegistrySource]:
        return _registry(), RegistrySource(
            origin="cache-fresh",
            url=kwargs.get("url", "https://test.example"),
            cache_path=kwargs.get("cache_path"),
            cache_age_seconds=3600.0,
        )

    monkeypatch.setattr(cli_init, "fetch_registry", fake_fetch)


def _run(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    rc = run(argv, stdout=out, stderr=err)
    return rc, out.getvalue(), err.getvalue()


# ---------------------------------------------------------------------- (a) empty dir


def test_scenario_empty_dir_fresh_init(patch_fetch: None, tmp_path: Path) -> None:
    rc, out, _ = _run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"]
    )
    assert rc == ExitCode.OK
    # All five wizard-owned files land.
    for name in (".gitignore", ".env", "pyproject.toml", ".mcp.json", "AGENTS.md"):
        assert (tmp_path / name).is_file(), f"{name} missing"
    # No staging dir leaked.
    assert not (tmp_path / ".parsimony-init-staging").exists()
    # Summary prints freshness + registry URL.
    assert "registry:" in out
    assert "cache (fresh)" in out


# ---------------------------------------------------------------------- (b) existing pyproject


def test_scenario_existing_pyproject_merge(patch_fetch: None, tmp_path: Path) -> None:
    existing = """\
# user annotations preserved through merge
[project]
name = "user-project"
version = "0.1.0"
dependencies = [
    "httpx",  # already here
]

[tool.ruff]
line-length = 100
"""
    (tmp_path / "pyproject.toml").write_text(existing)

    rc, _, _ = _run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"]
    )
    assert rc == ExitCode.OK

    # The user's metadata survived.
    merged = (tmp_path / "pyproject.toml").read_text()
    assert "# user annotations preserved through merge" in merged
    assert "[tool.ruff]" in merged
    data = tomllib.loads(merged)
    assert data["project"]["name"] == "user-project"
    # The wizard's dep was added without duplicating httpx.
    assert "httpx" in data["project"]["dependencies"]
    assert "parsimony-fred" in data["project"]["dependencies"]

    # The pre-existing file was backed up.
    backups = list(tmp_path.glob("pyproject.toml.parsimony.bak-*"))
    assert len(backups) == 1
    assert backups[0].read_text() == existing


# ---------------------------------------------------------------------- (c) existing .mcp.json with other servers


def test_scenario_existing_mcp_json_preserves_other_servers(
    patch_fetch: None, tmp_path: Path
) -> None:
    existing = json.dumps(
        {
            "mcpServers": {
                "some-other-tool": {
                    "command": "other-tool",
                    "args": ["--flag"],
                }
            }
        },
        indent=2,
    )
    (tmp_path / ".mcp.json").write_text(existing)

    rc, _, _ = _run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"]
    )
    assert rc == ExitCode.OK

    merged = json.loads((tmp_path / ".mcp.json").read_text())
    assert "some-other-tool" in merged["mcpServers"]
    assert merged["mcpServers"]["some-other-tool"]["command"] == "other-tool"
    # The parsimony entry uses env-var references, never values.
    assert merged["mcpServers"]["parsimony"]["env"]["FRED_API_KEY"] == "${FRED_API_KEY}"


# ---------------------------------------------------------------------- (d) --force skips backup


def test_scenario_force_skips_backup(patch_fetch: None, tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = \"x\"\nversion = \"0\"\n")
    rc, _, _ = _run(
        [
            "--yes",
            "--with", "parsimony-fred",
            "--into", str(tmp_path),
            "--force",
            "--skip-install",
        ]
    )
    assert rc == ExitCode.OK
    # With --force, NO timestamped backup sibling.
    assert not list(tmp_path.glob("pyproject.toml.parsimony.bak-*"))


# ---------------------------------------------------------------------- (e) non-TTY scripted run


def test_scenario_non_tty_scripted_yes_succeeds(
    patch_fetch: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--yes + --with lets CI / cron drive the wizard without a TTY.

    We don't monkeypatch isatty because the whole point is that the
    ``--yes`` path never reaches the prompt layer's TTY check.
    """
    rc, _, _ = _run(
        ["--yes", "--with", "parsimony-fred", "--into", str(tmp_path), "--skip-install"]
    )
    assert rc == ExitCode.OK
    # Without --yes, the same invocation on a non-TTY would hit
    # ExitCode.USAGE (verified in test_cli_init.py).


# ---------------------------------------------------------------------- Registry schema drift contract


def test_registry_schema_roundtrip_contract(tmp_path: Path) -> None:
    """End-to-end contract: a registry dump survives the consumer pipeline.

    Mirrors the generator's output shape (as if produced by
    ``tools/gen_registry.py``) and feeds it through
    :func:`fetch_registry` via a MockTransport, then through
    :func:`plan`. A schema break on either side surfaces here as
    a validation failure rather than in a user's terminal.
    """
    from parsimony_mcp.cli.init import InitInputs, plan
    from parsimony_mcp.cli.registry import fetch_registry

    dumped = _registry().model_dump_json().encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=dumped)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    registry, source = fetch_registry(
        url="https://example.test/registry.json",
        cache_path=None,
        client=client,
    )
    assert source.origin == "network"
    inputs = InitInputs(
        target_dir=tmp_path,
        selected_packages=("parsimony-fred", "parsimony-sdmx"),
        assume_yes=True,
    )
    ops = plan(inputs, registry)
    assert len(ops) == 5
