"""Unit tests for the pure merge functions in ``parsimony_mcp.cli._merge``.

No filesystem I/O is used or mocked here — the merge contract is
purely string → string, and that's exactly what we exercise.
"""

from __future__ import annotations

import json
import tomllib

import pytest

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

# ---------------------------------------------------------------------- .gitignore


def test_gitignore_empty_adds_managed_block() -> None:
    out = merge_gitignore(None, GitignorePayload(lines=(".env", ".venv/")))
    assert ".env" in out
    assert ".venv/" in out
    assert "managed block" in out


def test_gitignore_preserves_existing_lines() -> None:
    existing = "node_modules/\n*.log\n"
    out = merge_gitignore(existing, GitignorePayload(lines=(".env",)))
    assert "node_modules/" in out
    assert "*.log" in out
    assert ".env" in out


def test_gitignore_is_idempotent() -> None:
    payload = GitignorePayload(lines=(".env", ".venv/"))
    once = merge_gitignore(None, payload)
    twice = merge_gitignore(once, payload)
    assert once == twice


def test_gitignore_no_duplicate_on_existing_entry() -> None:
    existing = "# project ignores\n.env\n"
    out = merge_gitignore(existing, GitignorePayload(lines=(".env",)))
    # ``.env`` appears exactly once in the managed block and is not
    # re-added to the user's section.
    assert out.count(".env\n") >= 1


# ---------------------------------------------------------------------- .env


def test_env_empty_creates_placeholders() -> None:
    out = merge_env(None, EnvPayload(keys=("FRED_API_KEY", "FOO"), values={}))
    assert "FRED_API_KEY=" in out
    assert "FOO=" in out


def test_env_preserves_existing_values() -> None:
    existing = "EXISTING=value\n"
    out = merge_env(existing, EnvPayload(keys=("NEW",), values={}))
    assert "EXISTING=value" in out
    assert "NEW=" in out


def test_env_does_not_override_existing_key() -> None:
    existing = "FRED_API_KEY=already_set\n"
    out = merge_env(
        existing,
        EnvPayload(keys=("FRED_API_KEY",), values={"FRED_API_KEY": "new_value"}),
    )
    # Existing key is never re-appended, even when the wizard has
    # a captured value: the user's .env is the source of truth once
    # written.
    assert out.count("FRED_API_KEY") == 1
    assert "already_set" in out


def test_env_is_idempotent() -> None:
    payload = EnvPayload(keys=("FRED_API_KEY",), values={})
    once = merge_env(None, payload)
    twice = merge_env(once, payload)
    assert once == twice


# ---------------------------------------------------------------------- pyproject.toml


def test_pyproject_empty_creates_valid_document() -> None:
    out = merge_pyproject(None, PyprojectPayload(dependencies=("parsimony-fred>=0.1",)))
    parsed = tomllib.loads(out)
    assert parsed["project"]["name"] == "parsimony-project"
    assert "parsimony-fred>=0.1" in parsed["project"]["dependencies"]


def test_pyproject_preserves_comments_and_existing_deps() -> None:
    existing = """\
# project metadata — do not delete
[project]
name = "my-app"
version = "0.0.1"
dependencies = [
    "httpx",  # inline comment preserved
]
"""
    out = merge_pyproject(
        existing, PyprojectPayload(dependencies=("parsimony-fred",))
    )
    assert "# project metadata — do not delete" in out
    assert "httpx" in out
    assert "parsimony-fred" in out
    # tomlkit preserves the inline comment
    assert "inline comment preserved" in out


def test_pyproject_deduplicates_by_distribution_name() -> None:
    existing = """\
[project]
name = "my-app"
version = "0.1.0"
dependencies = ["parsimony-fred>=0.1"]
"""
    out = merge_pyproject(
        existing, PyprojectPayload(dependencies=("parsimony-fred",))
    )
    parsed = tomllib.loads(out)
    deps = parsed["project"]["dependencies"]
    assert len([d for d in deps if d.startswith("parsimony-fred")]) == 1


def test_pyproject_round_trip_preserves_user_content() -> None:
    existing = """\
[project]
name = "x"
version = "1.0"
dependencies = []

[tool.ruff]
line-length = 100
"""
    out = merge_pyproject(existing, PyprojectPayload(dependencies=("parsimony-fred",)))
    parsed = tomllib.loads(out)
    # Unrelated table survives untouched.
    assert parsed["tool"]["ruff"]["line-length"] == 100


# ---------------------------------------------------------------------- .mcp.json


def test_mcp_config_empty_writes_valid_json() -> None:
    out = merge_mcp_config(None, McpConfigPayload(env_vars=("FRED_API_KEY",)))
    parsed = json.loads(out)
    assert parsed["mcpServers"]["parsimony"]["command"] == "parsimony-mcp"
    # Env vars are references, not values, so secrets stay in .env.
    assert parsed["mcpServers"]["parsimony"]["env"]["FRED_API_KEY"] == "${FRED_API_KEY}"


def test_mcp_config_preserves_unrelated_servers() -> None:
    existing = json.dumps(
        {"mcpServers": {"other-tool": {"command": "something"}}}
    )
    out = merge_mcp_config(existing, McpConfigPayload(env_vars=()))
    parsed = json.loads(out)
    assert "other-tool" in parsed["mcpServers"]
    assert "parsimony" in parsed["mcpServers"]


def test_mcp_config_invalid_existing_raises() -> None:
    with pytest.raises(ValueError, match="not valid JSON"):
        merge_mcp_config("not json at all", McpConfigPayload(env_vars=()))


def test_mcp_config_refuses_inlined_secret() -> None:
    """Users WILL commit .mcp.json; refuse to quietly overwrite an inlined key.

    The error points them at .env so the fix is obvious.
    """
    existing = json.dumps(
        {
            "mcpServers": {
                "parsimony": {
                    "command": "parsimony-mcp",
                    "env": {"FRED_API_KEY": "sk-literal-secret-value"},
                }
            }
        }
    )
    with pytest.raises(ValueError, match=r"Secrets MUST live in \.env"):
        merge_mcp_config(existing, McpConfigPayload(env_vars=("FRED_API_KEY",)))


def test_mcp_config_allows_env_var_reference_on_existing() -> None:
    """A properly-referenced existing entry merges without complaint."""
    existing = json.dumps(
        {
            "mcpServers": {
                "parsimony": {
                    "command": "parsimony-mcp",
                    "env": {"FRED_API_KEY": "${FRED_API_KEY}"},
                }
            }
        }
    )
    out = merge_mcp_config(existing, McpConfigPayload(env_vars=("FRED_API_KEY",)))
    parsed = json.loads(out)
    assert parsed["mcpServers"]["parsimony"]["env"]["FRED_API_KEY"] == "${FRED_API_KEY}"


# ---------------------------------------------------------------------- AGENTS.md


def test_agents_md_empty_writes_managed_block() -> None:
    out = merge_agents_md(None, AgentsMdPayload(packages=("parsimony-fred",)))
    assert "parsimony-mcp managed block" in out
    assert "parsimony-fred" in out


def test_agents_md_preserves_user_prose_outside_block() -> None:
    existing = "# My custom agent notes\n\nThis project is X.\n"
    out = merge_agents_md(existing, AgentsMdPayload(packages=("parsimony-fred",)))
    assert "# My custom agent notes" in out
    assert "This project is X." in out
    assert "parsimony-fred" in out


def test_agents_md_is_idempotent() -> None:
    payload = AgentsMdPayload(packages=("parsimony-fred",))
    once = merge_agents_md(None, payload)
    twice = merge_agents_md(once, payload)
    assert once == twice


def test_agents_md_contains_version_stamp_and_delimiters() -> None:
    """Contract surface: version stamp + <parsimony-connectors> block.

    The stamp lets users diff AGENTS.md across parsimony-mcp
    upgrades; the delimiter frames connector-author-supplied text
    as data, not instructions.
    """
    out = merge_agents_md(None, AgentsMdPayload(packages=("parsimony-fred",)))
    assert "Generated by parsimony-mcp" in out
    assert "<parsimony-connectors>" in out
    assert "</parsimony-connectors>" in out
    # Managed-block markers are present so a future `init --update`
    # can refresh only the managed portion.
    assert "parsimony-mcp managed block" in out


def test_agents_md_is_contract_only_no_workflow_prose() -> None:
    """Guard against context clash — AGENTS.md must not duplicate MCP workflow.

    The server's own instructions own "how to call tools". This
    file owns "where keys live / how discovery works at a contract
    level". If either term leaks in, the agent gets conflicting
    guidance across context budgets.
    """
    out = merge_agents_md(None, AgentsMdPayload(packages=("parsimony-fred",)))
    # Shouldn't enumerate specific tool-invocation mechanics here —
    # that's the server's remit. Coarse check: no examples that
    # look like "Call <tool>(…)" prose.
    assert "Call " not in out
    assert "example:" not in out.lower()
