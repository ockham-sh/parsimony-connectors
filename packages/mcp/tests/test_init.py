"""Tests for parsimony_mcp.init — the slim scaffolder."""

from __future__ import annotations

import io
import stat
from pathlib import Path

import pytest

from parsimony_mcp.init import (
    ConnectorInfo,
    ExitCode,
    GitignoreMissingError,
    InitError,
    _is_env_gitignored,
    render_files,
    render_print_bundle,
    render_summary,
    run,
    write_files,
)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A scaffold-ready project directory: exists + .gitignore ignores .env."""
    project = tmp_path / "myproject"
    project.mkdir()
    (project / ".gitignore").write_text(".env\n")
    return project


@pytest.fixture
def two_connectors() -> list[ConnectorInfo]:
    return [
        ConnectorInfo(
            distribution="parsimony-fred",
            entry_point_name="parsimony_fred",
            env_vars=("FRED_API_KEY",),
            homepage="https://fred.stlouisfed.org",
        ),
        ConnectorInfo(
            distribution="parsimony-coingecko",
            entry_point_name="parsimony_coingecko",
            env_vars=("COINGECKO_API_KEY",),
            homepage="https://www.coingecko.com",
        ),
    ]


# --------------------------------------------------------------------- render


class TestRenderFiles:
    def test_returns_three_files(self, two_connectors: list[ConnectorInfo]) -> None:
        files = render_files(two_connectors)
        assert set(files) == {".mcp.json", ".env", "AGENTS.md"}

    def test_mcp_json_is_fixed_template(self, two_connectors: list[ConnectorInfo]) -> None:
        files = render_files(two_connectors)
        # Connector names must NOT appear in .mcp.json — its command/args
        # are fixed literals; interpolation here is a code-execution risk.
        assert "parsimony-fred" not in files[".mcp.json"]
        assert "parsimony-coingecko" not in files[".mcp.json"]
        assert '"command": "uv"' in files[".mcp.json"]
        assert '"--env-file"' in files[".mcp.json"]

    def test_agents_md_does_not_interpolate_plugin_metadata(
        self, two_connectors: list[ConnectorInfo]
    ) -> None:
        # Per Hunt + Willison: AGENTS.md is loaded into every agent
        # turn; any plugin string here is a prompt-injection vector.
        files = render_files(two_connectors)
        assert "parsimony-fred" not in files["AGENTS.md"]
        assert "FRED_API_KEY" not in files["AGENTS.md"]
        assert "fred.stlouisfed.org" not in files["AGENTS.md"]

    def test_agents_md_teaches_discover_fetch_handshake(
        self, two_connectors: list[ConnectorInfo]
    ) -> None:
        files = render_files(two_connectors)
        # The two-step handshake must appear verbatim.
        assert "discover" in files["AGENTS.md"].lower()
        assert "from parsimony import client" in files["AGENTS.md"]
        assert "do not invent connector names" in files["AGENTS.md"].lower()

    def test_agents_md_handles_truncation_directive(
        self, two_connectors: list[ConnectorInfo]
    ) -> None:
        files = render_files(two_connectors)
        assert "50-row" in files["AGENTS.md"] or "truncation" in files["AGENTS.md"]

    def test_agents_md_handles_do_not_retry_directive(
        self, two_connectors: list[ConnectorInfo]
    ) -> None:
        files = render_files(two_connectors)
        assert "DO NOT retry" in files["AGENTS.md"]

    def test_env_grouped_by_connector_with_homepage(
        self, two_connectors: list[ConnectorInfo]
    ) -> None:
        files = render_files(two_connectors)
        env_text = files[".env"]
        # Each group has a comment header naming the distribution and
        # the homepage URL where the key is obtained.
        assert "# parsimony-fred — https://fred.stlouisfed.org" in env_text
        assert "# parsimony-coingecko — https://www.coingecko.com" in env_text
        assert "FRED_API_KEY=" in env_text
        assert "COINGECKO_API_KEY=" in env_text

    def test_env_omits_connectors_with_no_env_vars(self) -> None:
        connectors = [
            ConnectorInfo(distribution="parsimony-foo", entry_point_name="foo", env_vars=()),
            ConnectorInfo(
                distribution="parsimony-bar", entry_point_name="bar", env_vars=("BAR_KEY",)
            ),
        ]
        env_text = render_files(connectors)[".env"]
        assert "parsimony-foo" not in env_text
        assert "parsimony-bar" in env_text

    def test_env_omits_failed_connectors(self) -> None:
        connectors = [
            ConnectorInfo(
                distribution="parsimony-broken",
                entry_point_name="broken",
                failed=True,
                failure_reason="ImportError",
            ),
            ConnectorInfo(
                distribution="parsimony-ok", entry_point_name="ok", env_vars=("OK_KEY",)
            ),
        ]
        env_text = render_files(connectors)[".env"]
        assert "parsimony-broken" not in env_text
        assert "parsimony-ok" in env_text

    def test_env_blank_state_when_no_connectors(self) -> None:
        env_text = render_files([])[".env"]
        assert "no connectors detected" in env_text
        assert "pip install parsimony-fred" in env_text

    def test_env_blank_state_when_only_failed_connectors(self) -> None:
        connectors = [
            ConnectorInfo(
                distribution="parsimony-broken",
                entry_point_name="broken",
                failed=True,
                failure_reason="ImportError",
            )
        ]
        env_text = render_files(connectors)[".env"]
        assert "no connectors detected" in env_text


# --------------------------------------------------------------------- write


class TestWriteFiles:
    def test_writes_three_files_into_clean_dir(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        files = render_files(two_connectors)
        result = write_files(files, project)
        assert (project / ".mcp.json").is_file()
        assert (project / ".env").is_file()
        assert (project / "AGENTS.md").is_file()
        assert len(result.written) == 3

    def test_env_is_mode_0600(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        write_files(render_files(two_connectors), project)
        st = (project / ".env").stat()
        # 0o077 is the "anyone but owner" bits; they must be unset.
        assert (st.st_mode & 0o077) == 0
        assert (st.st_mode & 0o600) == 0o600

    def test_other_files_are_mode_0644(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        write_files(render_files(two_connectors), project)
        for name in (".mcp.json", "AGENTS.md"):
            st = (project / name).stat()
            assert stat.S_IMODE(st.st_mode) == 0o644

    def test_refuses_to_overwrite_existing_files(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        (project / ".mcp.json").write_text("{}")
        with pytest.raises(InitError, match="already exist"):
            write_files(render_files(two_connectors), project)
        # The other files should NOT have been written either —
        # refusal is atomic.
        assert not (project / ".env").exists()
        assert not (project / "AGENTS.md").exists()

    def test_force_overwrites(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        (project / ".mcp.json").write_text("{}")
        result = write_files(render_files(two_connectors), project, force=True)
        assert (project / ".mcp.json").read_text().strip() != "{}"
        assert len(result.written) == 3

    def test_refuse_message_names_three_recovery_paths(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        (project / ".env").write_text("EXISTING=1\n")
        with pytest.raises(InitError) as excinfo:
            write_files(render_files(two_connectors), project)
        msg = str(excinfo.value)
        assert "--force" in msg
        assert "delete" in msg
        assert "--print" in msg

    def test_refuses_when_gitignore_does_not_ignore_env(
        self, tmp_path: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        project = tmp_path / "noguard"
        project.mkdir()
        # No .gitignore at all — the most common failure mode.
        with pytest.raises(GitignoreMissingError):
            write_files(render_files(two_connectors), project)
        assert not (project / ".env").exists()

    def test_refuses_when_gitignore_exists_but_does_not_ignore_env(
        self, tmp_path: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        project = tmp_path / "wrongignore"
        project.mkdir()
        (project / ".gitignore").write_text("__pycache__/\n*.pyc\n")
        with pytest.raises(GitignoreMissingError):
            write_files(render_files(two_connectors), project)

    def test_accepts_wildcard_env_pattern_in_gitignore(
        self, tmp_path: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        project = tmp_path / "wildcard"
        project.mkdir()
        (project / ".gitignore").write_text("*.env\n")
        write_files(render_files(two_connectors), project)
        assert (project / ".env").is_file()

    def test_refuses_to_write_through_symlink_at_env(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        # Create .env as a symlink to a file outside the project.
        target_outside = project.parent / "outside.txt"
        target_outside.write_text("original")
        (project / ".env").symlink_to(target_outside)
        with pytest.raises(InitError, match=r"symlink|already exist"):
            write_files(render_files(two_connectors), project, force=True)
        # The outside file must not have been clobbered.
        assert target_outside.read_text() == "original"


# --------------------------------------------------------------------- gitignore checker


class TestIsEnvGitignored:
    def test_no_gitignore_returns_false(self, tmp_path: Path) -> None:
        assert _is_env_gitignored(tmp_path) is False

    def test_explicit_env_line(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text(".env\n")
        assert _is_env_gitignored(tmp_path) is True

    def test_wildcard_env_line(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("*.env\n")
        assert _is_env_gitignored(tmp_path) is True

    def test_unrelated_lines_do_not_match(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("__pycache__/\n*.pyc\n")
        assert _is_env_gitignored(tmp_path) is False

    def test_comment_lines_skipped(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("# .env\n*.pyc\n")
        assert _is_env_gitignored(tmp_path) is False


# --------------------------------------------------------------------- summary / print


class TestRenderSummary:
    def test_lists_written_files(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        result = write_files(render_files(two_connectors), project)
        summary = render_summary(result, two_connectors)
        assert ".mcp.json" in summary
        assert ".env" in summary
        assert "AGENTS.md" in summary

    def test_lists_discovered_connectors(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        result = write_files(render_files(two_connectors), project)
        summary = render_summary(result, two_connectors)
        assert "parsimony-fred" in summary
        assert "parsimony-coingecko" in summary
        assert "Connectors discovered" in summary

    def test_lists_failed_connectors_under_skipped(self, project: Path) -> None:
        connectors = [
            ConnectorInfo(distribution="parsimony-ok", entry_point_name="ok", env_vars=("K",)),
            ConnectorInfo(
                distribution="parsimony-bad",
                entry_point_name="bad",
                failed=True,
                failure_reason="ImportError: no module named 'pandas'",
            ),
        ]
        result = write_files(render_files(connectors), project)
        summary = render_summary(result, connectors)
        assert "Skipped" in summary
        assert "parsimony-bad" in summary
        assert "ImportError" in summary

    def test_blank_state_summary_recommends_pip_install(self, project: Path) -> None:
        result = write_files(render_files([]), project)
        summary = render_summary(result, [])
        assert "0" in summary  # 0 connectors
        assert "pip install parsimony-fred" in summary

    def test_summary_ends_with_numbered_next_steps(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        result = write_files(render_files(two_connectors), project)
        summary = render_summary(result, two_connectors)
        # Friedman: next-action-led summary.
        assert "Next steps:" in summary
        assert "1. Open .env" in summary
        assert "2. Restart" in summary
        assert "3." in summary

    def test_dry_run_uses_would_write_verb(
        self, project: Path, two_connectors: list[ConnectorInfo]
    ) -> None:
        from parsimony_mcp.init import WriteResult

        result = WriteResult(
            target_dir=project, written=tuple(project / n for n in (".mcp.json", "AGENTS.md", ".env"))
        )
        summary = render_summary(result, two_connectors, dry_run=True)
        assert "would write" in summary
        assert "dry run" in summary


class TestRenderPrintBundle:
    def test_includes_file_separators(self, two_connectors: list[ConnectorInfo]) -> None:
        files = render_files(two_connectors)
        bundle = render_print_bundle(files)
        assert "# === FILE: .mcp.json ===" in bundle
        assert "# === FILE: AGENTS.md ===" in bundle
        assert "# === FILE: .env ===" in bundle


# --------------------------------------------------------------------- run() CLI integration


class TestRunCLI:
    def test_dry_run_writes_nothing(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: [])
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project), "--dry-run"], stdout=out, stderr=err)
        assert code == ExitCode.OK
        assert not (project / ".env").exists()
        assert "would write" in out.getvalue()

    def test_print_writes_to_stdout(
        self, project: Path, monkeypatch: pytest.MonkeyPatch, two_connectors: list[ConnectorInfo]
    ) -> None:
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: two_connectors)
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project), "--print"], stdout=out, stderr=err)
        assert code == ExitCode.OK
        assert not (project / ".env").exists()
        assert "# === FILE: .mcp.json ===" in out.getvalue()
        assert "FRED_API_KEY=" in out.getvalue()

    def test_normal_run_writes_files_and_exits_ok(
        self, project: Path, monkeypatch: pytest.MonkeyPatch, two_connectors: list[ConnectorInfo]
    ) -> None:
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: two_connectors)
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project)], stdout=out, stderr=err)
        assert code == ExitCode.OK
        assert (project / ".mcp.json").is_file()
        assert (project / ".env").is_file()
        assert (project / "AGENTS.md").is_file()
        assert "Connectors discovered" in out.getvalue()

    def test_existing_file_returns_usage_error(
        self, project: Path, monkeypatch: pytest.MonkeyPatch, two_connectors: list[ConnectorInfo]
    ) -> None:
        (project / ".env").write_text("X=1\n")
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: two_connectors)
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project)], stdout=out, stderr=err)
        assert code == ExitCode.USAGE_ERROR
        assert "--force" in err.getvalue()
        assert "--print" in err.getvalue()

    def test_missing_gitignore_returns_usage_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, two_connectors: list[ConnectorInfo]
    ) -> None:
        bare = tmp_path / "bare"
        bare.mkdir()
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: two_connectors)
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(bare)], stdout=out, stderr=err)
        assert code == ExitCode.USAGE_ERROR
        assert ".env" in err.getvalue()
        assert "gitignore" in err.getvalue().lower()

    def test_blank_state_writes_placeholder_env(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: [])
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project)], stdout=out, stderr=err)
        assert code == ExitCode.OK
        env_text = (project / ".env").read_text()
        assert "no connectors detected" in env_text

    def test_partial_state_writes_only_successful_connectors(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        connectors = [
            ConnectorInfo(distribution="parsimony-ok", entry_point_name="ok", env_vars=("OK",)),
            ConnectorInfo(
                distribution="parsimony-bad",
                entry_point_name="bad",
                failed=True,
                failure_reason="ImportError",
            ),
        ]
        monkeypatch.setattr("parsimony_mcp.init.discover_connectors", lambda: connectors)
        out = io.StringIO()
        err = io.StringIO()
        code = run(["--into", str(project)], stdout=out, stderr=err)
        assert code == ExitCode.OK
        env_text = (project / ".env").read_text()
        assert "OK=" in env_text
        assert "parsimony-bad" not in env_text
        assert "Skipped" in out.getvalue()
        assert "parsimony-bad" in out.getvalue()


# --------------------------------------------------------------------- discover (introspect_one)


class TestIntrospectOne:
    """The per-entry-point introspection captures success and failure."""

    def test_successful_module_yields_env_vars_and_homepage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sys
        import types

        from parsimony_mcp.init import _introspect_one

        # Build a fake plugin module and inject it.
        fake = types.ModuleType("fake_parsimony_plugin")
        fake.ENV_VARS = {"api_key": "FAKE_API_KEY"}  # type: ignore[attr-defined]
        fake.PROVIDER_METADATA = {  # type: ignore[attr-defined]
            "homepage": "https://example.com",
            "pricing": "free",
        }
        monkeypatch.setitem(sys.modules, "fake_parsimony_plugin", fake)

        ep = _make_entry_point("fake", "fake_parsimony_plugin", dist_name="parsimony-fake")
        info = _introspect_one(ep)
        assert info.failed is False
        assert info.distribution == "parsimony-fake"
        assert info.env_vars == ("FAKE_API_KEY",)
        assert info.homepage == "https://example.com"

    def test_import_error_yields_failed_with_reason(self) -> None:
        from parsimony_mcp.init import _introspect_one

        ep = _make_entry_point("missing", "definitely_not_a_real_module_xyz")
        info = _introspect_one(ep)
        assert info.failed is True
        assert info.failure_reason is not None
        assert "import failed" in info.failure_reason

    def test_non_dict_env_vars_yields_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import sys
        import types

        from parsimony_mcp.init import _introspect_one

        bad = types.ModuleType("bad_plugin")
        bad.ENV_VARS = ["NOT_A_DICT"]  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "bad_plugin", bad)

        ep = _make_entry_point("bad", "bad_plugin")
        info = _introspect_one(ep)
        assert info.failed is True
        assert info.failure_reason is not None
        assert "ENV_VARS must be dict" in info.failure_reason


def _make_entry_point(
    name: str, value: str, *, dist_name: str | None = None
) -> object:
    """Build a minimal entry-point-shaped object for testing."""
    import importlib.metadata as md

    ep = md.EntryPoint(name=name, value=value, group="parsimony.providers")
    if dist_name is not None:
        # EntryPoint.dist is read-only; wrap with a thin shim.
        class _Shim:
            def __init__(self, ep, dist_name):
                self._ep = ep
                self._dist_name = dist_name

            def __getattr__(self, attr):
                if attr == "dist":
                    return type("D", (), {"name": self._dist_name})()
                return getattr(self._ep, attr)

        return _Shim(ep, dist_name)
    return ep
