"""Tests for the interactive prompt layer.

A fake :class:`PromptIO` feeds scripted input. That keeps the tests
deterministic and sidesteps the real ``getpass.getpass`` (which
opens ``/dev/tty`` on POSIX and cannot be driven by redirected
stdin).
"""

from __future__ import annotations

import pytest

from parsimony_mcp.cli._prompts import (
    PromptAborted,
    TTYUnavailable,
    collect,
)
from parsimony_mcp.cli.registry_schema import ConnectorPackage, EnvVar, Registry
from tests._prompt_helpers import ScriptedIO


def _registry() -> Registry:
    return Registry(
        schema_version=1,
        connectors=(
            ConnectorPackage(
                package="parsimony-fred",
                display="FRED",
                summary="Federal Reserve Economic Data",
                tags=("macro", "tool"),
                env_vars=(
                    EnvVar(name="FRED_API_KEY", required=True, get_url="https://x/key"),
                ),
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
                summary="CoinGecko crypto",
                tags=("crypto", "tool"),
                env_vars=(EnvVar(name="COINGECKO_API_KEY", required=False),),
            ),
        ),
    )


# ---------------------------------------------------------------------- TTY gate


def test_non_tty_raises_with_scripted_recipe() -> None:
    io = ScriptedIO(inputs=[], tty=False)
    with pytest.raises(TTYUnavailable, match=r"`--yes --with parsimony-<name>`"):
        collect(_registry(), io=io)


# ---------------------------------------------------------------------- happy path


def test_accepts_recommended_and_continues() -> None:
    # User presses Enter at the menu to keep the pre-selected set,
    # enters the FRED key, declines to verify last-4, skips the
    # optional CoinGecko key, presses Enter on review.
    io = ScriptedIO(
        inputs=[
            "",            # accept default connector selection
            "secret123",   # FRED_API_KEY (required)
            "n",           # don't show last 4
            "",            # skip optional CoinGecko key
            "c",           # continue on review
        ],
    )
    choices = collect(_registry(), io=io)
    assert set(choices.selected_packages) == {
        "parsimony-fred",
        "parsimony-sdmx",
        "parsimony-coingecko",
    }
    assert choices.env_values == {"FRED_API_KEY": "secret123"}


def test_initial_selection_overrides_recommended() -> None:
    io = ScriptedIO(
        inputs=[
            "",            # accept selection (= initial)
            "secret",      # FRED key
            "n",
            "c",
        ],
    )
    choices = collect(
        _registry(), initial_selection=("parsimony-fred",), io=io
    )
    assert choices.selected_packages == ("parsimony-fred",)


# ---------------------------------------------------------------------- menu actions


def test_toggle_by_index() -> None:
    # Group order is macro, markets, crypto, regulatory, other.
    # Within each group, connectors are sorted by display (case-insensitive).
    # So the menu renders:
    #   macro  — 1. FRED, 2. SDMX
    #   crypto — 3. CoinGecko
    # Toggle "1" drops FRED; the required FRED key prompt is skipped.
    io = ScriptedIO(
        inputs=[
            "1",     # toggle off FRED
            "",      # accept selection
            "",      # skip optional CoinGecko key
            "c",     # continue at review
        ],
    )
    choices = collect(_registry(), io=io)
    assert "parsimony-fred" not in choices.selected_packages
    assert "parsimony-sdmx" in choices.selected_packages
    assert "parsimony-coingecko" in choices.selected_packages


def test_cancel_raises_prompt_aborted() -> None:
    io = ScriptedIO(inputs=["q"])  # quit at menu
    with pytest.raises(PromptAborted):
        collect(_registry(), io=io)


def test_review_cancel_raises_prompt_aborted() -> None:
    io = ScriptedIO(
        inputs=[
            "",            # accept default selection
            "secret",      # FRED key
            "n",
            "",            # skip optional
            "x",           # cancel at review
        ],
    )
    with pytest.raises(PromptAborted):
        collect(_registry(), io=io)


def test_review_repick_loops_back_to_menu() -> None:
    io = ScriptedIO(
        inputs=[
            "",            # accept default selection
            "first_key",   # FRED key
            "n",
            "",            # skip optional
            "p",           # re-pick at review
            "",            # accept selection again
            "second_key",  # FRED key re-entered
            "n",
            "",            # skip optional
            "c",           # continue
        ],
    )
    choices = collect(_registry(), io=io)
    assert choices.env_values["FRED_API_KEY"] == "second_key"


# ---------------------------------------------------------------------- key flow


def test_confirm_key_last_four_when_requested() -> None:
    io = ScriptedIO(
        inputs=[
            "",             # accept default selection
            "abcd_1234",    # FRED
            "y",            # show last 4
            "",             # skip optional
            "c",            # continue
        ],
    )
    collect(_registry(), io=io)
    joined = "".join(io.output)
    assert "...1234" in joined


def test_optional_key_blank_stays_unset() -> None:
    io = ScriptedIO(
        inputs=[
            "",             # accept default
            "key",          # FRED
            "n",
            "",             # skip CoinGecko
            "c",
        ],
    )
    choices = collect(_registry(), io=io)
    assert "COINGECKO_API_KEY" not in choices.env_values


def test_show_keys_path_uses_readline_not_getpass() -> None:
    # With show_keys=True, the prompt layer uses readline instead of
    # getpass, which matters for tests because redirected stdin
    # bypasses the real getpass's tty open.
    io = ScriptedIO(
        inputs=[
            "",            # accept default
            "visible_key", # FRED via readline
            "n",
            "",            # skip optional
            "c",
        ],
    )
    choices = collect(_registry(), show_keys=True, io=io)
    assert choices.env_values["FRED_API_KEY"] == "visible_key"


# ---------------------------------------------------------------------- output shape


def test_intro_shows_stage_indicator_not_bare_prompt() -> None:
    io = ScriptedIO(inputs=["q"])
    with pytest.raises(PromptAborted):
        collect(_registry(), io=io)
    joined = "".join(io.output)
    assert "Step 1 of" in joined
    assert "pick connectors" in joined


def test_required_vs_optional_labels_distinguish() -> None:
    io = ScriptedIO(
        inputs=[
            "",           # accept default selection (FRED, SDMX, CoinGecko)
            "key",        # FRED (required)
            "n",
            "",           # skip CoinGecko (optional)
            "c",          # continue
        ],
    )
    collect(_registry(), show_keys=True, io=io)
    joined = "".join(io.output)
    assert "required" in joined
    assert "optional" in joined
    assert "https://x/key" in joined  # signup URL surfaced for the required key
