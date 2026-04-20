"""Shared helpers for prompt-driven tests.

Kept as a regular module (not a fixture) so importing ``ScriptedIO``
at module scope doesn't require pytest to wire a fixture context.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ScriptedIO:
    """A :class:`parsimony_mcp.cli._prompts.PromptIO` fake.

    ``inputs`` is consumed left-to-right by every ``readline()`` or
    ``getpass()`` call. Output goes to the ``output`` list for
    assertion; set ``tty=False`` to simulate redirected stdin.
    """

    inputs: list[str]
    output: list[str] = field(default_factory=list)
    tty: bool = True

    def write(self, s: str) -> None:
        self.output.append(s)

    def _next(self) -> str:
        if not self.inputs:
            raise AssertionError(
                f"scripted input exhausted; output so far: {''.join(self.output)}"
            )
        return self.inputs.pop(0)

    def readline(self) -> str:
        return self._next() + "\n"

    def getpass(self, prompt: str) -> str:
        self.write(prompt)
        return self._next()

    def isatty(self) -> bool:
        return self.tty
