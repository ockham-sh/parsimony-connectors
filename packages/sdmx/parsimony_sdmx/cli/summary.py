"""Operator-readable end-of-run summary block."""

from __future__ import annotations

from collections.abc import Sequence

from parsimony_sdmx.core.outcomes import DatasetOutcome, OutcomeStatus

BAR = "=" * 60


def format_summary(outcomes: Sequence[DatasetOutcome]) -> str:
    """Deterministic summary block printed at the end of every run."""
    if not outcomes:
        return "No datasets processed."

    ok = [o for o in outcomes if o.status == OutcomeStatus.OK]
    empty = [o for o in outcomes if o.status == OutcomeStatus.EMPTY]
    failed = [o for o in outcomes if o.status == OutcomeStatus.FAILED]

    lines = [
        "",
        BAR,
        f"Summary: {len(outcomes)} dataset(s)",
        f"  ok:     {len(ok)}",
        f"  empty:  {len(empty)}",
        f"  failed: {len(failed)}",
        BAR,
    ]
    if failed:
        lines.append("")
        lines.append("Failed datasets:")
        for o in sorted(failed, key=lambda x: x.dataset_id):
            kind = o.kind.value if o.kind else "unknown"
            message_lines = (o.error_message or "").splitlines()
            message = message_lines[0][:160] if message_lines else ""
            lines.append(f"  {o.dataset_id} [{kind}]: {message}")
    return "\n".join(lines)
