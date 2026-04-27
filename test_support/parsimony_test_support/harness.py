"""Shared assertions and constants for connector unit tests."""

from __future__ import annotations

from typing import Any

from parsimony.errors import (
    ConnectorError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import Provenance, Result

# ---------------------------------------------------------------------------
# Canonical HTTP → ConnectorError mapping
# ---------------------------------------------------------------------------
#
# Every connector that routes HTTP errors through
# ``parsimony.transport.map_http_error`` must satisfy this table. It's the
# merge gate across the monorepo; parametrize your error-mapping test with
# it.
#
# 403 is intentionally treated the same as 401 (UnauthorizedError) by the
# kernel's mapper, so we only pin the primary ones.
STATUS_TO_EXC: list[tuple[int, type[ConnectorError]]] = [
    (401, UnauthorizedError),
    (402, PaymentRequiredError),
    (429, RateLimitError),
    (500, ProviderError),
    (503, ProviderError),
]

# ---------------------------------------------------------------------------
# Sentinel secret
# ---------------------------------------------------------------------------
#
# Use this string as the API key when exercising the happy path and error
# paths. If this string appears anywhere in the error message, serialised
# result, or provenance after a round-trip, the connector is leaking
# credentials.
CANARY_KEY = "live-looking-key-do-not-leak"


# ---------------------------------------------------------------------------
# Secret-leak assertion
# ---------------------------------------------------------------------------


def assert_no_secret_leak(target: Any, secret: str = CANARY_KEY) -> None:
    """Assert that ``secret`` does not appear in ``target``'s user-facing surface.

    Checks :func:`str` and :func:`repr` of the target. For a
    :class:`Result` we additionally check ``to_llm()`` and the provenance
    params (the agent-facing serialisations). For a :class:`ConnectorError`
    we check only ``str()`` — the chained ``__cause__`` (the raw
    ``httpx.HTTPStatusError``) commonly embeds the query-string API key,
    but redacting the chain is the consumer's job (e.g.
    ``parsimony_mcp.bridge.translate_error``) not the connector's. The
    contract at this layer is "the ConnectorError's own message is safe";
    the chain is for debugging with secrets masked by the caller.
    """
    needles: list[str] = [str(target)]

    # repr() is fair game unless target is a ConnectorError — its default
    # repr may include args containing the chain.
    if not isinstance(target, ConnectorError):
        needles.append(repr(target))

    if isinstance(target, Result):
        try:
            needles.append(target.to_llm())
        except Exception:
            pass
        if target.provenance is not None:
            needles.append(repr(target.provenance))
            needles.append(str(target.provenance.params))

    if isinstance(target, Provenance):
        needles.append(str(target.params))

    for needle in needles:
        if not needle:
            continue
        assert secret not in needle, (
            f"Secret leaked into output. "
            f"Secret starts with {secret[:8]}... — found in: {needle[:200]}"
        )


# ---------------------------------------------------------------------------
# Provenance-shape assertion
# ---------------------------------------------------------------------------


def assert_provenance_shape(
    result: Result,
    *,
    expected_source: str | None = None,
    required_param_keys: list[str] | None = None,
    require_fetched_at: bool = False,
) -> None:
    """Assert a :class:`Result` has well-formed provenance.

    * ``result.provenance`` is not None.
    * ``provenance.source`` is a non-empty string (and matches
      ``expected_source`` if provided).
    * ``provenance.params`` contains every key in ``required_param_keys``.
    * Optionally — if ``require_fetched_at=True`` — ``fetched_at`` is set.
      The kernel schema has ``fetched_at: datetime | None``, and many
      connectors legitimately leave it unset, so this check is opt-in.
    """
    assert result.provenance is not None, "Result.provenance is None"
    prov = result.provenance

    assert isinstance(prov.source, str) and prov.source, (
        f"Provenance.source must be a non-empty string, got {prov.source!r}"
    )
    if expected_source is not None:
        assert prov.source == expected_source, (
            f"Expected provenance.source={expected_source!r}, got {prov.source!r}"
        )

    if require_fetched_at:
        assert prov.fetched_at is not None, "Provenance.fetched_at is None"

    if required_param_keys:
        missing = [k for k in required_param_keys if k not in prov.params]
        assert not missing, (
            f"Provenance.params missing required keys: {missing}. "
            f"Got: {list(prov.params.keys())}"
        )
