"""Shared assertions and constants for connector unit tests."""

from __future__ import annotations

import contextlib
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
# Every connector that maps HTTP statuses through
# ``parsimony.transport.check_status`` must satisfy this table. It's the
# merge gate across the monorepo; parametrize your error-mapping test with
# it.
#
# 403 is intentionally treated the same as 401 (UnauthorizedError) by
# ``check_status``, so we only pin the primary ones.
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
    params (the agent-facing serialisations).

    For a :class:`ConnectorError` we check its own message **and its chained
    ``__cause__``/``__context__``** (typically the raw ``httpx.HTTPStatusError``):
    both ``str()`` of each link and its ``request.url``. The chain is what
    reaches every traceback and ``logging.exception()``, so masking it is the
    library's job, not the caller's. Status errors now come from
    ``parsimony.transport.check_status`` (raised fresh from the status code, no
    chained ``httpx`` cause to leak); the only remaining key-bearing ``httpx``
    object is a transport-failure exception, which ``HttpClient.request`` maps
    and scrubs internally. This chain walk stays as the regression guard for
    that surviving path.
    """
    needles: list[str] = [str(target)]

    # repr() is fair game unless target is a ConnectorError — its default
    # repr may include args containing the chain.
    if not isinstance(target, ConnectorError):
        needles.append(repr(target))

    if isinstance(target, BaseException):
        seen: set[int] = set()
        link: BaseException | None = target
        while link is not None and id(link) not in seen:
            seen.add(id(link))
            needles.append(str(link))
            try:
                # httpx exposes .request as a property that raises when unset.
                request = getattr(link, "request", None)
                if request is not None:
                    needles.append(str(request.url))
            except RuntimeError:
                pass
            link = link.__cause__ or link.__context__

    if isinstance(target, Result):
        with contextlib.suppress(Exception):
            needles.append(target.to_llm())
        if target.provenance is not None:
            needles.append(repr(target.provenance))
            needles.append(str(target.provenance.params))

    if isinstance(target, Provenance):
        needles.append(str(target.params))

    for needle in needles:
        if not needle:
            continue
        assert secret not in needle, (
            f"Secret leaked into output. Secret starts with {secret[:8]}... — found in: {needle[:200]}"
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
        assert prov.source == expected_source, f"Expected provenance.source={expected_source!r}, got {prov.source!r}"

    if require_fetched_at:
        assert prov.fetched_at is not None, "Provenance.fetched_at is None"

    if required_param_keys:
        missing = [k for k in required_param_keys if k not in prov.params]
        assert not missing, f"Provenance.params missing required keys: {missing}. Got: {list(prov.params.keys())}"


def entries_result_to_dataframe(result: Result, *, columns: list[str] | None = None) -> Any:
    """Return tabular enumerator data from a wrapped :class:`Result`."""
    import pandas as pd

    data = result.raw
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"expected TabularResult data frame, got {type(data)!r}")
    if columns is None:
        return data
    return data.reindex(columns=columns, fill_value="")
