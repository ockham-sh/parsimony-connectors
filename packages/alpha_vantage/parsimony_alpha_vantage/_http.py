"""Alpha Vantage transport — shared HTTP helpers and in-body error detection.

Alpha Vantage's quirk: JSON endpoints return HTTP 200 with one of
``Error Message`` / ``Note`` / ``Information`` as the only top-level key
when something is wrong. CSV endpoints do the same with a plain-text
``Information`` body. The helpers inspect the body and raise the
semantically-correct typed exception.

API-key redaction is handled by :func:`parsimony.transport.redact_url` —
the ``apikey`` query param is already in the kernel's sensitive-name set.
"""

from __future__ import annotations

import io
from typing import Any

import httpx
import pandas as pd
from parsimony.errors import (
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
)
from parsimony.transport import HttpClient, map_http_error, map_timeout_error

_DEFAULT_BASE_URL: str = "https://www.alphavantage.co"
_DEFAULT_TIMEOUT_SECONDS: float = 20.0
_DEFAULT_RATE_LIMIT_RETRY_AFTER: float = 60.0
_PROVIDER: str = "alpha_vantage"


def make_http(api_key: str, base_url: str = _DEFAULT_BASE_URL) -> HttpClient:
    """Construct the standard Alpha Vantage transport.

    The API key rides as a default query parameter (Alpha Vantage's auth
    convention). Timeout is 20s — provider is not latency-critical.
    """
    return HttpClient(
        base_url,
        query_params={"apikey": api_key},
        timeout=_DEFAULT_TIMEOUT_SECONDS,
    )


def _raise_for_in_body_error(body: Any, op_name: str) -> None:
    """Detect Alpha Vantage's in-body error envelopes in a JSON response.

    Alpha Vantage returns HTTP 200 for many error conditions and signals the
    actual failure mode via one of:

    * ``Error Message`` — bad query, unknown symbol, etc. → EmptyDataError
    * ``Note`` — free-tier rate limit → RateLimitError
    * ``Information`` (rate-limit language) → RateLimitError
    * ``Information`` (otherwise) → PaymentRequiredError (premium endpoint)
    """
    if not isinstance(body, dict):
        return
    if "Error Message" in body:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"Alpha Vantage error on '{op_name}': {body['Error Message']}",
        )
    if "Note" in body:
        raise RateLimitError(
            provider=_PROVIDER,
            retry_after=_DEFAULT_RATE_LIMIT_RETRY_AFTER,
            message=f"Alpha Vantage rate limit: {body['Note']}",
        )
    if "Information" in body and len(body) == 1:
        info_msg = body["Information"]
        if "per-second" in info_msg.lower() or "rate limit" in info_msg.lower():
            raise RateLimitError(
                provider=_PROVIDER,
                retry_after=_DEFAULT_RATE_LIMIT_RETRY_AFTER,
                message=f"Alpha Vantage rate limit: {info_msg}",
            )
        raise PaymentRequiredError(
            provider=_PROVIDER,
            message=f"Alpha Vantage: {info_msg}",
        )


async def av_fetch(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Single GET to Alpha Vantage's ``/query`` endpoint, return parsed JSON.

    Every Alpha Vantage JSON endpoint is the same URL (``/query``),
    differentiated by the ``function`` query parameter.
    """
    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)

    try:
        response = await http.request("GET", "/query", params=req_params)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)

    body = response.json()
    _raise_for_in_body_error(body, op_name)
    return body


async def av_fetch_csv(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> pd.DataFrame:
    """Fetch an Alpha Vantage CSV endpoint into a DataFrame.

    CSV endpoints (calendars, listing status) have the same quirks as the
    JSON endpoints — HTTP 200 with a rate-limit message in the body instead
    of a proper CSV. Detect and raise before pandas tries to parse it.
    """
    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)

    try:
        response = await http.request("GET", "/query", params=req_params)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)

    text = response.text
    if text.startswith("Information"):
        raise RateLimitError(
            provider=_PROVIDER,
            retry_after=_DEFAULT_RATE_LIMIT_RETRY_AFTER,
            message=f"Alpha Vantage rate limit on '{op_name}'",
        )

    return pd.read_csv(io.StringIO(text))


def strip_numbered_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Strip numbered prefixes from Alpha Vantage keys.

    ``"1. open"`` → ``"open"``, ``"01. symbol"`` → ``"symbol"``.
    """
    return {k.split(". ", 1)[-1] if ". " in k else k: v for k, v in d.items()}


def clean_none_strings(d: dict[str, Any]) -> dict[str, Any]:
    """Replace ``"None"`` string values with ``None`` for proper NaN coercion."""
    return {k: (None if v == "None" else v) for k, v in d.items()}


__all__ = [
    "av_fetch",
    "av_fetch_csv",
    "clean_none_strings",
    "make_http",
    "strip_numbered_keys",
]
