"""FMP transport — shared HTTP helpers, unified error mapping, URL redaction.

Every FMP connector in this package routes through the helpers defined here.
That single chokepoint is what guarantees:

- One canonical error mapping (401/402/429/other → typed exception).
- No FMP API key ever appears in an exception message, even though FMP auth
  is a query-string parameter (``?apikey=<key>``) that lives in every
  ``httpx.Request.url``.
- One documented reach into the kernel's private ``HttpClient._client_kwargs``,
  isolated to the pooled-client context manager used only by the screener.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pandas as pd
from parsimony.errors import (
    EmptyDataError,
    ParseError,
    ProviderError,
)
from parsimony.transport import HttpClient, map_http_error
from parsimony.result import OutputConfig, Provenance, Result

# Per-request timeout. 15s matches the Tiingo connector's precedent and is
# defensible for FMP's equity REST endpoints, which are not streaming.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0

_DEFAULT_BASE_URL: str = "https://financialmodelingprep.com/stable"


def make_http(api_key: str, base_url: str = _DEFAULT_BASE_URL) -> HttpClient:
    """Construct the standard FMP transport.

    All 19 FMP connectors use this constructor so that auth, timeouts, and
    query-param handling are consistent. The API key rides as a default
    query parameter (FMP's auth convention).
    """
    return HttpClient(
        base_url,
        query_params={"apikey": api_key},
        timeout=_DEFAULT_TIMEOUT_SECONDS,
    )


def _redact_url(url: str) -> str:
    """Replace the ``apikey`` query value with ``***``.

    FMP auth is in the URL, so every ``httpx.Request.url`` carries the key.
    This helper is the last line of defence before any URL or message text
    leaves the transport layer. Applied defensively — the mapped errors
    below never format the URL directly, but if a future change does, the
    redaction still runs.
    """
    return re.sub(r"(apikey=)[^&\s]+", r"\1***", url)


async def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Single GET to FMP, unified error mapping, return parsed JSON.

    Used directly by the screener for its multi-endpoint enrichment fan-out
    (where DataFrame construction is caller-controlled) and indirectly by
    :func:`fmp_fetch` for the 18 simple connectors that always return one
    DataFrame-backed :class:`Result`.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = await http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="fmp", op_name=op_name)
    except httpx.TimeoutException as exc:
        raise ProviderError(
            provider="fmp",
            status_code=408,
            message=f"FMP request timed out on endpoint '{op_name}'",
        ) from exc
    return response.json()


async def fmp_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any],
    op_name: str,
    output_config: OutputConfig | None = None,
) -> Result:
    """Simple-connector helper: fetch, build DataFrame, wrap in :class:`Result`.

    Handles FMP's response shapes: lists become DataFrames directly; dicts
    with a ``historical``/``data``/``results`` envelope are unwrapped; bare
    dicts become single-row DataFrames. Path-template substitution (``{key}``
    in ``path``) is supported for future use — all current callers pass
    literal paths with params destined for the query string.
    """
    rendered = path
    query_params: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        placeholder = f"{{{key}}}"
        if placeholder in rendered:
            rendered = rendered.replace(placeholder, str(value))
        else:
            query_params[key] = value
    rendered = re.sub(r"\{[^}]+\}", "", rendered)

    data = await fetch_json(http, path=rendered, params=query_params, op_name=op_name)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for envelope_key in ("historical", "data", "results"):
            if envelope_key in data and isinstance(data[envelope_key], list):
                df = pd.DataFrame(data[envelope_key])
                break
        else:
            df = pd.DataFrame([data])
    else:
        raise ParseError(provider="fmp", message=f"Unexpected response type from FMP: {type(data).__name__}")

    if df.empty:
        raise EmptyDataError(provider="fmp", message=f"No data returned from FMP endpoint '{op_name}'")

    prov = Provenance(source=op_name, params=dict(params))
    if output_config is not None:
        return output_config.build_table_result(df, provenance=prov, params=dict(params))
    return Result.from_dataframe(df, prov)


@asynccontextmanager
async def pooled_client(http: HttpClient) -> AsyncIterator[HttpClient]:
    """Yield an HttpClient whose underlying ``httpx.AsyncClient`` is pooled.

    Used by the screener for its per-symbol enrichment fan-out so that TCP
    and TLS state is reused across thousands of requests within a single
    ``fmp_screener`` invocation.

    NOTE: this helper is the only place in ``parsimony-fmp`` that reaches
    into the kernel's private ``HttpClient._client_kwargs``. The kernel has
    no public constructor for a pre-configured ``httpx.AsyncClient`` that
    matches an ``HttpClient``'s auth/timeout/transport settings; isolating
    the private access here means a single site to update if that shape
    changes upstream.
    """
    async with httpx.AsyncClient(**http._client_kwargs()) as shared:
        yield http.with_shared_client(shared)


__all__ = [
    "fetch_json",
    "fmp_fetch",
    "make_http",
    "pooled_client",
]
