"""FMP transport — shared fetch helpers.

Everything HTTP-generic is delegated to the kernel:

* error mapping → :func:`parsimony.transport.map_http_error`
* timeout mapping → :func:`parsimony.transport.map_timeout_error`
* API-key redaction → :func:`parsimony.transport.redact_url`
  (the ``apikey`` query param is already in the kernel's sensitive-name set)
* pooled-client context manager → :func:`parsimony.transport.pooled_client`
  (re-exported here for screener callers)

What remains FMP-specific is :func:`fmp_fetch`, the DataFrame-shaping
envelope for the 18 simple connectors.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
import pandas as pd
from parsimony.errors import (
    EmptyDataError,
    ParseError,
)
from parsimony.transport import HttpClient, map_http_error, map_timeout_error, pooled_client
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
        map_timeout_error(exc, provider="fmp", op_name=op_name)
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


__all__ = [
    "fetch_json",
    "fmp_fetch",
    "make_http",
    "pooled_client",
]
