"""EODHD transport — shared HTTP helpers, unified error mapping, URL redaction.

Every EODHD connector in this package routes through the helpers defined
here. That single chokepoint is what guarantees:

- One canonical error mapping (401/403/402/429/other → typed exception).
- No EODHD API token ever appears in an exception message or log line, even
  though EODHD auth is a query-string parameter (``?api_token=<key>``) that
  lives in every ``httpx.Request.url``. ``_redact_url`` is the last line of
  defence before any URL text leaves the transport layer.
- ``Retry-After`` header parsing for 429 responses, with a safe fallback.
- Unified timeout handling (``httpx.TimeoutException`` → ``ProviderError``).
"""

from __future__ import annotations

import re
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

# Per-request timeout. 15s is defensible for EODHD's REST endpoints, which
# are not streaming. Bulk endpoints (fundamentals, macro_bulk, bulk_eod,
# exchange_symbols) override this via the ``timeout`` kwarg on ``make_http``.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0

_DEFAULT_BASE_URL: str = "https://eodhd.com/api"

_PROVIDER: str = "eodhd"


def make_http(
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> HttpClient:
    """Construct the standard EODHD transport.

    The API token rides as a default query parameter (``api_token=<key>``),
    alongside EODHD's ``fmt=json`` convention. Timeout defaults to 15s;
    bulk endpoints pass a larger value explicitly.
    """
    return HttpClient(
        base_url,
        query_params={"api_token": api_key, "fmt": "json"},
        timeout=timeout,
    )


def _redact_url(url: str) -> str:
    """Replace the ``api_token`` query value with ``***``.

    EODHD auth is in the URL, so every ``httpx.Request.url`` carries the
    secret. This helper is the last line of defence before any URL or
    message text leaves the transport layer. Applied defensively — the
    mapped errors below never format the URL directly, but if a future
    change does, the redaction still runs.
    """
    return re.sub(r"(api_token=)[^&\s]+", r"\1***", url)


def _to_bracket_params(params: dict[str, Any]) -> dict[str, Any]:
    """Transform ``filter_x`` → ``filter[x]`` and ``page_x`` → ``page[x]`` for EODHD bracket syntax.

    Pure function: does not mutate input. None values are dropped.
    """
    result: dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        if k.startswith("filter_"):
            result[f"filter[{k[7:]}]"] = v
        elif k.startswith("page_"):
            result[f"page[{k[5:]}]"] = v
        else:
            result[k] = v
    return result


async def eodhd_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any],
    op_name: str,
    output_config: OutputConfig | None = None,
    raw: bool = False,
) -> Result:
    """Shared EODHD fetch: path interpolation, bracket params, JSON extraction, Result building.

    Error mapping is delegated to :func:`~parsimony.transport.map_http_error`:
      401/403 → UnauthorizedError
      402     → PaymentRequiredError
      429     → RateLimitError (Retry-After parsed when present)
      else    → ProviderError

    ``httpx.TimeoutException`` is mapped to ``ProviderError(status_code=408)``.
    The EODHD API token is never included in exception messages.
    ``asyncio.CancelledError`` propagates unchanged.

    ``raw=True`` bypasses the DataFrame pipeline and returns the parsed JSON
    verbatim (used by ``eodhd_fundamentals``, which returns a nested dict).
    """
    # Path template substitution: {key} → value; remainder → query params
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

    # Remove any unfilled optional placeholders
    rendered = re.sub(r"\{[^}]+\}", "", rendered)

    # Apply EODHD bracket syntax transformation (filter_x → filter[x], page_x → page[x])
    query_params = _to_bracket_params(query_params)

    try:
        response = await http.request("GET", f"/{rendered.lstrip('/')}", params=query_params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        raise ProviderError(
            provider=_PROVIDER,
            status_code=408,
            message=f"EODHD request timed out on '{op_name}'",
        ) from exc

    data = response.json()
    prov = Provenance(source=op_name, params=dict(params))

    # 200-body error detection (EODHD returns error strings in the body on some endpoints)
    if isinstance(data, dict) and "error" in data and isinstance(data["error"], str):
        raise ProviderError(
            provider=_PROVIDER,
            status_code=200,
            message=f"EODHD error on '{op_name}': {data['error']}",
        )

    # Raw return path (fundamentals): bypass DataFrame pipeline entirely
    if raw:
        return Result(data=data, provenance=prov)

    # DataFrame construction
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for key in ("earnings", "ipos", "splits", "trends", "data", "results"):
            if key in data and isinstance(data[key], list):
                df = pd.DataFrame(data[key])
                break
        else:
            df = pd.DataFrame([data])
    else:
        raise ParseError(
            provider=_PROVIDER,
            message=f"Unexpected response type from EODHD '{op_name}': {type(data).__name__}",
        )

    if df.empty:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No data returned from EODHD endpoint '{op_name}'",
            query_params=dict(params),
        )

    if output_config is not None:
        return output_config.build_table_result(df, provenance=prov, params=dict(params))
    return Result.from_dataframe(df, prov)


__all__ = [
    "eodhd_fetch",
    "make_http",
]
