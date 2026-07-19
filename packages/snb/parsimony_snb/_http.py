"""SNB transport — keyless httpx + the portal-internal JSON unlock.

Single host ``data.snb.ch``, plain ``httpx`` (no curl_cffi). Two response
families:

* The **public ``/api/...`` data + dimensions** paths return CSV / JSON and are
  not WAF-walled. ``/sitemap`` (the catalog enumeration source) is likewise open.
* The **portal-internal ``/json/...`` API** (``getCubeInfo``, ``getTopics...``) is
  fronted by an Airlock WAF: a stock request gets a ``/error_path/`` HTML page
  instead of JSON. The SPA unlocks it with one request header — ``x-epb-ajax:
  true`` — and a ``pageViewTime`` query param. We attach the header to every
  request (it is harmless on the open paths) so ``getCubeInfo`` works.

There is no API key — SNB is fully keyless; ``load()`` binds only the catalog
URL for search.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from parsimony.errors import ConnectorError
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import fetch_json, make_http_client

logger = logging.getLogger(__name__)

PROVIDER = "snb"
_BASE_URL = "https://data.snb.ch"
_TIMEOUT = 30.0

#: The SPA sends this on every XHR. Without it the portal's ``/json/...`` API is
#: WAF-walled (an ``/error_path/`` HTML page is returned in place of JSON). It is
#: ignored on the open ``/api/...`` + ``/sitemap`` paths, so we send it always.
_AJAX_HEADERS = {"x-epb-ajax": "true"}

#: Concurrency cap comment preserved for historical context; the serial enumerate
#: uses this as a serial iteration limit comment only.
_PROBE_CONCURRENCY = 20

_SITEMAP_PATH = "/sitemap"
_CUBE_INFO_PATH = "/json/table/getCubeInfo"


def client(timeout: float = _TIMEOUT) -> HttpClient:
    """Build the keyless SNB client (used for every endpoint family)."""
    return make_http_client(_BASE_URL, provider=PROVIDER, headers=_AJAX_HEADERS, timeout=timeout)


# ---------------------------------------------------------------------------
# Cube-id helpers (publication vs warehouse routing)
# ---------------------------------------------------------------------------


def is_warehouse_id(cube_id: str) -> bool:
    """Warehouse cube ids are SDMX-style and carry ``@`` (``BSTA@SNB.AUR_U.ODF``);
    publication cube ids are bare alphanumerics (``rendoblim``)."""
    return "@" in cube_id


def warehouse_api_id(portal_id: str) -> str:
    """Map a portal/sitemap warehouse id to the id the ``/api/warehouse`` path wants.

    The portal id ``BSTA@SNB.AUR_U.ODF`` must have its ``@`` replaced with ``.``
    (→ ``BSTA.SNB.AUR_U.ODF``) for ``/api/warehouse/cube/{id}/...`` — confirmed
    from the portal's own ``getApiLinks`` download URLs. The unencoded ``@`` is
    rejected with ``500 IllegalArgumentException: cubeId contains illegal
    characters``.
    """
    return portal_id.replace("@", ".")


def cube_data_path(cube_id: str, *, lang: str) -> str:
    """The CSV download path for a publication or warehouse cube."""
    if is_warehouse_id(cube_id):
        return f"/api/warehouse/cube/{warehouse_api_id(cube_id)}/data/csv/{lang}"
    return f"/api/cube/{cube_id}/data/csv/{lang}"


def cube_dimensions_path(cube_id: str, *, lang: str) -> str:
    """The dimensions JSON path for a publication or warehouse cube."""
    if is_warehouse_id(cube_id):
        return f"/api/warehouse/cube/{warehouse_api_id(cube_id)}/dimensions/{lang}"
    return f"/api/cube/{cube_id}/dimensions/{lang}"


def _page_view_time() -> str:
    """A ``YYYYMMDD_HHMMSS`` token for the ``/json/...`` API (any well-formed
    value works; the portal uses it for telemetry / cache-busting)."""
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def get_text(http: HttpClient, path: str, *, op_name: str, params: dict[str, str] | None = None) -> str:
    """GET *path* and return the raw text body (SNB cubes + the sitemap are not JSON).

    The raw-transport shape: ``request("GET")`` then :func:`~parsimony.transport.check_status`
    maps any non-2xx from the response status. The body is parsed separately.
    """
    response = http.request("GET", path, params=params, op_name=op_name)
    check_status(response, provider=PROVIDER, op_name=op_name)
    return response.text


def get_dimensions(http: HttpClient, cube_id: str, *, lang: str) -> dict[str, Any] | None:
    """Best-effort fetch of a cube's ``/dimensions`` payload.

    Returns the dict (with a ``dimensions`` key) or ``None`` for a retired /
    unreachable cube — cataloguing is a best-effort sweep, so a single cube's
    failure never fails the whole enumeration.
    """
    try:
        payload = fetch_json(
            http,
            path=cube_dimensions_path(cube_id, lang=lang),
            params=None,
            op_name="cube/dimensions",
        )
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment; skip on any failure
        logger.debug("SNB dimensions probe failed for %s: %s", cube_id, exc)
        return None
    return payload if isinstance(payload, dict) and "dimensions" in payload else None


def get_cube_info(http: HttpClient, cube_id: str, *, lang: str) -> dict[str, Any] | None:
    """Best-effort fetch of a cube's title/metadata via the portal ``getCubeInfo``.

    Returns ``{title, publishingTitle, unit, frequencySpecification, ...}`` or
    ``None``. This rides the reverse-engineered ``x-epb-ajax`` WAF unlock; if SNB
    changes it the caller degrades to a synthesized title — the completeness
    surface (sitemap + ``/api`` fetch) never depends on this endpoint.
    """
    params = {
        "lang": lang,
        "cubeId": cube_id,  # portal id — literal ``@`` for warehouse cubes
        "isWarehouse": "true" if is_warehouse_id(cube_id) else "false",
        "pageViewTime": _page_view_time(),
    }
    try:
        payload = fetch_json(
            http,
            path=_CUBE_INFO_PATH,
            params=params,
            op_name="getCubeInfo",
        )
    except (ConnectorError, ValueError) as exc:
        # ConnectorError (mapped 4xx/5xx) or a JSON-decode ValueError (a WAF HTML
        # page slipped through) — title enrichment is optional, so skip.
        logger.debug("SNB getCubeInfo failed for %s: %s", cube_id, exc)
        return None
    return payload if isinstance(payload, dict) else None


def fetch_sitemap(http: HttpClient) -> str:
    """GET the published XML sitemap (the authoritative cube enumeration)."""
    return get_text(http, _SITEMAP_PATH, op_name="sitemap")
