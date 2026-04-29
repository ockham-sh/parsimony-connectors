"""Context-managed ``sdmx1`` client that uses our shared HTTP session.

Replacing ``client.session`` with the session from :mod:`parsimony_sdmx.io.http`
gives the ``sdmx1``-mediated fetches the same TLS, retry, timeout, and
User-Agent behaviour as our direct fetches.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import Any
from urllib.parse import urlparse, urlunparse

import sdmx

from parsimony_sdmx.io.http import HttpConfig, build_session

_WB_BAD_HOST = "dataapi.worldbank.org"
_WB_GOOD_HOST = "api.worldbank.org"


def _install_wb_host_rewrite(session: Any) -> None:
    """Wrap ``session.send`` so requests to the deprecated WB host are rewritten.

    The World Bank's SDMX dataflow registry advertises ``dataapi.worldbank.org``
    as the canonical observation endpoint, but that host returns 404 for
    structure-aware requests. Rewriting at the session layer keeps the patch
    transparent to ``sdmx1`` while only firing for exact host matches (avoids
    paths like ``…/lookup?u=dataapi.worldbank.org`` accidentally triggering it).
    """
    original_send = session.send

    def _patched_send(request: Any, **kwargs: Any) -> Any:
        url = getattr(request, "url", "") or ""
        if url:
            parsed = urlparse(url)
            if parsed.hostname == _WB_BAD_HOST:
                request.url = urlunparse(parsed._replace(scheme="https", netloc=_WB_GOOD_HOST))
        return original_send(request, **kwargs)

    session.send = _patched_send  # type: ignore[method-assign]


@contextmanager
def sdmx_client(
    agency_id: str,
    http_config: HttpConfig | None = None,
    *,
    wb_url_rewrite: bool = False,
) -> Iterator[Any]:
    """Yield a configured ``sdmx1.Client`` and close its session on exit.

    When ``wb_url_rewrite`` is True the session rewrites requests against
    ``dataapi.worldbank.org`` to ``api.worldbank.org``. Required by the
    live observation fetcher; off by default so structure-only callers
    (catalog enumeration, codelist resolution) keep their existing behaviour.
    """
    client = sdmx.Client(source=agency_id)
    session = build_session(http_config)
    # Preserve cookies/headers set by sdmx1 defaults.
    original = getattr(client, "session", None)
    if original is not None:
        with suppress(AttributeError):
            session.cookies.update(original.cookies)
        for name, value in getattr(original, "headers", {}).items():
            session.headers.setdefault(name, value)
    client.session = session
    if wb_url_rewrite:
        _install_wb_host_rewrite(session)
    try:
        yield client
    finally:
        session.close()
