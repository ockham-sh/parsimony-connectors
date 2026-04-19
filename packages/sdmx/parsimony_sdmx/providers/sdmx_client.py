"""Context-managed ``sdmx1`` client that uses our shared HTTP session.

Replacing ``client.session`` with the session from :mod:`parsimony_sdmx.io.http`
gives the ``sdmx1``-mediated fetches the same TLS, retry, timeout, and
User-Agent behaviour as our direct fetches.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import sdmx

from parsimony_sdmx.io.http import HttpConfig, build_session


@contextmanager
def sdmx_client(
    agency_id: str,
    http_config: HttpConfig | None = None,
) -> Iterator[Any]:
    """Yield a configured ``sdmx1.Client`` and close its session on exit."""
    client = sdmx.Client(source=agency_id)
    session = build_session(http_config)
    # Preserve cookies/headers set by sdmx1 defaults.
    original = getattr(client, "session", None)
    if original is not None:
        try:
            session.cookies.update(original.cookies)
        except AttributeError:
            pass
        for name, value in getattr(original, "headers", {}).items():
            session.headers.setdefault(name, value)
    client.session = session
    try:
        yield client
    finally:
        session.close()
