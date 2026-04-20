"""Connector registry schema — the single owner for registry.json shape.

This module is the ONE definition of the registry's Pydantic models. It
lives here (with the init-wizard consumer) rather than in ``tools/``
because it needs to be importable by runtime code on installed users'
machines; the generator script in ``tools/gen_registry.py`` adds
``packages/mcp`` to ``sys.path`` to import it at build time.

Rationale for a single shared model (Dodds Principle 3 — type system
as armour; Fowler Principle 6 — architecture earns its boundaries):
keeping the producer and consumer behind one Pydantic model means a
schema change in one PR can't silently break the other side. Bumping
the schema version is a coordinated change.

Why strict+extra=forbid: a malformed or future-schema registry fails
fast with actionable pydantic errors at the boundary — not deep in
the prompt loop where the user sees ``KeyError: 'env_vars'``.

Note: this module does NOT use ``from __future__ import annotations``
on purpose. It's loaded directly via ``importlib.util`` by the
registry generator in ``tools/gen_registry.py`` — stringified
annotations break Pydantic's type resolution when the module isn't
imported through its normal package hierarchy.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION: Literal[1] = 1
"""Current registry schema version. Bump on any breaking change."""


class EnvVar(BaseModel):
    """A single environment variable a connector needs.

    ``name`` matches the OS env-var name (``FRED_API_KEY``). ``get_url``
    optionally points to a sign-up page shown in the init wizard's
    prompt. ``required=False`` means the connector has a reduced-tier
    fallback (e.g. CoinGecko's free tier).
    """

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    name: str = Field(..., min_length=1)
    get_url: str | None = None
    required: bool = True


class ConnectorPackage(BaseModel):
    """One installable connector distribution.

    ``package`` is the PyPI distribution name (``parsimony-fred``).
    ``display`` is what the init wizard shows in its menu (``FRED``).
    Tags come from the union of all ``@connector`` / ``@enumerator`` /
    ``@loader`` decorator tags in the package's ``__init__.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    package: str = Field(..., min_length=1)
    display: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    homepage: str | None = None
    pricing: str | None = None
    rate_limits: str | None = None
    tags: tuple[str, ...] = ()
    env_vars: tuple[EnvVar, ...] = ()


class Registry(BaseModel):
    """Top-level registry document.

    ``schema_version`` is pinned as a Literal so a registry carrying
    ``schema_version = 2`` fails validation against a v1 client with
    an actionable message, not a silent success.
    """

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    schema_version: Literal[1]
    connectors: tuple[ConnectorPackage, ...]
