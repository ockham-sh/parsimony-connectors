"""Maintainer tooling for catalog snapshot inspection and search validation.

This package is **not** part of any connector plugin contract. Connector
packages only depend on a published catalog URL (and optional env override).
Build scripts under ``packages/*/scripts/`` are one way to produce those
artifacts; this module validates whatever snapshot exists at a URL.
"""

from catalog_validate.fixtures import load_queries_file, write_queries_file  # noqa: F401
from catalog_validate.probes import generate_probes  # noqa: F401
from catalog_validate.registry import (  # noqa: F401
    EXCLUDED_COMMERCIAL_PROVIDERS,
    MACRO_CATALOG_PROVIDER_IDS,
    PROVIDER_SPECS,
    ProviderCatalogSpec,
)
from catalog_validate.runner import ValidationReport, validate_catalog  # noqa: F401

__all__ = [
    "PROVIDER_SPECS",
    "ProviderCatalogSpec",
    "ValidationReport",
    "generate_probes",
    "load_queries_file",
    "validate_catalog",
    "write_queries_file",
]
