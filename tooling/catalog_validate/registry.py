"""Registry of provider catalog build targets (maintainer reference only).

Macro/public statistical providers only. Commercial connectors with built-in
upstream search (FMP, Alpha Vantage, Tiingo, Finnhub, CoinGecko, EODHD, etc.)
are intentionally excluded — agents use those providers' native search tools.
"""

from __future__ import annotations

from dataclasses import dataclass

# Providers with first-party search APIs — no catalog build target.
EXCLUDED_COMMERCIAL_PROVIDERS: frozenset[str] = frozenset(
    {
        "alpha_vantage",
        "coingecko",
        "eodhd",
        "finnhub",
        "fmp",
        "fred",
        "polymarket",
        "sec_edgar",
        "tiingo",
    }
)


@dataclass(frozen=True, slots=True)
class ProviderCatalogSpec:
    """One published catalog bundle maintained by operator scripts."""

    provider: str
    default_url: str
    build_script: str
    search_mode: str
    """Human-readable expected search style: ``structured_first``, ``hybrid_lexical``, ``hybrid_semantic``."""

    queries_file: str
    """Curated probe file relative to parsimony-connectors repo root."""


# Canonical HF defaults; override per command with --catalog-url / env.
PROVIDER_SPECS: dict[str, ProviderCatalogSpec] = {
    "bde": ProviderCatalogSpec(
        provider="bde",
        default_url="hf://parsimony-dev/bde",
        build_script="packages/bde/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/bde/catalog_tests/queries.yaml",
    ),
    "bdf": ProviderCatalogSpec(
        provider="bdf",
        default_url="hf://parsimony-dev/bdf",
        build_script="packages/bdf/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/bdf/catalog_tests/queries.yaml",
    ),
    "bdp": ProviderCatalogSpec(
        provider="bdp",
        default_url="hf://parsimony-dev/bdp",
        build_script="packages/bdp/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/bdp/catalog_tests/queries.yaml",
    ),
    "boc": ProviderCatalogSpec(
        provider="boc",
        default_url="hf://parsimony-dev/boc",
        build_script="packages/boc/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/boc/catalog_tests/queries.yaml",
    ),
    "boj": ProviderCatalogSpec(
        provider="boj",
        default_url="hf://parsimony-dev/boj",
        build_script="packages/boj/scripts/build_catalog.py",
        search_mode="structured_first",
        queries_file="packages/boj/catalog_tests/queries.yaml",
    ),
    "destatis": ProviderCatalogSpec(
        provider="destatis",
        default_url="hf://parsimony-dev/destatis",
        build_script="packages/destatis/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/destatis/catalog_tests/queries.yaml",
    ),
    "rba": ProviderCatalogSpec(
        provider="rba",
        default_url="hf://parsimony-dev/rba",
        build_script="packages/rba/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/rba/catalog_tests/queries.yaml",
    ),
    "riksbank": ProviderCatalogSpec(
        provider="riksbank",
        default_url="hf://parsimony-dev/riksbank",
        build_script="packages/riksbank/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/riksbank/catalog_tests/queries.yaml",
    ),
    "snb": ProviderCatalogSpec(
        provider="snb",
        default_url="hf://parsimony-dev/snb",
        build_script="packages/snb/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/snb/catalog_tests/queries.yaml",
    ),
    "treasury": ProviderCatalogSpec(
        provider="treasury",
        default_url="hf://parsimony-dev/treasury",
        build_script="packages/treasury/scripts/build_catalog.py",
        search_mode="hybrid_semantic",
        queries_file="packages/treasury/catalog_tests/queries.yaml",
    ),
}

SDMX_ROOT_DEFAULT = "hf://parsimony-dev/sdmx"
SDMX_QUERIES_FILE = "packages/sdmx/catalog_tests/queries.yaml"

# Flat macro catalogs in PROVIDER_SPECS; multi-bundle: SDMX + BoJ (see their build scripts).
MACRO_CATALOG_PROVIDER_IDS: frozenset[str] = frozenset(PROVIDER_SPECS)
