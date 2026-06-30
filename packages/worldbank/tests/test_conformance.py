"""Release-blocking conformance test for parsimony-worldbank.

If this fails, the plugin violates the parsimony plugin contract and must
not be published.
"""

from __future__ import annotations

from parsimony.testing import assert_plugin_valid

import parsimony_worldbank


def test_conforms_to_parsimony_plugin_contract() -> None:
    """Assert the package satisfies the parsimony plugin contract."""
    assert_plugin_valid(parsimony_worldbank)


# ---------------------------------------------------------------------------
# load() contract
# ---------------------------------------------------------------------------


def test_load_returns_connectors() -> None:
    """``load()`` returns the ``CONNECTORS`` object unchanged."""
    loaded = parsimony_worldbank.load()
    assert loaded is parsimony_worldbank.CONNECTORS
    assert len(loaded) == 2


# ---------------------------------------------------------------------------
# Connector metadata — worldbank_fetch
# ---------------------------------------------------------------------------


def test_connector_has_correct_metadata() -> None:
    """The ``worldbank_fetch`` connector exposes expected name, tags, and output columns."""
    conn = parsimony_worldbank.CONNECTORS["worldbank_fetch"]

    # Name
    assert conn.name == "worldbank_fetch"

    # Tags
    assert "macro" in conn.tags
    assert "international" in conn.tags
    assert "development" in conn.tags

    # Output columns
    col_names = [col.name for col in conn.output_config.columns]
    expected = ["indicator_id", "indicator_name", "country", "country_iso3", "date", "value"]
    assert col_names == expected

    # Expected roles (lower-case as returned by ColumnRole.value)
    roles = {col.name: col.role.value for col in conn.output_config.columns}
    assert roles["indicator_id"] == "key"
    assert roles["date"] == "data"
    assert roles["value"] == "data"
    assert roles["country"] == "metadata"


# ---------------------------------------------------------------------------
# Connector metadata — worldbank_search
# ---------------------------------------------------------------------------


def test_search_connector_has_correct_metadata() -> None:
    """The ``worldbank_search`` connector exposes expected name, tags, and output columns."""
    conn = parsimony_worldbank.CONNECTORS["worldbank_search"]

    # Name
    assert conn.name == "worldbank_search"

    # Tags
    assert "macro" in conn.tags
    assert "international" in conn.tags
    assert "development" in conn.tags

    # Output columns
    col_names = [col.name for col in conn.output_config.columns]
    expected = ["indicator_id", "indicator_name", "source_note", "source_org", "page"]
    assert col_names == expected

    # Expected roles
    roles = {col.name: col.role.value for col in conn.output_config.columns}
    assert roles["indicator_id"] == "key"
    assert roles["indicator_name"] == "title"
    assert roles["source_note"] == "metadata"
    assert roles["page"] == "metadata"
