"""Catalog search relevance tests for parsimony-riksbank (fixture-backed, no published catalog)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_riksbank import RIKSBANK_ENUMERATE_OUTPUT, enumerate_riksbank, riksbank_search

_GROUPS_PAYLOAD = {
    "groupId": 1,
    "name": "Interest rates and exchange rates",
    "description": "",
    "childGroups": [
        {
            "groupId": 11,
            "name": "Exchange rates",
            "description": "FX rates.",
            "childGroups": [
                {"groupId": 130, "name": "Currencies against Swedish kronor", "childGroups": []},
            ],
        },
        {
            "groupId": 137,
            "name": "Riksbank interest rates",
            "childGroups": [
                {"groupId": 2, "name": "Riksbank key interest rates", "childGroups": []},
            ],
        },
    ],
}

_SERIES_PAYLOAD = [
    {
        "seriesId": "SECBREPOEFF",
        "source": "Sveriges Riksbank",
        "shortDescription": "Policy rate",
        "midDescription": "Policy rate",
        "longDescription": (
            "The policy rate is the interest rate at which the banks can borrow or deposit in the Riksbank."
        ),
        "groupId": 2,
        "observationMinDate": "1994-06-01",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SEKEURPMI",
        "source": "Refinitiv",
        "shortDescription": "EUR",
        "midDescription": "EUR Euroland, euro",
        "longDescription": "Euroland euro",
        "groupId": 130,
        "observationMinDate": "1999-01-04",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SEKUSDPMI",
        "source": "Refinitiv",
        "shortDescription": "USD",
        "midDescription": "USD United States dollar",
        "longDescription": "United States dollar",
        "groupId": 130,
        "observationMinDate": "1999-01-04",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SWESTR",
        "source": "Sveriges Riksbank",
        "shortDescription": "SWESTR",
        "midDescription": "SWESTR",
        "longDescription": "Overnight rate",
        "groupId": 999,
        "observationMinDate": "2021-09-01",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
]


def _mock_enumerate() -> None:
    """Mock every live endpoint ``enumerate_riksbank`` touches.

    SWEA ``/Groups`` + ``/Series`` carry the fixture series. Main's enumerate also
    queries Monetary Policy live (``/forecasts/series_ids``) — mock it empty so the
    fixture stays SWEA/SWESTR-focused. Turnover/Holdings are static (no HTTP).
    """
    respx.get("https://api.riksbank.se/swea/v1/Groups").mock(return_value=httpx.Response(200, json=_GROUPS_PAYLOAD))
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(return_value=httpx.Response(200, json=_SERIES_PAYLOAD))
    respx.get("https://api.riksbank.se/monetary_policy_data/v1/forecasts/series_ids").mock(
        return_value=httpx.Response(200, json={"data": []})
    )


@pytest.fixture
def riksbank_catalog_dir(tmp_path: Path) -> Path:
    with respx.mock:
        _mock_enumerate()
        df = enumerate_riksbank().data
        # Drop the SWEA-payload SWESTR; keep the static registry row (source="swestr").
        df = df[~((df["code"] == "SWESTR") & (df["source"] == "swea"))]
        entries = entities_from_raw(df, RIKSBANK_ENUMERATE_OUTPUT)
        catalog = Catalog("riksbank", indexes=discovery_indexes(entries), default_field="title")
        catalog.set_entities(entries)
        catalog.build()
        out_dir = tmp_path / "riksbank_catalog"
        catalog.save(out_dir)
    return out_dir


@respx.mock
def test_riksbank_enumerate_fx_rows_are_searchable() -> None:
    _mock_enumerate()
    df = enumerate_riksbank().data
    eur = df.loc[df["code"] == "SEKEURPMI"].iloc[0]
    assert "exchange rate" in eur["title"].lower()
    assert "EUR/SEK" in eur["title"]
    assert "exchange rate" in eur["description"].lower()
    assert "Currencies against Swedish kronor" in eur["description"]


def test_riksbank_search_euro_sek_exchange_rate(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(
        query="euro Swedish krona exchange rate",
        limit=5,
        catalog_url=str(riksbank_catalog_dir),
    )
    assert result.data.iloc[0]["code"] == "SEKEURPMI"


def test_riksbank_search_eur_sek_exchange_rate(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(query="EUR SEK exchange rate", limit=5, catalog_url=str(riksbank_catalog_dir))
    assert result.data.iloc[0]["code"] == "SEKEURPMI"


def test_riksbank_search_swestr_benchmark(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(
        query="overnight reference rate SWESTR benchmark",
        limit=5,
        catalog_url=str(riksbank_catalog_dir),
    )
    assert result.data.iloc[0]["code"] == "SWESTR"


def test_riksbank_search_policy_rate(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(query="policy rate", limit=5, catalog_url=str(riksbank_catalog_dir))
    assert result.data.iloc[0]["code"] == "SECBREPOEFF"


def test_riksbank_search_code_exact_match(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(query="code: SEKEURPMI", limit=5, catalog_url=str(riksbank_catalog_dir))
    assert result.data.iloc[0]["code"] == "SEKEURPMI"
    assert result.data.iloc[0]["score"] >= 1_000_000


def test_riksbank_search_usd_fx(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(query="USD SEK exchange rate", limit=5, catalog_url=str(riksbank_catalog_dir))
    codes = set(result.data["code"])
    assert "SEKUSDPMI" in codes


def test_riksbank_search_returns_source_column(riksbank_catalog_dir: Path) -> None:
    result = riksbank_search(query="code: SEKEURPMI", limit=1, catalog_url=str(riksbank_catalog_dir))
    assert "source" in result.data.columns
    assert result.data.iloc[0]["source"]
