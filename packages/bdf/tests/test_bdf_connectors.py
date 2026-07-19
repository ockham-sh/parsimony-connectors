"""Offline (respx-mocked) tests for the Banque de France connectors.

BdF requires an api_key sent in the ``Authorization: Apikey <KEY>`` header.
These tests cover every verb without touching the network:

* ``bdf_fetch`` — happy path, header presence, period filtering, EmptyData /
  Parse / InvalidParameter guards, no-key fast-fail, env fallback, secret
  stripping.
* ``enumerate_bdf`` — bounded crawl via the two export seams (datasets + the
  single full ``series`` export — archetype A, never a 41k-row build), exact
  column match, populated bilingual metadata, best-effort degradation, no-key
  fast-fail, secret stripping.
* ``bdf_search`` — ranked retrieval over a tiny in-process fixture catalog
  (never a cold full build / network).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest
import respx
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    UnauthorizedError,
)
from parsimony.result import Result
from parsimony_test_support import CANARY_KEY, assert_no_secret_leak

from parsimony_bdf import CONNECTORS, load
from parsimony_bdf.connectors import enumerate as enum_mod
from parsimony_bdf.connectors.enumerate import enumerate_bdf
from parsimony_bdf.connectors.fetch import bdf_fetch
from parsimony_bdf.outputs import BDF_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS
from parsimony_bdf.search import bdf_search

_KEY = CANARY_KEY

_BASE = "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"
_OBSERVATIONS_URL = f"{_BASE}/observations/exports/json"
_DATASETS_URL = f"{_BASE}/webstat-datasets/exports/json"
_SERIES_URL = f"{_BASE}/series/exports/json"

# Real-shape observations payload (flat array, one object per observation, the
# lean ``select`` projection the fetch connector requests).
_BDF_OBS_JSON = [
    {
        "series_key": "EXR.M.USD.EUR.SP00.E",
        "title_en": "US dollar/Euro spot rate",
        "title_fr": "Taux de change USD/EUR",
        "time_period_start": "2026-01-01",
        "obs_value": 1.0832,
    },
    {
        "series_key": "EXR.M.USD.EUR.SP00.E",
        "title_en": "US dollar/Euro spot rate",
        "title_fr": "Taux de change USD/EUR",
        "time_period_start": "2026-02-01",
        "obs_value": 1.0874,
    },
]

# Real-shape dataflow-stub payload (/webstat-datasets/exports/json).
_BDF_DATASETS_JSON = [
    {
        "dataset_id": "EXR",
        "name_en": "Exchange rates",
        "name_fr": "Taux de change",
        "description_en": "Euro foreign exchange reference rates.",
        "description_fr": "Cours de change de référence de l'euro.",
        "series_count": 2,
    },
]

# Real-shape series payload from the single full ``series`` export. ``path_en`` is
# a JSON array (Webstat's real shape); ``title_long_*`` may be empty per row.
_BDF_SERIES_JSON = [
    {
        "series_key": "EXR.M.USD.EUR.SP00.E",
        "dataset_id": "EXR",
        "title_en": "US dollar/Euro spot rate",
        "title_fr": "Taux de change dollar US/Euro",
        "title_long_en": "US dollar (USD)/Euro (EUR) spot exchange rate, monthly average",
        "title_long_fr": "Taux de change dollar US (USD)/Euro (EUR), moyenne mensuelle",
        "freq": "M",
        "ref_area": "FR",
        "source_agency": "ECB",
        "first_time_period_date": "1999-01-01",
        "last_time_period_date": "2026-02-01",
        "path_en": ["Rates and prices/Exchange rates"],
        "path_fr": ["Taux et cours/Taux de change"],
    },
    {
        "series_key": "EXR.M.GBP.EUR.SP00.E",
        "dataset_id": "EXR",
        "title_en": "Pound sterling/Euro spot rate",
        "title_fr": "Taux de change livre sterling/Euro",
        "title_long_en": "",
        "title_long_fr": "",
        "freq": "M",
        "ref_area": "FR",
        "source_agency": "ECB",
        "first_time_period_date": "1999-01-01",
        "last_time_period_date": "2026-02-01",
        "path_en": ["Rates and prices/Exchange rates"],
        "path_fr": ["Taux et cours/Taux de change"],
    },
]


# ---------------------------------------------------------------------------
# Public surface / collection
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    assert {c.name for c in CONNECTORS} == {"bdf_fetch", "enumerate_bdf", "bdf_search"}


def test_load_binds_api_key_off_call_surface() -> None:
    bundle = load(api_key=_KEY)
    # api_key disappears from the exposed signature on the keyed verbs.
    assert "api_key" not in bundle["bdf_fetch"].exposed_signature.parameters
    assert "api_key" not in bundle["enumerate_bdf"].exposed_signature.parameters
    # bdf_search is keyless — unaffected.
    assert "bdf_search" in bundle.names()


# ---------------------------------------------------------------------------
# bdf_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_bdf_fetch_parses_json_response() -> None:
    route = respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=_BDF_OBS_JSON))

    result = bdf_fetch.bind(api_key=_KEY)(key="EXR.M.USD.EUR.SP00.E")

    assert result.provenance.source == "bdf_fetch"
    df = result.raw
    assert list(df.columns) == ["key", "title", "date", "value"]
    assert len(df) == 2
    assert df.iloc[0]["title"] == "US dollar/Euro spot rate"
    assert df["date"].dtype.kind == "M"  # coerced in bdf_fetch
    assert df["value"].dtype.kind == "f"  # coerced in bdf_fetch
    assert df["value"].tolist() == [1.0832, 1.0874]

    # api_key rides the Authorization header (header auth → never a query param).
    sent = route.calls.last.request
    assert sent.headers["Authorization"] == f"Apikey {_KEY}"
    assert _KEY not in str(sent.url)


@respx.mock
def test_bdf_fetch_does_not_leak_secret_into_provenance() -> None:
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=_BDF_OBS_JSON))

    result = bdf_fetch.bind(api_key=_KEY)(key="EXR.M.USD.EUR.SP00.E")

    assert_no_secret_leak(result, secret=_KEY)
    assert "api_key" not in result.provenance.params


@respx.mock
def test_bdf_fetch_applies_period_filters_to_where_clause() -> None:
    route = respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=_BDF_OBS_JSON))

    bdf_fetch.bind(api_key=_KEY)(
        key="EXR.M.USD.EUR.SP00.E",
        start_period="2020-01-01",
        end_period="2020-12-31",
    )

    where = route.calls.last.request.url.params["where"]
    assert 'series_key="EXR.M.USD.EUR.SP00.E"' in where
    assert "time_period_start>=date'2020-01-01'" in where
    assert "time_period_start<=date'2020-12-31'" in where


@respx.mock
def test_bdf_fetch_handles_null_obs_value() -> None:
    # BdF marks missing observations with a null obs_value (OBS_STATUS=M).
    payload = [
        {"series_key": "X", "title_en": "X", "time_period_start": "2026-01-01", "obs_value": None},
        {"series_key": "X", "title_en": "X", "time_period_start": "2026-01-02", "obs_value": 3.84},
    ]
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=payload))

    df = bdf_fetch.bind(api_key=_KEY)(key="X").raw
    assert len(df) == 2
    assert pd.isna(df.iloc[0]["value"])
    assert df.iloc[1]["value"] == 3.84


@respx.mock
def test_bdf_fetch_raises_empty_data_on_empty_array() -> None:
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=[]))

    with pytest.raises(EmptyDataError) as exc:
        bdf_fetch.bind(api_key=_KEY)(key="XX")
    assert exc.value.query_params == {"key": "XX"}


@respx.mock
def test_bdf_fetch_raises_empty_data_when_no_rows_parse() -> None:
    # 200 with rows that all lack a usable date → nothing parses → EmptyData.
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=[{"obs_value": 1.0}]))

    with pytest.raises(EmptyDataError):
        bdf_fetch.bind(api_key=_KEY)(key="EXR.M.USD.EUR.SP00.E")


@respx.mock
def test_bdf_fetch_raises_parse_error_on_non_list_body() -> None:
    # 200 but the body is an object, not the expected array of rows.
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json={"error": "nope"}))

    with pytest.raises(ParseError):
        bdf_fetch.bind(api_key=_KEY)(key="EXR.M.USD.EUR.SP00.E")


def test_bdf_fetch_rejects_empty_key() -> None:
    with pytest.raises(InvalidParameterError):
        bdf_fetch.bind(api_key=_KEY)(key="   ")


def test_bdf_fetch_rejects_malformed_period() -> None:
    with pytest.raises(InvalidParameterError):
        bdf_fetch.bind(api_key=_KEY)(key="EXR.M.USD.EUR.SP00.E", start_period="2020")


def test_bdf_fetch_no_key_fast_fails_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BDF_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc:
        bdf_fetch(key="EXR.M.USD.EUR.SP00.E", api_key="")
    assert exc.value.env_var == "BDF_API_KEY"
    assert exc.value.provider == "bdf"


@respx.mock
def test_bdf_fetch_env_fallback_supplies_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BDF_API_KEY", _KEY)
    route = respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=_BDF_OBS_JSON))

    result = bdf_fetch(key="EXR.M.USD.EUR.SP00.E")

    assert not result.raw.empty
    assert route.calls.last.request.headers["Authorization"] == f"Apikey {_KEY}"


# ---------------------------------------------------------------------------
# enumerate_bdf  (bounded via the export seams — never a full 41k-row build)
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_bdf_bounded_shape_and_metadata() -> None:
    respx.get(_DATASETS_URL).mock(return_value=httpx.Response(200, json=_BDF_DATASETS_JSON))
    respx.get(_SERIES_URL).mock(return_value=httpx.Response(200, json=_BDF_SERIES_JSON))

    result = enumerate_bdf.bind(api_key=_KEY)()
    df = result.raw

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    # One dataset stub + two series rows.
    assert len(df) == 3
    entity_types = df["entity_type"].tolist()
    assert entity_types.count("dataset") == 1
    assert entity_types.count("series") == 2

    stub = df[df["entity_type"] == "dataset"].iloc[0]
    assert stub["code"] == "dataset:EXR"
    assert stub["dataset_id"] == "EXR"
    assert "Exchange rates" in stub["description"]

    # Series rows carry REAL metadata, not blanks/constants (eia dead-metadata lesson).
    series = df[df["entity_type"] == "series"]
    assert set(series["code"]) == {"EXR.M.USD.EUR.SP00.E", "EXR.M.GBP.EUR.SP00.E"}
    assert series["title"].astype(str).str.len().gt(0).all()
    assert series["description"].astype(str).str.len().gt(0).all()
    assert (series["frequency"] == "M").all()
    assert (series["ref_area"] == "FR").all()
    assert series["source_agency"].astype(str).str.len().gt(0).all()
    assert series["first_time_period"].astype(str).str.len().gt(0).all()
    # The breadcrumb path is carried (bilingual recall signal).
    assert series["path"].astype(str).str.contains("Exchange rates").all()
    # The description folds in the French title for cross-language recall.
    usd = series[series["code"] == "EXR.M.USD.EUR.SP00.E"].iloc[0]
    assert "dollar US" in usd["description"]

    # build_entities round-trips on the real-shape slice.
    entities = list(Result(raw=df, output_spec=BDF_ENUMERATE_OUTPUT).entities.values())
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdf"


@respx.mock
def test_enumerate_bdf_emits_stub_only_on_series_fetch_failure() -> None:
    respx.get(_DATASETS_URL).mock(return_value=httpx.Response(200, json=_BDF_DATASETS_JSON))
    # The series export fails on every attempt → None → stubs still emitted.
    respx.get(_SERIES_URL).mock(return_value=httpx.Response(500, text="boom"))

    result = enumerate_bdf.bind(api_key=_KEY)()
    df = result.raw

    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert len(df) == 1
    assert df.iloc[0]["entity_type"] == "dataset"


@respx.mock
def test_enumerate_bdf_emits_series_only_when_dataset_list_fails() -> None:
    # The two sources are independent: a failed dataflow list does NOT block the
    # full series enumeration (the series table is the real universe).
    respx.get(_DATASETS_URL).mock(return_value=httpx.Response(500, text="boom"))
    respx.get(_SERIES_URL).mock(return_value=httpx.Response(200, json=_BDF_SERIES_JSON))

    df = enumerate_bdf.bind(api_key=_KEY)().raw
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert (df["entity_type"] == "series").all()
    assert len(df) == 2


@respx.mock
def test_enumerate_bdf_empty_catalog_when_both_sources_fail() -> None:
    respx.get(_DATASETS_URL).mock(return_value=httpx.Response(500, text="boom"))
    respx.get(_SERIES_URL).mock(return_value=httpx.Response(500, text="boom"))

    df = enumerate_bdf.bind(api_key=_KEY)().raw
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert df.empty


@respx.mock
def test_enumerate_bdf_sends_auth_header_and_does_not_leak() -> None:
    ds_route = respx.get(_DATASETS_URL).mock(return_value=httpx.Response(200, json=_BDF_DATASETS_JSON))
    respx.get(_SERIES_URL).mock(return_value=httpx.Response(200, json=_BDF_SERIES_JSON))

    result = enumerate_bdf.bind(api_key=_KEY)()

    assert ds_route.calls.last.request.headers["Authorization"] == f"Apikey {_KEY}"
    assert _KEY not in str(ds_route.calls.last.request.url)
    assert_no_secret_leak(result, secret=_KEY)
    assert "api_key" not in result.provenance.params


def test_enumerate_bdf_no_key_fast_fails_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BDF_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc:
        enumerate_bdf(api_key="")
    assert exc.value.env_var == "BDF_API_KEY"
    assert exc.value.provider == "bdf"


def test_enumerate_bdf_series_seam_is_monkeypatchable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The _list_all_series seam can be swapped to bound the crawl in tests.

    Patching it means the full ~41k-row series export never fires — only the
    dataset-list request (here mocked) and the patched slice are used.
    """

    def _fake_series(_fetcher: object) -> Any:
        return _BDF_SERIES_JSON

    monkeypatch.setattr(enum_mod, "_list_all_series", _fake_series)

    with respx.mock:
        # No series route registered — if the seam fired a real request respx
        # would raise. Only the dataset-list route is needed.
        respx.get(_DATASETS_URL).mock(return_value=httpx.Response(200, json=_BDF_DATASETS_JSON))
        result = enumerate_bdf.bind(api_key=_KEY)()

    df = result.raw
    assert len(df) == 3  # 1 stub + 2 series, from the patched slice


# ---------------------------------------------------------------------------
# bdf_search  (over a tiny in-process fixture catalog — never a cold build)
# ---------------------------------------------------------------------------


def _enumerate_rows() -> list[dict[str, str]]:
    """Three real enumerator-shaped series rows for a fixture catalog."""
    base: dict[str, str] = {name: "" for name in ENUMERATE_COLUMNS}
    return [
        {
            **base,
            "code": "EXR.M.USD.EUR.SP00.E",
            "title": "US dollar/Euro spot exchange rate",
            "description": "US dollar (USD)/Euro (EUR) spot exchange rate, monthly average.",
            "entity_type": "series",
            "dataset_id": "EXR",
            "frequency": "M",
            "ref_area": "FR",
            "source_agency": "ECB",
        },
        {
            **base,
            "code": "ICP.M.FR.N.000000.4.ANR",
            "title": "France HICP all-items annual rate of change",
            "description": "Harmonised index of consumer prices, France, annual rate of change.",
            "entity_type": "series",
            "dataset_id": "ICP",
            "frequency": "M",
            "ref_area": "FR",
            "source_agency": "ECB",
        },
        {
            **base,
            "code": "RPP.Q.FR.N.A.D.00.0.0.0",
            "title": "France residential property prices",
            "description": "Residential property price index for France, quarterly.",
            "entity_type": "series",
            "dataset_id": "RPP",
            "frequency": "Q",
            "ref_area": "FR",
            "source_agency": "ECB",
        },
    ]


def _build_fixture_catalog(out_dir: Path) -> None:
    df = pd.DataFrame(_enumerate_rows(), columns=list(ENUMERATE_COLUMNS))
    entries = list(Result(raw=df, output_spec=BDF_ENUMERATE_OUTPUT).entities.values())
    catalog = Catalog("bdf", indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    catalog.save(out_dir)


def test_bdf_search_ranks_over_fixture_catalog(tmp_path: Path) -> None:
    out_dir = tmp_path / "bdf_catalog"
    _build_fixture_catalog(out_dir)

    result = bdf_search(query="dollar euro exchange rate", limit=5, catalog_url=str(out_dir))

    sdf = result.raw
    assert list(sdf.columns) == ["code", "title", "coverage", "score", "matched"]
    assert not sdf.empty
    assert len(sdf) <= 3
    assert sdf.iloc[0]["code"] == "EXR.M.USD.EUR.SP00.E"
    assert sdf["score"].notna().all()

    # Ranking discriminates: a different query surfaces a different top hit.
    infl = bdf_search(query="consumer prices annual rate of change", limit=5, catalog_url=str(out_dir))
    assert infl.raw.iloc[0]["code"] == "ICP.M.FR.N.000000.4.ANR"


def test_bdf_search_raises_empty_data_on_no_match(tmp_path: Path) -> None:
    out_dir = tmp_path / "bdf_catalog"
    _build_fixture_catalog(out_dir)

    with pytest.raises(EmptyDataError):
        bdf_search(query="zzzzz nonexistent xyzzy plover", limit=5, catalog_url=str(out_dir))
