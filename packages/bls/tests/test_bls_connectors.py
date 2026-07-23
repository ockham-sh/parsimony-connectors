"""Offline tests for the BLS connectors (respx for the API, monkeypatch for flat files)."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, RateLimitError

from parsimony_bls import CONNECTORS
from parsimony_bls.catalog_build import build_series_catalog, build_surveys_catalog
from parsimony_bls.connectors import enumerate_series as es
from parsimony_bls.connectors.enumerate_series import enumerate_bls_series
from parsimony_bls.connectors.enumerate_surveys import enumerate_bls_surveys
from parsimony_bls.connectors.fetch import bls_fetch


@pytest.fixture(autouse=True)
def _clear_bls_api_key(monkeypatch):
    """Neutralise any ambient ``BLS_API_KEY`` (the workspace ``.env`` exports one).

    Since the connectors now honour the env var, the unkeyed-behaviour tests must
    run with it absent to be deterministic; the env-fallback tests below re-set it
    explicitly (their own ``monkeypatch.setenv`` runs after this fixture).
    """
    monkeypatch.delenv("BLS_API_KEY", raising=False)


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {
        "bls_fetch",
        "enumerate_bls_surveys",
        "enumerate_bls_series",
        "bls_surveys_search",
        "bls_series_search",
    }


# ---------------------------------------------------------------------------
# bls_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_bls_fetch_returns_series_observations() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {
                            "seriesID": "LNS14000000",
                            "catalog": {"series_title": "Unemployment Rate"},
                            "data": [
                                {"year": "2026", "period": "M03", "value": "4.1"},
                                {"year": "2026", "period": "M02", "value": "-"},
                            ],
                        }
                    ]
                },
            },
        )
    )
    result = bls_fetch(series_id="LNS14000000", start_year="2026", end_year="2026")
    df = result.raw
    assert result.provenance.source == "bls_fetch"
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Unemployment Rate"
    assert df.iloc[0]["frequency"] == "Monthly"
    # Rows are sorted ascending by date, so the earlier M02 observation (whose
    # suppressed "-" value coerces to null) is first and the M03 value is last.
    # ``date`` is coerced to datetime in bls_fetch.
    assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2026-02-01", "2026-03-01"]
    assert df.iloc[0]["value"] != df.iloc[0]["value"]  # NaN
    assert df.iloc[1]["value"] == 4.1


def test_bls_fetch_refuses_span_over_unkeyed_cap() -> None:
    # BLS serves at most ~10 years per unkeyed call and silently truncates a
    # wider request; the connector refuses loud before making the call rather
    # than returning a silently-capped window. No network mock: it must raise
    # before any HTTP request.
    with pytest.raises(InvalidParameterError) as exc:
        bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2026")
    assert "10 calendar years" in str(exc.value)
    assert "unkeyed" in str(exc.value)


def test_bls_fetch_refuses_span_over_keyed_cap() -> None:
    # With a key the cap is ~20 years; a 21-year span still refuses.
    with pytest.raises(InvalidParameterError) as exc:
        bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2020", api_key="k")
    assert "20 calendar years" in str(exc.value)
    assert "keyed" in str(exc.value)


@respx.mock
def test_bls_fetch_allows_span_at_unkeyed_cap() -> None:
    # Exactly 10 years (2000..2009 inclusive) is at the cap and must go through.
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {
                            "seriesID": "LNS14000000",
                            "catalog": {"series_title": "Unemployment Rate"},
                            "data": [{"year": "2009", "period": "M01", "value": "9.0"}],
                        }
                    ]
                },
            },
        )
    )
    result = bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2009")
    assert len(result.raw) == 1


@respx.mock
def test_bls_fetch_raises_parse_error_on_bls_status_failure() -> None:
    # BLS signals failure in the body with HTTP 200 -- map a non-success status
    # (that isn't a quota threshold) to ParseError, NOT a fake status_code=0.
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json={"status": "REQUEST_NOT_PROCESSED", "message": ["Invalid series ID"]})
    )
    with pytest.raises(ParseError):
        bls_fetch(series_id="BAD", start_year="2026", end_year="2026")


@respx.mock
def test_bls_fetch_raises_parse_error_on_status_failure() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json={"status": "REQUEST_NOT_PROCESSED", "message": ["Invalid series ID"]})
    )
    with pytest.raises(ParseError):
        bls_fetch(series_id="BAD", start_year="2026", end_year="2026")


@respx.mock
def test_bls_fetch_maps_threshold_to_rate_limit() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_NOT_PROCESSED",
                "message": ["Request could not be serviced, the daily threshold has been reached."],
            },
        )
    )
    with pytest.raises(RateLimitError) as exc_info:
        bls_fetch(series_id="LNS14000000", start_year="2026", end_year="2026")
    assert exc_info.value.quota_exhausted is True


@respx.mock
def test_bls_fetch_raises_empty_data_when_no_series_returned() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}})
    )
    with pytest.raises(EmptyDataError):
        bls_fetch(series_id="XYZ", start_year="2026", end_year="2026")


@respx.mock
def test_bls_fetch_raises_empty_when_no_series() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}})
    )
    with pytest.raises(EmptyDataError):
        bls_fetch(series_id="XYZ", start_year="2026", end_year="2026")


# ---------------------------------------------------------------------------
# BLS_API_KEY env fallback (#86) — the key is optional, but when no api_key is
# passed the connector honours the documented env var for BOTH the payload
# (registrationkey) and the per-call year cap (20 keyed vs 10 unkeyed).
# ---------------------------------------------------------------------------

_SUCCESS_BODY = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {
        "series": [
            {
                "seriesID": "LNS14000000",
                "catalog": {"series_title": "Unemployment Rate"},
                "data": [{"year": "2009", "period": "M01", "value": "9.0"}],
            }
        ]
    },
}


@respx.mock
def test_bls_fetch_env_key_lands_in_payload_and_lifts_cap(monkeypatch) -> None:
    monkeypatch.setenv("BLS_API_KEY", "env-secret")
    route = respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json=_SUCCESS_BODY)
    )
    # 15-year span (2000..2014) exceeds the unkeyed cap of 10 but is under the
    # keyed cap of 20 — it only goes through because the env key lifted the cap.
    result = bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2014")
    assert len(result.raw) == 1

    import json

    body = json.loads(route.calls.last.request.content)
    assert body["registrationkey"] == "env-secret"


def test_bls_fetch_env_key_lifts_cap_without_reaching_network(monkeypatch) -> None:
    # A 21-year span still exceeds even the keyed cap, proving the cap tracks the
    # (env-resolved) key: with the env key the message says "keyed" / "20 years".
    monkeypatch.setenv("BLS_API_KEY", "env-secret")
    with pytest.raises(InvalidParameterError) as exc:
        bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2020")
    assert "20 calendar years" in str(exc.value)
    assert "keyed" in str(exc.value)


@respx.mock
def test_bls_fetch_without_env_key_omits_registrationkey(monkeypatch) -> None:
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    route = respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(200, json=_SUCCESS_BODY)
    )
    bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2009")

    import json

    body = json.loads(route.calls.last.request.content)
    assert "registrationkey" not in body


def test_bls_fetch_without_env_key_keeps_unkeyed_cap(monkeypatch) -> None:
    # No env key → the cap stays at 10, so an 11-year span refuses as "unkeyed".
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    with pytest.raises(InvalidParameterError) as exc:
        bls_fetch(series_id="LNS14000000", start_year="2000", end_year="2010")
    assert "10 calendar years" in str(exc.value)
    assert "unkeyed" in str(exc.value)


@respx.mock
def test_enumerate_surveys_env_key_lands_in_query(monkeypatch) -> None:
    monkeypatch.setenv("BLS_API_KEY", "env-secret")
    route = respx.get("https://api.bls.gov/publicAPI/v2/surveys").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {"survey": [{"survey_abbreviation": "CU", "survey_name": "CPI"}]},
            },
        )
    )
    enumerate_bls_surveys()
    assert "registrationkey=env-secret" in str(route.calls.last.request.url)


@respx.mock
def test_enumerate_surveys_without_env_key_omits_registrationkey(monkeypatch) -> None:
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    route = respx.get("https://api.bls.gov/publicAPI/v2/surveys").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {"survey": [{"survey_abbreviation": "CU", "survey_name": "CPI"}]},
            },
        )
    )
    enumerate_bls_surveys()
    assert "registrationkey" not in str(route.calls.last.request.url)


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_bls_fetch_rejects_non_four_digit_year() -> None:
    with pytest.raises(InvalidParameterError, match="start_year"):
        bls_fetch(series_id="LNS14000000", start_year="26", end_year="2026")


def test_bls_fetch_rejects_bad_year() -> None:
    with pytest.raises(InvalidParameterError, match="start_year"):
        bls_fetch(series_id="LNS14000000", start_year="26", end_year="2026")


def test_bls_fetch_rejects_empty_series_id() -> None:
    with pytest.raises(InvalidParameterError, match="series_id"):
        bls_fetch(series_id="   ", start_year="2026", end_year="2026")


# ---------------------------------------------------------------------------
# enumerate_bls_surveys (tier-1, API)
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_surveys_flags_headline() -> None:
    respx.get("https://api.bls.gov/publicAPI/v2/surveys").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "survey": [
                        {"survey_abbreviation": "CU", "survey_name": "Consumer Price Index"},
                        {"survey_abbreviation": "WS", "survey_name": "Work Stoppages"},
                    ]
                },
            },
        )
    )
    result = enumerate_bls_surveys()
    df = result.raw
    assert list(df.columns) == ["code", "title", "survey", "has_series_catalog"]
    flags = df.set_index("code")["has_series_catalog"].to_dict()
    assert flags["CU"] is True  # headline
    assert flags["WS"] is False  # not headline


# ---------------------------------------------------------------------------
# enumerate_bls_series (tier-2, flat files) -- monkeypatched
# ---------------------------------------------------------------------------

_COLUMNS = ["series_id", "area_code", "item_code", "seasonal", "series_title", "begin_year", "end_year"]
_ROWS = [
    {
        "series_id": "CUUR0000SA0",
        "area_code": "0000",
        "item_code": "SA0",
        "seasonal": "U",
        "series_title": "All items in U.S. city average",
        "begin_year": "1913",
        "end_year": "2026",
    },
    {
        "series_id": "CUUR0000SETB01",
        "area_code": "0000",
        "item_code": "SETB01",
        "seasonal": "U",
        "series_title": "Gasoline (all types) in U.S. city average",
        "begin_year": "1976",
        "end_year": "2026",
    },
]
_TABLES = {
    "area": {"0000": "U.S. city average"},
    "item": {"SA0": "All items", "SETB01": "Gasoline (all types)"},
    "seasonal": {"U": "Not Seasonally Adjusted"},
}


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def _patch_flatfiles(monkeypatch):
    def fake_rows(session, survey, *, max_bytes=0, max_rows=0):
        rows = _ROWS[:max_rows] if max_rows else _ROWS
        return _COLUMNS, rows

    def fake_tables(session, survey, columns=None):
        return _TABLES

    monkeypatch.setattr(es, "make_download_session", lambda: _FakeSession())
    monkeypatch.setattr(es, "fetch_series_rows", fake_rows)
    monkeypatch.setattr(es, "fetch_dimension_tables", fake_tables)


def test_enumerate_series_emits_dimension_metadata(_patch_flatfiles) -> None:
    result = enumerate_bls_series(survey="cu")
    df = result.raw
    assert set(df["code"]) == {"CUUR0000SA0", "CUUR0000SETB01"}
    row = df.set_index("code").loc["CUUR0000SETB01"]
    assert row["title"] == "Gasoline (all types) in U.S. city average"
    assert row["survey"] == "CU"
    # per-dimension code + resolved label columns (dim = column minus _code)
    assert row["area_code"] == "0000"
    assert row["area_label"] == "U.S. city average"
    assert row["item_label"] == "Gasoline (all types)"


def test_build_series_catalog_search_offline(_patch_flatfiles) -> None:
    catalog = build_series_catalog("CU")
    assert len(catalog.entities) == 2
    hits = catalog.search("gasoline", limit=5)
    assert hits[0].code == "CUUR0000SETB01"
    # exact code probe
    code_hits = catalog.search("code: CUUR0000SA0", limit=3)
    assert code_hits[0].code == "CUUR0000SA0"


def test_build_surveys_catalog_attaches_manifest(monkeypatch) -> None:
    # tier-1 enumerate is API-driven; stub it so the test stays offline.
    import pandas as pd
    from parsimony.result import Result

    from parsimony_bls import catalog_build as cb
    from parsimony_bls.outputs import BLS_SURVEYS_ENUM_OUTPUT

    def fake_surveys(api_key=""):
        df = pd.DataFrame([{"code": "CU", "title": "Consumer Price Index", "survey": "CU", "has_series_catalog": True}])
        return Result(raw=df, output_spec=BLS_SURVEYS_ENUM_OUTPUT)

    monkeypatch.setattr(cb, "enumerate_bls_surveys", fake_surveys)

    manifest = [{"id": "item", "values": [{"code": "SA0", "label": "All items"}]}]
    catalog = build_surveys_catalog(manifests={"CU": manifest})
    entity = catalog.entities[0]
    assert entity.code == "CU"
    assert entity.metadata["dimensions"] == manifest


# ---------------------------------------------------------------------------
# search connectors -- row shaping (catalog stubbed)
# ---------------------------------------------------------------------------


def test_bls_series_search_shapes_rows(monkeypatch) -> None:
    from parsimony_bls.connectors import search as se

    class _Match:
        def __init__(self, code, title, ns):
            self.code, self.title, self.score, self.namespace, self.metadata = code, title, 0.5, ns, {}
            self.matched = "lexical"
            self.coverage = 0.0

    class _Catalog:
        def search(self, query=None, limit=10, *, filter=None, field=None):
            return [_Match("CUUR0000SETB01", "Gasoline", "bls_series_cu")]

    def fake_get(namespace, *, catalog_root=None, build=None):
        return _Catalog()

    monkeypatch.setattr(se, "_get_or_load_catalog", fake_get)
    result = se.bls_series_search(query="gasoline", survey="CU")
    df = result.raw
    assert list(df.columns) == ["series_id", "title", "survey", "namespace", "coverage", "score", "matched"]
    assert df.iloc[0]["series_id"] == "CUUR0000SETB01"
    assert df.iloc[0]["survey"] == "CU"


def test_bls_series_search_empty_raises(monkeypatch) -> None:
    from parsimony_bls.connectors import search as se

    class _Catalog:
        def search(self, query=None, limit=10, *, filter=None, field=None):
            return []

    def fake_get(namespace, *, catalog_root=None, build=None):
        return _Catalog()

    monkeypatch.setattr(se, "_get_or_load_catalog", fake_get)
    with pytest.raises(EmptyDataError):
        se.bls_series_search(query="nothingmatches", survey="CU")


# ---------------------------------------------------------------------------
# F4 #3: dimension filters EXCLUDE (exact AND), they do not merely re-rank.
# Built against a real per-survey catalog (stub flat files) so the filter runs
# through the catalog's own entity_matches_filter, not a stub.
# ---------------------------------------------------------------------------


def _real_series_catalog(monkeypatch):
    from parsimony_bls.connectors import search as se

    catalog = build_series_catalog("CU")
    monkeypatch.setattr(se, "_get_or_load_catalog", lambda ns, *, catalog_root=None, build=None: catalog)
    return se


def test_bls_series_search_filter_excludes_nonmatching(_patch_flatfiles, monkeypatch) -> None:
    se = _real_series_catalog(monkeypatch)
    # "city average" is in BOTH series titles, so a soft query alone keeps both.
    both = list(se.bls_series_search(query="city average", survey="CU").raw["series_id"])
    assert {"CUUR0000SA0", "CUUR0000SETB01"} <= set(both)

    # Same query, but an exact item_code filter must DROP the non-matching variant,
    # not merely down-rank it (the F4 hazard).
    filtered = (
        se.bls_series_search(query="city average", survey="CU", filters={"item_code": "SETB01"})
        .raw["series_id"]
        .tolist()
    )
    assert filtered == ["CUUR0000SETB01"]


def test_bls_series_search_filter_only_no_query(_patch_flatfiles, monkeypatch) -> None:
    se = _real_series_catalog(monkeypatch)
    # No text query: a pure dimension filter enumerates the exact-matching series.
    rows = se.bls_series_search(query="", survey="CU", filters={"item_code": "SA0"}).raw
    assert rows["series_id"].tolist() == ["CUUR0000SA0"]


def test_bls_series_search_requires_query_or_filters(_patch_flatfiles, monkeypatch) -> None:
    from parsimony.errors import InvalidParameterError

    se = _real_series_catalog(monkeypatch)
    with pytest.raises(InvalidParameterError):
        se.bls_series_search(query="", survey="CU")
