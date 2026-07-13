"""Offline tests for the Banco de Portugal connectors.

BdP BPstat is public (no api_key); the template 401/429 contract does not apply.
These mocks exercise the JSON-stat 2.0 response shape for ``bdp_fetch`` and the
two-level paginated crawl for ``enumerate_bdp`` against a bounded, monkeypatched
domain slice. Bilingual enrichment is a *build-time* concern (see
``test_apply_enrichment``); the enumerator itself is crawl-only.
"""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd
import pytest
import respx
from parsimony.catalog.source import entities_from_raw
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_bdp import CONNECTORS
from parsimony_bdp.connectors import enumerate as enum_mod
from parsimony_bdp.connectors._catalog import apply_enrichment
from parsimony_bdp.connectors.enumerate import enumerate_bdp
from parsimony_bdp.connectors.fetch import bdp_fetch
from parsimony_bdp.outputs import BDP_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

_DATASET_URL = "https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    assert {c.name for c in CONNECTORS} == {"bdp_fetch", "enumerate_bdp", "bdp_search"}


def test_bdp_fetch_namespace_hint() -> None:
    assert dict(bdp_fetch.namespace_hints) == {"dataset_id": "bdp"}


# ---------------------------------------------------------------------------
# bdp_fetch
# ---------------------------------------------------------------------------


def _json_stat(
    *,
    dates: list[str] | None = None,
    values: list[float | None] | None = None,
    series: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dates = dates if dates is not None else ["2026-01-01", "2026-02-01"]
    values = values if values is not None else [100.0, 101.5]
    series = series if series is not None else [{"id": "s1", "label": "Consumer Prices"}]
    return {
        "role": {"time": ["reference_date"]},
        "dimension": {"reference_date": {"category": {"index": dates}}},
        "value": values,
        "extension": {"series": series},
    }


@respx.mock
def test_bdp_fetch_parses_json_stat_response() -> None:
    respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json=_json_stat()))

    result = bdp_fetch(domain_id=11, dataset_id="ABC")

    assert result.provenance.source == "bdp_fetch"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["series_id"] == "s1"
    assert df.iloc[0]["title"] == "Consumer Prices"
    assert df["date"].dtype.kind == "M"
    assert df["value"].dtype.kind == "f"
    assert df["value"].iloc[0] == pytest.approx(100.0)


@respx.mock
def test_bdp_fetch_melts_multiple_series_single_request() -> None:
    """Two series × two dates = a row-major value array of four; both surface
    from one request."""
    route = respx.get(_DATASET_URL).mock(
        return_value=httpx.Response(
            200,
            json=_json_stat(
                values=[1.0, 2.0, 3.0, 4.0],
                series=[{"id": "s1", "label": "First"}, {"id": "s2", "label": "Second"}],
            ),
        )
    )

    df = (bdp_fetch(domain_id=11, dataset_id="ABC")).data

    assert len(route.calls) == 1
    assert set(df["series_id"]) == {"s1", "s2"}
    assert df[df["series_id"] == "s2"]["value"].tolist() == [3.0, 4.0]


@respx.mock
def test_bdp_fetch_tolerates_null_value() -> None:
    respx.get(_DATASET_URL).mock(
        return_value=httpx.Response(200, json=_json_stat(values=[100.0, None]))
    )
    df = (bdp_fetch(domain_id=11, dataset_id="ABC")).data
    assert df["value"].isna().any()
    assert df["value"].notna().any()


@respx.mock
def test_bdp_fetch_drops_none_params_and_uppercases_lang() -> None:
    route = respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json=_json_stat()))

    bdp_fetch(domain_id=11, dataset_id="ABC")

    sent = route.calls.last.request
    assert sent.url.params["lang"] == "EN"
    assert "series_ids" not in sent.url.params
    assert "obs_since" not in sent.url.params
    assert "obs_to" not in sent.url.params


@respx.mock
def test_bdp_fetch_forwards_filter_and_window_params() -> None:
    route = respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json=_json_stat()))

    bdp_fetch(
        domain_id=11,
        dataset_id="ABC",
        series_ids="s1,s2",
        start_date="2024-01-01",
        end_date="2024-12-31",
        lang="pt",
    )

    sent = route.calls.last.request
    assert sent.url.params["series_ids"] == "s1,s2"
    assert sent.url.params["obs_since"] == "2024-01-01"
    assert sent.url.params["obs_to"] == "2024-12-31"
    assert sent.url.params["lang"] == "PT"


@respx.mock
def test_bdp_fetch_raises_empty_data_on_no_observations() -> None:
    respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json={"role": {}, "dimension": {}, "value": []}))

    with pytest.raises(EmptyDataError) as exc:
        bdp_fetch(domain_id=11, dataset_id="ABC")
    assert exc.value.query_params["dataset_id"] == "ABC"


@respx.mock
def test_bdp_fetch_raises_parse_error_on_non_dict_shape() -> None:
    # HTTP 200 but a JSON list, not the expected JSON-stat object -> ParseError (§5.8).
    respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    with pytest.raises(ParseError):
        bdp_fetch(domain_id=11, dataset_id="ABC")


@respx.mock
def test_bdp_fetch_maps_404_to_provider_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(_DATASET_URL).mock(return_value=httpx.Response(404, text="<html>Not Found</html>"))

    with pytest.raises(ProviderError) as exc:
        bdp_fetch(domain_id=11, dataset_id="ABC")
    assert exc.value.status_code == 404


def test_bdp_fetch_rejects_empty_dataset_id() -> None:
    with pytest.raises(InvalidParameterError):
        bdp_fetch(domain_id=11, dataset_id="   ")


def test_bdp_fetch_rejects_unknown_lang() -> None:
    with pytest.raises(InvalidParameterError):
        bdp_fetch(domain_id=11, dataset_id="ABC", lang="fr")


def test_bdp_fetch_rejects_malformed_period() -> None:
    with pytest.raises(InvalidParameterError):
        bdp_fetch(domain_id=11, dataset_id="ABC", start_date="01/2024")


# ---------------------------------------------------------------------------
# enumerate_bdp — bounded, mocked two-level paginated crawl
# ---------------------------------------------------------------------------

_BASE = "https://bpstat.bportugal.pt/data/v1"
_DOMAIN = 48
_DS1 = "ds1hex"
_DS2 = "ds2hex"

_BOUNDED_DOMAINS = [
    {
        "id": _DOMAIN,
        "label": "Coincident indicators",
        "description": "Activity indicators",
        "has_series": True,
        "num_series": 3,
        "num_datasets": 2,
        "obs_updated_at": "2026-01-01",
    }
]


def _detail(series: list[dict[str, Any]], next_page: str | None = None) -> dict[str, Any]:
    return {
        "role": {"time": ["reference_date"]},
        "dimension": {"reference_date": {"category": {"index": ["2026-01-01"]}}},
        "value": [1.0] * len(series),
        "extension": {"series": series, "next_page": next_page},
    }


def _ds_list(label: str, ds_id: str, n: int, next_page: str | None) -> dict[str, Any]:
    """A one-item datasets-list page payload."""
    item = {"label": label, "extension": {"id": ds_id, "num_series": n, "obs_updated_at": "2026-01-01"}}
    return {"link": {"item": [item]}, "extension": {"next_page": next_page}}


def _mock_two_level_crawl() -> None:
    """A domain whose datasets list paginates (page1→page2) AND whose first
    dataset detail paginates (page1→page2) — the two completeness levers."""
    # Datasets list: page 1 (ds1) → next_page → page 2 (ds2), no further.
    respx.get(f"{_BASE}/domains/{_DOMAIN}/datasets/", params={"page": "2"}).mock(
        return_value=httpx.Response(200, json=_ds_list("Dataset two", _DS2, 1, None))
    )
    respx.get(f"{_BASE}/domains/{_DOMAIN}/datasets/").mock(
        return_value=httpx.Response(
            200,
            json=_ds_list("Dataset one", _DS1, 2, f"{_BASE}/domains/{_DOMAIN}/datasets/?lang=EN&page=2"),
        )
    )
    # ds1 detail: page 1 (s1) → next_page → page 2 (s2).
    respx.get(f"{_BASE}/domains/{_DOMAIN}/datasets/{_DS1}/", params={"page": "2"}).mock(
        return_value=httpx.Response(200, json=_detail([{"id": "s2", "label": "Consumption coincident indicator"}]))
    )
    respx.get(f"{_BASE}/domains/{_DOMAIN}/datasets/{_DS1}/").mock(
        return_value=httpx.Response(
            200,
            json=_detail(
                [{"id": "s1", "label": "Activity coincident indicator"}],
                next_page=f"{_BASE}/domains/{_DOMAIN}/datasets/{_DS1}/?lang=EN&page=2&page_size=100&obs_last_n=1",
            ),
        )
    )
    # ds2 detail: single page (s3).
    respx.get(f"{_BASE}/domains/{_DOMAIN}/datasets/{_DS2}/").mock(
        return_value=httpx.Response(200, json=_detail([{"id": "s3", "label": "Investment coincident indicator"}]))
    )


@respx.mock
def test_enumerate_bdp_two_level_paginated_crawl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enum_mod, "_list_domains", lambda fetcher: _async_return(_BOUNDED_DOMAINS))
    _mock_two_level_crawl()

    df = (enumerate_bdp()).data

    # @enumerator enforces an EXACT column match.
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    # 1 domain + 2 datasets + 3 series (s1,s2 from ds1's two pages, s3 from ds2).
    assert set(df["entity_type"]) == {"domain", "dataset", "series"}
    assert (df["entity_type"] == "dataset").sum() == 2
    series = df[df["entity_type"] == "series"]
    assert set(series["code"]) == {
        f"{_DOMAIN}:{_DS1}:s1",
        f"{_DOMAIN}:{_DS1}:s2",
        f"{_DOMAIN}:{_DS2}:s3",
    }
    # Datasets-list pagination fix: ds2 (page 2 of the list) is present.
    assert any(df["code"] == f"dataset:{_DOMAIN}:{_DS2}")
    # Dataset-detail pagination: s2 (page 2 of ds1's detail) is present.
    assert any(series["code"] == f"{_DOMAIN}:{_DS1}:s2")
    # Crawl-only: EN title, real description prose, empty PT/short_label.
    assert series["title"].str.len().gt(0).all()
    assert series["description"].str.contains("Banco de Portugal").all()
    assert (series["title_pt"] == "").all()
    assert (df["source"] == "bpstat").all()

    # entities_from_raw round-trips on the real slice (catalog-build entry point).
    entities = entities_from_raw(df, BDP_ENUMERATE_OUTPUT)
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdp"


@respx.mock
def test_enumerate_bdp_empty_catalog_on_domains_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enum_mod, "_list_domains", lambda fetcher: _async_return([]))

    df = (enumerate_bdp()).data
    assert len(df) == 0
    assert list(df.columns) == list(ENUMERATE_COLUMNS)


# ---------------------------------------------------------------------------
# Build-time bilingual enrichment (pure overlay)
# ---------------------------------------------------------------------------


def test_apply_enrichment_folds_bilingual_metadata() -> None:
    base = dict.fromkeys(ENUMERATE_COLUMNS, "")
    rows = [
        {**base, "code": f"domain:{_DOMAIN}", "entity_type": "domain", "title": "Coincident", "description": "dom"},
        {
            **base,
            "code": f"{_DOMAIN}:{_DS1}:s1",
            "entity_type": "series",
            "title": "Activity coincident indicator",
            "description": "Activity coincident indicator. Banco de Portugal BPstat.",
        },
    ]
    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))

    enriched = apply_enrichment(
        df,
        enrich_en={
            "s1": {
                "label": "Activity coincident indicator",
                "short_label": "Activity coinc. PT M",
                "description": "Coincident indicators - Economic activity - Portugal - Monthly",
            }
        },
        enrich_pt={
            "s1": {
                "label": "Indicador coincidente para a atividade",
                "short_label": "",
                "description": "Indicadores coincidentes - atividade economica - Portugal - Mensal",
            }
        },
    )

    # Input frame is not mutated (immutable overlay).
    assert df.loc[1, "title_pt"] == ""

    srow = enriched[enriched["entity_type"] == "series"].iloc[0]
    assert srow["short_label"] == "Activity coinc. PT M"
    assert srow["title_pt"] == "Indicador coincidente para a atividade"
    # Both languages folded into the indexed description.
    assert "Portugal" in srow["description"]
    assert "Mensal" in srow["description"]
    assert srow["description"].startswith("Activity coincident indicator")
    # Stub rows pass through untouched.
    assert (enriched[enriched["entity_type"] == "domain"]["title_pt"] == "").all()


def _async_return(value: Any) -> Any:
    return value
