"""Happy-path + branch tests for the Banco de Portugal connectors.

BdP BPstat is public (no api_key); the template 401/429 contract does not
apply. The mocks exercise the JSON-stat 2.0 response shape for ``bdp_fetch``
and the (domains → datasets → detail) crawl for ``enumerate_bdp`` against a
bounded, monkeypatched domain slice.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

import parsimony_bdp
from parsimony_bdp import (
    BDP_ENUMERATE_OUTPUT,
    CONNECTORS,
    bdp_fetch,
    enumerate_bdp,
)

_DATASET_URL = "https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bdp_fetch", "enumerate_bdp", "bdp_search"}


def test_bdp_fetch_namespace_hint() -> None:
    assert dict(bdp_fetch.namespace_hints) == {"dataset_id": "bdp"}


# ---------------------------------------------------------------------------
# bdp_fetch
# ---------------------------------------------------------------------------


def _json_stat(
    *,
    dates: list[str] | None = None,
    values: list[float] | None = None,
    series: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """A minimal JSON-stat 2.0 dataset-detail payload matching the live shape."""
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
    # Real coercion: declared dtypes apply.
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
def test_bdp_fetch_drops_none_params_and_uppercases_lang() -> None:
    route = respx.get(_DATASET_URL).mock(return_value=httpx.Response(200, json=_json_stat()))

    bdp_fetch(domain_id=11, dataset_id="ABC")

    sent = route.calls.last.request
    # fetch_json drops None-valued params — no series_ids/obs_since/obs_to leak.
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
    # EmptyData carries query_params for parameter-adjustment hints (no DO NOT retry).
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

    # BPstat returns 404 (HTML body) for an unknown dataset; canonical mapping
    # surfaces it as ProviderError(404).
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


# ---------------------------------------------------------------------------
# enumerate_bdp — bounded, mocked crawl
# ---------------------------------------------------------------------------

_DATASET_ID = "ds123"
# A single small leaf domain to bound the offline crawl.
_BOUNDED_DOMAINS = [
    {
        "id": 48,
        "label": "Coincident indicators",
        "description": "Activity indicators",
        "has_series": True,
        "num_series": 2,
        "num_datasets": 1,
        "obs_updated_at": "2026-01-01",
    }
]


def _mock_enumerate_routes() -> None:
    base = "https://bpstat.bportugal.pt/data/v1"
    # Dataset list under domain 48.
    respx.get(f"{base}/domains/48/datasets/").mock(
        return_value=httpx.Response(
            200,
            json={
                "link": {
                    "item": [
                        {
                            "label": "Coincident indicators dataset",
                            "extension": {
                                "id": _DATASET_ID,
                                "num_series": 2,
                                "obs_updated_at": "2026-01-01",
                            },
                        }
                    ]
                }
            },
        )
    )
    # Dataset detail (single page, two series).
    respx.get(f"{base}/domains/48/datasets/{_DATASET_ID}/").mock(
        return_value=httpx.Response(
            200,
            json={
                "role": {"time": ["reference_date"]},
                "dimension": {
                    "reference_date": {
                        "label": "Reference date",
                        "category": {"index": ["2026-01-01", "2026-02-01"]},
                    },
                    "29": {"label": "Unit of measure", "category": {"label": {"u": "Percent"}}},
                    "p": {"label": "Periodicity", "category": {"label": {"m": "Monthly"}}},
                },
                "value": [4.4, 4.2, 3.8, 3.6],
                "extension": {
                    "series": [
                        {"id": "12099329", "label": "Activity coincident indicator"},
                        {"id": "12099330", "label": "Consumption coincident indicator"},
                    ],
                    "next_page": None,
                },
            },
        )
    )
    # PT-label sweep.
    respx.get(f"{base}/series/").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"id": "12099329", "label": "Indicador coincidente de atividade"},
                {"id": "12099330", "label": "Indicador coincidente de consumo"},
            ],
        )
    )


@respx.mock
def test_enumerate_bdp_bounded_crawl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl one mocked leaf domain; assert exact enumerator schema + real
    content across domain/dataset/series rows."""
    monkeypatch.setattr(
        parsimony_bdp,
        "_list_domains",
        lambda fetcher: _async_return(_BOUNDED_DOMAINS),
    )
    _mock_enumerate_routes()

    result = enumerate_bdp()
    df = result.data

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in BDP_ENUMERATE_OUTPUT.columns]
    # 1 domain row + 1 dataset row + 2 series rows.
    assert len(df) == 4
    assert set(df["entity_type"]) == {"domain", "dataset", "series"}

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) == 2
    # Real content in declared metadata columns — not just column presence.
    assert series_rows["title"].str.len().gt(0).all()
    assert series_rows["description"].str.len().gt(0).all()
    assert (series_rows["frequency"] == "Monthly").all()
    assert (series_rows["units"] == "Percent").all()
    # PT labels folded in.
    assert series_rows["title_pt"].str.contains("coincidente").all()
    # KEY shape: series codes are "{domain}:{dataset}:{series}".
    assert (series_rows["code"] == f"48:{_DATASET_ID}:12099329").any()
    assert (df["source"] == "bpstat").all()

    # build_entities round-trips on the real slice (catalog-build entry point).
    entities = BDP_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "bdp"


@respx.mock
def test_enumerate_bdp_empty_catalog_on_domains_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed /domains fetch yields a header-only frame with the declared
    schema columns, not a crash."""
    monkeypatch.setattr(parsimony_bdp, "_list_domains", lambda fetcher: _async_return([]))

    df = (enumerate_bdp()).data
    assert len(df) == 0
    assert list(df.columns) == [c.name for c in BDP_ENUMERATE_OUTPUT.columns]


def _async_return(value: Any) -> Any:
    return value
