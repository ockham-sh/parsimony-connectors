"""Tests for the ``sdmx_datasets_search`` connector."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from parsimony.catalog import Catalog, Entity
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from pydantic import ValidationError

from parsimony_sdmx.connectors import datasets_search as search_module
from parsimony_sdmx.connectors.datasets_search import (
    DEFAULT_CATALOG_ROOT,
    PARSIMONY_SDMX_CATALOG_URL_ENV,
    _clear_catalog_lru,
    sdmx_datasets_search,
)


@pytest.fixture(autouse=True)
def _reset_lru() -> Iterator[None]:
    _clear_catalog_lru()
    yield
    _clear_catalog_lru()


def _catalog_with_entities(namespace: str, entities: list[Entity]) -> Catalog:
    catalog = Catalog(namespace)
    catalog.set_entities(entities)
    catalog.build()
    return catalog


def test_sdmx_datasets_search_requires_agency() -> None:
    with pytest.raises((TypeError, ValidationError)):
        sdmx_datasets_search(query="Title")  # type: ignore[call-arg]


def test_sdmx_datasets_search_rejects_unknown_agency() -> None:
    with pytest.raises(ConnectorError, match="Unknown agency"):
        sdmx_datasets_search(query="Title", agency="NOPE")


def test_sdmx_datasets_search_single_agency(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        calls.append(namespace)
        return _catalog_with_entities(
            namespace,
            [
                Entity(
                    namespace=namespace,
                    code="ECB|YC",
                    title="Yield curve",
                    metadata={"dimensions": []},
                )
            ],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    df = sdmx_datasets_search(query="yield", agency="ECB", limit=5).raw
    assert df.iloc[0]["dataset_id"] == "YC"
    assert df.iloc[0]["agency"] == "ECB"
    assert calls == ["sdmx_datasets_ecb"]


def test_search_matches_titles_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """A flow whose non-title metadata matches the query must not surface:
    the search surface is the title — a flow's identity — nothing else."""

    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        return _catalog_with_entities(
            namespace,
            [
                Entity(
                    namespace=namespace,
                    code="ECB|UNE",
                    title="Unemployment rate",
                    metadata={"dimensions": []},
                ),
                Entity(
                    namespace=namespace,
                    code="ECB|ILC",
                    title="Benefit entitlement",
                    metadata={
                        "dimensions": [],
                        "description": "risk; examples: Sickness, Unemployment, Work-related accident",
                    },
                ),
            ],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    df = sdmx_datasets_search(query="unemployment", agency="ECB", limit=5).raw
    assert list(df["dataset_id"]) == ["UNE"]


def test_sdmx_datasets_search_empty_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        search_module,
        "_get_or_load_catalog",
        lambda namespace, **kwargs: _catalog_with_entities(namespace, []),
    )
    with pytest.raises(EmptyDataError, match="agency='ECB'"):
        sdmx_datasets_search(query="nonsense", agency="ECB")


def test_set_catalog_lru_size_rejects_zero() -> None:
    with pytest.raises(InvalidParameterError):
        search_module.set_catalog_lru_size(0)


def test_default_catalog_root_is_hf_dev_sdmx() -> None:
    assert DEFAULT_CATALOG_ROOT == "hf://parsimony-dev/sdmx"


def test_catalog_url_env_constant() -> None:
    assert PARSIMONY_SDMX_CATALOG_URL_ENV == "PARSIMONY_SDMX_CATALOG_URL"


def test_agency_and_dataset_id_feed_sdmx_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Emitted agency + dataset_id must validate as sdmx_fetch params without joining."""
    from parsimony_sdmx.connectors.fetch import SdmxFetchParams

    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        return _catalog_with_entities(
            namespace,
            [
                Entity(
                    namespace=namespace,
                    code="ECB|YC",
                    title="Yield curve",
                    metadata={"dimensions": ["FREQ", "REF_AREA"]},
                )
            ],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    row = sdmx_datasets_search(query="yield", agency="ECB", limit=1).raw.iloc[0]
    params = SdmxFetchParams(
        agency=str(row["agency"]),
        dataset_id=str(row["dataset_id"]),
        series_key="M.EUR",
    )
    assert params.agency == "ECB"
    assert params.dataset_id == "YC"
