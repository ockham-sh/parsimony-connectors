"""Tests for ``parsimony_sdmx.CATALOGS`` and ``RESOLVE_CATALOG``.

Replaces the deleted ``test_catalog_planning.py`` — the plan-generator
machinery moved from ``parsimony_sdmx._catalog_planning`` into the
top-level plugin surface consumed by :func:`parsimony.publish.publish`.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import parsimony_sdmx
from parsimony_sdmx import CATALOGS, RESOLVE_CATALOG
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import series_namespace


def _write_datasets(root: Path, agency: str, dataset_ids: list[str]) -> None:
    (root / agency).mkdir(parents=True)
    table = pa.Table.from_pylist(
        [{"dataset_id": did, "agency_id": agency, "title": f"title {did}"} for did in dataset_ids],
        schema=pa.schema(
            [
                pa.field("dataset_id", pa.string(), nullable=False),
                pa.field("agency_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
            ]
        ),
    )
    pq.write_table(table, root / agency / "datasets.parquet")


async def _collect(gen):
    out = []
    async for item in gen:
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# CATALOGS async generator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalogs_yields_static_datasets_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))
    entries = await _collect(parsimony_sdmx.CATALOGS())
    assert entries[0][0] == "sdmx_datasets"
    # Not the raw Connector — it's wrapped in a closure that binds
    # ``outputs_root`` to whatever PARSIMONY_SDMX_OUTPUTS_ROOT resolves to.
    assert callable(entries[0][1])


@pytest.mark.asyncio
async def test_catalogs_fans_out_to_one_per_dataset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_datasets(tmp_path, "ECB", ["YC", "MIR"])
    _write_datasets(tmp_path, "ESTAT", ["prc_hicp_manr"])
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))

    entries = await _collect(CATALOGS())
    namespaces = {ns for ns, _ in entries}
    assert namespaces == {
        "sdmx_datasets",
        "sdmx_series_ecb_yc",
        "sdmx_series_ecb_mir",
        "sdmx_series_estat_prc_hicp_manr",
    }


@pytest.mark.asyncio
async def test_catalogs_skips_agencies_without_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_datasets(tmp_path, "ECB", ["YC"])  # only ECB built locally
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))

    entries = await _collect(CATALOGS())
    namespaces = {ns for ns, _ in entries}
    assert namespaces == {"sdmx_datasets", "sdmx_series_ecb_yc"}


# ---------------------------------------------------------------------------
# RESOLVE_CATALOG — targeted namespace lookup
# ---------------------------------------------------------------------------


def test_resolve_catalog_returns_static_datasets_enumerator() -> None:
    fn = RESOLVE_CATALOG("sdmx_datasets")
    # Wrapped closure that binds ``outputs_root`` at call time — compare by
    # callability + identifying suffix rather than by Connector identity.
    assert callable(fn)
    assert fn is not None
    assert "datasets" in fn.__name__


def test_resolve_catalog_parses_simple_agency_namespace() -> None:
    fn = RESOLVE_CATALOG("sdmx_series_ecb_yc")
    assert callable(fn)
    # Callable name carries the bound (agency, dataset_id) for readability.
    assert fn is not None
    assert "ECB" in fn.__name__
    assert "yc" in fn.__name__


def test_resolve_catalog_parses_multi_token_agency_namespace() -> None:
    """``imf_data`` is a two-token agency — parser must prefer the longest
    agency match over the greedy ``imf`` prefix.
    """
    fn = RESOLVE_CATALOG("sdmx_series_imf_data_pgi")
    assert fn is not None
    assert "IMF_DATA" in fn.__name__


def test_resolve_catalog_rejects_unknown_agency() -> None:
    assert RESOLVE_CATALOG("sdmx_series_unknown_yc") is None


def test_resolve_catalog_rejects_missing_dataset_tail() -> None:
    # `sdmx_series_ecb` has no dataset_id component — reject.
    assert RESOLVE_CATALOG("sdmx_series_ecb") is None


def test_resolve_catalog_returns_none_for_foreign_namespace() -> None:
    assert RESOLVE_CATALOG("fred") is None
    assert RESOLVE_CATALOG("some_other_plugin_namespace") is None


def test_series_namespace_helper_matches_resolver_parse() -> None:
    """Round-trip: composing the namespace via :func:`series_namespace` must
    be parseable by ``RESOLVE_CATALOG`` back to a callable.
    """
    ns = series_namespace(AgencyId.ECB, "YC")
    assert RESOLVE_CATALOG(ns) is not None
