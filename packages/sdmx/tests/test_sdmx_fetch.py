"""Tests for ``parsimony_sdmx.connectors.fetch.sdmx_fetch``.

The connector reaches out to the live SDMX endpoint via ``sdmx1.Client``.
We replace ``sdmx_client`` with a context manager yielding a fake client,
and stub ``sdmx.to_pandas`` so the entire test runs without hitting the
network and without depending on real SDMX message shapes.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError, InvalidParameterError


def _fake_dsd(dim_ids: list[str]) -> Any:
    """Fake DSD whose ``dimensions`` iterable yields objects with an ``id``."""
    return SimpleNamespace(
        dimensions=[SimpleNamespace(id=d) for d in dim_ids],
    )


def _make_fake_client(
    *,
    structure_msg: Any,
    data_msg: Any,
    raise_on_get: Exception | None = None,
) -> MagicMock:
    client = MagicMock()
    client.dataflow.return_value = structure_msg
    client.datastructure.return_value = structure_msg
    if raise_on_get is not None:
        client.get.side_effect = raise_on_get
    else:
        client.get.return_value = data_msg
    return client


def _structure_msg(dim_ids: list[str], dataset_id: str) -> Any:
    dsd = _fake_dsd(dim_ids + ["TIME_PERIOD"])
    dataflow = SimpleNamespace(structure=SimpleNamespace(id="DSD_X"))
    return SimpleNamespace(
        dataflow={dataset_id: dataflow},
        structure={"DSD_X": dsd},
        codelist={},
    )


@pytest.fixture
def patch_sdmx(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace ``sdmx_client`` and ``sdmx.to_pandas`` with controllable stubs."""
    state: dict[str, Any] = {"client": None, "to_pandas_input": None}

    @contextmanager
    def _fake_sdmx_client(_agency_id: str, **_kwargs: Any) -> Any:
        yield state["client"]

    monkeypatch.setattr(
        "parsimony_sdmx.providers.sdmx_client.sdmx_client",
        _fake_sdmx_client,
    )

    def _fake_to_pandas(data: Any) -> pd.DataFrame:
        state["to_pandas_input"] = data
        return state["to_pandas_output"]

    import sdmx as sdmx_lib

    monkeypatch.setattr(sdmx_lib, "to_pandas", _fake_to_pandas)
    return state


def _call_sdmx_fetch(params):
    from parsimony_sdmx.connectors.fetch import sdmx_fetch

    return sdmx_fetch(
        dataset_ref=params.dataset_key,
        series_ref=params.series_key,
        start_period=params.start_period,
        end_period=params.end_period,
    )


class TestSdmxFetch:
    def test_returns_table_result_with_expected_schema(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        dim_ids = ["FREQ", "REF_AREA", "CURRENCY"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg,
            data_msg=data_msg,
        )
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["M", "M"],
                "REF_AREA": ["DE", "FR"],
                "CURRENCY": ["EUR", "EUR"],
                "TIME_PERIOD": ["2024-01", "2024-01"],
                "value": [1.0, 2.0],
            }
        ).set_index(["FREQ", "REF_AREA", "CURRENCY", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(
            dataset_key="ECB-YC",
            series_key="M.DE.EUR",
        )
        result = _call_sdmx_fetch(params)

        df = result.data
        # Declared columns lead in config order; the per-flow dims (as {dim}_code,
        # matching sdmx_series_search) + series_url trail (caught by the greedy-last
        # "*" wildcard as METADATA). Dimension labels ride in `title`, not columns.
        assert list(df.columns) == [
            "series_key",
            "title",
            "TIME_PERIOD",
            "value",
            "FREQ_code",
            "REF_AREA_code",
            "CURRENCY_code",
            "series_url",
        ]
        assert list(df["series_key"]) == ["M.DE.EUR", "M.FR.EUR"]
        assert list(df["FREQ_code"]) == ["M", "M"]
        assert "FREQ_label" not in df.columns  # dimension labels live in `title`, not restated
        # Title falls back to series_key when no labels are known.
        assert all(df["title"].str.len() > 0)

    def test_empty_data_raises_empty_data_error(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.Series([], name="value", dtype=float)

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M.DE")
        with pytest.raises(EmptyDataError):
            _call_sdmx_fetch(params)

    def test_unknown_agency_rejected_at_param_validation(self) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        with pytest.raises(InvalidParameterError, match="Unknown agency"):
            SdmxFetchParams(dataset_key="BOGUS-X", series_key="M.DE")

    def test_dataset_key_must_include_agency_prefix(self) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        with pytest.raises(InvalidParameterError, match="must include agency prefix"):
            SdmxFetchParams(dataset_key="YCONLY", series_key="M.DE")

    def test_series_url_metadata_for_ecb(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["M"],
                "REF_AREA": ["DE"],
                "TIME_PERIOD": ["2024-01"],
                "value": [1.0],
            }
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M.DE")
        result = _call_sdmx_fetch(params)

        assert "series_url" in result.data.columns
        parsed = urlparse(str(result.data["series_url"].iloc[0]))
        assert parsed.scheme == "https"
        assert parsed.hostname == "data.ecb.europa.eu"

    def test_no_series_url_for_wb_wdi(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "WDI")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["A"],
                "REF_AREA": ["USA"],
                "TIME_PERIOD": ["2024"],
                "value": [1.0],
            }
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="WB_WDI-WDI", series_key="A.USA")
        result = _call_sdmx_fetch(params)
        assert "series_url" not in result.data.columns or result.data["series_url"].isna().all()

    def test_fetch_result_carries_output_schema(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony.result import ColumnRole

        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        # Round-trip through the actual (decorated) fetch to confirm the static
        # OutputConfig is applied to the result.
        dim_ids = ["FREQ"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {"FREQ": ["M"], "TIME_PERIOD": ["2024-01"], "value": [1.0]}
        ).set_index(["FREQ", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M")
        result = _call_sdmx_fetch(params)

        roles = {c.name: c.role for c in result.output_schema.columns}
        assert roles["series_key"] == ColumnRole.KEY
        assert roles["title"] == ColumnRole.TITLE
        assert roles["value"] == ColumnRole.DATA
        assert roles["TIME_PERIOD"] == ColumnRole.DATA
        # The per-flow dimension's code column is caught as METADATA by the wildcard.
        assert roles["FREQ_code"] == ColumnRole.METADATA
        # series_key carries no namespace — matches sdmx_series_search's key column.
        key_col = next(c for c in result.output_schema.columns if c.name == "series_key")
        assert key_col.namespace is None
        # TIME_PERIOD stays the raw SDMX period string, not coerced to datetime.
        assert result.data["TIME_PERIOD"].iloc[0] == "2024-01"


class TestSdmxFetchBatch:
    """Batched multi-series fetch: fan-out, request-order, caps, all-or-nothing.

    These stub ``_fetch_one_series`` so they exercise only the orchestration the connector adds —
    the per-series I/O path is unchanged and covered by ``TestSdmxFetch`` above.
    """

    @staticmethod
    def _stub_one(monkeypatch: pytest.MonkeyPatch, *, fail_on: set[str] | None = None, delay_first: bool = False):
        from parsimony.errors import ProviderError

        from parsimony_sdmx.connectors import fetch as fetch_mod

        calls: list[str] = []

        def fake_one(params: Any, structure: Any = None) -> pd.DataFrame:
            calls.append(params.series_key)
            if fail_on and params.series_key in fail_on:
                raise ProviderError(provider="sdmx", status_code=400, message=f"bad key {params.series_key}")
            if delay_first and params.series_key.endswith("0"):
                import time

                time.sleep(0.05)  # slow the first request; order must still hold
            # Include the columns the output schema declares (series_key, title,
            # TIME_PERIOD, value); dims are absent here, which the "*" wildcard tolerates.
            return pd.DataFrame(
                {
                    "series_key": [params.series_key],
                    "title": [params.series_key],
                    "TIME_PERIOD": ["2024-01"],
                    "value": [1.0],
                }
            )

        monkeypatch.setattr(fetch_mod, "_fetch_one_series", fake_one)
        # Structure resolution is orthogonal to the orchestration these tests exercise —
        # stub it out so it doesn't reach the network (see TestStructureSharing for its own tests).
        monkeypatch.setattr(fetch_mod, "_resolve_structure", lambda dataset_key: None)
        return fetch_mod, calls

    def test_list_of_keys_stacks_into_one_table(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fetch_mod, calls = self._stub_one(monkeypatch)
        keys = ["M.I15.TOTAL.ES", "M.RCH_A.TOTAL.ES", "M.RCH_M.TOTAL.ES"]
        result = fetch_mod.sdmx_fetch(dataset_ref="ESTAT-PRC_HICP_MINR", series_ref=keys)
        assert list(result.data["series_key"]) == keys
        assert set(calls) == set(keys)

    def test_request_order_preserved_despite_concurrency(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fetch_mod, _ = self._stub_one(monkeypatch, delay_first=True)
        keys = [f"M.K{i}" for i in range(5)]  # K0 is slowest; map must still return in input order
        result = fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref=keys)
        assert list(result.data["series_key"]) == keys

    def test_single_string_takes_unbatched_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fetch_mod, calls = self._stub_one(monkeypatch)
        result = fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref="M.DE.EUR")
        assert list(result.data["series_key"]) == ["M.DE.EUR"]
        assert calls == ["M.DE.EUR"]

    def test_empty_list_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fetch_mod, _ = self._stub_one(monkeypatch)
        with pytest.raises(InvalidParameterError, match="at least one series key"):
            fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref=[])

    def test_over_cap_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from parsimony_sdmx.connectors.fetch import _MAX_BATCH_SERIES

        fetch_mod, _ = self._stub_one(monkeypatch)
        too_many = [f"M.X{i}" for i in range(_MAX_BATCH_SERIES + 1)]
        # The cap error must signpost the OR-string fast path, not just refuse the list —
        # a blocked caller should learn the escape hatch at the point of failure.
        with pytest.raises(InvalidParameterError, match="at most") as exc:
            fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref=too_many)
        msg = str(exc.value)
        assert "OR" in msg and "'+'" in msg

    def test_over_length_string_rejected_names_or_split(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from parsimony_sdmx.connectors.fetch import _SERIES_KEY_MAX_CHARS

        fetch_mod, _ = self._stub_one(monkeypatch)
        # A single OR-string past the char cap must translate to a typed, actionable error
        # (not a raw pydantic ValidationError) that names the "split into <=N-char OR-strings,
        # pass a list" remedy.
        codes = "+".join(f"CP{i:02d}" for i in range(60))
        long_key = f"M.N.{codes}.DE"
        assert len(long_key) > _SERIES_KEY_MAX_CHARS
        with pytest.raises(InvalidParameterError, match="capped at") as exc:
            fetch_mod.sdmx_fetch(dataset_ref="ECB-ICP", series_ref=long_key)
        msg = str(exc.value)
        assert str(_SERIES_KEY_MAX_CHARS) in msg
        assert "OR-string" in msg and "list" in msg

    def test_one_bad_key_fails_whole_batch_no_silent_drop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from parsimony.errors import ProviderError

        fetch_mod, _ = self._stub_one(monkeypatch, fail_on={"M.BAD.ES"})
        with pytest.raises(ProviderError, match="bad key M.BAD.ES"):
            fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref=["M.GOOD.ES", "M.BAD.ES"])


class TestStructureSharing:
    """A same-flow batch must resolve the DSD/codelists once, not once per key.

    Regression coverage for the ECB ``YC`` flow report: a 12-key batch re-fetched and
    re-parsed the whole structure (incl. a 1000+ code codelist) independently per key.
    """

    def test_structure_fetched_once_across_a_batch(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {"FREQ": ["M"], "REF_AREA": ["DE"], "TIME_PERIOD": ["2024-01"], "value": [1.0]}
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        result = sdmx_fetch(dataset_ref="ECB-YC", series_ref=["M.DE", "M.FR", "M.ES"])

        client = patch_sdmx["client"]
        # One structure resolution shared by the whole batch...
        assert client.dataflow.call_count == 1
        # ...but every key still gets its own data request.
        assert client.get.call_count == 3
        assert len(result.data) == 3

    def test_single_key_call_also_resolves_structure_once(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {"FREQ": ["M"], "REF_AREA": ["DE"], "TIME_PERIOD": ["2024-01"], "value": [1.0]}
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        sdmx_fetch(dataset_ref="ECB-YC", series_ref="M.DE")

        client = patch_sdmx["client"]
        assert client.dataflow.call_count == 1
        assert client.get.call_count == 1


class TestFlowPrefixStrip:
    """``series_ref`` may still carry a flow-id prefix (e.g. from an older ``sdmx_series_search``
    result, or copy-pasted from a provider's raw SDMX-CSV ``KEY`` column); ``sdmx_fetch`` should
    defensively strip it rather than 400 at the provider on a duplicated flow id."""

    def test_strip_flow_prefix_removes_matching_prefix(self) -> None:
        from parsimony_sdmx.connectors.fetch import _strip_flow_prefix

        assert _strip_flow_prefix("YC.B.U2.EUR", "YC") == "B.U2.EUR"
        assert _strip_flow_prefix("yc.B.U2.EUR", "YC") == "B.U2.EUR"

    def test_strip_flow_prefix_leaves_bare_key_untouched(self) -> None:
        from parsimony_sdmx.connectors.fetch import _strip_flow_prefix

        assert _strip_flow_prefix("B.U2.EUR", "YC") == "B.U2.EUR"

    def test_strip_flow_prefix_does_not_touch_unrelated_leading_segment(self) -> None:
        from parsimony_sdmx.connectors.fetch import _strip_flow_prefix

        # A dimension code that happens to equal the dataset_id is not a flow prefix.
        assert _strip_flow_prefix("YC.YC.EUR", "YC") == "YC.EUR"

    def test_sdmx_fetch_strips_prefixed_series_ref_before_request(self, patch_sdmx: dict[str, Any]) -> None:
        from parsimony_sdmx.connectors.fetch import sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(structure_msg=structure_msg, data_msg=data_msg)
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {"FREQ": ["M"], "REF_AREA": ["DE"], "TIME_PERIOD": ["2024-01"], "value": [1.0]}
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        sdmx_fetch(dataset_ref="ECB-YC", series_ref="YC.M.DE")

        client = patch_sdmx["client"]
        assert client.get.call_args.kwargs["key"] == "M.DE"

    def test_sdmx_fetch_strips_prefix_from_every_key_in_a_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fetch_mod, calls = TestSdmxFetchBatch._stub_one(monkeypatch)
        fetch_mod.sdmx_fetch(dataset_ref="ECB-YC", series_ref=["YC.M.DE", "M.FR"])
        assert calls == ["M.DE", "M.FR"]


class TestSdmxFetchOutput:
    def test_static_schema_roles_and_wildcard(self) -> None:
        from parsimony.result import ColumnRole

        from parsimony_sdmx.connectors.fetch import SDMX_FETCH_OUTPUT

        by_name = {c.name: c for c in SDMX_FETCH_OUTPUT.columns}
        assert by_name["series_key"].role == ColumnRole.KEY
        # No namespace — matches sdmx_series_search's key column (the join target).
        assert by_name["series_key"].namespace is None
        assert by_name["title"].role == ColumnRole.TITLE
        # TIME_PERIOD stays a raw SDMX period string (dtype 'auto' = no coercion).
        assert by_name["TIME_PERIOD"].role == ColumnRole.DATA
        assert by_name["TIME_PERIOD"].dtype == "auto"
        assert by_name["value"].role == ColumnRole.DATA
        assert by_name["value"].dtype == "numeric"
        # Per-flow dimension columns + optional series_url are caught by the
        # wildcard as METADATA rather than enumerated statically.
        assert by_name["*"].role == ColumnRole.METADATA
