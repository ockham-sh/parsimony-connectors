import json
from dataclasses import FrozenInstanceError

import pytest
import requests
from lxml import etree

from parsimony_sdmx.core.errors import (
    CodelistMissingError,
    ParquetWriteError,
    SdmxFetchError,
    TitleBuildError,
)
from parsimony_sdmx.core.outcomes import (
    DatasetOutcome,
    FailureKind,
    OutcomeStatus,
)
from parsimony_sdmx.io.http import classify_exception


class TestDatasetOutcome:
    def test_minimal_ok_outcome(self) -> None:
        o = DatasetOutcome(dataset_id="YC", agency_id="ECB", status=OutcomeStatus.OK)
        assert o.dataset_id == "YC"
        assert o.status == OutcomeStatus.OK
        assert o.rows == 0
        assert o.kind is None

    def test_is_frozen(self) -> None:
        o = DatasetOutcome(dataset_id="YC", agency_id="ECB", status=OutcomeStatus.OK)
        with pytest.raises(FrozenInstanceError):
            o.rows = 10  # type: ignore[misc]

    def test_failed_outcome_carries_kind_and_message(self) -> None:
        o = DatasetOutcome(
            dataset_id="YC",
            agency_id="ECB",
            status=OutcomeStatus.FAILED,
            kind=FailureKind.TIMEOUT,
            error_message="read timed out",
        )
        assert o.status == OutcomeStatus.FAILED
        assert o.kind == FailureKind.TIMEOUT


class TestClassifyException:
    def test_timeout(self) -> None:
        assert classify_exception(requests.exceptions.Timeout()) == FailureKind.TIMEOUT

    def test_connection_error(self) -> None:
        assert (
            classify_exception(requests.exceptions.ConnectionError())
            == FailureKind.HTTP_ERROR
        )

    def test_http_error(self) -> None:
        resp = requests.Response()
        resp.status_code = 404
        err = requests.exceptions.HTTPError(response=resp)
        assert classify_exception(err) == FailureKind.HTTP_ERROR

    def test_sdmx_fetch_error(self) -> None:
        assert classify_exception(SdmxFetchError("boom")) == FailureKind.HTTP_ERROR

    def test_title_build_error(self) -> None:
        assert classify_exception(TitleBuildError("x")) == FailureKind.PARSE_ERROR

    def test_codelist_missing_error(self) -> None:
        assert classify_exception(CodelistMissingError("x")) == FailureKind.PARSE_ERROR

    def test_parquet_write_error(self) -> None:
        assert classify_exception(ParquetWriteError("x")) == FailureKind.PARSE_ERROR

    def test_xml_syntax_error_is_parse_error(self) -> None:
        try:
            etree.fromstring(b"<not-closed>")
        except etree.XMLSyntaxError as exc:
            assert classify_exception(exc) == FailureKind.PARSE_ERROR
        else:
            pytest.fail("Expected XMLSyntaxError")

    def test_json_decode_error_is_parse_error(self) -> None:
        try:
            json.loads("{not json")
        except json.JSONDecodeError as exc:
            assert classify_exception(exc) == FailureKind.PARSE_ERROR
        else:
            pytest.fail("Expected JSONDecodeError")

    def test_bare_value_error_is_unknown_not_parse_error(self) -> None:
        # Narrowed: a bare ValueError is a programmer bug, not a parse
        # error, so it must surface as UNKNOWN.
        assert classify_exception(ValueError("bad data")) == FailureKind.UNKNOWN

    def test_bare_key_error_is_unknown(self) -> None:
        assert classify_exception(KeyError("missing")) == FailureKind.UNKNOWN

    def test_bare_type_error_is_unknown(self) -> None:
        assert classify_exception(TypeError("wrong type")) == FailureKind.UNKNOWN

    def test_unknown(self) -> None:
        class Weird(Exception):
            pass

        assert classify_exception(Weird()) == FailureKind.UNKNOWN
