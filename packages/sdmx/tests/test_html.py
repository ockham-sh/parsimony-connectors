import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.html import HTML_PARSER, parse_html, validate_sdmx_id


class TestParseHtml:
    def test_parser_is_lxml(self) -> None:
        assert HTML_PARSER == "lxml"

    def test_parses_simple_document(self) -> None:
        soup = parse_html("<html><body><p id='x'>hi</p></body></html>")
        p = soup.find("p")
        assert p is not None
        assert p["id"] == "x"

    def test_accepts_bytes(self) -> None:
        soup = parse_html(b"<html><body><p>hi</p></body></html>")
        p = soup.find("p")
        assert p is not None
        assert p.text == "hi"


class TestValidateSdmxId:
    @pytest.mark.parametrize(
        "candidate",
        ["YC", "ECB-YC", "NAMA_10_GDP", "WDI", "A1.b-c_d", "x" * 200],
    )
    def test_accepts_valid_ids(self, candidate: str) -> None:
        assert validate_sdmx_id(candidate) == candidate

    @pytest.mark.parametrize(
        "candidate",
        [
            "",
            "1Leading",  # must start with letter
            "-Leading",
            ".Leading",
            "has space",
            "has/slash",
            "has\\backslash",
            "has$dollar",
            "has;semi",
            "has\x00null",
            "has\nnewline",
            "<script>",
            "x" * 201,
        ],
    )
    def test_rejects_invalid_ids(self, candidate: str) -> None:
        with pytest.raises(SdmxFetchError, match="Invalid SDMX identifier"):
            validate_sdmx_id(candidate)
