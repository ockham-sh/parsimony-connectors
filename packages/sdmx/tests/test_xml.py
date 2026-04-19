import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.xml import iter_elements, parse_xml

NS = "http://example.org/ns"


class TestIterElements:
    def test_yields_local_tag_matches(self) -> None:
        xml = b"<root><item>a</item><item>b</item><other/></root>"
        texts = [e.text for e in iter_elements(xml, "item")]
        assert texts == ["a", "b"]

    def test_yields_namespaced_tag_with_clark_notation(self) -> None:
        xml = (
            f'<root xmlns="{NS}">'
            "<item>a</item><item>b</item>"
            "</root>"
        ).encode()
        tag = f"{{{NS}}}item"
        texts = [e.text for e in iter_elements(xml, tag)]
        assert texts == ["a", "b"]

    def test_empty_input_raises(self) -> None:
        with pytest.raises(SdmxFetchError, match="Malformed XML"):
            list(iter_elements(b"", "item"))

    def test_malformed_xml_raises(self) -> None:
        with pytest.raises(SdmxFetchError, match="Malformed XML"):
            list(iter_elements(b"<root><item></root>", "item"))

    def test_no_matches_yields_nothing(self) -> None:
        xml = b"<root><other/></root>"
        assert [e.tag for e in iter_elements(xml, "item")] == []

    def test_element_is_cleared_after_yield(self) -> None:
        """Callers MUST read inside the loop; references kept past yield see cleared data.

        This test documents the memory-bounding contract: after a yield,
        the generator clears the element before advancing.
        """
        xml = b"<root><item>a</item><item>b</item></root>"
        retained: list[object] = []
        for elem in iter_elements(xml, "item"):
            retained.append(elem)  # anti-pattern; purely to observe the contract
        # After iteration, all retained element texts have been cleared.
        for elem in retained:
            assert elem.text is None  # type: ignore[attr-defined]


class TestParseXml:
    def test_happy_path(self) -> None:
        root = parse_xml(b"<root><a>1</a></root>")
        assert root.tag == "root"
        assert root.find("a").text == "1"

    def test_malformed_raises(self) -> None:
        with pytest.raises(SdmxFetchError, match="Malformed XML"):
            parse_xml(b"<root>")


class TestXxeProtection:
    """Hardened parsers must not expand external or internal DTD entities."""

    INTERNAL_ENTITY_XML = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE doc [<!ENTITY secret "LEAKED_VALUE">]>'
        b"<doc><item>&secret;</item></doc>"
    )

    EXTERNAL_ENTITY_XML = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE doc [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        b"<doc><item>&xxe;</item></doc>"
    )

    def test_internal_entity_not_expanded_via_parse(self) -> None:
        """Even a trivial internal entity must not be expanded."""
        root = parse_xml(self.INTERNAL_ENTITY_XML)
        item = root.find("item")
        # With resolve_entities=False, &secret; is not expanded. Depending on
        # lxml version the text may be empty or contain the placeholder, but
        # it must NOT contain the expanded value.
        text = (item.text or "")
        assert "LEAKED_VALUE" not in text

    def test_internal_entity_not_expanded_via_iterparse(self) -> None:
        items = list(iter_elements(self.INTERNAL_ENTITY_XML, "item"))
        assert len(items) == 1
        text = items[0].text or ""
        assert "LEAKED_VALUE" not in text

    def test_external_entity_is_not_resolved(self) -> None:
        """External file:// entity must not be fetched."""
        # Parser should either skip the entity or raise; in neither case
        # should /etc/passwd content appear in the output.
        try:
            root = parse_xml(self.EXTERNAL_ENTITY_XML)
            item = root.find("item")
            text = (item.text or "")
            # Typical /etc/passwd line starts with root:x:0 — must not appear.
            assert "root:" not in text
            assert "bin:" not in text
        except SdmxFetchError:
            # Parser rejected the input — also acceptable.
            pass
