import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.xml import iter_elements

NS = "http://example.org/ns"


class TestIterElements:
    def test_yields_local_tag_matches(self) -> None:
        xml = b"<root><item>a</item><item>b</item><other/></root>"
        texts = [e.text for e in iter_elements(xml, "item")]
        assert texts == ["a", "b"]

    def test_yields_namespaced_tag_with_clark_notation(self) -> None:
        xml = (f'<root xmlns="{NS}"><item>a</item><item>b</item></root>').encode()
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


class TestXxeProtection:
    """Hardened parsers must not expand external or internal DTD entities."""

    INTERNAL_ENTITY_XML = (
        b'<?xml version="1.0"?><!DOCTYPE doc [<!ENTITY secret "LEAKED_VALUE">]><doc><item>&secret;</item></doc>'
    )

    def test_internal_entity_not_expanded_via_iterparse(self) -> None:
        items = list(iter_elements(self.INTERNAL_ENTITY_XML, "item"))
        assert len(items) == 1
        text = items[0].text or ""
        assert "LEAKED_VALUE" not in text
