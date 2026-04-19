from parsimony_sdmx.providers.ecb_series_attrs import (
    GENERIC_NS,
    parse_ecb_series_attributes,
)


def _xml(body: str) -> bytes:
    # Wraps a snippet of <generic:Series> blocks in a minimal SDMX XML doc.
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<doc xmlns:generic="{GENERIC_NS}">{body}</doc>'
    ).encode()


def _series(
    key_values: dict[str, str],
    attr_values: dict[str, str] | None = None,
) -> str:
    key_xml = "".join(
        f'<generic:Value id="{k}" value="{v}"/>' for k, v in key_values.items()
    )
    attrs_xml = ""
    if attr_values:
        attrs_xml = "<generic:Attributes>" + "".join(
            f'<generic:Value id="{k}" value="{v}"/>' for k, v in attr_values.items()
        ) + "</generic:Attributes>"
    return (
        f"<generic:Series>"
        f"<generic:SeriesKey>{key_xml}</generic:SeriesKey>"
        f"{attrs_xml}"
        f"</generic:Series>"
    )


class TestParseEcbSeriesAttributes:
    def test_single_series_with_title_and_title_compl(self) -> None:
        xml = _xml(
            _series(
                {"FREQ": "A", "REF_AREA": "U2"},
                {"TITLE": "Short title", "TITLE_COMPL": "Full description"},
            )
        )
        out = parse_ecb_series_attributes(xml, ("FREQ", "REF_AREA"))
        assert out == {"A.U2": ("Short title", "Full description")}

    def test_multiple_series(self) -> None:
        xml = _xml(
            _series({"FREQ": "A", "REF_AREA": "U2"}, {"TITLE": "T1"})
            + _series({"FREQ": "M", "REF_AREA": "U2"}, {"TITLE_COMPL": "TC2"})
        )
        out = parse_ecb_series_attributes(xml, ("FREQ", "REF_AREA"))
        assert out == {
            "A.U2": ("T1", None),
            "M.U2": (None, "TC2"),
        }

    def test_missing_attributes_yields_none_tuple(self) -> None:
        xml = _xml(_series({"FREQ": "A", "REF_AREA": "U2"}))
        out = parse_ecb_series_attributes(xml, ("FREQ", "REF_AREA"))
        assert out == {"A.U2": (None, None)}

    def test_respects_dim_order(self) -> None:
        xml = _xml(_series({"FREQ": "A", "REF_AREA": "U2", "CURRENCY": "EUR"}))
        out = parse_ecb_series_attributes(
            xml, ("REF_AREA", "CURRENCY", "FREQ")
        )
        assert out == {"U2.EUR.A": (None, None)}

    def test_series_missing_dim_is_skipped(self) -> None:
        xml = _xml(_series({"FREQ": "A"}))  # REF_AREA missing
        out = parse_ecb_series_attributes(xml, ("FREQ", "REF_AREA"))
        assert out == {}

    def test_empty_doc_yields_empty_map(self) -> None:
        xml = _xml("")
        out = parse_ecb_series_attributes(xml, ("FREQ",))
        assert out == {}

    def test_ignores_non_title_attributes(self) -> None:
        xml = _xml(
            _series(
                {"FREQ": "A"},
                {"TITLE": "T", "OTHER_ATTR": "irrelevant"},
            )
        )
        out = parse_ecb_series_attributes(xml, ("FREQ",))
        assert out == {"A": ("T", None)}
