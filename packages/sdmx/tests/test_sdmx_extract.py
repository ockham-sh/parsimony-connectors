from types import SimpleNamespace

from parsimony_sdmx.providers.sdmx_extract import (
    clean_text,
    extract_dsd_dim_order,
    extract_flow_title,
    extract_raw_codelists,
    extract_series_dim_values,
)


class TestCleanText:
    def test_plain_text_passthrough(self) -> None:
        assert clean_text("Yield curve parameters") == "Yield curve parameters"

    def test_strips_paragraph_tags(self) -> None:
        assert clean_text("<p>HICP measures prices</p>") == "HICP measures prices"

    def test_replaces_br_with_space(self) -> None:
        out = clean_text("line1<br>line2<br/>line3")
        assert out == "line1 line2 line3"

    def test_collapses_whitespace(self) -> None:
        assert clean_text("a   b\n\nc\t\td") == "a b c d"

    def test_unescapes_entities(self) -> None:
        assert clean_text("price &amp; volume") == "price & volume"

    def test_empty_returns_empty(self) -> None:
        assert clean_text("") == ""

    def test_strips_nested_markup(self) -> None:
        out = clean_text("<div><p>Name</p> <em>source</em></div>")
        assert out == "Name source"

    def test_leaves_bare_text_untouched(self) -> None:
        out = clean_text("HICP - monthly data (1996-2025)")
        assert out == "HICP - monthly data (1996-2025)"


def _named(localizations: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(localizations=localizations)


def _code(code_id: str, locs: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(id=code_id, name=_named(locs))


def _codelist(code_id_to_locs: dict[str, dict[str, str]]) -> list[SimpleNamespace]:
    # Codelists in sdmx1 iterate as lists of codes.
    return [_code(cid, locs) for cid, locs in code_id_to_locs.items()]


def _dim(
    dim_id: str,
    codelist_id: str | None = None,
    via_concept: bool = False,
) -> SimpleNamespace:
    if codelist_id is None:
        return SimpleNamespace(
            id=dim_id, local_representation=None, concept_identity=None
        )
    enumerated = SimpleNamespace(id=codelist_id)
    if via_concept:
        core = SimpleNamespace(enumerated=enumerated)
        concept = SimpleNamespace(core_representation=core)
        return SimpleNamespace(
            id=dim_id, local_representation=None, concept_identity=concept
        )
    local = SimpleNamespace(enumerated=enumerated)
    return SimpleNamespace(
        id=dim_id, local_representation=local, concept_identity=None
    )


class TestExtractFlowTitle:
    def test_name_plus_description(self) -> None:
        flow = SimpleNamespace(
            id="YC",
            name=_named({"en": "Yield Curve"}),
            description=_named({"en": "Yield curve parameters"}),
        )
        assert extract_flow_title(flow) == "Yield Curve - Yield curve parameters"

    def test_html_stripped_from_description(self) -> None:
        flow = SimpleNamespace(
            id="HICP",
            name=_named({"en": "HICP - monthly data"}),
            description=_named(
                {"en": "<p>The HICP measures <em>consumer</em> prices</p>"}
            ),
        )
        out = extract_flow_title(flow)
        assert out == "HICP - monthly data - The HICP measures consumer prices"

    def test_only_name(self) -> None:
        flow = SimpleNamespace(
            id="YC",
            name=_named({"en": "Yield Curve"}),
            description=_named({}),
        )
        assert extract_flow_title(flow) == "Yield Curve"

    def test_falls_back_to_flow_id_when_name_missing(self) -> None:
        flow = SimpleNamespace(
            id="YC",
            name=_named({}),
            description=_named({"en": "d"}),
        )
        assert extract_flow_title(flow) == "YC - d"

    def test_language_fallback(self) -> None:
        flow = SimpleNamespace(
            id="YC",
            name=_named({"fr": "Courbe"}),
            description=_named({}),
        )
        assert extract_flow_title(flow, ("en", "fr")) == "Courbe"


class TestExtractDsdDimOrder:
    def test_excludes_time_period(self) -> None:
        dsd = SimpleNamespace(
            dimensions=[
                _dim("FREQ"),
                _dim("REF_AREA"),
                _dim("TIME_PERIOD"),
            ]
        )
        assert extract_dsd_dim_order(dsd) == ["FREQ", "REF_AREA"]

    def test_preserves_order(self) -> None:
        dsd = SimpleNamespace(
            dimensions=[_dim("A"), _dim("B"), _dim("C")]
        )
        assert extract_dsd_dim_order(dsd) == ["A", "B", "C"]

    def test_can_keep_time_when_requested(self) -> None:
        dsd = SimpleNamespace(dimensions=[_dim("TIME_PERIOD"), _dim("A")])
        assert extract_dsd_dim_order(dsd, exclude_time=False) == ["TIME_PERIOD", "A"]


class TestExtractRawCodelists:
    def test_basic_extraction(self) -> None:
        dsd = SimpleNamespace(
            dimensions=[
                _dim("FREQ", codelist_id="CL_FREQ"),
                _dim("REF_AREA", codelist_id="CL_AREA"),
                _dim("TIME_PERIOD"),  # no codelist, skipped as time
            ]
        )
        msg = SimpleNamespace(
            codelist={
                "CL_FREQ": _codelist({"A": {"en": "Annual"}, "M": {"en": "Monthly"}}),
                "CL_AREA": _codelist({"U2": {"en": "Euro area"}}),
            }
        )
        raw = extract_raw_codelists(dsd, msg)
        assert raw == {
            "FREQ": {"A": {"en": "Annual"}, "M": {"en": "Monthly"}},
            "REF_AREA": {"U2": {"en": "Euro area"}},
        }

    def test_dim_without_codelist_is_skipped(self) -> None:
        dsd = SimpleNamespace(
            dimensions=[_dim("FREQ"), _dim("REF_AREA", codelist_id="CL_AREA")]
        )
        msg = SimpleNamespace(codelist={"CL_AREA": _codelist({"U2": {"en": "E"}})})
        raw = extract_raw_codelists(dsd, msg)
        assert "FREQ" not in raw
        assert "REF_AREA" in raw

    def test_codelist_resolved_via_concept_identity(self) -> None:
        dsd = SimpleNamespace(
            dimensions=[_dim("FREQ", codelist_id="CL_FREQ", via_concept=True)]
        )
        msg = SimpleNamespace(codelist={"CL_FREQ": _codelist({"A": {"en": "Annual"}})})
        raw = extract_raw_codelists(dsd, msg)
        assert raw == {"FREQ": {"A": {"en": "Annual"}}}

    def test_missing_codelist_in_msg_skipped(self) -> None:
        dsd = SimpleNamespace(dimensions=[_dim("FREQ", codelist_id="GONE")])
        msg = SimpleNamespace(codelist={})
        assert extract_raw_codelists(dsd, msg) == {}


class TestExtractSeriesDimValues:
    def _sk(self, values: dict[str, str]) -> SimpleNamespace:
        kvs = {
            dim: SimpleNamespace(id=dim, value=code)
            for dim, code in values.items()
        }
        return SimpleNamespace(values=kvs)

    def test_dict_input(self) -> None:
        sks = {
            "S1": self._sk({"FREQ": "A", "REF_AREA": "U2"}),
            "S2": self._sk({"FREQ": "M", "REF_AREA": "U2"}),
        }
        out = list(extract_series_dim_values(sks))
        assert out == [
            {"FREQ": "A", "REF_AREA": "U2"},
            {"FREQ": "M", "REF_AREA": "U2"},
        ]

    def test_list_input(self) -> None:
        sks = [
            self._sk({"FREQ": "A"}),
            self._sk({"FREQ": "M"}),
        ]
        out = list(extract_series_dim_values(sks))
        assert out == [{"FREQ": "A"}, {"FREQ": "M"}]

    def test_empty_values_yield_empty_dict(self) -> None:
        sks = [SimpleNamespace(values={})]
        out = list(extract_series_dim_values(sks))
        assert out == [{}]
