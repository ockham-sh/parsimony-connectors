from parsimony_sdmx.core.titles import (
    augment_with_ecb_attributes,
    compose_observation_title,
    compose_series_title,
    format_code_with_label,
)


class TestComposeSeriesTitle:
    def test_basic_three_dims(self) -> None:
        dim_values = {"FREQ": "A", "REF_AREA": "U2", "CURRENCY": "EUR"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {
            "FREQ": {"A": "Annual"},
            "REF_AREA": {"U2": "Euro area"},
            "CURRENCY": {"EUR": "Euro"},
        }
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "A: Annual - U2: Euro area - EUR: Euro"

    def test_respects_dsd_order_not_dict_order(self) -> None:
        dim_values = {"CURRENCY": "EUR", "FREQ": "A", "REF_AREA": "U2"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {
            "FREQ": {"A": "Annual"},
            "REF_AREA": {"U2": "Euro area"},
            "CURRENCY": {"EUR": "Euro"},
        }
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out.startswith("A: Annual")
        assert out.endswith("EUR: Euro")

    def test_raw_code_fallback_when_label_missing(self) -> None:
        dim_values = {"FREQ": "A", "REF_AREA": "XX"}
        dsd_order = ("FREQ", "REF_AREA")
        labels = {"FREQ": {"A": "Annual"}, "REF_AREA": {}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "A: Annual - XX"

    def test_raw_code_fallback_when_dim_absent_from_labels(self) -> None:
        dim_values = {"FREQ": "A", "UNKNOWN_DIM": "Z"}
        dsd_order = ("FREQ", "UNKNOWN_DIM")
        labels = {"FREQ": {"A": "Annual"}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "A: Annual - Z"

    def test_missing_series_value_skipped(self) -> None:
        dim_values = {"FREQ": "A"}  # REF_AREA missing
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {"FREQ": {"A": "Annual"}, "CURRENCY": {"EUR": "Euro"}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "A: Annual"

    def test_empty_label_string_falls_back_to_raw_code(self) -> None:
        dim_values = {"FREQ": "A"}
        dsd_order = ("FREQ",)
        labels = {"FREQ": {"A": ""}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "A"

    def test_empty_inputs(self) -> None:
        assert compose_series_title({}, (), {}) == ""
        assert compose_series_title({"FREQ": "A"}, (), {"FREQ": {"A": "Annual"}}) == ""

    def test_empty_code_string_is_skipped(self) -> None:
        dim_values = {"FREQ": "", "REF_AREA": "U2"}
        dsd_order = ("FREQ", "REF_AREA")
        labels = {"REF_AREA": {"U2": "Euro area"}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "U2: Euro area"

    def test_separator_is_space_dash_space(self) -> None:
        dim_values = {"A": "x", "B": "y"}
        labels = {"A": {"x": "X-val"}, "B": {"y": "Y-val"}}
        out = compose_series_title(dim_values, ("A", "B"), labels)
        assert " - " in out
        assert out.count(" - ") == 1


class TestComposeObservationTitle:
    def test_labels_only_no_codes(self) -> None:
        dim_values = {"FREQ": "M", "REF_AREA": "DE", "CURRENCY": "EUR"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {
            "FREQ": {"M": "Monthly"},
            "REF_AREA": {"DE": "Germany"},
            "CURRENCY": {"EUR": "Euro"},
        }
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Monthly - Germany - Euro"

    def test_respects_dsd_order(self) -> None:
        dim_values = {"CURRENCY": "EUR", "FREQ": "M", "REF_AREA": "DE"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {
            "FREQ": {"M": "Monthly"},
            "REF_AREA": {"DE": "Germany"},
            "CURRENCY": {"EUR": "Euro"},
        }
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Monthly - Germany - Euro"

    def test_raw_code_fallback_when_label_missing(self) -> None:
        dim_values = {"FREQ": "M", "REF_AREA": "XX"}
        dsd_order = ("FREQ", "REF_AREA")
        labels = {"FREQ": {"M": "Monthly"}, "REF_AREA": {}}
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Monthly - XX"

    def test_raw_code_fallback_when_dim_absent_from_labels(self) -> None:
        dim_values = {"FREQ": "M", "UNKNOWN_DIM": "Z"}
        dsd_order = ("FREQ", "UNKNOWN_DIM")
        labels = {"FREQ": {"M": "Monthly"}}
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Monthly - Z"

    def test_missing_series_value_skipped(self) -> None:
        dim_values = {"FREQ": "M"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {"FREQ": {"M": "Monthly"}, "CURRENCY": {"EUR": "Euro"}}
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Monthly"

    def test_empty_inputs(self) -> None:
        assert compose_observation_title({}, (), {}) == ""
        assert compose_observation_title({"FREQ": "M"}, (), {"FREQ": {"M": "Monthly"}}) == ""

    def test_empty_code_string_is_skipped(self) -> None:
        dim_values = {"FREQ": "", "REF_AREA": "DE"}
        dsd_order = ("FREQ", "REF_AREA")
        labels = {"REF_AREA": {"DE": "Germany"}}
        out = compose_observation_title(dim_values, dsd_order, labels)
        assert out == "Germany"

    def test_separator_is_space_dash_space(self) -> None:
        dim_values = {"A": "x", "B": "y"}
        labels = {"A": {"x": "X-val"}, "B": {"y": "Y-val"}}
        out = compose_observation_title(dim_values, ("A", "B"), labels)
        assert " - " in out
        assert out.count(" - ") == 1


class TestFormatCodeWithLabel:
    def test_code_and_label_render_with_parentheses(self) -> None:
        assert format_code_with_label("DE", "Germany") == "DE (Germany)"

    def test_label_none_returns_bare_code(self) -> None:
        assert format_code_with_label("DE", None) == "DE"

    def test_empty_label_returns_bare_code(self) -> None:
        assert format_code_with_label("DE", "") == "DE"
        assert format_code_with_label("DE", "   ") == "DE"

    def test_label_equals_code_case_insensitive_returns_bare_code(self) -> None:
        assert format_code_with_label("EUR", "EUR") == "EUR"
        assert format_code_with_label("EUR", "eur") == "EUR"
        assert format_code_with_label("eur", "EUR") == "eur"

    def test_empty_code_returns_empty_string(self) -> None:
        assert format_code_with_label("", "Germany") == ""
        assert format_code_with_label("   ", "Germany") == ""

    def test_strips_whitespace_around_code_and_label(self) -> None:
        assert format_code_with_label("  DE  ", "  Germany  ") == "DE (Germany)"


class TestAugmentWithEcbAttributes:
    def test_no_augments_returns_base(self) -> None:
        assert augment_with_ecb_attributes("base") == "base"
        assert augment_with_ecb_attributes("base", None, None) == "base"

    def test_title_is_prefixed_to_base(self) -> None:
        out = augment_with_ecb_attributes("CODE: label", "HICP Inflation rate")
        assert out == "HICP Inflation rate - CODE: label"

    def test_title_compl_is_dropped(self) -> None:
        # TITLE_COMPL duplicates dim labels / provenance; intentionally dropped.
        out = augment_with_ecb_attributes("CODE: label", "Short", "Full description with provenance")
        assert out == "Short - CODE: label"
        assert "Full description" not in out

    def test_title_compl_alone_ignored(self) -> None:
        out = augment_with_ecb_attributes("CODE: label", None, "Full description")
        assert out == "CODE: label"

    def test_empty_title_falls_back_to_base(self) -> None:
        assert augment_with_ecb_attributes("base", "", "") == "base"
        assert augment_with_ecb_attributes("base", "", "Full") == "base"
        assert augment_with_ecb_attributes("base", "Short", "") == "Short - base"

    def test_base_is_preserved_when_title_present(self) -> None:
        # Opposite of the prior contract: the codelist base must survive
        # alongside TITLE so every dim label (including FREQ) stays indexed.
        out = augment_with_ecb_attributes("M: Monthly - DE: Germany", "HICP Inflation rate")
        assert "M: Monthly" in out
        assert "DE: Germany" in out
        assert out.startswith("HICP Inflation rate")
