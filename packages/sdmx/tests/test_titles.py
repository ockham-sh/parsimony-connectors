from parsimony_sdmx.core.titles import (
    augment_with_ecb_attributes,
    compose_series_title,
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


class TestAugmentWithEcbAttributes:
    def test_no_augments_returns_base(self) -> None:
        assert augment_with_ecb_attributes("base") == "base"
        assert augment_with_ecb_attributes("base", None, None) == "base"

    def test_both_title_and_title_compl(self) -> None:
        out = augment_with_ecb_attributes("base", "Short", "Full description")
        assert out == "base | Short - Full description"

    def test_only_title(self) -> None:
        out = augment_with_ecb_attributes("base", "Short", None)
        assert out == "base | Short"

    def test_only_title_compl(self) -> None:
        out = augment_with_ecb_attributes("base", None, "Full description")
        assert out == "base | Full description"

    def test_empty_strings_are_absent(self) -> None:
        assert augment_with_ecb_attributes("base", "", "") == "base"
        assert augment_with_ecb_attributes("base", "", "Full") == "base | Full"
        assert augment_with_ecb_attributes("base", "Short", "") == "base | Short"

    def test_augment_separator_is_pipe(self) -> None:
        out = augment_with_ecb_attributes("X", "Y", "Z")
        assert " | " in out
        assert out.startswith("X | ")
