from parsimony_sdmx.core.titles import (
    choose_series_title,
    compose_observation_title,
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
        assert out == "Annual - Euro area - Euro"

    def test_respects_dsd_order_not_dict_order(self) -> None:
        dim_values = {"CURRENCY": "EUR", "FREQ": "A", "REF_AREA": "U2"}
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {
            "FREQ": {"A": "Annual"},
            "REF_AREA": {"U2": "Euro area"},
            "CURRENCY": {"EUR": "Euro"},
        }
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out.startswith("Annual")
        assert out.endswith("Euro")

    def test_raw_code_fallback_when_label_missing(self) -> None:
        dim_values = {"FREQ": "A", "REF_AREA": "XX"}
        dsd_order = ("FREQ", "REF_AREA")
        labels = {"FREQ": {"A": "Annual"}, "REF_AREA": {}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "Annual - XX"

    def test_raw_code_fallback_when_dim_absent_from_labels(self) -> None:
        dim_values = {"FREQ": "A", "UNKNOWN_DIM": "Z"}
        dsd_order = ("FREQ", "UNKNOWN_DIM")
        labels = {"FREQ": {"A": "Annual"}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "Annual - Z"

    def test_missing_series_value_skipped(self) -> None:
        dim_values = {"FREQ": "A"}  # REF_AREA missing
        dsd_order = ("FREQ", "REF_AREA", "CURRENCY")
        labels = {"FREQ": {"A": "Annual"}, "CURRENCY": {"EUR": "Euro"}}
        out = compose_series_title(dim_values, dsd_order, labels)
        assert out == "Annual"

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
        assert out == "Euro area"

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


class TestChooseSeriesTitle:
    def test_no_source_title_returns_fallback(self) -> None:
        assert choose_series_title("base") == "base"
        assert choose_series_title("base", None, None) == "base"

    def test_source_title_replaces_fallback(self) -> None:
        out = choose_series_title("fallback", "HICP Inflation rate")
        assert out == "HICP Inflation rate"

    def test_title_compl_is_dropped(self) -> None:
        out = choose_series_title("fallback", "Short", "Full description with provenance")
        assert out == "Short"
        assert "Full description" not in out

    def test_title_compl_alone_ignored(self) -> None:
        out = choose_series_title("fallback", None, "Full description")
        assert out == "fallback"

    def test_empty_title_falls_back_to_base(self) -> None:
        assert choose_series_title("base", "", "") == "base"
        assert choose_series_title("base", "", "Full") == "base"
        assert choose_series_title("base", "Short", "") == "Short"
