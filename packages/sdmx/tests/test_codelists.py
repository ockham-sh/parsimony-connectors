from parsimony_sdmx.core.codelists import pick_label, resolve_codelists


class TestPickLabel:
    def test_first_preference_wins(self) -> None:
        assert pick_label({"en": "Euro", "fr": "Euro FR"}, ("en",)) == "Euro"

    def test_second_preference_when_first_missing(self) -> None:
        assert pick_label({"fr": "Taux", "de": "Zins"}, ("en", "fr")) == "Taux"

    def test_falls_back_to_any_language_when_no_pref_matches(self) -> None:
        # Only non-English available, no pref matches → return any.
        assert pick_label({"de": "Zinssatz"}, ("en", "fr")) == "Zinssatz"

    def test_empty_preferred_value_is_skipped(self) -> None:
        # Empty string in preferred lang must not be returned.
        assert pick_label({"en": "", "fr": "Taux"}, ("en", "fr")) == "Taux"

    def test_all_empty_returns_none(self) -> None:
        assert pick_label({"en": "", "fr": ""}, ("en",)) is None

    def test_empty_localizations_returns_none(self) -> None:
        assert pick_label({}, ("en",)) is None

    def test_respects_preference_order(self) -> None:
        locs = {"en": "A", "fr": "B", "de": "C"}
        assert pick_label(locs, ("de", "fr", "en")) == "C"


class TestResolveCodelists:
    def test_basic_happy_path(self) -> None:
        raw = {
            "FREQ": {
                "A": {"en": "Annual"},
                "M": {"en": "Monthly"},
            },
            "REF_AREA": {
                "U2": {"en": "Euro area"},
            },
        }
        out = resolve_codelists(raw, ("en",))
        assert out == {
            "FREQ": {"A": "Annual", "M": "Monthly"},
            "REF_AREA": {"U2": "Euro area"},
        }

    def test_code_with_no_usable_label_is_dropped(self) -> None:
        raw = {"FREQ": {"A": {"en": "Annual"}, "X": {"en": ""}}}
        out = resolve_codelists(raw, ("en",))
        assert out == {"FREQ": {"A": "Annual"}}

    def test_language_fallback_applied_per_code(self) -> None:
        raw = {
            "FREQ": {
                "A": {"en": "Annual"},      # English present
                "M": {"fr": "Mensuel"},     # Only French
            },
        }
        out = resolve_codelists(raw, ("en", "fr"))
        assert out == {"FREQ": {"A": "Annual", "M": "Mensuel"}}

    def test_empty_dim_yields_empty_dict(self) -> None:
        raw: dict[str, dict[str, dict[str, str]]] = {"FREQ": {}}
        out = resolve_codelists(raw, ("en",))
        assert out == {"FREQ": {}}

    def test_default_language_pref_is_english(self) -> None:
        raw = {"FREQ": {"A": {"en": "Annual", "fr": "Annuel"}}}
        out = resolve_codelists(raw)
        assert out == {"FREQ": {"A": "Annual"}}
