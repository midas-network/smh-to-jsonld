"""Unit tests for the output-type definition lookup (utils/output_types.py)."""

from utils.output_types import LEARN_MORE_URL, get_output_type_definition


def test_known_key_returns_full_definition():
    defn = get_output_type_definition("cdf")
    assert defn is not None
    assert defn["label"] == "cumulative distribution function"
    assert defn["iri"] == "http://www.probonto.org/ontology#PROB_c0000025"
    assert defn["definition_source"].startswith("MIDAS")
    assert defn["definition"]


def test_lookup_normalizes_case_and_whitespace():
    assert get_output_type_definition("CDF ") == get_output_type_definition("cdf")
    assert get_output_type_definition("  Quantile") == get_output_type_definition("quantile")


def test_quantile_uses_stato_iri():
    defn = get_output_type_definition("quantile")
    assert defn["iri"] == "http://purl.obolibrary.org/obo/STATO_0000291"
    assert defn["definition_source"] == "STATO"


def test_sample_has_no_iri():
    defn = get_output_type_definition("sample")
    assert defn is not None
    assert defn["iri"] is None


def test_unknown_key_returns_none():
    assert get_output_type_definition("not-a-real-type") is None


def test_none_input_returns_none():
    assert get_output_type_definition(None) is None


def test_learn_more_url_exposed():
    assert LEARN_MORE_URL.startswith("https://docs.hubverse.io")
