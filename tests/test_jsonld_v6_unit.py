"""Unit tests for pure-function logic in pipeline/create_jsonld_v6.py and supporting utils.

Run with:
    pytest tests/test_jsonld_v6_unit.py -v
"""

import pytest
import pandas as pd

from pipeline.create_jsonld_v6 import (
    _extract_round_id,
    extract_diseases,
    extract_target_metadata,
    build_target_objects,
    merge_field_values,
    safe_temporal_coverage,
)
from utils.location import get_location_info
from utils.temporal import calculate_temporal_coverage


# ---------------------------------------------------------------------------
# _extract_round_id
# ---------------------------------------------------------------------------


class TestExtractRoundId:
    """Tests for _extract_round_id: resolves a concrete date string from a round definition."""

    def test_fixed_round_id_returned_directly(self):
        round_data = {
            "round_id_from_variable": False,
            "round_id": "2025-07-27",
            "model_tasks": [],
        }
        assert _extract_round_id(round_data) == "2025-07-27"

    def test_variable_round_id_extracts_first_required_value(self):
        round_data = {
            "round_id_from_variable": True,
            "round_id": "origin_date",
            "model_tasks": [
                {
                    "task_ids": {
                        "origin_date": {"required": ["2025-07-27"], "optional": None}
                    }
                }
            ],
        }
        assert _extract_round_id(round_data) == "2025-07-27"

    def test_variable_round_id_no_required_returns_none(self):
        round_data = {
            "round_id_from_variable": True,
            "round_id": "origin_date",
            "model_tasks": [
                {
                    "task_ids": {
                        "origin_date": {"required": None, "optional": ["2025-07-27"]}
                    }
                }
            ],
        }
        assert _extract_round_id(round_data) is None

    def test_missing_round_id_from_variable_key_defaults_to_fixed(self):
        """round_id_from_variable defaults to False when absent."""
        round_data = {"round_id": "2023-11-12", "model_tasks": []}
        assert _extract_round_id(round_data) == "2023-11-12"

    def test_integer_round_id_coerced_to_string(self):
        round_data = {
            "round_id_from_variable": False,
            "round_id": 1,
            "model_tasks": [],
        }
        assert _extract_round_id(round_data) == "1"


# ---------------------------------------------------------------------------
# extract_diseases
# ---------------------------------------------------------------------------


class TestExtractDiseases:
    """Tests for extract_diseases: reads disease list from round config."""

    def test_v6_additional_metadata_disease(self):
        round_config = {
            "additional_metadata": {
                "disease": [
                    {
                        "name": "respiratory syncytial virus infectious disease",
                        "uri": "http://purl.obolibrary.org/obo/MONDO_0001577",
                    }
                ]
            }
        }
        result = extract_diseases(round_config)
        assert len(result) == 1
        assert result[0]["name"] == "respiratory syncytial virus infectious disease"
        assert result[0]["uri"] == "http://purl.obolibrary.org/obo/MONDO_0001577"

    def test_backward_compatible_top_level_disease(self):
        round_config = {"disease": [{"name": "RSV", "uri": "http://example.org/rsv"}]}
        result = extract_diseases(round_config)
        assert result == [{"name": "RSV", "uri": "http://example.org/rsv"}]

    def test_no_disease_returns_empty_list(self):
        assert extract_diseases({}) == []

    def test_additional_metadata_takes_precedence_over_top_level(self):
        round_config = {
            "additional_metadata": {"disease": [{"name": "A"}]},
            "disease": [{"name": "B"}],
        }
        result = extract_diseases(round_config)
        assert len(result) == 1
        assert result[0]["name"] == "A"

    def test_multiple_diseases_returned(self):
        round_config = {
            "additional_metadata": {
                "disease": [
                    {"name": "Influenza", "uri": "http://example.org/flu"},
                    {"name": "RSV", "uri": "http://example.org/rsv"},
                ]
            }
        }
        result = extract_diseases(round_config)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# extract_target_metadata
# ---------------------------------------------------------------------------


class TestExtractTargetMetadata:
    """Tests for extract_target_metadata: parses target definitions from tasks.json round config."""

    @pytest.fixture
    def round_config(self):
        return {
            "model_tasks": [
                {
                    "target_metadata": [
                        {
                            "target_id": "inc hosp",
                            "target_name": "Weekly incident RSV hospitalizations",
                            "description": "Weekly RSV hospitalizations",
                            "target_units": "count",
                            "target_keys": {"target": "inc hosp"},
                            "target_type": "discrete",
                            "is_step_ahead": True,
                            "time_unit": "week",
                            "additional_metadata": {
                                "alternative_name": "incident hospitalization count",
                                "uri": "http://purl.obolibrary.org/obo/APOLLO_SV_00000645",
                            },
                        }
                    ]
                }
            ]
        }

    def test_target_id_is_key(self, round_config):
        result = extract_target_metadata(round_config)
        assert "inc hosp" in result

    def test_uri_extracted_from_additional_metadata(self, round_config):
        result = extract_target_metadata(round_config)
        assert result["inc hosp"]["uri"] == "http://purl.obolibrary.org/obo/APOLLO_SV_00000645"

    def test_alternative_name_extracted_from_additional_metadata(self, round_config):
        result = extract_target_metadata(round_config)
        assert result["inc hosp"]["alternative_name"] == "incident hospitalization count"

    def test_core_fields_preserved(self, round_config):
        result = extract_target_metadata(round_config)
        t = result["inc hosp"]
        assert t["target_name"] == "Weekly incident RSV hospitalizations"
        assert t["target_units"] == "count"
        assert t["is_step_ahead"] is True
        assert t["time_unit"] == "week"

    def test_duplicate_targets_deduped_first_wins(self):
        round_config = {
            "model_tasks": [
                {
                    "target_metadata": [
                        {
                            "target_id": "inc hosp",
                            "target_name": "First",
                            "additional_metadata": {},
                        }
                    ]
                },
                {
                    "target_metadata": [
                        {
                            "target_id": "inc hosp",
                            "target_name": "Duplicate — should be ignored",
                            "additional_metadata": {},
                        }
                    ]
                },
            ]
        }
        result = extract_target_metadata(round_config)
        assert len(result) == 1
        assert result["inc hosp"]["target_name"] == "First"


# ---------------------------------------------------------------------------
# build_target_objects
# ---------------------------------------------------------------------------


class TestBuildTargetObjects:
    """Tests for build_target_objects: converts target metadata into variableMeasured entries."""

    @pytest.fixture
    def inc_hosp_meta(self):
        return {
            "inc hosp": {
                "target_id": "inc hosp",
                "target_name": "Weekly incident RSV hospitalizations",
                "description": "Weekly RSV hospitalizations",
                "target_units": "count",
                "target_keys": {"target": "inc hosp"},
                "target_type": "discrete",
                "is_step_ahead": True,
                "time_unit": "week",
                "uri": "http://purl.obolibrary.org/obo/APOLLO_SV_00000645",
                "alternative_name": "incident hospitalization count",
            }
        }

    def test_produces_property_value_type(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert len(result) == 1
        assert result[0]["@type"] == "PropertyValue"

    def test_uri_mapped_to_identifier(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert result[0]["identifier"] == "http://purl.obolibrary.org/obo/APOLLO_SV_00000645"

    def test_step_ahead_sets_temporal_unit(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert result[0]["temporalUnit"] == "week"

    def test_non_step_ahead_omits_temporal_unit(self):
        meta = {
            "peak size hosp": {
                "target_id": "peak size hosp",
                "target_name": "Peak size of hospitalization",
                "description": "Peak",
                "target_units": "count",
                "target_keys": {"target": "peak size hosp"},
                "target_type": "discrete",
                "is_step_ahead": False,
                "time_unit": None,
                "uri": "http://w3id.org/midas-metadata/midas100",
                "alternative_name": "peak hospitalization count",
            }
        }
        result = build_target_objects(meta, {"target": ["peak size hosp"]})
        assert "temporalUnit" not in result[0]

    def test_unobserved_target_excluded(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc inf"]})
        assert result == []

    def test_empty_observed_set_includes_all(self, inc_hosp_meta):
        """When distinct_field_values["target"] is empty, no filtering is applied."""
        result = build_target_objects(inc_hosp_meta, {"target": []})
        assert len(result) == 1

    def test_alternative_name_mapped_to_alternate_name(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert result[0]["alternateName"] == "incident hospitalization count"

    def test_unit_text_mapped(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert result[0]["unitText"] == "count"

    def test_target_id_and_type_preserved(self, inc_hosp_meta):
        result = build_target_objects(inc_hosp_meta, {"target": ["inc hosp"]})
        assert result[0]["target_id"] == "inc hosp"
        assert result[0]["target_type"] == "discrete"


# ---------------------------------------------------------------------------
# merge_field_values
# ---------------------------------------------------------------------------


class TestMergeFieldValues:
    """Tests for merge_field_values: accumulates model field values into a global dict."""

    def test_new_field_added(self):
        global_dict = {}
        merge_field_values(global_dict, {"target": ["inc hosp"]})
        assert global_dict == {"target": ["inc hosp"]}

    def test_existing_values_deduped(self):
        global_dict = {"target": ["inc hosp"]}
        merge_field_values(global_dict, {"target": ["inc hosp", "inc inf"]})
        assert global_dict["target"].count("inc hosp") == 1
        assert "inc inf" in global_dict["target"]

    def test_multiple_fields_merged(self):
        global_dict = {}
        merge_field_values(global_dict, {"target": ["inc hosp"], "location": ["US"]})
        merge_field_values(global_dict, {"target": ["inc inf"], "location": ["US", "06"]})
        assert set(global_dict["target"]) == {"inc hosp", "inc inf"}
        assert set(global_dict["location"]) == {"US", "06"}


# ---------------------------------------------------------------------------
# safe_temporal_coverage
# ---------------------------------------------------------------------------


class TestSafeTemporalCoverage:
    """Tests for safe_temporal_coverage: defensive wrapper around calculate_temporal_coverage."""

    def test_missing_origin_date_field_returns_empty(self):
        assert safe_temporal_coverage({"horizon": [1, 2]}) == {}

    def test_missing_horizon_field_returns_empty(self):
        assert safe_temporal_coverage({"origin_date": ["2025-07-27"]}) == {}

    def test_empty_origin_date_list_returns_empty(self):
        assert safe_temporal_coverage({"origin_date": [], "horizon": [1, 45]}) == {}

    def test_empty_horizon_list_returns_empty(self):
        assert safe_temporal_coverage({"origin_date": ["2025-07-27"], "horizon": []}) == {}

    def test_valid_fields_return_start_and_end(self):
        result = safe_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 2, 45]}
        )
        assert "startDate" in result
        assert "endDate" in result

    def test_end_date_is_after_start_date(self):
        result = safe_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 45]}
        )
        assert result["endDate"] > result["startDate"]


# ---------------------------------------------------------------------------
# calculate_temporal_coverage (utils/temporal.py)
# ---------------------------------------------------------------------------


class TestCalculateTemporalCoverage:
    """Tests for calculate_temporal_coverage: converts origin_date + horizon to date range."""

    def test_start_date_equals_origin_date(self):
        result = calculate_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 2, 45]}
        )
        assert result["startDate"] == pd.Timestamp("2025-07-27")

    def test_end_date_uses_max_horizon(self):
        result = calculate_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 2, 45]}
        )
        expected = (
            pd.Timestamp("2025-07-27")
            - pd.DateOffset(days=1)
            + pd.DateOffset(weeks=45)
        )
        assert result["endDate"] == expected

    def test_end_date_known_value_for_round_2025_07_27(self):
        """Concrete regression: 45 weeks from 2025-07-27 ends on 2026-06-06."""
        result = calculate_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 45]}
        )
        assert result["endDate"] == pd.Timestamp("2026-06-06")

    def test_end_date_after_start_date(self):
        result = calculate_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": [1, 29]}
        )
        assert result["endDate"] > result["startDate"]

    def test_horizon_strings_coerced_to_int(self):
        """Horizon values provided as strings are coerced correctly."""
        result = calculate_temporal_coverage(
            {"origin_date": ["2025-07-27"], "horizon": ["1", "45"]}
        )
        assert result["startDate"] == pd.Timestamp("2025-07-27")


# ---------------------------------------------------------------------------
# get_location_info (utils/location.py)
# ---------------------------------------------------------------------------


class TestGetLocationInfo:
    """Tests for get_location_info: maps FIPS codes to structured location objects."""

    def test_state_fips_name_california(self):
        result = get_location_info("06")
        assert result["gn:name"] == "California"

    def test_state_fips_iso3166_2_code(self):
        result = get_location_info("06")
        assert result["iso3166-2:code"] == "US-CA"

    def test_state_fips_code_stored(self):
        result = get_location_info("06")
        assert result["gn:fipsCode"] == "06"

    def test_has_geonames_id_uri(self):
        result = get_location_info("06")
        assert result["@id"].startswith("http://sws.geonames.org/")

    def test_us_national_fips_name(self):
        result = get_location_info("US")
        assert result["gn:name"] == "United States"

    def test_county_fips_includes_state_name(self):
        # 06001 = Alameda County, California
        result = get_location_info("06001")
        assert "California" in result["gn:name"]

    def test_feature_type(self):
        result = get_location_info("06")
        assert result["@type"] == "gn:Feature"

    def test_context_has_required_namespaces(self):
        result = get_location_info("06")
        ctx = result["@context"]
        assert "gn" in ctx
        assert "iso3166-2" in ctx

    def test_texas_fips(self):
        result = get_location_info("48")
        assert result["gn:name"] == "Texas"
        assert result["iso3166-2:code"] == "US-TX"
