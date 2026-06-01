"""Integration tests: full v6 round generation for round 2025-07-27.

These tests run process_round() against real data in data/2025-07-27/ and
validate the produced artifacts (per-model JSON-LD, consolidated round JSON-LD,
and HTML summary).

Run with:
    pytest tests/test_jsonld_v6_integration.py -v

To regenerate snapshots after an intentional change:
    1. Run the pipeline:  python pipeline/create_jsonld_v6_0_0.py --round_dir data/2025-07-27
    2. Copy the normalized key fields into tests/snapshots/round_2025-07-27_key_fields.json
       following the schema already in that file.
"""

import json
import os

import pytest
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
ROUND_ID = "2025-07-27"
SNAPSHOT_PATH = REPO_ROOT / "tests" / "snapshots" / "round_2025-07-27_key_fields.json"

# Stable ordered list of model names inferred from data/2025-07-27/model-metadata/
EXPECTED_MODEL_NAMES = sorted(
    [
        "CEPH-ASTRA_RSV",
        "Ensemble",
        "Ensemble_LOP",
        "Ensemble_LOP_untrimmed",
        "JHU_UNC-flepiMoP",
        "UNCC-Hierbin",
        "UT-ImmunoSEIRS",
    ]
)


# ---------------------------------------------------------------------------
# Module-scoped fixtures — run the pipeline ONCE for all tests in this module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def round_output(tmp_path_factory):
    """Run process_round for 2025-07-27 and return the tmp output directory.

    The pipeline relies on relative paths (data/<round_id>/), so we temporarily
    change the working directory to the repo root while it runs.
    """
    from pipeline.create_jsonld_v6_0_0 import process_round

    out = tmp_path_factory.mktemp("v6_output")
    original_cwd = os.getcwd()
    os.chdir(REPO_ROOT)
    try:
        process_round(str(REPO_ROOT / "data" / ROUND_ID), str(out))
    finally:
        os.chdir(original_cwd)
    return out


@pytest.fixture(scope="module")
def consolidated_jsonld(round_output):
    path = round_output / f"round_{ROUND_ID}_v6.0.0.jsonld"
    assert path.exists(), f"Consolidated round JSON-LD not produced at: {path}"
    with open(path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def jhu_jsonld(round_output):
    path = round_output / ROUND_ID / "JHU_UNC-flepiMoP.jsonld"
    assert path.exists(), f"JHU model JSON-LD not produced at: {path}"
    with open(path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def html_content(round_output):
    path = round_output / ROUND_ID / f"round_{ROUND_ID}_v6.0.0.html"
    assert path.exists(), f"Round HTML not produced at: {path}"
    with open(path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Consolidated round JSON-LD
# ---------------------------------------------------------------------------


class TestConsolidatedRoundJsonLD:
    """Validates structure and semantic content of the consolidated round JSON-LD file."""

    def test_schema_context_is_schema_org(self, consolidated_jsonld):
        assert "schema.org" in consolidated_jsonld["@context"]

    def test_type_is_dataset(self, consolidated_jsonld):
        assert consolidated_jsonld["@type"] == "Dataset"

    def test_name_contains_round_id(self, consolidated_jsonld):
        assert ROUND_ID in consolidated_jsonld["name"]

    def test_identifier_is_round_id(self, consolidated_jsonld):
        assert consolidated_jsonld["identifier"] == ROUND_ID

    def test_round_id_field(self, consolidated_jsonld):
        assert consolidated_jsonld["roundId"] == ROUND_ID

    def test_has_part_is_non_empty_list(self, consolidated_jsonld):
        parts = consolidated_jsonld["hasPart"]
        assert isinstance(parts, list)
        assert len(parts) > 0

    def test_number_of_items_matches_has_part_count(self, consolidated_jsonld):
        assert consolidated_jsonld["numberOfItems"] == len(consolidated_jsonld["hasPart"])

    def test_all_parts_are_datasets(self, consolidated_jsonld):
        for part in consolidated_jsonld["hasPart"]:
            assert part.get("@type") == "Dataset", (
                f"Part '{part.get('name')}' has unexpected @type: {part.get('@type')}"
            )

    def test_expected_number_of_models(self, consolidated_jsonld):
        assert consolidated_jsonld["numberOfItems"] == len(EXPECTED_MODEL_NAMES)

    def test_all_expected_models_present(self, consolidated_jsonld):
        actual_names = sorted(p["name"] for p in consolidated_jsonld["hasPart"])
        assert actual_names == EXPECTED_MODEL_NAMES

    def test_health_condition_type(self, consolidated_jsonld):
        hc = consolidated_jsonld["healthCondition"]
        assert hc["@type"] == "MedicalCondition"

    def test_health_condition_is_rsv(self, consolidated_jsonld):
        hc = consolidated_jsonld["healthCondition"]
        assert "respiratory syncytial virus" in hc["name"].lower()

    def test_health_condition_uri(self, consolidated_jsonld):
        hc = consolidated_jsonld["healthCondition"]
        assert hc["uri"] == "http://purl.obolibrary.org/obo/MONDO_0001577"

    def test_work_example_midas_type_annotations(self, consolidated_jsonld):
        we_type = consolidated_jsonld["workExample"]["@type"]
        assert isinstance(we_type, list)
        assert "https://midasnetwork.us/ontology/class-datasetsmidas97.html" in we_type


# ---------------------------------------------------------------------------
# Per-model JSON-LD files
# ---------------------------------------------------------------------------


class TestPerModelJsonLD:
    """Validates that per-model JSON-LD files are created and well-structured."""

    def test_all_expected_model_files_exist(self, round_output):
        for name in EXPECTED_MODEL_NAMES:
            path = round_output / ROUND_ID / f"{name}.jsonld"
            assert path.exists(), f"Expected model file missing: {path}"

    def test_jhu_has_schema_context(self, jhu_jsonld):
        assert "schema.org" in jhu_jsonld["@context"]

    def test_jhu_type_is_dataset(self, jhu_jsonld):
        assert jhu_jsonld["@type"] == "Dataset"

    def test_jhu_has_author_list(self, jhu_jsonld):
        assert isinstance(jhu_jsonld.get("author"), list)
        assert len(jhu_jsonld["author"]) > 0

    def test_jhu_author_has_person_type(self, jhu_jsonld):
        assert jhu_jsonld["author"][0]["@type"] == "Person"

    def test_jhu_work_example_has_variable_measured(self, jhu_jsonld):
        we = jhu_jsonld.get("workExample", {})
        vm = we.get("variableMeasured", [])
        assert len(vm) > 0, "variableMeasured should contain at least one target"

    def test_jhu_variable_measured_has_available_output_types(self, jhu_jsonld):
        vm = jhu_jsonld["workExample"]["variableMeasured"]
        for entry in vm:
            assert "available_output_types" in entry
            assert isinstance(entry["available_output_types"], list)
            assert len(entry["available_output_types"]) > 0

    def test_jhu_variable_measured_has_identifier(self, jhu_jsonld):
        vm = jhu_jsonld["workExample"]["variableMeasured"]
        for entry in vm:
            assert "identifier" in entry, (
                f"variableMeasured entry missing 'identifier': {entry.get('name')}"
            )

    def test_jhu_variable_measured_type(self, jhu_jsonld):
        vm = jhu_jsonld["workExample"]["variableMeasured"]
        for entry in vm:
            assert entry["@type"] == "PropertyValue"

    def test_jhu_work_example_has_spatial_coverage(self, jhu_jsonld):
        we = jhu_jsonld.get("workExample", {})
        sc = we.get("spatialCoverage", [])
        assert len(sc) > 0, "spatialCoverage should contain at least one location"

    def test_jhu_spatial_coverage_has_fips_code(self, jhu_jsonld):
        sc = jhu_jsonld["workExample"]["spatialCoverage"]
        for loc in sc:
            assert "gn:fipsCode" in loc

    def test_jhu_work_example_round_info(self, jhu_jsonld):
        is_part_of = jhu_jsonld["workExample"].get("isPartOf", {})
        assert is_part_of.get("identifier") == ROUND_ID

    def test_jhu_work_example_has_temporal_coverage(self, jhu_jsonld):
        we = jhu_jsonld.get("workExample", {})
        assert "temporalCoverage" in we, "temporalCoverage should be present"

    def test_jhu_temporal_coverage_is_interval(self, jhu_jsonld):
        tc = jhu_jsonld["workExample"]["temporalCoverage"]
        assert "/" in tc, f"temporalCoverage should be ISO interval (start/end): got '{tc}'"

    def test_jhu_has_encoding_format(self, jhu_jsonld):
        we = jhu_jsonld.get("workExample", {})
        assert "encodingFormat" in we

    def test_jhu_encoding_format_has_parquet(self, jhu_jsonld):
        formats = jhu_jsonld["workExample"]["encodingFormat"]
        names = [f.get("name") for f in formats]
        assert "Apache Parquet" in names


# ---------------------------------------------------------------------------
# Round HTML output
# ---------------------------------------------------------------------------


class TestRoundHTML:
    """Validates that the round HTML file is well-formed and contains expected content."""

    def test_has_doctype(self, html_content):
        assert "<!DOCTYPE html>" in html_content

    def test_has_html_tag(self, html_content):
        assert "<html" in html_content

    def test_has_body_tag(self, html_content):
        assert "<body" in html_content

    def test_has_closing_html_tag(self, html_content):
        assert "</html>" in html_content

    def test_has_model_index(self, html_content):
        assert "Model Index" in html_content

    def test_has_spatial_coverage_section(self, html_content):
        assert "Spatial Coverage" in html_content

    def test_targets_show_available_output_types(self, html_content):
        assert "Available Output Types:" in html_content

    def test_all_model_names_in_html(self, html_content):
        for name in EXPECTED_MODEL_NAMES:
            assert name in html_content, f"Model '{name}' missing from HTML output"

    def test_reasonable_file_size(self, round_output):
        path = round_output / ROUND_ID / f"round_{ROUND_ID}_v6.0.0.html"
        assert path.stat().st_size > 5_000, "HTML file seems too small — may be incomplete"


# ---------------------------------------------------------------------------
# Snapshot regression tests
# ---------------------------------------------------------------------------


class TestSnapshot:
    """Regression snapshot tests that catch unexpected changes to stable output fields.

    To update the snapshot after an intentional change:
        1. Run:  python pipeline/create_jsonld_v6_0_0.py --round_dir data/2025-07-27
        2. Update tests/snapshots/round_2025-07-27_key_fields.json manually.
    """

    @pytest.fixture(scope="class")
    def snapshot(self):
        assert SNAPSHOT_PATH.exists(), (
            f"Snapshot file missing: {SNAPSHOT_PATH}\n"
            "Generate it by running the pipeline and saving key fields to that path."
        )
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)

    def test_identifier_matches_snapshot(self, consolidated_jsonld, snapshot):
        assert consolidated_jsonld["identifier"] == snapshot["identifier"]

    def test_number_of_items_matches_snapshot(self, consolidated_jsonld, snapshot):
        assert consolidated_jsonld["numberOfItems"] == snapshot["numberOfItems"]

    def test_health_condition_matches_snapshot(self, consolidated_jsonld, snapshot):
        assert consolidated_jsonld["healthCondition"] == snapshot["healthCondition"]

    def test_model_names_match_snapshot(self, consolidated_jsonld, snapshot):
        actual_names = sorted(p["name"] for p in consolidated_jsonld["hasPart"])
        assert actual_names == snapshot["model_names"]
