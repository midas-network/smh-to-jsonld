"""Integration tests: v5 round generation for round 2023-11-12.

These tests run process_single_round() against real data in data/2023-11-12/ and
validate that the consolidated round JSON-LD model list matches expected models.

Run with:
    pytest tests/test_jsonld_v5_integration.py -v
"""

import json
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
ROUND_ID = "2023-11-12"
EXPECTED_MODEL_NAMES = sorted(
    [
        "CEPH-MetaRSV",
        "CU-RSV_SVIRS",
        "Ensemble",
        "Ensemble_LOP",
        "Ensemble_LOP_all",
        "Ensemble_LOP_untrimmed",
        "JHU_UNC-flepiMoP",
        "MOBS_NEU-GLEAM_RSV",
        "NIH-RSV_MSIRS",
        "NIH-RSV_Phenomenological",
        "NIH-RSV_WIN",
        "NotreDame-FRED",
        "PSI-PROF",
        "USC-SIkJalpha",
        "UT-ImmunoSEIRS",
        "UVA-EpiHiperRSV",
    ]
)


@pytest.fixture(scope="module")
def v5_round_output(tmp_path_factory):
    """Run v5 JSON-LD generation for 2023-11-12 and return temp output dir."""
    from pipeline.create_jsonld_v5_1_0 import process_single_round

    out = tmp_path_factory.mktemp("v5_output")
    original_cwd = os.getcwd()
    os.chdir(REPO_ROOT)
    try:
        process_single_round(
            round_dir=ROUND_ID,
            base_dir=str(REPO_ROOT / "data"),
            metadata_subdir="model-metadata",
            output_dir=str(out),
        )
    finally:
        os.chdir(original_cwd)

    return out


@pytest.fixture(scope="module")
def consolidated_jsonld(v5_round_output):
    path = v5_round_output / f"round_{ROUND_ID}_v5.1.0.jsonld"
    assert path.exists(), f"Consolidated round JSON-LD not produced at: {path}"
    with open(path) as f:
        return json.load(f)


class TestV5ConsolidatedRoster:
    def test_expected_number_of_models(self, consolidated_jsonld):
        assert consolidated_jsonld["numberOfItems"] == len(EXPECTED_MODEL_NAMES)

    def test_all_expected_models_present(self, consolidated_jsonld):
        actual_names = sorted(part["name"] for part in consolidated_jsonld["hasPart"])
        assert actual_names == EXPECTED_MODEL_NAMES

    def test_has_part_count_matches_number_of_items(self, consolidated_jsonld):
        assert consolidated_jsonld["numberOfItems"] == len(consolidated_jsonld["hasPart"])

    def test_all_parts_have_non_empty_encoding_format(self, consolidated_jsonld):
        for part in consolidated_jsonld["hasPart"]:
            formats = part["workExample"].get("encodingFormat")
            assert formats, f"encodingFormat missing or empty for {part['name']}"

    def test_all_parts_have_parquet_encoding_format(self, consolidated_jsonld):
        for part in consolidated_jsonld["hasPart"]:
            format_names = {
                file_format.get("name")
                for file_format in part["workExample"].get("encodingFormat", [])
            }
            assert "Apache Parquet" in format_names, (
                f"Apache Parquet encodingFormat missing for {part['name']}"
            )


class TestV5RoundDirectoryOutput:
    def test_duplicate_round_html_not_written_to_round_directory(self, v5_round_output):
        path = v5_round_output / ROUND_ID / f"round_{ROUND_ID}_v5.1.0.html"
        assert not path.exists(), f"Duplicate round HTML should not be produced at: {path}"
