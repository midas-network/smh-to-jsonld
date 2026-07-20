"""Tests for round display names in tasks configuration."""

import json
from types import SimpleNamespace

from pipeline.clean_output import clean_output
from pipeline.create_jsonld_v6_0_0 import create_consolidated_round_jsonld_v6
from pipeline.jsonld_to_html import generate_header_section
from run_pipeline import find_consolidated_jsonld_files, get_round_id_from_jsonld_file
from utils.jsonld import (
    add_round_info,
    build_round_documentation,
    create_consolidated_round_jsonld,
    format_round_filename_stem,
    format_round_name,
)
from utils.tasks_json_parser import read_tasks_config


def test_format_round_name_uses_configured_name():
    assert format_round_name("2025-07-27", "Round 1 - 2025-2026") == "Round 1 - 2025-2026"


def test_format_round_name_falls_back_for_blank_name():
    assert format_round_name("2025-07-27", "   ") == "Round 2025-07-27"


def test_format_round_filename_stem_uses_configured_name_with_underscores():
    assert (
        format_round_filename_stem("2025-07-27", "Round 1 - 2025-2026")
        == "Round_1_-_2025-2026"
    )


def test_format_round_filename_stem_falls_back_to_date_based_name():
    assert format_round_filename_stem("2025-07-27") == "round_2025-07-27"


def test_build_round_documentation_uses_internal_round_name():
    assert build_round_documentation(
        "midas-network/rsv-scenario-modeling-hub", "round1"
    ) == {
        "url": (
            "https://github.com/midas-network/rsv-scenario-modeling-hub/"
            "blob/main/auxiliary-data/rounds/round1.md"
        ),
        "rawUrl": (
            "https://raw.githubusercontent.com/midas-network/rsv-scenario-modeling-hub/"
            "refs/heads/main/auxiliary-data/rounds/round1.md"
        ),
    }


def test_v5_add_round_info_uses_configured_round_name():
    jsonld_data = {"workExample": {}}
    round_documentation = build_round_documentation(
        "midas-network/rsv-scenario-modeling-hub", "round3"
    )
    add_round_info(
        jsonld_data,
        "2025-07-27",
        "Round 1 - 2025-2026",
        round_documentation,
    )
    assert jsonld_data["workExample"]["isPartOf"] == {
        "@type": "Event",
        "name": "Round 1 - 2025-2026",
        "identifier": "2025-07-27",
        "url": (
            "https://github.com/midas-network/rsv-scenario-modeling-hub/"
            "blob/main/auxiliary-data/rounds/round3.md"
        ),
        "sameAs": (
            "https://raw.githubusercontent.com/midas-network/rsv-scenario-modeling-hub/"
            "refs/heads/main/auxiliary-data/rounds/round3.md"
        ),
        "subjectOf": {
            "@type": "CreativeWork",
            "name": "Scenario/round definition",
            "encodingFormat": "text/markdown",
            "url": (
                "https://github.com/midas-network/rsv-scenario-modeling-hub/"
                "blob/main/auxiliary-data/rounds/round3.md"
            ),
            "sameAs": (
                "https://raw.githubusercontent.com/midas-network/rsv-scenario-modeling-hub/"
                "refs/heads/main/auxiliary-data/rounds/round3.md"
            ),
        },
    }


def test_tasks_config_reads_round_name_from_additional_metadata(tmp_path):
    tasks_path = tmp_path / "tasks.json"
    tasks_path.write_text(
        json.dumps(
            {
                "schema_version": "https://example.org/v5.1.0/tasks-schema.json",
                "rounds": [
                    {
                        "round_id": "2025-07-27",
                        "round_id_from_variable": False,
                        "additional_metadata": {
                            "round_name": "Round 1 - 2025-2026",
                            "internal_round_name": "round3",
                        },
                        "model_tasks": [],
                    }
                ],
            }
        )
    )
    (tmp_path / "admin.json").write_text(
        json.dumps(
            {
                "repository": {
                    "host": "github",
                    "owner": "midas-network",
                    "name": "rsv-scenario-modeling-hub",
                }
            }
        )
    )

    config = read_tasks_config(str(tasks_path))

    round_config = config.get_round_by_id("2025-07-27")
    assert round_config.round_name == "Round 1 - 2025-2026"
    assert round_config.internal_round_name == "round3"
    assert round_config.round_documentation["url"].endswith(
        "/blob/main/auxiliary-data/rounds/round3.md"
    )


def test_v5_consolidated_file_uses_configured_round_name(tmp_path):
    round_output_dir = tmp_path / "2025-07-27"
    round_output_dir.mkdir()
    (round_output_dir / "Example.jsonld").write_text(
        json.dumps({"@type": "Dataset", "name": "Example"})
    )
    stale_jsonld = tmp_path / "round_2025-07-27_v5.1.0.jsonld"
    stale_html = tmp_path / "round_2025-07-27_v5.1.0.html"
    stale_jsonld.write_text(
        json.dumps(
            {
                "@type": "Dataset",
                "identifier": "2025-07-27",
                "roundId": "2025-07-27",
                "hasPart": [],
            }
        )
    )
    stale_html.write_text("<html></html>")

    round_obj = SimpleNamespace(
        round_id="2025-07-27",
        round_name="Round 1 - 2025-2026",
        round_documentation=build_round_documentation(
            "midas-network/rsv-scenario-modeling-hub", "round3"
        ),
        diseases=[],
    )
    config = SimpleNamespace(
        rounds=[round_obj],
        get_round_by_id=lambda round_id: round_obj if round_id == "2025-07-27" else None,
    )

    output_path = create_consolidated_round_jsonld(
        str(round_output_dir),
        "2025-07-27",
        config,
        {},
        {},
        output_dir=str(tmp_path),
        schema_version="5.1.0",
    )

    assert output_path.endswith("Round_1_-_2025-2026_v5.1.0.jsonld")
    assert not stale_jsonld.exists()
    assert not stale_html.exists()
    with open(output_path) as f:
        data = json.load(f)
    assert data["url"].endswith("/blob/main/auxiliary-data/rounds/round3.md")


def test_v6_consolidated_file_uses_configured_round_name(tmp_path):
    round_output_dir = tmp_path / "2025-07-27"
    round_output_dir.mkdir()
    (round_output_dir / "Example.jsonld").write_text(
        json.dumps({"@type": "Dataset", "name": "Example"})
    )
    stale_jsonld = tmp_path / "round_2025-07-27_v6.0.0.jsonld"
    stale_html = tmp_path / "round_2025-07-27_v6.0.0.html"
    stale_jsonld.write_text(
        json.dumps(
            {
                "@type": "Dataset",
                "identifier": "2025-07-27",
                "roundId": "2025-07-27",
                "hasPart": [],
            }
        )
    )
    stale_html.write_text("<html></html>")

    output_path = create_consolidated_round_jsonld_v6(
        round_output_dir,
        tmp_path,
        "2025-07-27",
        "Round 1 - 2025-2026",
        build_round_documentation("midas-network/rsv-scenario-modeling-hub", "round3"),
        diseases=[],
    )

    assert output_path.name == "Round_1_-_2025-2026_v6.0.0.jsonld"
    assert not stale_jsonld.exists()
    assert not stale_html.exists()
    with open(output_path) as f:
        data = json.load(f)
    assert data["url"].endswith("/blob/main/auxiliary-data/rounds/round3.md")


def test_clean_output_matches_name_based_file_by_round_id(tmp_path):
    jsonld_path = tmp_path / "Round_1_-_2025-2026_v6.0.0.jsonld"
    html_path = tmp_path / "Round_1_-_2025-2026_v6.0.0.html"
    jsonld_path.write_text(
        json.dumps(
            {
                "@type": "Dataset",
                "identifier": "2025-07-27",
                "roundId": "2025-07-27",
                "hasPart": [],
            }
        )
    )
    html_path.write_text("<html></html>")

    success = clean_output(str(tmp_path), rounds=["2025-07-27"])

    assert success
    assert not jsonld_path.exists()
    assert not html_path.exists()


def test_run_pipeline_discovers_name_based_consolidated_file(tmp_path):
    consolidated_path = tmp_path / "Round_1_-_2025-2026_v6.0.0.jsonld"
    model_path = tmp_path / "Example.jsonld"
    consolidated_path.write_text(
        json.dumps(
            {
                "@type": "Dataset",
                "identifier": "2025-07-27",
                "roundId": "2025-07-27",
                "hasPart": [{"name": "Example"}],
            }
        )
    )
    model_path.write_text(json.dumps({"@type": "Dataset", "identifier": "Example"}))

    assert get_round_id_from_jsonld_file(consolidated_path) == "2025-07-27"
    assert find_consolidated_jsonld_files(tmp_path) == [consolidated_path]


def test_html_header_includes_round_documentation_link():
    html = generate_header_section(
        {
            "name": "Round 1 - 2025-2026 Scenario Projection Models Collection",
            "description": "Collection of model output from round 2025-07-27",
            "identifier": "2025-07-27",
            "numberOfItems": 7,
            "subjectOf": {
                "@type": "CreativeWork",
                "url": (
                    "https://github.com/midas-network/rsv-scenario-modeling-hub/"
                    "blob/main/auxiliary-data/rounds/round3.md"
                ),
                "sameAs": (
                    "https://raw.githubusercontent.com/midas-network/"
                    "rsv-scenario-modeling-hub/refs/heads/main/"
                    "auxiliary-data/rounds/round3.md"
                ),
            },
        }
    )

    assert "Scenario/Round Definition" in html
    assert "https://github.com/midas-network/rsv-scenario-modeling-hub/blob/main/auxiliary-data/rounds/round3.md" in html
    assert "raw markdown" in html
