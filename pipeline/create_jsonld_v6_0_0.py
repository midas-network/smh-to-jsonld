"""JSON-LD generation for Hubverse schema v6.0.0 rounds.

Usage (single round):
    uv run python pipeline/create_jsonld_v6_0_0.py --round_dir data/2025-07-27
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
from pathlib import Path

# Add parent directory to path to allow imports from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.jsonld import yaml_to_jsonld
from utils.location import get_location_info
from utils.loggings import setup_logging
from utils.model_output_smh import (
    get_distinct_field_values,
    get_hub_ds,
    get_output_file_types,
)
from utils.temporal import calculate_temporal_coverage

SCHEMA_VERSION = "6.0.0"


def parse_command_line_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=f"Create JSON-LD for a single v{SCHEMA_VERSION} round directory."
    )
    parser.add_argument(
        "--round_dir",
        default="data/2025-07-27",
        help="Round directory containing hub-config, model-metadata, and model-output",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Directory where round JSON-LD outputs should be written",
    )
    return parser.parse_args()


def merge_field_values(global_dict, model_field_values):
    """Merge model field values into the global field values dictionary."""
    for field, values in model_field_values.items():
        if field not in global_dict:
            global_dict[field] = values
        else:
            for value in values:
                if value not in global_dict[field]:
                    global_dict[field].append(value)


def find_yaml_files(metadata_dir):
    """Find YAML files in the metadata directory."""
    metadata_path = Path(metadata_dir)
    yaml_files = sorted(
        f.name for f in metadata_path.iterdir() if f.suffix.lower() in {".yaml", ".yml"}
    )

    if not yaml_files:
        logging.warning(f"No YAML files found in '{metadata_dir}'")
        return []

    logging.info(f"Found {len(yaml_files)} model metadata files")
    return yaml_files


def prepare_round_output_directory(output_dir, round_id):
    """Prepare output folder for the current round."""
    round_output_dir = Path(output_dir) / round_id
    if round_output_dir.exists():
        shutil.rmtree(round_output_dir)
    round_output_dir.mkdir(parents=True, exist_ok=True)
    return round_output_dir


def _extract_round_id(round_data):
    """Extract a concrete round id from a round definition in tasks.json."""
    if not round_data.get("round_id_from_variable", False):
        value = round_data.get("round_id")
        return str(value) if value is not None else None

    variable_name = round_data.get("round_id")
    for model_task in round_data.get("model_tasks", []):
        task_ids = model_task.get("task_ids", {})
        variable_cfg = task_ids.get(variable_name, {})
        required_values = variable_cfg.get("required") if isinstance(variable_cfg, dict) else None
        if required_values:
            return str(required_values[0])
    return None


def load_tasks_and_round_config(round_path, expected_round_id):
    """Load tasks.json and return the round config matching the requested round id."""
    tasks_path = Path(round_path) / "hub-config" / "tasks.json"
    if not tasks_path.exists():
        raise FileNotFoundError(f"tasks.json not found at {tasks_path}")

    with open(tasks_path, "r") as f:
        tasks_data = json.load(f)

    matched_round = None
    discovered_rounds = []
    for round_data in tasks_data.get("rounds", []):
        round_id = _extract_round_id(round_data)
        if round_id:
            discovered_rounds.append(round_id)
        if round_id == expected_round_id:
            matched_round = round_data
            break

    if matched_round is None:
        raise ValueError(
            f"Round '{expected_round_id}' was not found in {tasks_path}. "
            f"Available round ids: {', '.join(discovered_rounds) if discovered_rounds else 'none'}"
        )

    return tasks_data, matched_round


def extract_target_metadata(round_config):
    """Extract target metadata from a v6 tasks.json round definition."""
    target_metadata = {}

    for model_task in round_config.get("model_tasks", []):
        output_type_names = sorted((model_task.get("output_type") or {}).keys())

        for target in model_task.get("target_metadata", []):
            target_id = target.get("target_id")
            if not target_id:
                continue

            if target_id in target_metadata:
                target_metadata[target_id]["_output_type_set"].update(output_type_names)
                continue

            additional_metadata = target.get("additional_metadata") or {}
            target_metadata[target_id] = {
                "target_id": target_id,
                "target_name": target.get("target_name"),
                "description": target.get("description"),
                "target_units": target.get("target_units"),
                "target_keys": target.get("target_keys"),
                "target_type": target.get("target_type"),
                "is_step_ahead": target.get("is_step_ahead"),
                "time_unit": target.get("time_unit"),
                "uri": target.get("uri") or additional_metadata.get("uri"),
                "alternative_name": target.get("alternative_name")
                or additional_metadata.get("alternative_name"),
                "_output_type_set": set(output_type_names),
            }

    for target in target_metadata.values():
        output_type_set = target.pop("_output_type_set", set())
        if output_type_set:
            target["available_output_types"] = sorted(output_type_set)

    return target_metadata


def extract_diseases(round_config):
    """Extract disease metadata from round-level v6 tasks.json additional metadata."""
    additional_metadata = round_config.get("additional_metadata") or {}
    disease_list = additional_metadata.get("disease")
    if disease_list:
        return disease_list

    # Backward compatibility: old schema used top-level "disease".
    return round_config.get("disease", [])


def initialize_work_example(jsonld_data):
    """Ensure workExample exists in the JSON-LD data."""
    if "workExample" not in jsonld_data:
        jsonld_data["workExample"] = {
            "@type": "Dataset",
            "description": "RSV disease projection outputs",
        }


def add_round_info(jsonld_data, round_id):
    """Add round information to the workExample."""
    jsonld_data["workExample"]["isPartOf"] = {
        "@type": "Event",
        "name": f"Round {round_id}",
        "identifier": round_id,
    }


def add_file_formats(jsonld_data, file_types):
    """Add file format information to the workExample."""
    if not file_types:
        return

    jsonld_data["workExample"]["encodingFormat"] = []

    if file_types.get("parquet", 0) > 0 or file_types.get("gz.parquet", 0) > 0:
        jsonld_data["workExample"]["encodingFormat"].append(
            {
                "@type": "FileFormat",
                "name": "Apache Parquet",
                "fileExtension": ".parquet",
            }
        )


def add_spatial_coverage(jsonld_data, locations):
    """Add spatial coverage information to the workExample."""
    jsonld_data["workExample"]["spatialCoverage"] = []

    for location_fips in locations:
        location_info = get_location_info(str(location_fips))
        if location_info:
            jsonld_data["workExample"]["spatialCoverage"].append(location_info)


def add_temporal_coverage(jsonld_data, temporal_coverage):
    """Add temporal coverage to workExample if available."""
    if "startDate" in temporal_coverage and "endDate" in temporal_coverage:
        jsonld_data["workExample"]["temporalCoverage"] = (
            f"{temporal_coverage['startDate']}/{temporal_coverage['endDate']}"
        )


def build_target_objects(target_metadata, distinct_field_values):
    """Build variableMeasured entries using target metadata and observed targets."""
    observed_targets = {str(t) for t in distinct_field_values.get("target", [])}
    target_obj_list = []

    for target_id, target in target_metadata.items():
        if observed_targets and target_id not in observed_targets:
            continue

        target_obj = {
            "@type": "PropertyValue",
            "name": target.get("target_name") or target_id,
        }

        if target.get("uri"):
            target_obj["identifier"] = target["uri"]

        if target.get("alternative_name"):
            target_obj["alternateName"] = target["alternative_name"]

        if target.get("description"):
            target_obj["description"] = target["description"]

        if target.get("target_units"):
            target_obj["unitText"] = target["target_units"]

        if target.get("target_id"):
            target_obj["target_id"] = target["target_id"]

        if target.get("target_type"):
            target_obj["target_type"] = target["target_type"]

        if target.get("target_keys"):
            target_obj["target_keys"] = target["target_keys"]

        if target.get("available_output_types"):
            target_obj["available_output_types"] = target["available_output_types"]

        if target.get("is_step_ahead") and target.get("time_unit"):
            target_obj["temporalUnit"] = target["time_unit"]

        target_obj_list.append(target_obj)

    return target_obj_list


def safe_temporal_coverage(distinct_field_values):
    """Calculate temporal coverage only when required fields are available."""
    if "origin_date" not in distinct_field_values or "horizon" not in distinct_field_values:
        return {}

    if not distinct_field_values.get("origin_date") or not distinct_field_values.get("horizon"):
        return {}

    try:
        return calculate_temporal_coverage(distinct_field_values)
    except Exception as exc:
        logging.warning(f"Unable to calculate temporal coverage: {exc}")
        return {}


def enrich_jsonld_with_model_output_v6(
    jsonld_data,
    round_id,
    model_name,
    round_path,
    target_metadata,
    distinct_field_values,
):
    """Enrich JSON-LD data with model output data for the matched v6 round."""
    output_types = distinct_field_values.get("output_type", [])
    locations = distinct_field_values.get("location", [])
    age_groups = distinct_field_values.get("age_group", [])
    temporal_coverage = safe_temporal_coverage(distinct_field_values)
    target_obj_list = build_target_objects(target_metadata, distinct_field_values)

    model_output_dir = Path(round_path) / "model-output" / model_name
    file_types = get_output_file_types(
        round_id,
        model=model_name,
        directory=str(model_output_dir),
    )

    initialize_work_example(jsonld_data)
    add_round_info(jsonld_data, round_id)
    jsonld_data["workExample"]["output_type"] = [output_types]

    add_file_formats(jsonld_data, file_types)

    if target_obj_list:
        jsonld_data["workExample"]["variableMeasured"] = target_obj_list

    add_spatial_coverage(jsonld_data, locations)

    if age_groups:
        jsonld_data["workExample"]["ageGroups"] = [str(age_group) for age_group in age_groups]

    add_temporal_coverage(jsonld_data, temporal_coverage)


def process_single_model(
    yaml_file,
    round_id,
    round_path,
    metadata_dir,
    round_output_dir,
    target_metadata,
    global_field_values_dict,
    field_values_by_model,
):
    """Process a single model metadata file into JSON-LD."""
    model_name = Path(yaml_file).stem
    yaml_path = Path(metadata_dir) / yaml_file
    model_output_dir = Path(round_path) / "model-output" / model_name

    logging.info(f"Processing {model_name}...")

    jsonld_data = yaml_to_jsonld(str(yaml_path))

    if model_output_dir.exists():
        try:
            hub_df, hub_schema = get_hub_ds(round_id, model_name)
            distinct_field_values = get_distinct_field_values(hub_df, hub_schema)
            field_values_by_model[model_name] = distinct_field_values
            merge_field_values(global_field_values_dict, distinct_field_values)

            enrich_jsonld_with_model_output_v6(
                jsonld_data,
                round_id,
                model_name,
                round_path,
                target_metadata,
                distinct_field_values,
            )
        except Exception as exc:
            logging.warning(f"Skipping model output enrichment for {model_name}: {exc}")
    else:
        logging.warning(f"Model output folder not found for {model_name}: {model_output_dir}")

    output_path = Path(round_output_dir) / f"{model_name}.jsonld"
    with open(output_path, "w") as f:
        json.dump(jsonld_data, f, indent=2)

    return {
        "model": model_name,
        "yaml_file": str(yaml_path),
        "jsonld_file": str(output_path),
        "round_id": round_id,
    }


def create_consolidated_round_jsonld_v6(round_output_dir, output_dir, round_id, diseases):
    """Create a consolidated round-level JSON-LD for all model JSON-LD files."""
    logging.info(f"Creating consolidated JSON-LD for round {round_id}...")

    jsonld_files = sorted(
        f
        for f in os.listdir(round_output_dir)
        if f.endswith(".jsonld") and not f.startswith("round_")
    )

    consolidated = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "name": f"Round {round_id} Scenario Projection Models Collection",
        "description": f"Collection of model output from round {round_id}",
        "identifier": round_id,
        "roundId": round_id,
        "hasPart": [],
        "workExample": {
            "@type": [
                "Dataset",
                "https://midasnetwork.us/ontology/class-datasetsmidas97.html",
                "https://midasnetwork.us/ontology/class-oboobcs_0000267.html",
            ],
            "description": "RSV disease projection outputs",
        },
    }

    disease_entries = []
    for disease in diseases:
        if disease.get("name") or disease.get("uri"):
            disease_entries.append(
                {
                    "@type": "MedicalCondition",
                    "name": disease.get("name"),
                    "uri": disease.get("uri"),
                }
            )

    if len(disease_entries) == 1:
        consolidated["healthCondition"] = disease_entries[0]
    elif disease_entries:
        consolidated["healthCondition"] = disease_entries

    for jsonld_file in jsonld_files:
        file_path = Path(round_output_dir) / jsonld_file
        try:
            with open(file_path, "r") as f:
                model_data = json.load(f)
            consolidated["hasPart"].append(model_data)
        except Exception as exc:
            logging.error(f"Error reading {file_path}: {exc}")

    consolidated["numberOfItems"] = len(consolidated["hasPart"])

    output_path = Path(output_dir) / f"round_{round_id}_v{SCHEMA_VERSION}.jsonld"
    with open(output_path, "w") as f:
        json.dump(consolidated, f, indent=2)

    logging.info(
        f"Consolidated JSON-LD written to {output_path} "
        f"with {consolidated['numberOfItems']} models included"
    )

    return output_path


def process_round(round_dir, output_dir):
    """Process a single round directory."""
    round_path = Path(round_dir)
    if not round_path.exists() or not round_path.is_dir():
        raise FileNotFoundError(f"Round directory not found: {round_dir}")

    round_id = round_path.name
    if not re.match(r"\d{4}-\d{2}-\d{2}", round_id):
        raise ValueError(
            f"Round directory name must be YYYY-MM-DD. Found '{round_id}' in {round_dir}"
        )

    metadata_dir = round_path / "model-metadata"
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {metadata_dir}")

    _, round_config = load_tasks_and_round_config(round_path, round_id)
    target_metadata = extract_target_metadata(round_config)
    diseases = extract_diseases(round_config)

    round_output_dir = prepare_round_output_directory(output_dir, round_id)
    yaml_files = find_yaml_files(metadata_dir)

    global_field_values_dict = {}
    field_values_by_model = {}
    results = []

    for yaml_file in yaml_files:
        result = process_single_model(
            yaml_file,
            round_id,
            round_path,
            metadata_dir,
            round_output_dir,
            target_metadata,
            global_field_values_dict,
            field_values_by_model,
        )
        results.append(result)

    create_consolidated_round_jsonld_v6(
        round_output_dir, output_dir, round_id, diseases
    )

    return results


def log_processing_summary(results, args):
    """Log run summary."""
    logging.info(f"Processed {len(results)} models")
    logging.info(f"Round directory: {args.round_dir}")
    logging.info(f"Output directory: {args.output}")
    logging.info("Processing complete")


def main():
    """Main entry point."""
    setup_logging()
    args = parse_command_line_arguments()

    logging.info(f"Starting v{SCHEMA_VERSION} JSON-LD generation...")
    results = process_round(args.round_dir, args.output)
    log_processing_summary(results, args)


if __name__ == "__main__":
    main()
