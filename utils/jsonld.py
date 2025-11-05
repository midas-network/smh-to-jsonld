import json
import logging
import os

import yaml

from utils.location import get_location_info
from utils.model_output_smh import get_output_file_types
from utils.tasks_smh import get_targets
from utils.temporal import calculate_temporal_coverage


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
        "identifier": round_id
    }


def add_file_formats(jsonld_data, file_types):
    """Add file format information to the workExample."""
    if not file_types:
        return

    jsonld_data["workExample"]["encodingFormat"] = []

    if file_types.get("parquet", 0) > 0:
        jsonld_data["workExample"]["encodingFormat"].append({
            "@type": "FileFormat",
            "name": "Apache Parquet",
            "fileExtension": ".parquet"
        })


def add_targets(jsonld_data, target_obj_list):
    """Add target variables to the workExample."""
    for target_obj in target_obj_list:
        if "variableMeasured" not in jsonld_data["workExample"]:
            jsonld_data["workExample"]["variableMeasured"] = []
        jsonld_data["workExample"]["variableMeasured"].append(target_obj)


def add_spatial_coverage(jsonld_data, locations):
    """Add spatial coverage information to the workExample."""
    jsonld_data["workExample"]["spatialCoverage"] = []

    for location_fips in locations:
        location_info = get_location_info(location_fips)
        if location_info:
            jsonld_data["workExample"]["spatialCoverage"].append(location_info)
            logging.debug(f"Added location info for FIPS {location_fips}")


def add_temporal_coverage(jsonld_data, temporal_coverage):
    """Add temporal coverage information to the workExample."""
    if "startDate" in temporal_coverage and "endDate" in temporal_coverage:
        jsonld_data["workExample"]["temporalCoverage"] = (
            f"{temporal_coverage['startDate']}/{temporal_coverage['endDate']}"
        )
    elif "interval" in temporal_coverage:
        jsonld_data["workExample"]["temporalCoverage"] = temporal_coverage["interval"]


def enrich_jsonld_with_model_output(jsonld_data, round_id, model_name, config, distinct_field_values):
    """Enrich JSON-LD data with information from model output files."""
    # Extract field values
    output_types = distinct_field_values.get("output_type", [])
    locations = distinct_field_values.get("location", [])
    age_groups = distinct_field_values.get("age_group", [])

    # Get additional data
    target_obj_list = get_targets(config, round_id, distinct_field_values)
    temporal_coverage = calculate_temporal_coverage(distinct_field_values)
    file_types = get_output_file_types(
        round_id, model_name,
        directory=os.path.join("data", round_id, "model-output", model_name)
    )

    # Initialize and populate workExample
    initialize_work_example(jsonld_data)
    add_round_info(jsonld_data, round_id)

    jsonld_data["workExample"]["output_type"] = [output_types]

    if file_types:
        logging.debug(f"Found {len(file_types)} files for {model_name}")
        add_file_formats(jsonld_data, file_types)

    add_targets(jsonld_data, target_obj_list)
    add_spatial_coverage(jsonld_data, locations)

    if age_groups:
        jsonld_data["workExample"]["ageGroups"] = age_groups

    add_temporal_coverage(jsonld_data, temporal_coverage)


def remove_none_values(obj):
    """Remove None values recursively from a dictionary or list."""
    if isinstance(obj, dict):
        return {key: remove_none_values(value) for key, value in obj.items() if value is not None}
    elif isinstance(obj, list):
        return [remove_none_values(item) for item in obj if item is not None]
    else:
        return obj

def yaml_to_jsonld(yaml_file_path):
    """Convert a YAML file to JSON-LD using schema.org vocabulary"""
    with open(yaml_file_path, 'r') as file:
        data = yaml.safe_load(file)

    # Create the basic JSON-LD structure
    if len(data.get("team_abbr")) > 0:
        team_name = data.get("team_abbr") + "-" + data.get("model_abbr")
    else:
        team_name = data.get("model_abbr")
    jsonld = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "name": team_name,
        ##"alternateName": data.get("model_abbr"),
        "description": data.get("methods_long") or data.get("methods"),
        "version": data.get("model_version"),
        "license": data.get("license"),

        # Add RSV disease information

        "version": data.get("model_version")
        # Add RSV disease information
    }

    missing_val = ["NA", "na", "TBD", "N/A", "NaN"]

    if data.get("license") not in missing_val:
        jsonld["license"] = data.get("license")

    if data.get("website_url") not in missing_val:
        jsonld["website"] = data.get("website_url")

    # Add the organization (team)
    jsonld["producer"] = {
        "@type": "Organization",
        "name": data.get("team_name")
    }

    if data.get("team_funding") and data.get("team_funding") not in missing_val:
        jsonld["producer"]["funder"] = {
            "@type": "Organization",
            "description": data.get("team_funding")
        }

    # Add contributors as authors
    if "model_contributors" in data and data["model_contributors"]:
        jsonld["author"] = []
        for contributor in data["model_contributors"]:
            person = {
                "@type": "Person",
                "name": contributor.get("name"),
                "affiliation": {
                    "@type": "Organization",
                    "name": contributor.get("affiliation")
                } if contributor.get("affiliation") else None,
                "email": contributor.get("email")
            }
            jsonld["author"].append(person)

    # Add data inputs
    if data.get("data_inputs"):
        jsonld["isBasedOn"] = {
            "@type": "Dataset",
            "description": data.get("data_inputs")
        }

    # Clean up None values
    jsonld = remove_none_values(jsonld)

    return jsonld

def create_consolidated_round_jsonld(round_output_dir, round_id, config, global_field_values_dict, field_values_by_model):
    """
    Create a consolidated JSON-LD file for the entire round that includes data from all model JSON-LD files.

    Parameters:
        round_output_dir (str): Directory containing individual model JSON-LD files
        round_id (str): Round identifier
        results (list): List of dictionaries with model results information

    Returns:
        str: Path to the consolidated JSON-LD file
    """
    logging.info(f"Creating consolidated JSON-LD for round {round_id}...")

    # Get all JSON-LD files in the round output directory
    jsonld_files = [f for f in os.listdir(round_output_dir) if f.endswith('.jsonld') and not f.startswith('round_')]

    # Create the basic structure for the consolidated JSON-LD
    consolidated = {"@context": "https://schema.org/", "@type": "Dataset",
                    "name": f"Round {round_id} Scenario Projection Models Collection",
                    "description": f"Collection of model output from round {round_id}", "identifier": round_id,
                    "hasPart": [], "workExample": {
            "@type": [
                "Dataset",
                "https://midasnetwork.us/ontology/class-datasetsmidas97.html",  # Model output
                "https://midasnetwork.us/ontology/class-oboobcs_0000267.html"  # Scenario analysis
            ],
            "description": "RSV disease projection outputs",
        }}

    for round_cfg in config.rounds:
        if round_cfg.round_id == round_id:
            diseases = round_cfg.diseases
            for disease in diseases:
                consolidated["healthCondition"] = {
                    "@type": "MedicalCondition",
                    "name": disease.name,
                    "uri": disease.uri
                }

    consolidated["roundId"] = round_id







    # Collect model data
    for jsonld_file in jsonld_files:
        file_path = os.path.join(round_output_dir, jsonld_file)
        try:
            with open(file_path, 'r') as f:
                model_data = json.load(f)

            # Include the complete model data in hasPart instead of just a reference
            consolidated["hasPart"].append(model_data)

        except Exception as e:
            logging.error(f"Error processing {jsonld_file}: {e}")

    # Add summary statistics
    model_count = len(consolidated["hasPart"])
    consolidated["numberOfItems"] = model_count

    # Write the consolidated file
    consolidated_file_path = os.path.join("output", f"round_{round_id}.jsonld")
    with open(consolidated_file_path, 'w') as f:
        json.dump(consolidated, f, indent=2)

    logging.info(f"Consolidated JSON-LD written to {consolidated_file_path} with {model_count} models included")
    return consolidated_file_path
