import json
import logging
import os

import yaml

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
