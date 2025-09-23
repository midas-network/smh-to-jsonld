import os
import re
import shutil
import logging
import sys

import pandas as pd
import yaml
import json

from utils.output_parse import get_distinct_field_values, get_output_file_types
from utils.tasks_json_parser import read_tasks_config
from utils.lookup import get_location_from_fips, STATE_FIPS

# State abbreviation mapping
STATE_ABBR = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'District of Columbia': 'DC',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
    'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
    'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'American Samoa': 'AS', 'Guam': 'GU', 'Northern Mariana Islands': 'MP',
    'Puerto Rico': 'PR', 'Virgin Islands': 'VI', 'United States': 'US'
}


def get_origin_date_from_tasks_json(round_id, config):
    """
    Extract the origin date from tasks.json configuration.

    Parameters:
        config: The tasks.json configuration object

    Returns:
        str: The origin date in ISO 8601 format (YYYY-MM-DD)
    """
    # Go through all rounds and tasks
    for round_config in config.get_all_rounds():
        config_round_id = round_config.round_id
        if config_round_id == round_id:
            for task in round_config.model_tasks:
                # Access origin_date directly
                if hasattr(task, 'origin_date') and task.origin_date:
                    return task.origin_date.isoformat()  # Convert to ISO 8601 format

    return None  # Return None if no origin date is found


def calculate_temporal_coverage(round_id, config, global_field_values_dict):
    origin_dates = get_distinct_task_from_tasks_json(round_id, "origin_date", config, global_field_values_dict)
    horizons = get_distinct_task_from_tasks_json(round_id, "horizon", config, global_field_values_dict, sort=False)
    # cast horizons to int
    horizons = [int(horizon) for horizon in horizons]
    # Calculate temporal coverage, origin_date - 1 + horizon * 7
    temporal_coverage = {}
    for origin_date in origin_dates:
        ##get the max horizon for each origin_date
        max_horizon = max(horizons)
        ## convert origin_date to datetime object
        origin_date_datetime = pd.to_datetime(origin_date)
        ## calculate temporal coverage
        ## subtract 1 from origin_date
        startDate = origin_date_datetime

        ## add max_horizon * 7 to origin_date
        endDate = startDate - pd.DateOffset(days=1) + pd.DateOffset(weeks=max_horizon)
        temporal_coverage["startDate"] = startDate
        temporal_coverage["endDate"] = endDate

    return temporal_coverage


def get_distinct_task_from_tasks_json(round_id, task_name, config, global_field_values_dict, sort=True):
    """
    Extract all unique location values (both required and optional) from tasks.json.

    Parameters:
        config: The tasks.json configuration object

    Returns:
        list: A sorted list of unique location codes
    """
    task_values = set()

    for round_config in config.get_all_rounds():
        config_round_id = round_config.round_id
        if config_round_id == round_id:
            for task in round_config.model_tasks:
                # Access task_ids to get location information
                if hasattr(task, 'task_ids'):
                    # Add required locations if they exist

                    if task.task_ids[task_name].required is not None:
                        for task_value in task.task_ids[task_name].required:
                            if task_value in global_field_values_dict[task_name]:
                                task_values.add(str(task_value))
                            else:
                                print("Skipping required" + task_name + " not in global field values:", task_value)

                    # Add optional locations if they exist
                    if task.task_ids[task_name].optional is not None:
                        for task_value in task.task_ids[task_name].optional:
                            if task_value in global_field_values_dict[task_name]:
                                task_values.add(str(task_value))
                            else:
                                print("Skipping optional " + task_name + " not in global field values:", task_value)
            if sort:
                return sorted(list(task_values))
            else:
                return list(task_values)
    return []


def setup_logging(data_dir="data"):
    """Setup logging to both console and file."""
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Configure logging to write to both file and console
    log_file = os.path.join(data_dir, "last_run.log")

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler
    file_handler = logging.FileHandler(log_file, mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_location_info(fips_code):
    """Generate location information for a given FIPS code."""
    fips_code = str(fips_code)
    location_name = get_location_from_fips(fips_code)

    # Extract state name for state-level FIPS
    if len(fips_code) == 2 or (len(fips_code) == 5 and fips_code[2:] == '000'):
        state_code = fips_code[:2]
        state_name = STATE_FIPS.get(state_code, "Unknown")
    else:
        # For county FIPS, extract state
        state_code = fips_code[:2]
        state_name = STATE_FIPS.get(state_code, "Unknown")

    # Create a geonames-like ID
    geonames_id = f"fips_{fips_code}"

    location_info = {
        "@context": {
            "iso3166-1": "http://www.iso.org/iso-3166-1#",
            "iso3166-2": "http://www.iso.org/iso-3166-2#",
            "gn": "http://www.geonames.org/ontology#",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#"
        },
        "@id": f"http://sws.geonames.org/{geonames_id}/",
        "@type": "gn:Feature",
        "gn:name": location_name,
        "iso3166-1:alpha2": "US",
        "iso3166-1:alpha3": "USA",
        "iso3166-1:numeric": "840",
        "gn:fipsCode": fips_code
    }

    # Add state abbreviation if available
    state_abbr = STATE_ABBR.get(state_name, "")
    if state_abbr:
        location_info["iso3166-2:code"] = f"US-{state_abbr}"

    return location_info


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


def get_target_metadata_from_tasks(config, round_id):
    """Extract target metadxrata from tasks.json for each target."""
    target_metadata = {}

    # Go through all rounds and tasks
    for round_config in config.get_all_rounds():
        config_round_id = round_config.round_id
        if config_round_id == round_id:
            for task in round_config.model_tasks:
                if hasattr(task, 'target_metadata'):
                    for target in task.target_metadata:
                        # Access attributes directly instead of using .get()
                        if hasattr(target, 'target_id') and target.target_id not in target_metadata:
                            # Check if URI exists and log it
                            if hasattr(target, 'uri'):
                                logging.info(
                                    f"Round {round_id}: Found URI '{target.uri}' for target '{target.target_id}'")
                            else:
                                logging.warning(
                                    f"Round {round_id}: No URI attribute found for target '{target.target_id}'")

                            # Store the target metadata
                            target_metadata[target.target_id] = target

    logging.info(f"Extracted metadata for {len(target_metadata)} unique targets")
    return target_metadata


def get_targets_from_tasks_json(config, round_id, global_field_values_dict):
    target_metadata_dict = get_target_metadata_from_tasks(config, round_id)

    target_obj_list = []
    for target in target_metadata_dict:
        if target in (global_field_values_dict['target']) and len(global_field_values_dict['target']) > 1:
            # Get rich metadata from tasks.json
            target_md = target_metadata_dict.get(target, None)

            target_obj = {
                "@type": "PropertyValue",
                "name": target_md.target_name if target_md and hasattr(target_md, "target_name") else target
            }

            # Add URI if available - this is the key part for including the URI
            if target_md and hasattr(target_md, "uri") and target_md.uri:
                target_obj["identifier"] = target_md.uri

            # Add alternative name if available
            if target_md and hasattr(target_md, "alternative_name") and target_md.alternative_name:
                target_obj["alternateName"] = target_md.alternative_name

            # Add description
            if target_md and hasattr(target_md, "description") and target_md.description:
                target_obj["description"] = target_md.description

            # Add units
            if target_md and hasattr(target_md, "target_units") and target_md.target_units:
                target_obj["unitText"] = target_md.target_units

            if target_md and hasattr(target_md, "target_id") and target_md.target_id:
                target_obj["target_id"] = target_md.target_id

            if target_md and hasattr(target_md, "target_type") and target_md.target_type:
                target_obj["target_type"] = target_md.target_type

            if target_md and hasattr(target_md, "target_keys") and target_md.target_keys:
                target_obj["target_keys"] = target_md.target_keys

            # Add time unit if step-ahead target
            if target_md and hasattr(target_md, "is_step_ahead") and target_md.is_step_ahead:
                if hasattr(target_md, "time_unit"):
                    target_obj["temporalUnit"] = target_md.time_unit

            target_obj_list.append(target_obj)
        else:
            print("Skipping target metadata for target:", target)

    return target_obj_list


def process_metadata_for_round(round_id, metadata_dir, output_dir, config):
    # Find all YAML files
    yaml_files = [f for f in os.listdir(metadata_dir) if f.endswith(('.yaml', '.yml'))]

    if not yaml_files:
        logging.info(f"No YAML files found in '{metadata_dir}'")
        return []

    logging.info(f"Found {len(yaml_files)} YAML files to process")

    # Read tasks.json configuration
    # Get locations from tasks.json instead of from output files
    global_field_values_dict = {}
    results = []
    for yaml_file in yaml_files:
        model_name = os.path.splitext(yaml_file)[0]
        yaml_path = os.path.join(metadata_dir, yaml_file)

        logging.info(f"Processing {model_name}...")
        jsonld_data = yaml_to_jsonld(yaml_path)

        # Add round_id to the JSON-LD data

        # Also add it to the workExample if it exists
        if "workExample" in jsonld_data:
            jsonld_data["workExample"]["isPartOf"] = {
                "@type": "Event",
                "name": f"Round {round_id}",
                "identifier": round_id
            }

        # Ensure workExample exists and is specified as a Dataset
        if "workExample" not in jsonld_data:
            jsonld_data["workExample"] = {
                "@type": "Dataset",
                "description": "RSV disease projection outputs",
            }

        # Process targets with ontology references

        # Add targets to variableMeasured

        # After processing targets, add this code to process locations

        # After processing targets and locations, add file format information
        file_types = get_output_file_types(round_id, model_name,
                                           directory=os.path.join("data", round_id, "model-output", model_name))
        if file_types:
            logging.info(f"Found {len(file_types)} files for {model_name}")

            # Add file format information to workExample
            if "workExample" in jsonld_data:
                jsonld_data["workExample"]["encodingFormat"] = []

                if file_types["parquet"] > 0:
                    jsonld_data["workExample"]["encodingFormat"].append({
                        "@type": "FileFormat",
                        "name": "Apache Parquet",
                        "fileExtension": ".parquet"
                    })
        # if directory exists
        if os.path.exists(os.path.join("data", round_id, "model-output", model_name)):
            distinct_field_values_for_this_model = \
                (get_distinct_field_values(round_id, model_name, directory="data", reset_cache=True))
            # For each field in the model's distinct values
            for field, values in distinct_field_values_for_this_model.items():
                # If field doesn't exist in global dict yet, initialize it
                if field not in global_field_values_dict:
                    global_field_values_dict[field] = values
                # If field exists, append only new values
                else:
                    for value in values:
                        if value not in global_field_values_dict[field]:
                            global_field_values_dict[field].append(value)

        # Write output directly to the output directory, not in a nested round_id directory
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{model_name}.jsonld")
        with open(output_path, 'w') as f:
            json.dump(jsonld_data, f, indent=2)

        results.append({
            'model': model_name,
            'yaml_file': yaml_path,
            'jsonld_file': output_path,
            # 'location_count': len(results_dict.get("location", [])),
            # 'target_count': len(results_dict.get("target", [])),
            'round_id': round_id
        })

    return (results, global_field_values_dict)


def get_diseases_by_round(tasks_json_path):
    """
    Extract disease information from each round in the tasks.json file.

    Parameters:
        tasks_json_path (str): Path to the tasks.json file

    Returns:
        dict: Dictionary with round_id as key and list of diseases as value
    """
    # Load the tasks.json file
    with open(tasks_json_path, 'r') as f:
        tasks_data = json.load(f)

    diseases_by_round = {}

    # Iterate through each round
    for round_data in tasks_data['rounds']:
        round_id = round_data.get('round_id')
        diseases = round_data.get('disease', [])

        # If round_id is from a variable, mark it accordingly
        if round_data.get('round_id_from_variable', False):
            round_id = f"{round_id} (variable)"

        diseases_by_round[round_id] = diseases

    return diseases_by_round


def create_consolidated_round_jsonld(round_output_dir, round_id, config, global_field_values_dict):
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

    # consolidated["healthCondition"] = {
    #     "@type": "MedicalCondition",
    #     "name": "Respiratory Syncytial Virus",
    #     "alternateName": "RSV",
    #     "code": {
    #         "@type": "MedicalCode",
    #         "codeValue": "B974",
    #         "codingSystem": "ICD-10",
    #         "description": "Respiratory syncytial virus as the cause of diseases classified elsewhere"
    #     }
    # }

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

    target_obj_list = get_targets_from_tasks_json(config, round_id, global_field_values_dict)
    for target_obj in target_obj_list:
        if "variableMeasured" not in consolidated["workExample"]:
            consolidated["workExample"]["variableMeasured"] = [target_obj]
        else:
            consolidated["workExample"]["variableMeasured"].append(target_obj)

    round_config = config.get_round_by_id(round_id)
    if round_config:
        output_types = []
        for task in round_config.model_tasks:
            for output_type_name, output_type_config in task.output_type.items():
                # Create a serializable representation
                output_types.append({
                    "type": output_type_name,
                    "output_type_id": output_type_config.output_type_id,
                    "value": output_type_config.value
                })
        # Remove duplicates
        unique_output_types_str = {json.dumps(d, sort_keys=False) for d in output_types}
        unique_output_types = [json.loads(s) for s in unique_output_types_str]
        consolidated["workExample"]["output_type"] = unique_output_types

    locations = get_distinct_task_from_tasks_json(round_id, "location", config, global_field_values_dict)

    # Add spatial coverage to workExample
    if "workExample" in consolidated:
        consolidated["workExample"]["spatialCoverage"] = []

    for location_fips in locations:
        location_info = get_location_info(location_fips)
        if location_info:
            consolidated["workExample"]["spatialCoverage"].append(location_info)
            logging.debug(f"Added location info for FIPS {location_fips}")

        # Add age groups to workExample
    age_groups = get_distinct_task_from_tasks_json(round_id, "age_group", config, global_field_values_dict)
    if age_groups:
        consolidated["workExample"]["ageGroups"] = age_groups

    temporal_coverage = calculate_temporal_coverage(round_id, config, global_field_values_dict)

    # Add temporal coverage to workExample
    if "workExample" in consolidated:
        # Assuming temporal_coverage returns start/end dates
        if "startDate" in temporal_coverage and "endDate" in temporal_coverage:
            consolidated["workExample"]["temporalCoverage"] = (
                f"{temporal_coverage['startDate']}/{temporal_coverage['endDate']}"
            )
        # Or if it returns an ISO 8601 time interval directly
        elif "interval" in temporal_coverage:
            consolidated["workExample"]["temporalCoverage"] = temporal_coverage["interval"]

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


def process_all_metadata(base_dir="data", metadata_subdir="model-metadata", output_dir="output"):
    """Process metadata from each round directory separately."""
    # Identify round directories (date-based folders like 2023-11-12)
    round_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and
                  re.match(r'\d{4}-\d{2}-\d{2}', d)]

    if not round_dirs:
        logging.info(f"No round directories found in '{base_dir}'.")
        return []

    logging.info(f"Found {len(round_dirs)} round directories: {', '.join(round_dirs)}")

    results = []
    for round_dir in round_dirs:
        round_path = os.path.join(base_dir, round_dir)
        metadata_dir_full = os.path.join(round_path, metadata_subdir)

        # Check if metadata directory exists for this round
        if not os.path.isdir(metadata_dir_full):
            logging.info(f"Metadata directory '{metadata_dir_full}' not found for round {round_dir}. Skipping.")
            continue

        # Create corresponding output directory for this round
        round_output_dir = os.path.join(output_dir, round_dir)
        if os.path.exists(round_output_dir):
            shutil.rmtree(round_output_dir)
        os.makedirs(round_output_dir, exist_ok=True)

        logging.info(f"Processing round directory: {round_dir}")

        # Load the tasks.json config for this round
        tasks_json_path = os.path.join(round_path, "hub-config", "tasks.json")
        if not os.path.exists(tasks_json_path):
            logging.warning(f"tasks.json not found at {tasks_json_path}. Skipping round {round_dir}.")
            continue

        config = read_tasks_config(tasks_json_path)

        rounds = config.get_all_rounds()

        for round_config in rounds:
            round_id = round_config.round_id

            logging.info(f"Processing round ID: {round_id} from directory: {round_dir}")

            # Process metadata for this round and save to the round-specific output directory
            res = process_metadata_for_round(round_id, metadata_dir_full, round_output_dir, config)
            round_results = res[0]
            global_field_values_dict = res[1]

            results.extend(round_results)

            logging.info(
                f"Completed processing round {round_id} from directory {round_dir}: {len(round_results)} models processed")

            # Create consolidated JSON-LD for the round
        create_consolidated_round_jsonld(round_output_dir, round_id, config, global_field_values_dict)

    logging.info(f"Successfully processed {len(results)} models across all rounds.")
    return results


if __name__ == "__main__":
    import argparse

    # Setup logging
    logger = setup_logging()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process model metadata files and convert to JSON-LD.')
    parser.add_argument('--base_dir', default='data', help='Base directory containing round directories')
    parser.add_argument('--metadata_subdir', default='model-metadata',
                        help='Subdirectory containing YAML metadata files')
    parser.add_argument('--output', default='output', help='Directory to output JSON-LD files')
    args = parser.parse_args()

    # Process metadata with specified directories
    logging.info(f"Starting metadata processing...")
    results = process_all_metadata(base_dir=args.base_dir, metadata_subdir=args.metadata_subdir, output_dir=args.output)

    logging.info(f"Added location information to {sum(1 for r in results if r.get('location_count', 0) > 0)} models")
    logging.info(f"Base directory: {args.base_dir}")
    logging.info(f"Metadata subdirectory: {args.metadata_subdir}")
    logging.info(f"Output directory: {args.output}")
    logging.info("Processing complete")
