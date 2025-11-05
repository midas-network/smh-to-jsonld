import os
import re
import shutil
import logging
import sys
import pandas as pd
import json
from utils.jsonld import yaml_to_jsonld, create_consolidated_round_jsonld
from utils.location import get_location_info
from utils.loggings import setup_logging
from utils.model_output_smh import get_distinct_field_values, get_output_file_types, get_hub_schema, get_hub_ds
from utils.tasks_smh import get_targets
from utils.tasks_json_parser import read_tasks_config
from utils.temporal import calculate_temporal_coverage


def process_metadata_for_round(round_id, metadata_dir, output_dir, config):
    # Find all YAML files
    yaml_files = [f for f in os.listdir(metadata_dir) if f.endswith(('.yaml', '.yml'))]

    if not yaml_files:
        logging.warning(f"No YAML files found in '{metadata_dir}'")
        return []

    logging.debug(f"Found {len(yaml_files)} YAML files to process")

    round_config = config.get_round_by_id(round_id)

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

    # Read tasks.json configuration
    # Get locations from tasks.json instead of from output files
    global_field_values_dict = {}
    field_values_by_model = {}
    round_results = []
    for yaml_file in yaml_files:
        output_types = []
        model_name = os.path.splitext(yaml_file)[0]
        yaml_path = os.path.join(metadata_dir, yaml_file)

        logging.info(f"Processing {model_name}...")
        jsonld_data = yaml_to_jsonld(yaml_path)

        file_types = get_output_file_types(round_id, model_name,
                                           directory=os.path.join("data", round_id, "model-output", model_name))

        if os.path.exists(os.path.join("data", round_id, "model-output", model_name)):
            hub_df, hub_schema = get_hub_ds(round_id, model_name)
            distinct_field_values_for_this_model = \
                (get_distinct_field_values(hub_df, hub_schema))
            field_values_by_model[model_name] = distinct_field_values_for_this_model
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

            # Filter unique_output_types to only those used by this model
            used_output_types = distinct_field_values_for_this_model.get("output_type", [])
            output_types.append(used_output_types)

            target_obj_list = get_targets(config, round_id,
                                          distinct_field_values_for_this_model)

            locations = distinct_field_values_for_this_model["location"]

            # Add age groups to workExample
            age_groups = distinct_field_values_for_this_model["age_group"]

            temporal_coverage = calculate_temporal_coverage(distinct_field_values_for_this_model)
            # Ensure workExample exists and is specified as a Dataset
            if "workExample" not in jsonld_data:
                jsonld_data["workExample"] = {
                    "@type": "Dataset",
                    "description": "RSV disease projection outputs",
                }

            if "workExample" in jsonld_data:
                jsonld_data["workExample"]["isPartOf"] = {
                    "@type": "Event",
                    "name": f"Round {round_id}",
                    "identifier": round_id
                }

            jsonld_data["workExample"]["output_type"] = output_types

            if file_types:
                logging.debug(f"Found {len(file_types)} files for {model_name}")

                # Add file format information to workExample
                if "workExample" in jsonld_data:
                    jsonld_data["workExample"]["encodingFormat"] = []

                    if file_types["parquet"] > 0:
                        jsonld_data["workExample"]["encodingFormat"].append({
                            "@type": "FileFormat",
                            "name": "Apache Parquet",
                            "fileExtension": ".parquet"
                        })

            for target_obj in target_obj_list:
                if "variableMeasured" not in jsonld_data["workExample"]:
                    jsonld_data["workExample"]["variableMeasured"] = [target_obj]
                else:
                    jsonld_data["workExample"]["variableMeasured"].append(target_obj)

            # Add spatial coverage to workExample
            if "workExample" in jsonld_data:
                jsonld_data["workExample"]["spatialCoverage"] = []

            for location_fips in locations:
                location_info = get_location_info(location_fips)
                if location_info:
                    jsonld_data["workExample"]["spatialCoverage"].append(location_info)
                    logging.debug(f"Added location info for FIPS {location_fips}")

            if age_groups:
                jsonld_data["workExample"]["ageGroups"] = age_groups

            # Add temporal coverage to workExample
            if "workExample" in jsonld_data:
                # Assuming temporal_coverage returns start/end dates
                if "startDate" in temporal_coverage and "endDate" in temporal_coverage:
                    jsonld_data["workExample"]["temporalCoverage"] = (
                        f"{temporal_coverage['startDate']}/{temporal_coverage['endDate']}"
                    )
                # Or if it returns an ISO 8601 time interval directly
                elif "interval" in temporal_coverage:
                    jsonld_data["workExample"]["temporalCoverage"] = temporal_coverage["interval"]

        # Write output directly to the output directory, not in a nested round_id directory
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{model_name}.jsonld")
        with open(output_path, 'w') as f:
            json.dump(jsonld_data, f, indent=2)

        round_results.append({
            'model': model_name,
            'yaml_file': yaml_path,
            'jsonld_file': output_path,
            # 'location_count': len(results_dict.get("location", [])),
            # 'target_count': len(results_dict.get("target", [])),
            'round_id': round_id
        })

    return round_results, global_field_values_dict, field_values_by_model


def process_all_metadata(base_dir="data", metadata_subdir="model-metadata", output_dir="output"):
    """Process metadata from each round directory separately."""
    # Identify round directories (date-based folders like 2023-11-12)
    round_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and
                  re.match(r'\d{4}-\d{2}-\d{2}', d)]

    if not round_dirs:
        logging.info(f"No round directories found in '{base_dir}'.")
        return []

    logging.debug(f"Found {len(round_dirs)} round directories: {', '.join(round_dirs)}")

    metadata_results = []
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

        logging.debug(f"Processing round directory: {round_dir}")

        # Load the tasks.json config for this round
        tasks_json_path = os.path.join(round_path, "hub-config", "tasks.json")
        if not os.path.exists(tasks_json_path):
            logging.warning(f"tasks.json not found at {tasks_json_path}. Skipping round {round_dir}.")
            continue

        config = read_tasks_config(tasks_json_path)

        rounds = config.get_all_rounds()

        for round_config in rounds:
            round_id = round_config.round_id

            # Process metadata for this round and save to the round-specific output directory
            round_results, global_field_values_dict, field_values_by_model = process_metadata_for_round(round_id,
                                                                                                        metadata_dir_full,
                                                                                                        round_output_dir,
                                                                                                        config)

            metadata_results.extend(round_results)

            logging.info(
                f"Completed processing round {round_id} from directory {round_dir}: {len(round_results)} models processed")

            create_consolidated_round_jsonld(round_output_dir, round_id, config, global_field_values_dict,
                                             field_values_by_model)

    logging.info(f"Successfully processed {len(metadata_results)} models across all rounds.")
    return metadata_results


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
    logging.debug(f"Starting metadata processing...")
    results = process_all_metadata(base_dir=args.base_dir, metadata_subdir=args.metadata_subdir, output_dir=args.output)

    logging.info(f"Added location information to {sum(1 for r in results if r.get('location_count', 0) > 0)} models")
    logging.info(f"Base directory: {args.base_dir}")
    logging.info(f"Metadata subdirectory: {args.metadata_subdir}")
    logging.info(f"Output directory: {args.output}")
    logging.info("Processing complete")
