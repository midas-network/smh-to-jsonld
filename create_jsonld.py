import argparse
import json
import logging
import os
import re
import shutil

from utils.jsonld import yaml_to_jsonld, create_consolidated_round_jsonld, enrich_jsonld_with_model_output
from utils.loggings import setup_logging
from utils.model_output_smh import get_distinct_field_values, get_hub_ds
from utils.tasks_json_parser import read_tasks_config


def get_unique_output_types(round_config):
    """Extract unique output types from round configuration."""
    output_types = []
    for task in round_config.model_tasks:
        for output_type_name, output_type_config in task.output_type.items():
            output_types.append({
                "type": output_type_name,
                "output_type_id": output_type_config.output_type_id,
                "value": output_type_config.value
            })

    # Remove duplicates
    unique_output_types_str = {json.dumps(d, sort_keys=False) for d in output_types}
    return [json.loads(s) for s in unique_output_types_str]


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
    """Find all YAML files in the metadata directory."""
    yaml_files = [f for f in os.listdir(metadata_dir) if f.endswith(('.yaml', '.yml'))]

    if not yaml_files:
        logging.warning(f"No YAML files found in '{metadata_dir}'")
        return []

    logging.debug(f"Found {len(yaml_files)} YAML files to process")
    return yaml_files


def prepare_round_output_directory(output_dir, round_dir):
    """Prepare the output directory for a specific round."""
    round_output_dir = os.path.join(output_dir, round_dir)
    if os.path.exists(round_output_dir):
        shutil.rmtree(round_output_dir)
    os.makedirs(round_output_dir, exist_ok=True)
    return round_output_dir


def load_round_config(round_path, round_dir):
    """Load the tasks.json configuration for a round."""
    tasks_json_path = os.path.join(round_path, "hub-config", "tasks.json")

    if not os.path.exists(tasks_json_path):
        logging.warning(f"tasks.json not found at {tasks_json_path}. Skipping round {round_dir}.")
        return None

    return read_tasks_config(tasks_json_path)


def find_round_directories(base_dir):
    """Find all round directories (date-based folders like 2023-11-12)."""
    round_dirs = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d)) and re.match(r'\d{4}-\d{2}-\d{2}', d)
    ]

    if not round_dirs:
        logging.info(f"No round directories found in '{base_dir}'.")
        return []

    logging.debug(f"Found {len(round_dirs)} round directories: {', '.join(round_dirs)}")
    return round_dirs


def process_single_model(yaml_file, metadata_dir, output_dir, round_id, config,
                         global_field_values_dict, field_values_by_model):
    """Process a single model's metadata file."""
    model_name = os.path.splitext(yaml_file)[0]
    yaml_path = os.path.join(metadata_dir, yaml_file)
    model_output_dir = os.path.join("data", round_id, "model-output", model_name)

    logging.info(f"Processing {model_name}...")

    # Load base JSON-LD from YAML
    jsonld_data = yaml_to_jsonld(yaml_path)

    # Process model output if it exists
    if os.path.exists(model_output_dir):
        hub_df, hub_schema = get_hub_ds(round_id, model_name)
        distinct_field_values = get_distinct_field_values(hub_df, hub_schema)

        # Store field values for this model
        field_values_by_model[model_name] = distinct_field_values

        # Merge into global field values
        merge_field_values(global_field_values_dict, distinct_field_values)

        # Enrich JSON-LD with model output data
        enrich_jsonld_with_model_output(jsonld_data, round_id, model_name, config, distinct_field_values)

    # Write JSON-LD output file
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{model_name}.jsonld")
    with open(output_path, 'w') as f:
        json.dump(jsonld_data, f, indent=2)

    return {
        'model': model_name,
        'yaml_file': yaml_path,
        'jsonld_file': output_path,
        'round_id': round_id
    }


def process_metadata_for_round(round_id, metadata_dir, output_dir, config):
    """Process all metadata files for a specific round."""
    yaml_files = find_yaml_files(metadata_dir)
    if not yaml_files:
        return [], {}, {}

    round_config = config.get_round_by_id(round_id)
    unique_output_types = get_unique_output_types(round_config)

    global_field_values_dict = {}
    field_values_by_model = {}
    round_results = []

    for yaml_file in yaml_files:
        result = process_single_model(
            yaml_file, metadata_dir, output_dir, round_id, config,
            global_field_values_dict, field_values_by_model
        )
        round_results.append(result)

    return round_results, global_field_values_dict, field_values_by_model


def process_single_round(round_dir, base_dir, metadata_subdir, output_dir):
    """Process metadata for a single round directory."""
    round_path = os.path.join(base_dir, round_dir)
    metadata_dir_full = os.path.join(round_path, metadata_subdir)

    # Check if metadata directory exists for this round
    if not os.path.isdir(metadata_dir_full):
        logging.info(f"Metadata directory '{metadata_dir_full}' not found for round {round_dir}. Skipping.")
        return []

    # Prepare output directory
    round_output_dir = prepare_round_output_directory(output_dir, round_dir)
    logging.debug(f"Processing round directory: {round_dir}")

    # Load configuration
    config = load_round_config(round_path, round_dir)
    if config is None:
        return []

    # Process all rounds in the config
    metadata_results = []
    rounds = config.get_all_rounds()

    for round_config in rounds:
        round_id = round_config.round_id

        # Process metadata for this round
        round_results, global_field_values_dict, field_values_by_model = process_metadata_for_round(
            round_id, metadata_dir_full, round_output_dir, config
        )

        metadata_results.extend(round_results)

        logging.info(
            f"Completed processing round {round_id} from directory {round_dir}: "
            f"{len(round_results)} models processed"
        )

        # Create consolidated JSON-LD for the round
        create_consolidated_round_jsonld(
            round_output_dir, round_id, config,
            global_field_values_dict, field_values_by_model
        )

    return metadata_results


def process_all_metadata(base_dir="data", metadata_subdir="model-metadata", output_dir="output"):
    """Process metadata from each round directory separately."""
    round_dirs = find_round_directories(base_dir)
    if not round_dirs:
        return []

    metadata_results = []

    for round_dir in round_dirs:
        round_results = process_single_round(round_dir, base_dir, metadata_subdir, output_dir)
        metadata_results.extend(round_results)

    logging.info(f"Successfully processed {len(metadata_results)} models across all rounds.")
    return metadata_results


def parse_command_line_arguments():
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(description='Process model metadata files and convert to JSON-LD.')
    parser.add_argument('--base_dir', default='data',
                        help='Base directory containing round directories')
    parser.add_argument('--metadata_subdir', default='model-metadata',
                        help='Subdirectory containing YAML metadata files')
    parser.add_argument('--output', default='output',
                        help='Directory to output JSON-LD files')
    return parser.parse_args()


def log_processing_summary(results, args):
    """Log a summary of the processing results."""
    location_count = sum(1 for r in results if r.get('location_count', 0) > 0)

    logging.info(f"Added location information to {location_count} models")
    logging.info(f"Base directory: {args.base_dir}")
    logging.info(f"Metadata subdirectory: {args.metadata_subdir}")
    logging.info(f"Output directory: {args.output}")
    logging.info("Processing complete")


def main():
    """Main entry point for the script."""
    # Setup logging
    setup_logging()

    # Parse command line arguments
    args = parse_command_line_arguments()

    # Process metadata with specified directories
    logging.debug("Starting metadata processing...")
    results = process_all_metadata(
        base_dir=args.base_dir,
        metadata_subdir=args.metadata_subdir,
        output_dir=args.output
    )

    # Log summary
    log_processing_summary(results, args)


if __name__ == "__main__":
    main()
