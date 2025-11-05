import logging
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tabulate import tabulate
from hubdata import connect_hub, create_hub_schema
from pathlib import Path
import datetime

def serialize_for_json(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return obj

def get_hub_schema(round):
    hub_connection = connect_hub(Path('data' + os.sep + round + os.sep))
    schema = create_hub_schema(hub_connection.tasks)
    return schema

def get_parquet_files_for_model(round_id, directory="data/model-output", model=None):
    """
    Returns a list of parquet files for a specific model.

    Args:
        directory (str): Directory containing model output subdirectories
        model (str): Model name to filter by (corresponds to subdirectory name)

    Returns:
        list: List of dictionaries containing path, model name, and filename
    """
   
    if not os.path.isdir(directory):
        logging.warning(f"Directory '{directory}' not found.")
        return []

    parquet_files = []
    for root, _, files in os.walk(directory):
        # Extract model name from path (last directory component)
        current_model = os.path.basename(root)

        # Skip if model filter is provided and doesn't match
        if model and current_model != model:
            continue

        for file in files:
            if file.startswith(round_id) and (file.endswith('.parquet') or file.endswith('.gz.parquet')):
                parquet_files.append({
                    'path': os.path.join(root, file),
                    'model': current_model,
                    'filename': file
                })

    return parquet_files


def get_hub_ds(round_id, model):
    hub_connection = connect_hub(Path('data' + os.sep + round_id + os.sep))
    schema = create_hub_schema(hub_connection.tasks)

    hub_ds = hub_connection.get_dataset()
    pa_table = hub_ds.to_table(filter=pc.field('model_id') == model)
    df = pa_table.to_pandas()
    return df, schema

def get_distinct_field_values(hub_df, hub_schema):
    existing_fields = [field for field in hub_schema.names]
    result = {}
    for field in existing_fields:
        if field != 'value':
            result[field] = hub_df[field].dropna().drop_duplicates().tolist()

    return result

def get_output_file_types(round_id, model=None, directory="data/model-output"):
    """
    Get the output file types for a specific model.

    Args:
        model (str, optional): Model name to filter by
        directory (str, optional): Directory containing model output subdirectories

    Returns:
        dict: Dictionary mapping file types to their counts
    """
    # Get the parquet files for the model
    parquet_files = get_parquet_files_for_model(round_id, directory, model)

    if not parquet_files:
        return {}

    # Dictionary to store file type counts
    file_types = {}
    file_types["parquet"] = 0
    file_types["gz.parquet"] = 0

    # Process each file, parquet files can either end with .gz.parquet or .parquet
    for file_info in parquet_files:
        file_name = file_info['filename']
        file_type = file_name.split('.')[1]
        if file_type == "gz":
            file_type = "gz.parquet"
        file_types[file_type] += 1

    return file_types

