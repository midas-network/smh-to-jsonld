import logging
import os
import pandas as pd
import pyarrow.parquet as pq
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


def view_parquet_files(directory="data/model-output", column="location", limit=10, model=None):
    """View Parquet files in the specified directory and its subdirectories"""

    # Check if directory exists
    if not os.path.isdir(directory):
        print(f"Directory '{directory}' not found.")
        return

    # Find all parquet files in directory and subdirectories
    parquet_files = []
    for root, _, files in os.walk(directory):
        # Extract model name from path (last directory component)
        current_model = os.path.basename(root)

        # Skip if model filter is provided and doesn't match
        if model and current_model != model:
            continue

        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append({
                    'path': os.path.join(root, file),
                    'model': current_model,
                    'filename': file
                })

    if not parquet_files:
        print(f"No parquet files found in '{directory}'" +
              (f" for model '{model}'" if model else ""))
        return

    print(f"Found {len(parquet_files)} parquet files in '{directory}'" +
          (f" for model '{model}'" if model else ""))

    # Process each file
    for file_info in parquet_files:
        file_path = file_info['path']
        print(f"\n{'=' * 50}")
        print(f"Model: {file_info['model']}")
        print(f"File: {file_info['filename']}")

        try:
            # Show schema
            schema = pq.read_schema(file_path)
            print("\nSchema:")
            for field in schema:
                print(f"- {field.name}: {field.type}")

            # Check if requested column exists
            if column not in schema.names:
                print(f"\nColumn '{column}' not found in this file.")
                print(f"Available columns: {schema.names}")
                continue

            # Read and display the column data
            table = pq.read_table(file_path, columns=[column])
            df = table.to_pandas()

            print(f"\nValues in column '{column}' (first {limit} rows):")
            try:
                print(tabulate(df.head(limit), headers='keys', tablefmt='grid'))
            except NameError:
                print(df.head(limit))

            # Show basic stats for the column
            if pd.api.types.is_numeric_dtype(df[column]):
                print(f"\nNumeric statistics for '{column}':")
                print(df[column].describe())
            else:
                unique_count = df[column].nunique()
                null_count = df[column].isna().sum()
                print(f"\nUnique values: {unique_count}")
                print(f"Null values: {null_count}")
                print("\nMost common values:")
                print(df[column].value_counts().head(5))

        except Exception as e:
            print(f"Error processing file: {str(e)}")


def get_distinct_field_values(round_id, model, directory, schema):
    """
    Extract distinct values for specified fields from parquet files.

    Args:
        fields (list): List of field names to extract distinct values from
        model (str, optional): Model name to filter by
        directory (str, optional): Directory containing model output subdirectories

    Returns:
        dict: Dictionary mapping field names to lists of their distinct values
    """
    # Get the parquet files for the model

    dir = os.path.join(directory, round_id, "model-output")
    parquet_files = get_parquet_files_for_model(round_id, dir, model)
    if not parquet_files:
        return {}

    # Process each file
    for file_info in parquet_files:
        file_path = file_info['path']
        result = {}
        try:
            logging.debug(f"    Reading file: {file_path}")
            # Check which fields exist in this file
            #schema = pq.read_schema(file_path)
            field_data = {field: [] for field in schema.names}
            existing_fields = [field for field in schema.names]
            logging.debug(f"Fields in parquet file:{existing_fields}")

            if not existing_fields:
                continue

            # Read only the needed columns
            table = pq.read_table(file_path, columns=existing_fields, schema=schema)
            df = table.to_pandas()

            # Extract data for each field
            for field in existing_fields:
                if field != 'value':
                    result[field] = df[field].dropna().drop_duplicates().tolist()

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")

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

