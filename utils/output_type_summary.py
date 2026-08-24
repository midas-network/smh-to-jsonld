"""Summarize output-type information from a model-output DataFrame.

These summaries (number of samples, compound task ID set, submitted quantiles)
are shared by both the JSON-LD creation step and the HTML rendering step so the
same information lands in the ``.jsonld`` file and the ``.html`` page. Keeping the
computation here — rather than in the HTML renderer — lets the canonical JSON-LD
carry the metadata, which is what people query against.
"""

from numbers import Real
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

# Columns that jointly identify a single sample trajectory (a "run").
SAMPLE_ID_COLUMNS = ("run_grouping", "stochastic_run")

# Columns that are not task variables and therefore never part of the
# compound task ID set.
OUTPUT_METADATA_EXCLUDE_COLUMNS = {
    "model_id",
    "run_grouping",
    "stochastic_run",
    "output_type",
    "output_type_id",
    "value",
}


def load_model_output_from_dir(model_output_dir, model=None):
    """Load a model's output parquet files into a DataFrame.

    Reads the raw parquet directly (not through the hub task schema) so that
    sample-output columns such as ``run_grouping`` and ``stochastic_run`` — which
    are not declared as task IDs in ``tasks.json`` — are preserved.
    """
    model_dir = Path(model_output_dir)
    parquet_files = sorted(model_dir.glob("*.parquet"))

    if not parquet_files:
        return pd.DataFrame()

    pa_table = ds.dataset([str(path) for path in parquet_files], format="parquet").to_table()
    df = pa_table.to_pandas()

    # Be robust to mixed files by keeping only rows for this model when available.
    if model is not None and "model_id" in df.columns:
        df = df[df["model_id"] == model]

    return df


def filter_by_output_type(df, output_type):
    if df.empty or "output_type" not in df.columns:
        return pd.DataFrame(columns=df.columns)

    output_type_values = df["output_type"].astype(str).str.strip().str.lower()
    return df[output_type_values == output_type].copy()


def format_metadata_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, Real) and not isinstance(value, bool):
        return f"{float(value):g}"
    return str(value)


def format_unique_values_for_display(series):
    values = series.dropna().drop_duplicates().tolist()
    if not values:
        return []

    numeric_values = pd.to_numeric(pd.Series(values), errors="coerce")
    if numeric_values.notna().all():
        sort_keyed_values = sorted(zip(numeric_values.tolist(), values), key=lambda item: item[0])
        sorted_values = [value for _, value in sort_keyed_values]
    else:
        sorted_values = sorted(values, key=lambda value: str(value))

    formatted_values = []
    seen = set()
    for value in sorted_values:
        formatted = format_metadata_value(value)
        if formatted and formatted not in seen:
            formatted_values.append(formatted)
            seen.add(formatted)

    return formatted_values


def summarize_quantile_output(df):
    quantile_df = filter_by_output_type(df, "quantile")
    if quantile_df.empty or "output_type_id" not in quantile_df.columns:
        return {}

    quantiles = format_unique_values_for_display(quantile_df["output_type_id"])
    if not quantiles:
        return {}

    return {"quantiles": quantiles}


def summarize_sample_output(df):
    sample_df = filter_by_output_type(df, "sample")
    if sample_df.empty:
        return {}

    missing_columns = [column for column in SAMPLE_ID_COLUMNS if column not in sample_df.columns]
    if missing_columns:
        return {"missing_columns": missing_columns}

    task_columns = [
        column
        for column in sample_df.columns
        if column not in OUTPUT_METADATA_EXCLUDE_COLUMNS
    ]
    sample_count_df = sample_df.groupby(task_columns).size().reset_index().rename(columns={0: 'n'})
    sample_count = sample_count_df['n'].unique().tolist()
    if len(sample_count) > 1:
        return []
    sample_count = sample_count[0]

    if not task_columns:
        return {
            "sample_count": sample_count,
            "compound_task_id_set": [],
        }

    group_nunique = sample_df.groupby(
        list(SAMPLE_ID_COLUMNS),
        sort=True,
        dropna=False,
    )[task_columns].nunique(dropna=False)

    compound_task_id_set = []
    seen = set()
    for _, row in group_nunique.iterrows():
        for column in task_columns:
            if row[column] == 1 and column not in seen:
                compound_task_id_set.append(column)
                seen.add(column)

    return {
        "sample_count": sample_count,
        "compound_task_id_set": compound_task_id_set,
    }


def summarize_output_type_metadata(df):
    if df.empty:
        return {}

    metadata = {}

    sample_summary = summarize_sample_output(df)
    if sample_summary:
        metadata["sample"] = sample_summary

    quantile_summary = summarize_quantile_output(df)
    if quantile_summary:
        metadata["quantile"] = quantile_summary

    return metadata
