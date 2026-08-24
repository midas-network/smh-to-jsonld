"""Unit tests for parquet snippet loading in pipeline/jsonld_to_html.py."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.jsonld_to_html import (
    generate_output_type_metadata_html,
    generate_output_types_section,
    get_first_n_rows_of_output,
    summarize_quantile_output,
    summarize_sample_output,
)


def _write_model_parquet(base_dir: Path, round_id: str, model: str, filename: str) -> None:
    model_dir = base_dir / "data" / round_id / "model-output" / model
    model_dir.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(
        pd.DataFrame(
            {
                "model_id": [model, model, model, model],
                "target": ["inc hosp", "inc hosp", "inc hosp", "inc hosp"],
                "value": [1, 2, 3, 4],
            }
        )
    )
    pq.write_table(table, model_dir / filename)


def test_get_first_n_rows_reads_parquet(monkeypatch, tmp_path):
    round_id = "2025-07-27"
    model = "Ensemble"
    _write_model_parquet(tmp_path, round_id, model, f"{round_id}-{model}.parquet")

    monkeypatch.chdir(tmp_path)
    html = get_first_n_rows_of_output(3, round_id, model)

    assert "<table" in html
    assert "inc hosp" in html
    assert ">4<" in html
    assert "..." in html


def test_get_first_n_rows_reads_gz_parquet(monkeypatch, tmp_path):
    round_id = "2025-07-27"
    model = "JHU_UNC-flepiMoP"
    _write_model_parquet(tmp_path, round_id, model, f"{round_id}-{model}.gz.parquet")

    monkeypatch.chdir(tmp_path)
    html = get_first_n_rows_of_output(3, round_id, model)

    assert "<table" in html
    assert "inc hosp" in html
    assert ">4<" in html


def test_get_first_n_rows_returns_empty_when_no_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    html = get_first_n_rows_of_output(3, "2025-07-27", "MissingModel")
    assert html == ""


def test_output_types_section_renders_definition_and_iri():
    model = {"workExample": {"output_type": [["cdf", "quantile", "sample"]]}}
    html = generate_output_types_section(model)

    # Label + definition text for a known type.
    assert "quantile" in html
    assert "A quantile is a data item" in html
    # STATO IRI link for quantile.
    assert "http://purl.obolibrary.org/obo/STATO_0000291" in html
    assert "Ontology reference (STATO)" in html
    # sample has no ontology term -> label rendered, but no link.
    assert "sample" in html
    assert 'href="None"' not in html


def test_output_types_section_falls_back_for_unknown_type():
    model = {"workExample": {"output_type": [["not-a-real-type"]]}}
    html = generate_output_types_section(model)

    assert "not-a-real-type" in html
    assert "Ontology reference" not in html


def test_output_types_section_empty_without_output_type():
    assert generate_output_types_section({"workExample": {}}) == ""


def test_summarize_sample_output_counts_samples_and_compound_task_ids():
    # Each task cell (a location) holds two sample trajectories, and each
    # trajectory (run) spans both locations. So there are 2 samples per cell,
    # location varies within a sample, and target/horizon are constant within
    # a sample.
    df = pd.DataFrame(
        {
            "output_type": ["sample", "sample", "sample", "sample"],
            "run_grouping": [1, 1, 2, 2],
            "stochastic_run": [1, 1, 1, 1],
            "location": ["01", "02", "01", "02"],
            "target": ["inc hosp", "inc hosp", "inc hosp", "inc hosp"],
            "horizon": [1, 1, 1, 1],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )

    summary = summarize_sample_output(df)

    assert summary["sample_count"] == 2
    # target and horizon are constant within a sample and are kept even though
    # they are globally constant (the PR #30 fix); location varies within a
    # sample, so it is excluded.
    assert set(summary["compound_task_id_set"]) == {"target", "horizon"}
    assert "location" not in summary["compound_task_id_set"]


def test_summarize_sample_output_groups_by_run_tuple_without_string_collision():
    # These two run tuples would collide if the run id were built by joining the
    # columns into a string: ("1-2", "3") and ("1", "2-3") both become "1-2-3".
    # Grouping on the tuple must keep them separate, so "location" — constant
    # within each true run — stays in the compound task ID set.
    df = pd.DataFrame(
        {
            "output_type": ["sample", "sample"],
            "run_grouping": ["1-2", "1"],
            "stochastic_run": ["3", "2-3"],
            "location": ["01", "02"],
            "target": ["inc hosp", "inc hosp"],
            "value": [1.0, 2.0],
        }
    )

    summary = summarize_sample_output(df)

    assert "location" in summary["compound_task_id_set"]


def test_summarize_quantile_output_sorts_and_formats_quantiles():
    df = pd.DataFrame(
        {
            "output_type": ["quantile", "quantile", "quantile", "sample"],
            "output_type_id": [0.5, 0.025, 0.1, None],
            "value": [10, 1, 5, 7],
        }
    )

    summary = summarize_quantile_output(df)

    assert summary["quantiles"] == ["0.025", "0.1", "0.5"]


def test_output_types_section_renders_parquet_metadata():
    model = {"workExample": {"output_type": [["quantile", "sample"]]}}
    metadata = {
        "quantile": {"quantiles": ["0.025", "0.5", "0.975"]},
        "sample": {"sample_count": 25, "compound_task_id_set": ["location"]},
    }

    html = generate_output_types_section(model, metadata)

    assert "Submitted quantiles" in html
    assert "0.025, 0.5, 0.975" in html
    assert "Number of samples" in html
    assert "Number of samples:</strong> 25" in html
    assert "Compound task ID set" in html
    assert "location" in html
    assert "https://docs.hubverse.io/en/latest/user-guide/sample-output-type.html" in html


def test_ensemble_output_types_section_labels_quantiles_as_calculated():
    model = {
        "name": "Team-Ensemble",
        "workExample": {"output_type": [["quantile"]]},
    }
    metadata = {"quantile": {"quantiles": ["0.025", "0.5", "0.975"]}}

    html = generate_output_types_section(model, metadata)

    # Ensemble models calculate their quantiles rather than submitting them.
    assert "Calculated quantiles" in html
    assert "Submitted quantiles" not in html


def test_sample_metadata_displays_empty_compound_task_id_set_as_brackets():
    metadata = {"sample": {"sample_count": 300, "compound_task_id_set": []}}

    html = generate_output_type_metadata_html("sample", metadata)

    # An empty compound task ID set does NOT mean the samples are independent
    # across task variables (it is the opposite), so it must be displayed
    # literally as [] rather than described as independent.
    assert "independent" not in html.lower()
    assert "[]" in html
    assert "Compound task ID set" in html


def test_sample_metadata_quotes_compound_task_id_columns():
    metadata = {
        "sample": {
            "sample_count": 25,
            "compound_task_id_set": ["origin_date", "target", "location"],
        }
    }

    html = generate_output_type_metadata_html("sample", metadata)

    # The compound task ID set is a list of column names, so quote each one.
    assert '"origin_date", "target", "location"' in html


def test_sample_metadata_escapes_html_special_characters():
    metadata = {
        "sample": {
            "sample_count": 5,
            "compound_task_id_set": ["loc<script>", "age&group"],
        }
    }

    html = generate_output_type_metadata_html("sample", metadata)

    assert "loc<script>" not in html
    assert "loc&lt;script&gt;" in html
    assert "age&amp;group" in html


def test_missing_sample_columns_are_escaped():
    metadata = {"sample": {"missing_columns": ["bad<col>"]}}

    html = generate_output_type_metadata_html("sample", metadata)

    assert "bad<col>" not in html
    assert "bad&lt;col&gt;" in html


def test_quantile_metadata_escapes_html_special_characters():
    metadata = {"quantile": {"quantiles": ["0.5<b>", "0.9&"]}}

    html = generate_output_type_metadata_html("quantile", metadata)

    assert "0.5<b>" not in html
    assert "0.5&lt;b&gt;" in html
    assert "0.9&amp;" in html
