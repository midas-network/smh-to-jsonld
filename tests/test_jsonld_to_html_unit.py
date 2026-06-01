"""Unit tests for parquet snippet loading in pipeline/jsonld_to_html.py."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.jsonld_to_html import get_first_n_rows_of_output


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
