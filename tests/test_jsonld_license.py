from pathlib import Path

import pytest
import yaml

from utils.jsonld import DEFAULT_UNKNOWN_LICENSE, normalize_license, yaml_to_jsonld


@pytest.mark.parametrize(
    "value",
    [None, "", "NA", "na", "N/A", "NaN", "TBD", "unknown", " null "],
)
def test_unknown_license_uses_free_to_read_fallback(value):
    assert normalize_license(value) == DEFAULT_UNKNOWN_LICENSE


def test_declared_license_is_preserved():
    assert normalize_license(" CC-BY-4.0 ") == "CC-BY-4.0"


def test_yaml_to_jsonld_applies_license_fallback(tmp_path: Path):
    metadata_path = tmp_path / "model.yaml"
    metadata_path.write_text(
        yaml.safe_dump(
            {
                "team_abbr": "NIH",
                "model_abbr": "RSV",
                "model_version": "1.0",
                "methods": "Test model",
                "license": "NA",
                "website_url": "NA",
                "team_name": "NIH",
                "model_contributors": [],
            }
        ),
        encoding="utf-8",
    )

    result = yaml_to_jsonld(metadata_path)

    assert result["license"] == "zenodo-freetoread-1.0"
